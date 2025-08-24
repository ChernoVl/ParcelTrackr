/**
 * Amazon → Sheets (orders/returns/refunds) — v0.5 (refactor + logs)
 * - Structured logs (LOG.debug/info/warn/error with scopes + timing)
 * - Clear function docs + guard clauses
 * - Safe wrappers for Gmail/Sheets interactions
 * - Same functionality as v0.4, easier to read/diagnose
 */

//////////////////////////////
// Constants & Config
//////////////////////////////

const LABELS = { TO_PARSE: 'Amazon/To-Parse', PARSED: 'Amazon/Parsed' };
const SHEETS = { SETTINGS:'Settings', ORDERS:'Orders', ITEMS:'Items', RETURNS:'Returns', EMAIL_LOG:'EmailLog' };
const MAX_THREADS_PER_RUN  = 100;
const MAX_MESSAGES_PER_RUN = 300;

const STATUS_ORDER = {
  'Ordered': 1,
  'Shipped': 2,
  'Delivered': 3,
  'Return Requested': 4,
  'Shipped Back': 5,
  'Received': 6,
  'Refunded': 7
};

const ORDER_DEFAULTS = {
  gmail_message_id: '',// Ordered, Shipped, Delivered, Cancelled
  order_id: '', // Ordered, Shipped, Delivered, Cancelled
  status: '', // Ordered, Shipped, Delivered, Cancelled
  event_time: '',  // Ordered, Shipped, Delivered, Cancelled
  log_time: '', // Ordered, Shipped, Delivered, Cancelled
  buyer_email: '',// Ordered
  seller: '',// Ordered
  purchase_channel: '',// Ordered
  order_total: '',// Ordered
  currency: '', // Ordered
  items: '', // Ordered
  order_link: '' // Ordered
};

const RETURN_DEFAULTS = {
  gmail_message_id: '',// Ordered, Shipped, Delivered, Cancelled
  order_id: '', // Ordered, Shipped, Delivered, Cancelled
  status: '', // Ordered, Shipped, Delivered, Cancelled
  event_time: '',  // Ordered, Shipped, Delivered, Cancelled
  log_time: '', // Ordered, Shipped, Delivered, Cancelled
  status_history: '',// Ordered
  refund_subtotal: '',// Ordered
  estimated_total_refund: '',// Ordered
  shipping_amount: '',// Ordered
  total_refund: '', // Ordered
  dropoff_by: '',
  dropoff_location: '',
  card_last4: '',
  invoice_link: '',
  items: ''
};

// ---- Item status ranking (newest "wins") ----
const ITEM_STATUS_RANK = {
  // Order lifecycle
  'Ordered': 1,
  'Shipped': 2,
  'Delivered': 3,

  // Return lifecycle
  'ReturnRequested': 4,
  'ReturnDropoffConfirmed': 5,
  'RefundIssued': 6
};

// Normalize for item keys (title fallback)
function _norm_(s) {
  return String(s || '')
    .toLowerCase()
    .replace(/&amp;/g, '&')
    .replace(/[^a-z0-9$€£.\-\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

//////////////////////////////
// Logging helpers
//////////////////////////////

/** Tiny structured logger with scopes + timing. */
const LOG = (() => {
  const now = () => Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd'T'HH:mm:ssXXX");
  const pad = (n, w = 5) => String(n).padStart(w, ' ');
  const fmt = (lvl, msg, ctx) => {
    const base = `[${now()}] ${lvl} ${msg}`;
    if (!ctx) return base;
    try { return base + ' ' + JSON.stringify(ctx); } catch (_) { return base + ' ' + String(ctx); }
  };
  const write = (lvl, msg, ctx) => Logger.log(fmt(lvl, msg, ctx));
  const scope = (name) => {
    const start = Date.now();
    write('INFO', `▶ ${name} start`);
    return {
      end(extra) { write('INFO', `■ ${name} done in ${pad(Date.now() - start)}ms`, extra); }
    };
  };
  return {
    debug: (m, c) => write('DEBUG', m, c),
    info:  (m, c) => write('INFO',  m, c),
    warn:  (m, c) => write('WARN',  m, c),
    error: (m, c) => write('ERROR', m, c),
    scope
  };
})();

//////////////////////////////
// UI
//////////////////////////////

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Amazon Sync')
    .addItem('Run Sync (write)', 'runSync')
    .addToUi();
}

//////////////////////////////
// Entrypoints
//////////////////////////////

/** Full run: parse, upsert, and log. */
function runSync() {
  const sc = LOG.scope('RunSync');
  try {
    const cfg = readSettings_();
    ensureLabels_();

    const processed = loadProcessedMessageIds_(); // only skip messages processed successfully/partially
    LOG.info('Loaded processed IDs', { count: processed.size });

    const messages = fetchCandidateEmails_(cfg.runWindowDays);
    LOG.info('Fetched candidate messages', { count: messages.length, windowDays: cfg.runWindowDays });

    // Accumulators
    const counts = { OrderConfirm:0, Shipment:0, Delivery:0, Return:0, Refund:0, Other:0 };
    const orderList = [], returnList = [], emailLogs = [], itemEvents = [];

    // helper: push per-item events into itemEvents
    const pushItems = (base, items) => {
      if (!Array.isArray(items) || !items.length) return;
      const event_time = base.event_time || base.status_event_at || iso_(base.date || new Date(), cfg.tz);
      const log_time   = isoNow_(cfg.tz);
      for (const it of items) {
        const qty = Math.max(1, Number(it.qty || 1));
        const unit = (it.item_price != null ? Number(it.item_price)
                    : it.unit_price != null ? Number(it.unit_price)
                    : '');
        const line = (typeof unit === 'number' && isFinite(unit)) ? +(unit * qty).toFixed(2) : '';

        itemEvents.push({
          order_id: String(base.order_id || '').trim(),
          status: base.status,
          event_time,
          log_time,

          // identity
          product_id: it.product_id || '',
          item_title: it.item_title || it.title || '',

          // economics (when known)
          qty,
          unit_price: (typeof unit === 'number' && isFinite(unit)) ? unit : '',
          currency: it.currency || base.currency || '',
          line_total: line,

          // media / links (when present)
          item_image_url: it.item_image_url || '',
          item_image_thumb: it.item_image_thumb || '',
          return_qr_url: it.return_qr_url || base.return_qr_url || '',
          return_qr_thumb: it.return_qr_thumb || base.return_qr_thumb || '',
          invoice_link: base.invoice_link || ''
        });
      }
    };

    for (const msg of messages) {
      const mid = msg.getId();
      if (processed.has(mid)) {
        LOG.debug('Skip already processed', { mid });
        continue;
      }

      let parseResult = 'Success';
      let notes = '';
      let orderId = '';
      const content = getMessageContent_(msg);
      const { type } = classifyEmail_(content.subject);
      counts[type] = (counts[type] || 0) + 1;

      try {
        orderId = extractOrderId_(content.plainText) || '';
        if (!orderId) throw new Error('No order_id found (Ordered)');

        switch (type) {
          case 'Ordered': {
            const order = parseOrdered_(orderId, content, cfg.tz);
            // Ensure these three are set for items
            order.order_id = order.order_id || orderId;
            order.status   = 'Ordered';
            order.event_time = order.order_date_local || iso_(content.date, cfg.tz);

            // orders table
            orderList.push(ensureOrderShape_(order));

            // items table (per-item)
            if (order._items && order._items.length) {
              pushItems(order, order._items);
            }
            break;
          }

          case 'Shipped': {
            const order = parseShipped_(content, cfg.tz);
            order.order_id  = order.order_id || orderId;
            order.status    = 'Shipped';
            order.event_time = order.event_time || iso_(content.date, cfg.tz);

            orderList.push(ensureOrderShape_(order));

            if (order._items && order._items.length) {
              pushItems(order, order._items);
            }
            break;
          }

          case 'Delivered': {
            const order = parseDelivered_(content, cfg.tz);
            order.order_id  = order.order_id || orderId;
            order.status    = 'Delivered';
            order.event_time = order.event_time || iso_(content.date, cfg.tz);

            orderList.push(ensureOrderShape_(order));

            if (order._items && order._items.length) {
              pushItems(order, order._items);
            }
            break;
          }

          case 'Cancelled': {
            const order = parseCancelled_(content, cfg.tz);
            order.order_id  = order.order_id || orderId;
            order.status    = 'Cancelled';
            order.event_time = order.event_time || iso_(content.date, cfg.tz);

            orderList.push(ensureOrderShape_(order));
            // Usually no per-item list in cancel emails; if you have it, you can push:
            if (order._items && order._items.length) pushItems(order, order._items);
            break;
          }

          case 'ReturnRequested': {
            const rr = parseReturnRequested_(content, cfg.tz);
            rr.order_id  = rr.order_id || orderId;
            rr.status    = 'ReturnRequested';
            rr.event_time = rr.event_time || iso_(content.date, cfg.tz);

            returnList.push(ensureReturnShape_(rr));
            if (rr.items && rr.items.length) pushItems(rr, rr.items);
            break;
          }

          case 'ReturnDropoffConfirmed': {
            const rdc = parseReturnDropoffConfirmed_(content, cfg.tz);
            rdc.order_id  = rdc.order_id || orderId;
            rdc.status    = 'ReturnDropoffConfirmed';
            rdc.event_time = rdc.event_time || iso_(content.date, cfg.tz);

            returnList.push(ensureReturnShape_(rdc));
            if (rdc.items && rdc.items.length) pushItems(rdc, rdc.items);
            break;
          }

          case 'RefundIssued': {
            const rf = parseRefundIssued_(content, cfg.tz);
            rf.order_id  = rf.order_id || orderId;
            rf.status    = 'RefundIssued';
            rf.event_time = rf.event_time || iso_(content.date, cfg.tz);

            returnList.push(ensureReturnShape_(rf));
            if (rf.items && rf.items.length) {
              // include refund fields on each item event (so Items has them)
              const base = Object.assign({}, rf, {
                invoice_link: rf.invoice_link || '',
              });
              pushItems(base, rf.items);
              // enrich last pushed items with refund money/last4 if you want:
              // (upsertItems_ will just copy what we set here)
              for (let i = itemEvents.length - rf.items.length; i < itemEvents.length; i++) {
                if (i >= 0) {
                  const e = itemEvents[i];
                  e.refund_subtotal = (rf.refund_subtotal != null ? Number(rf.refund_subtotal) : '');
                  e.total_refund    = (rf.total_refund != null    ? Number(rf.total_refund)    : '');
                  e.last4           = rf.last4 || '';
                }
              }
            }
            break;
          }

          default:
            parseResult = 'Partial';
            notes = 'Unrecognized type';
        }

        // If you re-enable label move, keep it wrapped:
        // try { if (parseResult !== 'Failed') markParsed_(msg); } catch (e2) { notes += (notes ? ' | ' : '') + 'Label move failed: ' + e2; }

      } catch (e) {
        parseResult = 'Failed';
        notes = String(e && e.message ? e.message : e);
        LOG.warn('Parse failed for message', { mid, type, notes });
      }

      emailLogs.push(buildEmailLogRow_(msg, parseResult, type, orderId, notes));
    }

    // Writes
    if (orderList.length) {
      LOG.info('Upserting Orders', { count: orderList.length });
      upsertOrders_(orderList);
    }
    if (returnList.length) {
      LOG.info('Upserting Returns', { count: returnList.length });
      upsertReturns_(returnList);
    }
    if (emailLogs.length) {
      ensureEmailLogHeaders_(); 
      LOG.info('Appending EmailLog', { count: emailLogs.length });
      appendRows_(SHEETS.EMAIL_LOG, emailLogs);
    }
    if (itemEvents.length) {
      LOG.info('Upserting Items', { count: itemEvents.length });
      upsertItems_(itemEvents);
    }

    LOG.info('Parsed counts', counts);
  } catch (e) {
    LOG.error('RunSync fatal', { err: String(e) });
    throw e;
  } finally {
    sc.end();
  }
}


//////////////////////////////
// Settings
//////////////////////////////

/** Read Settings sheet with sane fallbacks. */
function readSettings_() {
  const tz = getSetting_('TIMEZONE') || Session.getScriptTimeZone() || 'America/Los_Angeles';
  const runWindowDays = Number(getSetting_('RUN_WINDOW_DAYS') || 60);
  LOG.debug('Settings', { tz, runWindowDays });
  return { tz, runWindowDays };
}
function getSetting_(key) {
  const sh = getSheet_(SHEETS.SETTINGS);
  const values = sh.getDataRange().getValues();
  for (let r = 1; r < values.length; r++) {
    if (String(values[r][0]).trim() === key) return String(values[r][1]).trim();
  }
  return '';
}

//////////////////////////////
// Gmail
//////////////////////////////

/** Ensure Amazon labels exist. */
function ensureLabels_() {
  ['TO_PARSE','PARSED'].forEach(k => {
    const name = LABELS[k];
    if (!GmailApp.getUserLabelByName(name)) {
      GmailApp.createLabel(name);
      LOG.info('Created label', { name });
    }
  });
}

/** Fetch candidate emails limited by time window & caps. */
function fetchCandidateEmails_(days) {
  const sc = LOG.scope('fetchCandidateEmails_');
  try {
    const query = `label:${LABELS.TO_PARSE} newer_than:${Math.max(1, days)}d`;
    const threads = GmailApp.search(query, 0, MAX_THREADS_PER_RUN);
    const messages = [];
    for (const th of threads) {
      for (const m of th.getMessages()) {
        messages.push(m);
        if (messages.length >= MAX_MESSAGES_PER_RUN) {
          LOG.debug('Hit messages cap', { cap: MAX_MESSAGES_PER_RUN });
          sc.end({ threads: threads.length, messages: messages.length });
          return messages;
        }
      }
    }
    sc.end({ threads: threads.length, messages: messages.length });
    return messages;
  } catch (e) {
    LOG.error('Gmail search failed', { err: String(e) });
    throw e;
  }
}

/** Move thread label from To-Parse → Parsed. */
function markParsed_(msg) {
  try {
    const toParse = GmailApp.getUserLabelByName(LABELS.TO_PARSE);
    const parsed  = GmailApp.getUserLabelByName(LABELS.PARSED);
    const thread  = msg.getThread();
    if (toParse) thread.removeLabel(toParse); // THREAD-level
    if (parsed)  thread.addLabel(parsed);
  } catch (e) {
    LOG.warn('Label move failed', { mid: msg.getId(), err: String(e) });
    throw e;
  }
}

//////////////////////////////
// Classification
//////////////////////////////

/** Lightweight subject classifier. */
function classifyEmail_(subject) {
  const s = String(subject || '').toLowerCase().trim();

  if (s.startsWith('ordered:')) return { type: 'Ordered' };
  if (s.startsWith('shipped:') || s.includes('on the way')) return { type: 'Shipped' };
  if (s.startsWith('delivered:')) return { type: 'Delivered' };
  if (s.includes('has been canceled')) return { type: 'Cancelled' };
  if (s.startsWith('your refund for')) return { type: 'RefundIssued' };
  if (s.startsWith('your return drop off confirmation')) return { type: 'ReturnDropoffConfirmed' };
  if (s.startsWith('your return of')) return { type: 'ReturnRequested' };

  return { type: 'Other' };
}










//////////////////////////////
// Parsers
//////////////////////////////

function parseOrdered_(order_id, content, tz) {
  return {
    gmail_message_id: content.id,
    order_id,
    status: 'Ordered',
    event_time: iso_(content.date, tz || 'Etc/UTC'),
    log_time: isoNow_(tz),
    buyer_email: (content.to || '').split(',')[0] || '',
    seller: content.from,
    order_total: extractTotalAmount_(content.plainText) || '',
    currency: inferCurrency_(content.plainText) || '',
    purchase_channel: inferPurchaseChannel_(content.from, content.htmlText),
    order_link: extractOrderViewLink_(content.plainText),
    items: extractLineItems_(content.plainText) // Parse all items (title, qty, unit price)
  };
}

function parseShipped_(content, tz) {
  return { 
    gmail_message_id: content.id,
    order_id: extractOrderId_(content.plainText), 
    status: 'Shipped',
    event_time:  iso_(content.date, tz || 'Etc/UTC'),
    log_time: isoNow_(tz),
  };
}
function parseDelivered_(content, tz) {
  return { 
    gmail_message_id: content.id,
    order_id: extractOrderId_(content.plainText),
    status: 'Delivered', 
    event_time:  iso_(content.date, tz || 'Etc/UTC'),
    log_time: isoNow_(tz),
  };
}
function parseCancelled_(content, tz) {
  return { 
    gmail_message_id: content.id,
    order_id: extractOrderId_(content.plainText),
    status: 'Cancelled', 
    event_time:  iso_(content.date, tz || 'Etc/UTC'),
    log_time: isoNow_(tz),
  };
}

/**
 * ReturnRequested_
 */
/** Parse a "ReturnRequested" email (PLAIN TEXT ONLY)
 * Returns only the fields you asked for:
 * - order_id
 * - event_time            (ISO, from email date)
 * - gmail_message_id
 * - log_time       (ISO now)
 * - refund_subtotal       (number)
 * - total_estimated_refund(number)
 * - shipping_amount       (number)
 * - dropoff_by_date       (ISO date, no time)
 * - dropoff_location      (string)
 * - items                 ([{title, qty}])
 */
function parseReturnRequested_(content, tz) {
  const tzUse = tz || Session.getScriptTimeZone() || 'Etc/UTC'; // FIXME add to all parcers
  const plain = String(content.plainText || '')
    .replace(/\r/g, '')
    .replace(/\n{3,}/g, '\n\n'); // compact triple+ newlines to double
  // LOG.debug(plain);
  const { return_qr_url, return_qr_thumb } = extractReturnQr_(content);

  return {
    gmail_message_id: content.id,
    order_id: extractOrderId_(plain),
    status: 'Requested',
    event_time: iso_(content.date, tzUse),
    log_time: isoNow_(tzUse),
    refund_subtotal: _findAmountAfterLabel_(plain, /refund subtotal/i),
    total_estimated_refund: _findAmountAfterLabel_(plain, /total estimated refund/i),
    shipping_amount: _findAmountAfterLabel_(plain, /shipping/i),
    dropoff_by: _extractDropoffByDate_(plain, tzUse, content.date),
    dropoff_location: _extractDropoffLocation_(plain),
    items: _extractBracketedItemsWithQuantities_(plain, content),
    return_qr_url,
    return_qr_thumb,
    return_link: extractReturnManageLink_(plain),
  };
}
function _findAmountAfterLabel_(s, labelRegex) { //FIXME duplicate
  if (!s) return '';
  // Match lines like: "<Label>  $12.34" or "<Label>:  $12.34"
  const re = new RegExp(String(labelRegex.source) + `[\\s:\\-]*([\\$€£]?\\s?\\d{1,3}(?:,\\d{3})*(?:\\.\\d{2})?)`, 'i');
  const m = s.match(re);
  return m ? normalizeAmount_(m[1]) : '';
}
function _extractDropoffByDate_(s, tz, fallbackDate) {
  const rx = /dropoff by\s*:\s*([\w]{3,},?\s+[A-Za-z]{3}\s+\d{1,2})/i;
  const m = s.match(rx);
  if (!m) return '';
  const txt = m[1].trim();            // e.g., "Mon, Aug 18"
  const year = (fallbackDate instanceof Date) ? fallbackDate.getFullYear() : new Date().getFullYear();

  // Parse "Mon, Aug 18"
  const m2 = txt.match(/^[A-Za-z]{3,},?\s+([A-Za-z]{3})\s+(\d{1,2})$/);
  if (!m2) return '';
  const mon = m2[1].toLowerCase();
  const day = Number(m2[2]);
  const monthMap = { jan:0,feb:1,mar:2,apr:3,may:4,jun:5,jul:6,aug:7,sep:8,oct:9,nov:10,dec:11 };
  if (!(mon in monthMap) || !day) return '';

  // Build date at local midnight in tz
  const d = new Date(year, monthMap[mon], day, 0, 0, 0);
  // Return just the date part in ISO (YYYY-MM-DD); if you prefer full ISO with tz, use iso_(d, tz)
  return Utilities.formatDate(d, tz, 'yyyy-MM-dd');
}
function _extractDropoffLocation_(s) {
  const lines = String(s || '').split('\n');
  for (let i = 0; i < lines.length; i++) {
    if (/^dropoff location/i.test(lines[i].trim())) {
      for (let j = i + 1; j < lines.length; j++) {
        const L = lines[j].trim();
        if (L) return L;
      }
    }
  }
  return '';
}
/**
 * Extract return items from plain text.
 * Looks for [Title](url) ... Quantity: N
 * Returns array of { title, qty, product_id }
 */
function _extractBracketedItemsWithQuantities_(s, content) {
  const lines = String(s || '').split('\n');
  const items = [];
  let title = '';
  let productId = '';
  
  for (let i = 0; i < lines.length; i++) {
    const L = lines[i].trim();
    if (!L) continue;

    // [Title] or [Title](url)
    const mt = L.match(/^\[([^\]]{4,})\](?:\(([^)]+)\))?/);
    if (mt) {
      title = mt[1].trim();

      // If there’s a link, try to parse product_id from it
      if (mt[2]) {
        const pm = mt[2].match(/\/gp\/product\/([A-Z0-9]{10})/i);
        productId = pm ? pm[1] : '';
      }
      continue;
    }

    if (title) {
      const mq = L.match(/^quantity\s*:\s*(\d+)/i);
      if (mq) {
        const img = getReturnItemImage_(content, productId);
        items.push({
          title,
          qty: Math.max(1, Number(mq[1])),
          product_id: productId || '',
          item_image_url: img.url,
          item_image_thumb: img.thumbFormula
        });
        title = '';
        productId = '';
      }
    }
  }
  return items;
}
function _extractBracketedItemsWithQuantitie2_(s, content) {
  const lines = String(s || '').split('\n');
  const items = [];

  let pendingTitle = '';
  let pendingAsin  = '';   // can be captured from either the title link or the bare "(https...product/ASIN)" line

  // helper: try to pull ASIN from a string
  const grabAsin = (str) => {
    const m = String(str || '').match(/\/gp\/product\/([A-Z0-9]{10})/i);
    return m ? m[1] : '';
  };

  for (let i = 0; i < lines.length; i++) {
    const L = lines[i].trim();
    if (!L) continue;

    // 1) If we see a bare "(https://.../gp/product/ASIN...)" line, cache the ASIN for the next title
    const bareLink = L.match(/^\((https?:\/\/[^\s)]+)\)\s*$/);
    if (bareLink) {
      const maybeAsin = grabAsin(bareLink[1]);
      if (maybeAsin) pendingAsin = maybeAsin;
      continue;
    }

    // 2) [Title] or [Title](url) — prefer ASIN from the link; fallback to the cached bare link
    const mt = L.match(/^\[([^\]]{4,})\](?:\(([^)]+)\))?/);
    if (mt) {
      pendingTitle = mt[1].trim();
      // if link present on the title line, take its ASIN; else keep any cached one
      const asinFromTitle = mt[2] ? grabAsin(mt[2]) : '';
      if (asinFromTitle) pendingAsin = asinFromTitle;
      continue;
    }

    // 3) After we have a title, look ahead (including this line) for "Quantity: N"
    if (pendingTitle) {
      // accept "Quantity: 1" anywhere on the line (e.g., "Order # ... Quantity: 1")
      let mq = L.match(/quantity\s*:\s*(\d+)/i);

      // if not on this line, peek up to the next 2 lines
      if (!mq) {
        for (let j = 1; j <= 2 && i + j < lines.length; j++) {
          const Lj = lines[i + j].trim();
          mq = Lj.match(/quantity\s*:\s*(\d+)/i);
          if (mq) { i = i + j; break; } // advance i to the quantity line we consumed
        }
      }

      if (mq) {
        const qty = Math.max(1, Number(mq[1]));
        const img = getReturnItemImage_(content, pendingAsin);
        items.push({
          title: pendingTitle,
          qty,
          product_id: pendingAsin || '',
          item_image_url: img.url,
          item_image_thumb: img.thumbFormula
        });
        // reset for the next item
        pendingTitle = '';
        pendingAsin  = '';
      }
    }
  }

  return items;
}
/**
 * Extract the "Cancel to modify your return" management link from plain text.
 * - Looks for the label
 * - Grabs the first URL either in (...) on the same line OR on the next line
 * - Returns '' if not found
 */
function extractReturnManageLink_(plainText) {
  const t = String(plainText || '');

  // 1) Find the line containing the label
  const labelRe = /cancel to modify your return/i;
  const lines = t.split('\n');
  for (let i = 0; i < lines.length; i++) {
    if (!labelRe.test(lines[i])) continue;

    // 2a) URL inside parentheses on the same line:  ... (https://...)
    let m = lines[i].match(/\((https?:\/\/[^\s)]+)\)/i);
    if (m) return m[1];

    // 2b) Otherwise, try to capture the first URL on the SAME line
    m = lines[i].match(/https?:\/\/\S+/i);
    if (m) return m[0];

    // 2c) Or on the VERY NEXT non-empty line
    if (i + 1 < lines.length) {
      const next = lines[i + 1].trim();
      if (next) {
        const m2 = next.match(/https?:\/\/\S+/i);
        if (m2) return m2[0];
      }
    }
    break; // label found; no need to keep scanning
  }
  return '';
}


/**
 * parseReturnDropoffConfirmed_
 */
/** Parse a "ReturnDropoffConfirmed" email (PLAIN TEXT ONLY)
 * Returns ONLY:
 * - order_id
 * - dropoff_at                  (ISO from email date)
 * - refund_subtotal             (number)
 * - shipping_amount             (number)
 * - total_estimated_refund      (number)
 * - refund_card_last4           (string '3792' or '')
 * - items                       ([{title, qty}])
 */
function parseReturnDropoffConfirmed_(content, tz) {
  const tzUse = tz || Session.getScriptTimeZone() || 'Etc/UTC';
  const plain = String(content.plainText || '');
  const order_id = extractOrderId_(plain);
  const total_estimated_refund = _findAmountAfterLabel_(plain, /total estimated refund/i);
  LOG.debug('[ReturnDropoffConfirmed]', {order_id}, {total_estimated_refund});
  LOG.debug(plain);

  return {
    gmail_message_id: content.id,
    order_id,
    status: 'Dropoffed',
    event_time: iso_(content.date, tzUse),
    log_time: isoNow_(tzUse),
    refund_subtotal: _findAmountAfterLabel_(plain, /refund subtotal/i),
    shipping_amount: _findAmountAfterLabel_(plain, /shipping/i),
    total_estimated_refund,
    card_last4: _extractCardLast4_(plain),
    items: _extractBracketedItemsWithQuantitie2_(plain, content)
  };
}
function _findAmountAfterLabel_(s, labelRegex) {// Fixme exist
  if (!s) return '';
  const re = new RegExp(String(labelRegex.source) + `[\\s:\\-]*([\\$€£]?\\s?\\d{1,3}(?:,\\d{3})*(?:\\.\\d{2})?)`, 'i');
  const m = s.match(re);
  return m ? normalizeAmount_(m[1]) : '';
}
function _extractCardLast4_(s) {
  const m = String(s || '').match(/ending\s+(?:in|with)\s*(\d{4})/i);
  return m ? m[1] : '';
}

/**
 * RefundIssued_
 */
function parseRefundIssued_(content, tz) {
  const src = String(content.plainText || '');
  const refundText = _cutRefundMainSection_(src);
  const { refund_subtotal, total_refund } = _extractRefundAmounts_(refundText);

  const row = {
    gmail_message_id: content.id,
    order_id: extractOrderId_(refundText),
    status: 'Issued',
    event_time: iso_(content.date, tz),
    log_time: isoNow_(tz),
    refund_subtotal: refund_subtotal != null ? refund_subtotal : '',
    total_refund:    total_refund    != null ? total_refund    : '',
    card_last4: _extractCardLast4_(refundText),
    invoice_link: _extractInvoiceLink_(refundText),
    items: _extractItemsFromRefund_(refundText),
  };

  Logger.log(
    '[RefundIssued] order=%s subtotal=%s total=%s last4=%s items=%s invoice=%s',
    row.order_id, row.refund_subtotal, row.total_refund, row.card_last4, row.items_count, row.invoice_link
  );

  return row;
}
function _cutRefundMainSection_(plain) {
  let cut = String(plain || '');
  const ixProducts = cut.search(/^\s*products related to your return\b/i);
  if (ixProducts >= 0) return cut.slice(0, ixProducts);

  const lines = cut.split('\n');
  let endIdx = lines.length;
  for (let i = 0; i < lines.length; i++) {
    if (/^view invoice\b/i.test(lines[i])) {
      // include the URL line if present
      if (i + 1 < lines.length && /^https?:\/\//i.test(lines[i + 1].trim())) {
        endIdx = i + 2;
      } else {
        endIdx = i + 1;
      }
      break;
    }
  }
  return lines.slice(0, endIdx).join('\n');
}
function _extractRefundAmounts_(s) {
  const refund_subtotal = _amountAfterLabel_(s, /refund subtotal/i);
  const total_refund    = _amountAfterLabel_(s, /total refund/i);
  return { refund_subtotal, total_refund };
}
function _extractCardLast4_(s) {
  const m = s.match(/ending\s+in\s+(\d{4})/i);
  return m ? m[1] : '';
}
function _extractInvoiceLink_(s) {
  let m = s.match(/view invoice\s*\((https?:\/\/[^\s)]+)\)/i);
  if (m) return m[1];

  const lines = s.split('\n').map(x => x.trim());
  for (let i = 0; i < lines.length; i++) {
    if (/^view invoice$/i.test(lines[i]) && lines[i + 1] && /^https?:\/\//i.test(lines[i + 1])) {
      return lines[i + 1];
    }
  }
  return '';
}
/**
 * Extract refunded items from plain text.
 * Looks for [Title](url) ... Quantity: N
 * Returns array of { title, qty, product_id }
 */
function _extractItemsFromRefund_(s) {
  const lines = String(s || '').split('\n');
  const items = [];
  let pendingTitle = '';
  let pendingProductId = '';

  for (let i = 0; i < lines.length; i++) {
    const L = lines[i].trim();
    if (!L) continue;

    // Match [Title](https://...)
    const mTitle = L.match(/^\[([^\]]{4,})\](?:\(([^)]+)\))?/);
    if (mTitle) {
      pendingTitle = mTitle[1].trim();

      // Extract product ID if URL present
      if (mTitle[2]) {
        const pm = mTitle[2].match(/\/gp\/product\/([A-Z0-9]{10})/i);
        pendingProductId = pm ? pm[1] : '';
      }
      continue;
    }

    // Quantity line comes after title
    if (pendingTitle) {
      const mQty = L.match(/^quantity\s*:\s*(\d+)/i);
      if (mQty) {
        const qty = Math.max(1, Number(mQty[1]));
        items.push({
          title: pendingTitle,
          qty,
          product_id: pendingProductId || ''
        });
        pendingTitle = '';
        pendingProductId = '';
      }
    }
  }

  return items;
}
function _amountAfterLabel_(s, labelRe) {
  const re = new RegExp(labelRe.source + '\\s*[:\\-]?\\s*\\$?\\s*(\\d{1,3}(?:,\\d{3})*(?:\\.\\d{2}))', 'i');
  const m = s.match(re);
  return m ? normalizeAmount_(m[1]) : null;
}

/**
 * Extract rmaId from plain text (URLs inside the email).
 * Looks for query param like ?rmaId=XXXXX (letters/numbers only).
 *
 * @param {string} plain - plain text body of the email
 * @return {string} rmaId if found, else ''
 */
function extractProductId_(plain) {
  if (!plain) return '';

  // Regex: capture value of rmaId query parameter
  const re = /[?&]rmaId=([A-Za-z0-9_-]+)/i;

  const match = plain.match(re);
  return match ? match[1] : '';
}


//////////////////////////////
// Helpers: message, text, regex
//////////////////////////////

function getMessageContent_(msg) { 
  const subject = msg.getSubject() || '';
  const htmlText = (msg.getBody() || '')
    .replace(/\r/g, '\n')
    .replace(/\n{3,}/g, '\n\n'); // collapse 3+ newlines into 2
  const plainText = (msg.getPlainBody() || '')
    .replace(/\r/g, '\n')
    .replace(/\n{3,}/g, '\n\n'); // same for plain text

  return {
    id: msg.getId(),
    threadId: msg.getThread().getId(),
    subject,
    htmlText,
    plainText,
    date: msg.getDate(),
    from: msg.getFrom() || '',
    to: msg.getTo() || '',
    label: LABELS.TO_PARSE
  };
}
function extractOrderId_(text) {
  const m = String(text || '').match(/(\d{3}-\d{7}-\d{7})/);
  return m ? m[1] : '';
}
function inferCurrency_(plain) {
  const text = String(plain || '');
  // Look for something like "56.96 USD" or "9,99 EUR"
  const m = text.match(/\b\d[\d,]*(?:\.\d{2})?\s*(USD|EUR|GBP|AUD|CAD|JPY|INR)\b/i);
  if (m) return m[1].toUpperCase();
  return '';
}
function extractTotalAmount_(text) {
  if (!text) return null;
  const lines = text.split(/\n+/).map(l => l.trim()).filter(Boolean);

  for (let i = 0; i < lines.length; i++) {
    if (/^total$/i.test(lines[i])) {
      const next = lines[i + 1];
      if (next) {
        // extract numeric part, e.g. "56.96 USD" → "56.96"
        const m = next.match(/(\d+(?:\.\d{2})?)/);
        return m ? parseFloat(m[1]) : null;
      }
    }
  }
  return null;
}
function largestAmount_(text) {
  const matches = (text || '').match(/[\$€£]?\s?\d{1,3}(?:,\d{3})*(?:\.\d{2})/g) || [];
  const nums = matches.map(normalizeAmount_).filter(v => typeof v === 'number');
  return nums.length ? Math.max.apply(null, nums) : null;
}
function normalizeAmount_(s) {
  const clean = String(s || '').replace(/[^\d.,]/g, '').replace(/,/g, '');
  const num = Number(clean);
  return isFinite(num) ? num : null;
}
function extractAfterLabel_(text, labels) {
  for (const label of labels) {
    const m = (text || '').match(new RegExp(label + '\\s*[:\\-]\\s*(.+)', 'i'));
    if (m) return m[1].trim();
  }
  return '';
}
function iso_(date, tz) { return Utilities.formatDate(date, tz, "yyyy-MM-dd'T'HH:mm:ssXXX"); }
function isoNow_(tz) { return iso_(new Date(), tz); }
function formatDateId_(date, tz) { return Utilities.formatDate(date, tz, 'yyyyMMddHHmmss'); }

//////////////////////////////
// Product/QR scraping
//////////////////////////////

function extractHrefByText_(html, texts) {
  for (const t of texts) {
    const rx = new RegExp(`<a[^>]+href="([^"]+)"[^>]*>\\s*${t}\\s*<\\/a>`, 'i');
    const m = rx.exec(html);
    if (m) return m[1];
  }
  return '';
}
function extractImageByAltOrNearText_(html, alts) {
  for (const a of alts) {
    const rx = new RegExp(`<img[^>]+(?:alt="${a}"[^>]*|[^>]*alt="[^"]*${a}[^"]*")[^>]+src="([^"]+)"[^>]*>`, 'i');
    const m = rx.exec(html);
    if (m) return m[1];
  }
  return '';
}

//////////////////////////////
// Sheets helpers (safe & logged)
//////////////////////////////

// function getSheet_(name) {
//   const sh = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(name);
//   if (!sh) throw new Error(`Missing sheet: ${name}`);
//   return sh;
// }
function getHeaders_(sheetName) {
  const sh = getSheet_(sheetName);
  const headers = sh.getRange(1, 1, 1, Math.max(1, sh.getLastColumn())).getValues()[0].map(h => String(h||'').trim());
  return { sh, headers };
}
// function ensureColumns_(sheetName, headers, obj) {
//   const sh = getSheet_(sheetName);
//   let changed = false;
//   for (const k of Object.keys(obj)) {
//     if (!headers.includes(k)) {
//       headers.push(k);
//       sh.getRange(1, headers.length, 1, 1).setValue(k);
//       changed = true;
//       LOG.debug('Added column', { sheetName, col: k });
//     }
//   }
//   if (changed) SpreadsheetApp.flush();
//   return headers;
// }
function buildIndex_(sheetName, keyCol) {
  const sh = getSheet_(sheetName);
  const values = sh.getDataRange().getValues();
  const headers = values[0] || [];
  const idx = headers.indexOf(keyCol);
  const map = new Map();
  if (idx === -1) return map;
  for (let r = 1; r < values.length; r++) {
    const key = String(values[r][idx] || '').trim();
    if (key) map.set(key, r + 1);
  }
  return map;
}

//////////////////////////////
// Returns upsert (item_key–keyed: order_id+product_id OR order_id+title)
//////////////////////////////
function upsertReturns_(returnList) {
  if (!Array.isArray(returnList) || returnList.length === 0) return;

  const { sh: sheet } = getHeaders_(SHEETS.RETURNS); // we only need the sheet handle
  const hdrs = ensureReturnsHeaders_();              // <-- guarantees A1 headers

  // Index rows by product_id (1-based column lookup)
  const productIdCol = hdrs.indexOf('product_id') + 1;
  if (productIdCol < 1) throw new Error('Returns: missing "product_id" header');

  // Build row index for existing product_ids
  const rowIndexByItemKey = new Map();
  const lastRow = sheet.getLastRow();
  if (lastRow > 1) {
    const ids = sheet.getRange(2, productIdCol, lastRow - 1, 1).getValues();
    for (let i = 0; i < ids.length; i++) {
      const key = String(ids[i][0] || '').trim();
      if (key && !rowIndexByItemKey.has(key)) rowIndexByItemKey.set(key, i + 2);
    }
  }

  // Cache to avoid re-reading the same row
  const rowCache = new Map();

  // Walk each parsed return object
  for (const input of returnList) {
    const orderId = String(input.order_id || '').trim();
    if (!orderId) { Logger.log('[Returns] Skip — missing order_id: %s', JSON.stringify(input)); continue; }

    // Normalize the items array; if none provided, make a single synthetic item
    const items = Array.isArray(input.items) && input.items.length
      ? input.items
      : [{ title: String(input.title || '').trim(), qty: Math.max(1, Number(input.qty || 1) || 1), product_id: String(input.product_id || '') }];

    // For each item: upsert one row
    for (const it of items) {
      const itemTitle = String(it.title || '').trim();
      const itemQty   = Math.max(1, Number(it.qty || 1) || 1);
      const productId = String(it.product_id || '').trim();

      // const itemKey = _makeItemKey_(orderId, productId, itemTitle);
      const itemKey = productId;
      if (!itemKey) { 
        Logger.log('[Returns] Skip item — could not form key: order_id=%s, product_id=%s, title=%s', orderId, productId, itemTitle); 
        continue; 
      }

      // Compose incoming row object (per‑item)
      const incoming = {
        order_id: orderId,
        product_id: productId || '',
        item_title: itemTitle || '',
        qty: itemQty,
        item_image_thumb: it.item_image_thumb,
        item_image_url: it.item_image_url,

        return_qr_thumb: input.return_qr_thumb,
        return_qr_url: input.return_qr_url,

        gmail_message_id: input.gmail_message_id || '',
        invoice_link: input.invoice_link || '',
        return_link: input.return_link,

        refund_subtotal: input.refund_subtotal ?? '',
        total_estimated_refund: input.total_estimated_refund ?? '',
        shipping_amount: input.shipping_amount ?? '',
        card_last4: input.card_last4 || '',

        dropoff_by: input.dropoff_by || '',
        dropoff_location: input.dropoff_location || '',

        status: input.status || '',
        event_time: input.event_time || '',
        log_time: input.log_time || '',
        status_history: input.status_history || ''
      };

      // Insert or update
      let row = rowIndexByItemKey.get(itemKey);
      if (!row) {
        const seeded = mergeReturnItemRow_({}, incoming);
        row = sheet.getLastRow() + 1;
        sheet.getRange(row, 1, 1, hdrs.length).setValues([objToRow_(hdrs, seeded)]);
        rowIndexByItemKey.set(itemKey, row);
        rowCache.set(row, seeded);
        Logger.log('[Returns] + Insert item row (%s)', itemKey);
        continue;
      }

      // existing row → merge
      let current = rowCache.get(row);
      if (!current) {
        current = rowToObj_(hdrs, sheet.getRange(row, 1, 1, hdrs.length).getValues()[0]);
      }
      const merged = mergeReturnItemRow_(current, incoming);
      sheet.getRange(row, 1, 1, hdrs.length).setValues([objToRow_(hdrs, merged)]);
      rowCache.set(row, merged);
      Logger.log('[Returns] ~ Update item row (%s)', itemKey);
    }
  }
}
//////////////////////////////
// Merge per item row
//////////////////////////////
/**
 * Merge a parsed return "item row" into an existing Returns row (same item_key).
 * - Copies non-empty scalars (never overwrites with '', null, undefined) except status/event_time
 * - Appends newest status event (status + event_time) to history (newest-first, dedup exact pair)
 * - Sets current status by rank (RefundIssued > ReturnDropoffConfirmed > ReturnRequested)
 */
function mergeReturnItemRow_(existing, incoming) {
  const out = Object.assign({}, existing);

  // Copy non-empty scalars; status/event_time handled below
  for (const [k, v] of Object.entries(incoming)) {
    if (k === 'status' || k === 'event_time') continue;
    if (v !== '' && v !== null && v !== undefined) out[k] = v;
  }

  // History
  let history = [];
  try { history = existing.status_history ? JSON.parse(existing.status_history) : []; }
  catch (_) { history = []; }

  const newStatus = String(incoming.status || '').trim();
  const eventAt   = String(incoming.event_time || '');
  if (newStatus && eventAt) {
    const newest = { status: newStatus, at: eventAt };
    const key = h => `${String(h.status||'').trim()}::${String(h.at||'').trim()}`;
    const newestKey = key(newest);
    const deduped = [newest];
    for (const h of history) if (key(h) !== newestKey) deduped.push(h);
    history = deduped;
  }

  // Choose current status from history (ranked, tie‑break by latest timestamp)
  const { status, at } = _pickCurrentReturnStatus_(history);
  if (status) {
    out.status = status;
    if (at) out.event_time = at;
  }
  out.status_history = JSON.stringify(history);

  return out;
}
function _pickCurrentReturnStatus_(history) {
  if (!Array.isArray(history) || !history.length) return { status: '', at: '' };
  const RANK = {
    'ReturnRequested': 1,
    'ReturnDropoffConfirmed': 2,
    'RefundIssued': 3
  };
  let best = { status: '', at: '', rank: -1 };
  for (const h of history) {
    const st = String(h.status || '').trim();
    const at = String(h.at || '');
    const r  = RANK[st] || 0;
    if (r > best.rank) {
      best = { status: st, at, rank: r };
    } else if (r === best.rank && at && (!best.at || at > best.at)) {
      best = { status: st, at, rank: r };
    }
  }
  return best;
}
//////////////////////////////
// Key helpers
//////////////////////////////
/**
 * Build a deterministic per‑item key:
 *   order_id + '|' + (product_id || normalized_title)
 */
function _makeItemKey_(orderId, productId, title) {
  const oid = String(orderId || '').trim();
  if (!oid) return '';
  const pid = String(productId || '').trim();
  if (pid) return `${oid}|asin:${pid}`;
  const t = _normTitle_(title || '');
  if (!t) return '';
  return `${oid}|title:${t}`;
}
function _normTitle_(t) {
  return String(t || '')
    .toLowerCase()
    .replace(/&amp;/g, '&')
    .replace(/[^a-z0-9$€£.\-\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}
/** Ensure Returns sheet headers exist starting at A1 (creates if empty). */
function ensureReturnsHeaders_() {
  const sh = getSheet_(SHEETS.RETURNS);

  // All columns we populate for Returns (one item per row design)
  const REQUIRED = [
    // identity / linking
    'order_id','product_id','gmail_message_id','invoice_link',
    // amounts / meta
    'refund_subtotal','total_estimated_refund','shipping_amount','card_last4',
    // dropoff meta
    'dropoff_by','dropoff_location',
    // items (one item per row; items_json optional if you keep arrays)
    'item_title','qty','item_image_url','item_image_thumb',
    // QR
    'return_qr_url','return_qr_thumb',
    // status & timestamps
    'status','event_time','log_time','status_history'
  ];

  // What exists now?
  let existing = [];
  const lastCol = sh.getLastColumn();
  if (lastCol > 0) {
    existing = sh.getRange(1, 1, 1, lastCol).getValues()[0].map(h => String(h||'').trim());
  }

  // If the header row is empty → write all headers starting at A1 in one go
  const isEmptyHeaderRow = existing.length === 0 || existing.every(h => !h);
  if (isEmptyHeaderRow) {
    sh.getRange(1, 1, 1, REQUIRED.length).setValues([REQUIRED]);
    SpreadsheetApp.flush();
    return REQUIRED.slice(); // return a copy
  }

  // Otherwise, append any missing headers to the right (don’t reorder existing)
  let hdrs = existing.slice();
  for (const col of REQUIRED) {
    if (!hdrs.includes(col)) {
      hdrs.push(col);
      sh.getRange(1, hdrs.length, 1, 1).setValue(col);
    }
  }
  SpreadsheetApp.flush();
  return hdrs;
}





function appendRows_(sheetName, rows) {
  if (!rows.length) return;
  const sc = LOG.scope(`appendRows_${sheetName}`);
  try {
    const { sh, headers } = getHeaders_(sheetName);
    let hdrs = headers.slice();
    for (const r of rows) { if (!Array.isArray(r)) hdrs = ensureColumns_(sheetName, hdrs, r); }
    const arrs = rows.map(r => Array.isArray(r) ? r : hdrs.map(h => r[h] ?? ''));
    sh.getRange(sh.getLastRow() + 1, 1, arrs.length, hdrs.length).setValues(arrs);
  } finally {
    sc.end({ count: rows.length });
  }
}

//////////////////////////////
// EmailLog helpers
//////////////////////////////

function buildEmailLogRow_(msg, result, type, orderId, notes) {
  const thread = msg.getThread();
  const threadUrl = `https://mail.google.com/mail/u/0/#inbox/${thread.getId()}`;
  return {
    gmail_message_id: msg.getId(),
    thread_id: msg.getThread().getId(),
    thread_permalink: threadUrl,
    label_when_parsed: LABELS.TO_PARSE,
    parsed_at: Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd'T'HH:mm:ssXXX"),
    parse_result: result,
    detected_type: type,
    order_id: orderId || '',
    notes: notes || ''
  };
}
function loadProcessedMessageIds_() {
  const sh = getSheet_(SHEETS.EMAIL_LOG);
  const vals = sh.getDataRange().getValues();
  const hdr = vals[0] || [];
  const idCol = hdr.indexOf('gmail_message_id');
  const resCol = hdr.indexOf('parse_result');
  const set = new Set();
  for (let r = 1; r < vals.length; r++) {
    const id = String(vals[r][idCol] || '').trim();
    const res = String(vals[r][resCol] || '').trim();
    if (id && res && res !== 'Failed') set.add(id);
  }
  return set;
}
function ensureEmailLogHeaders_() {
  const sh = getSheet_(SHEETS.EMAIL_LOG);
  const REQUIRED = [
    'gmail_message_id',
    'thread_id',
    'thread_permalink',
    'label_when_parsed',
    'parsed_at',
    'parse_result',
    'detected_type',
    'order_id',
    'notes'
  ];

  const lastCol = sh.getLastColumn();
  const headers = lastCol > 0
    ? sh.getRange(1, 1, 1, lastCol).getValues()[0].map(h => String(h || '').trim())
    : [];

  const allBlank = headers.length === 0 || headers.every(h => !h);
  if (allBlank) {
    // Fresh sheet (or header row was entirely blank): write all required starting at A1
    sh.getRange(1, 1, 1, REQUIRED.length).setValues([REQUIRED]);
    SpreadsheetApp.flush();
    LOG.info('EmailLog: wrote required headers from column A', { headers: REQUIRED });
    return REQUIRED;
  }

  // Existing headers: append any missing ones to the right
  let hdrs = headers.slice();
  for (const col of REQUIRED) {
    if (!hdrs.includes(col)) {
      hdrs.push(col);
      sh.getRange(1, hdrs.length, 1, 1).setValue(col);
      LOG.debug('EmailLog: added missing header', { col });
    }
  }
  SpreadsheetApp.flush();
  return hdrs;
}
function ensureColumns_(sheetName, headers, obj) {
  const sh = getSheet_(sheetName);

  // If header row is effectively blank (common on brand-new sheets),
  // treat as no headers so we start at column A instead of B.
  const isAllBlank = !headers.length || headers.every(h => !String(h || '').trim());
  if (isAllBlank) headers = [];

  let changed = false;
  for (const k of Object.keys(obj)) {
    if (!headers.includes(k)) {
      headers.push(k);
      sh.getRange(1, headers.length, 1, 1).setValue(k);
      changed = true;
      LOG.debug('Added column', { sheetName, col: k });
    }
  }
  if (changed) SpreadsheetApp.flush();
  return headers;
}



//////////////////////////////
// Amounts from HTML
//////////////////////////////

function extractLabeledAmountHTML_(html, labels) {
  const h = String(html || '');
  for (const label of labels) {
    const re = new RegExp(label + '[^\\d$€£]{0,60}([\\$€£]?\\s?\\d{1,3}(?:,\\d{3})*(?:\\.\\d{2})?)', 'i');
    const m = h.match(re);
    if (m) return normalizeAmount_(m[1]);
  }
  return null;
}

//////////////////////////////
// Payment / Marketplace
//////////////////////////////

function inferPurchaseChannel_(fromStr, html) {
  const s = (fromStr + ' ' + html).toLowerCase();
  if (s.includes('amazon.com')) return 'US';
  if (s.includes('amazon.ca'))  return 'CA';
  if (s.includes('amazon.co.uk')) return 'UK';
  if (s.includes('amazon.de'))  return 'DE';
  if (s.includes('amazon.fr'))  return 'FR';
  if (s.includes('amazon.it'))  return 'IT';
  if (s.includes('amazon.es'))  return 'ES';
  if (s.includes('amazon.com.au')) return 'AU';
  return '';
}

//////////////////////////////
// Row <-> Object
//////////////////////////////

function rowToObj_(headers, row) {
  const o = {};
  headers.forEach((h, i) => o[h] = row[i]);
  return o;
}
// ---- Cell value sanitizer: make sure everything going into setValues() is a primitive
function _toCell_(v) {
  if (v === null || v === undefined) return '';
  if (v instanceof Date) return v;
  const t = typeof v;
  if (t === 'string' || t === 'number' || t === 'boolean') return v;

  // Try common image-ish / rich objects:
  try {
    if (v && typeof v === 'object') {
      if (typeof v.thumbFormula === 'string') return v.thumbFormula;   // our own thumbnail formula carrier
      if (typeof v.url === 'string')         return v.url;             // our own { url, thumbFormula }
      if (typeof v.getContentUrl === 'function') return v.getContentUrl(); // EmbeddedImage-like
      if (typeof v.getUrl === 'function')        return v.getUrl();        // Link-like
      if (typeof v.toString === 'function')      return String(v.toString());
    }
  } catch (_) {}
  return String(v); // last resort
}
// Map object to row using headers, with sanitization
function objToRow_(headers, obj) {
  return headers.map(h => _toCell_(obj.hasOwnProperty(h) ? obj[h] : ''));
}


//////////////////////////////
// Merge Orders (status + history)
//////////////////////////////

function mergeOrder_(existing, incoming) {
  const out = Object.assign({}, existing);

  // --- 1) Copy non-empty scalars (never wipe with '') ---
  for (const [k, v] of Object.entries(incoming)) {
    if (k === 'status' || k === 'event_time') continue; // handled via history below

    if (k === 'order_total') {
      // set-once only
      if (!existing.order_total && v !== '' && v !== null && v !== undefined) {
        out.order_total = v;
      }
      continue;
    }

    if (v !== '' && v !== null && v !== undefined) out[k] = v;
  }

  // --- 2) Load & normalize existing history ---
  let history = [];
  try { history = existing.status_history ? JSON.parse(existing.status_history) : []; }
  catch (_) { history = []; }

  // Canonicalize events to {status, at} strings
  history = Array.isArray(history) ? history.filter(e => e && typeof e === 'object') : [];
  history = history.map(e => ({
    status: String(e.status || '').trim(),
    at: String(e.at || '').trim()
  })).filter(e => e.status); // keep only events with a status

  // --- 3) Add incoming event (if any) ---
  const incStatus = String(incoming.status || '').trim();
  const incAt     = String(incoming.event_time || incoming.log_time || '').trim();

  if (incStatus) {
    const newest = { status: incStatus, at: incAt };
    const keyOf  = (ev) => `${ev.status}::${ev.at}`;
    const seen   = new Set([keyOf(newest)]);
    const dedup  = [newest];
    for (const ev of history) {
      const k = keyOf(ev);
      if (!seen.has(k)) { seen.add(k); dedup.push(ev); }
    }
    history = dedup;
  }

  // --- 4) Sort newest-first by timestamp; tie-break by rank; final tie: keep order ---
  const rank = (s) => STATUS_ORDER[s] || 0;
  const cmpDescIso = (a, b) => {
    const A = a.at, B = b.at;
    if (A && B && A !== B) return B.localeCompare(A); // later ISO time first
    if (!A && B) return 1;   // put dated events first
    if (A && !B) return -1;
    // tie by time -> higher rank first
    const rdiff = rank(b.status) - rank(a.status);
    if (rdiff !== 0) return rdiff;
    return 0; // stable sort keeps earlier array order as last tie-break
  };
  history.sort(cmpDescIso);

  // --- 5) Adopt current status from history head (if any) ---
  if (history.length) {
    out.status = history[0].status;
    out.status_changed_at = history[0].at || out.status_changed_at || '';
  } else if (incStatus) {
    // no history but we do have an incoming status (without timestamp perhaps)
    out.status = incStatus;
    if (incAt) out.status_changed_at = incAt;
  }

  // --- 6) Persist newest-first history ---
  out.status_history = JSON.stringify(history);
  return out;
}

//////////////////////////////
// Upsert Orders with status merge + history tracking
//////////////////////////////

function upsertOrders_(objs) {
  const { sh } = getHeaders_(SHEETS.ORDERS); // we only need the sheet; headers are rebuilt below

  const REQUIRED = [
    'items_summary','items_count','items_total',
    'order_total','currency',
    'status','event_time','log_time',
    'order_id','order_link',
    'delivered_at','shipped_at','ordered_at',
    'status_history',
    'buyer_email','seller','purchase_channel',
    'items_json',
  ];

  // Ensure headers exist and get a name->col map
  const colByName = ensureRequiredHeaders_(sh, REQUIRED);

  // Find the order_id column dynamically
  const orderIdCol = colByName.get('order_id');
  if (!orderIdCol) throw new Error('Orders sheet is missing "order_id" header');

  SpreadsheetApp.flush();

  // Build index of existing rows: order_id -> row
  const rowByOrderId = new Map();
  const lastRow = sh.getLastRow();
  if (lastRow > 1) {
    const idVals = sh.getRange(2, orderIdCol, lastRow - 1, 1).getValues();
    for (let i = 0; i < idVals.length; i++) {
      const id = String(idVals[i][0] || '').trim();
      if (id && !rowByOrderId.has(id)) rowByOrderId.set(id, i + 2);
    }
  }

  // Recompute current headers (now that they’re guaranteed to exist)
  const { headers } = getHeaders_(SHEETS.ORDERS);
  const hdrs = headers.slice();

  const rowCache = new Map();

  for (const incoming of objs) {
    const id = String(incoming.order_id || '').trim();
    if (!id) continue;

    // NOTE: your earlier code checks `incoming.items` but then reads `_items`.
    // Make it consistent:
    if (incoming.items && incoming.items.length) {
      Object.assign(incoming, summarizeItemsForOrderRow_(incoming.items));
    }

    let row = rowByOrderId.get(id);
    if (!row) {
      const seeded = mergeOrder_({}, incoming);
      row = sh.getLastRow() + 1;
      sh.getRange(row, 1, 1, hdrs.length).setValues([objToRow_(hdrs, seeded)]);
      rowByOrderId.set(id, row);
      rowCache.set(row, seeded);
      continue;
    }

    let current = rowCache.get(row);
    if (!current) {
      current = rowToObj_(hdrs, sh.getRange(row, 1, 1, hdrs.length).getValues()[0]);
    }

    const merged = mergeOrder_(current, incoming);

    // Optional per‑status timestamp stamping
    const st = String(merged.status || '').trim();
    const at = merged.status_changed_at || incoming.event_time || incoming.log_time || '';
    const setOnce = (field, want) => { if (st === want && at && !current[field]) merged[field] = at; };
    setOnce('ordered_at',   'Ordered');
    setOnce('shipped_at',   'Shipped');
    setOnce('delivered_at', 'Delivered');

    sh.getRange(row, 1, 1, hdrs.length).setValues([objToRow_(hdrs, merged)]);
    rowCache.set(row, merged);
  }
}
function ensureRequiredHeaders_(sh, requiredNames) {
  // Read a wide header row (use getMaxColumns to see blanks too)
  const row = sh.getRange(1, 1, 1, sh.getMaxColumns()).getValues()[0];

  // Map existing non-empty headers → column index (1-based)
  const colByName = new Map();
  let nextFreeCol = 1;
  for (let c = 1; c <= row.length; c++) {
    const name = String(row[c - 1] || '').trim();
    if (name) {
      colByName.set(name, c);
      if (c >= nextFreeCol) nextFreeCol = c + 1;
    }
  }

  // Helper to find the next truly empty header cell (skips already-named cells)
  const findNextEmptyCol = () => {
    let c = 1;
    while (c <= row.length) {
      if (!String(row[c - 1] || '').trim()) return c;
      c++;
    }
    // If no empty cell in the current header row, append after the last non-empty
    return nextFreeCol;
  };

  // Place missing headers into the first empty header cell (starting at A1 if blank)
  for (const name of requiredNames) {
    if (!colByName.has(name)) {
      const col = findNextEmptyCol();
      sh.getRange(1, col, 1, 1).setValue(name);
      row[col - 1] = name;            // keep our local snapshot in sync
      colByName.set(name, col);
      if (col >= nextFreeCol) nextFreeCol = col + 1;
    }
  }

  return colByName; // name -> 1-based column
}



//////////////////////////////
// Seller extraction
//////////////////////////////

function extractSeller_(html, plain) {
  const h = String(html || '');
  let m = h.match(/Sold\s+by[:\s]+<\/?[^>]*a[^>]*>([^<]+)<\/a>/i);
  if (m && m[1]) return m[1].trim();
  m = h.match(/Sold\s+by[:\s]+([^<\n]+)/i);
  if (m && m[1]) return m[1].replace(/&amp;/g,'&').trim();

  const p = String(plain || '');
  m = p.match(/Sold\s+by[:\s]+([^\n]+)/i);
  if (m && m[1]) return m[1].trim();
  return '';
}

//////////////////////////////
// Line-item extraction
//////////////////////////////

function extractLineItems_(plain) {
  const items = [];
  if (!plain) return items;

  const lines = plain.split(/\n+/).map(l => l.trim()).filter(Boolean);

  for (let i = 0; i < lines.length; i++) {
    if (lines[i].startsWith('*')) {
      const title = lines[i].replace(/^\*\s*/, '').trim();
      let quantity = 1;
      let price = null;

      // look ahead for quantity
      if (i + 1 < lines.length && /^quantity/i.test(lines[i + 1])) {
        const m = lines[i + 1].match(/(\d+)/);
        if (m) quantity = parseInt(m[1], 10);
      }

      // look ahead for price
      if (i + 2 < lines.length && /\d+(\.\d{2})?\s*USD/i.test(lines[i + 2])) {
        const m = lines[i + 2].match(/([\d,.]+\.\d{2})/);
        if (m) price = parseFloat(m[1]);
      }

      items.push({ title, quantity, price });
    }
  }

  return items;
}
function extractOrderViewLink_(plain) {
  if (!plain) return '';
  const re = /(https:\/\/www\.amazon\.com\/gp\/css\/order-details\?orderID=[^\s]+)/i;
  const m = plain.match(re);
  return m ? m[1] : '';
}

//////////////////////////////
// Items sheet
//////////////////////////////

function ensureItemHeaders_(hdrs, sh) {
  const need = ['order_id','line_id','item_title','qty','item_price','line_total','currency'];
  need.forEach(c => {
    if (!hdrs.includes(c)) {
      hdrs.push(c);
      sh.getRange(1, hdrs.length, 1, 1).setValue(c);
    }
  });
  SpreadsheetApp.flush();
  return hdrs;
}

/** Delete existing Items rows for an order (bottom-up to avoid shifting). */
function deleteItemsForOrder_(orderId) {
  const sc = LOG.scope('deleteItemsForOrder_');
  try {
    const sh = getSheet_(SHEETS.ITEMS);
    const lastRow = sh.getLastRow();
    if (lastRow < 2) return;
    const ids = sh.getRange(2, 1, lastRow - 1, 1).getValues(); // col A = order_id
    let deleted = 0;
    for (let i = ids.length - 1; i >= 0; i--) {
      if (String(ids[i][0] || '').trim() === orderId) {
        sh.deleteRow(i + 2);
        deleted++;
      }
    }
    sc.end({ orderId, deleted });
  } catch (e) {
    LOG.warn('deleteItemsForOrder_ failed', { orderId, err: String(e) });
    throw e;
  }
}

/** Replace (delete + insert) item rows for a single order. */
function replaceItemsForOrder_(orderId, items) {
  const { sh, headers } = getHeaders_(SHEETS.ITEMS);
  let hdrs = ensureItemHeaders_(headers.slice(), sh);

  deleteItemsForOrder_(orderId);

  const rows = [];
  items.forEach((it, i) => {
    const qty = Math.max(1, Number(it.qty || 1));
    const unit = it.item_price !== '' && it.item_price != null ? Number(it.item_price) : '';
    const lineTotal = (typeof unit === 'number' && isFinite(unit)) ? unit * qty : '';
    const obj = {
      order_id: orderId,
      line_id: `${orderId}#${i+1}`,
      item_title: it.item_title || '',
      qty,
      item_price: unit,
      line_total: lineTotal,
      currency: it.currency || ''
    };
    rows.push(hdrs.map(h => obj.hasOwnProperty(h) ? obj[h] : ''));
  });

  if (rows.length) {
    sh.getRange(sh.getLastRow() + 1, 1, rows.length, hdrs.length).setValues(rows);
  }
  LOG.debug('replaceItemsForOrder_ wrote rows', { orderId, rows: rows.length });
}

/** Summarize items for compact fields on Orders row. */
function summarizeItemsForOrderRow_(items) {
  const itemDetails = [];       // array of normalized item objects (for JSON storage)
  let totalQuantity = 0;        // total number of items
  let totalAmount = 0;          // total cost across all items
  let detectedCurrency = '';    // currency from the first item that has one

  items.forEach(item => {
    // normalize quantity (default to 1, minimum 1)
    const quantity = Math.max(1, Number(item.quantity || 1));

    // normalize unit price (if provided)
    const unitPrice = (item.price !== '' && item.price != null) ? Number(item.price) : NaN;

    // calculate line total for this item (unit price × qty)
    const lineTotal = isFinite(unitPrice) ? +(unitPrice * quantity).toFixed(2) : '';

    // accumulate totals
    if (isFinite(unitPrice) && isFinite(lineTotal)) totalAmount += lineTotal;
    totalQuantity += quantity;

    // capture currency from first valid item
    if (!detectedCurrency && item.currency) detectedCurrency = item.currency;

    // push normalized version of the item
    itemDetails.push({
      title: (item.title || '').slice(0, 500), // cap title length
      qty: quantity,
      unit_price: isFinite(unitPrice) ? +unitPrice.toFixed(2) : '',
      line_total: lineTotal
    });
  });

  // make a human-readable string (summary)
  const summaryText = itemDetails.map(detail => {
    const unitText = (detail.unit_price !== '' ? `${detectedCurrency} ${detail.unit_price.toFixed(2)}` : '').trim();
    const lineText = (detail.line_total !== '' ? `${detectedCurrency} ${detail.line_total.toFixed(2)}` : '').trim();
    return `${detail.qty} × ${detail.title}${unitText ? ` — ${unitText}` : ''}${lineText ? ` (${lineText})` : ''}`;
  }).join('\n');

  // return normalized fields to store in sheet
  return {
    items_count: totalQuantity || '',
    items_total: totalAmount ? +totalAmount.toFixed(2) : '',
    items_json: JSON.stringify(itemDetails),
    items_summary: summaryText
  };
}


function ensureOrderShape_(partial) {
  const out = Object.assign({}, ORDER_DEFAULTS, partial || {});
  // never let undefined/null leak to Sheets – coerce to '' for scalars
  for (const k in out) if (out[k] == null) out[k] = '';
  return out;
}
function ensureReturnShape_(partial) {
  const out = Object.assign({}, RETURN_DEFAULTS, partial || {});
  // never let undefined/null leak to Sheets – coerce to '' for scalars
  for (const k in out) if (out[k] == null) out[k] = '';
  return out;
}

/**
 * Try to find the product image URL in the email HTML near an anchor that points to /gp/product/{ASIN}.
 * Returns '' if nothing found. We only accept Amazon CDN images (m.media-amazon.com / images-na.ssl-images-amazon.com).
 */
function extractItemImageForAsinFromHtml_(html, asin) {
  if (!html || !asin) return '';
  const H = String(html).replace(/\s+/g, ' '); // normalize

  // Match the <a> that links to the ASIN (via product link or redirect)
  const aRe = new RegExp(`<a[^>]+href="[^"]*(?:/gp/product/${asin}|/gp/r\\.html[^"]*${asin})[^"]*"[^>]*>`, 'i');
  const am = H.match(aRe);
  if (!am) return '';

  // Look in a window around that anchor for an <img>
  const at = am.index;
  const win = H.slice(Math.max(0, at - 1500), Math.min(H.length, at + 2500));

  // Match Amazon images (either direct or behind Google's proxy)
  const imgRe = /<img[^>]+src="([^"]+\.(?:jpg|jpeg|png)(?:[^"]*)?)"[^>]*>/i;
  const im = win.match(imgRe);
  if (!im) return '';

  let url = im[1];

  // If it's a Google proxy URL with a "#realUrl" suffix, extract the real one
  const hashIdx = url.indexOf('#');
  if (hashIdx !== -1) {
    const real = url.slice(hashIdx + 1);
    if (/^https?:\/\/(m\.media-amazon\.com|images-na\.ssl-images-amazon\.com)/i.test(real)) {
      url = real;
    }
  }

  // Only accept Amazon CDN images
  if (!/^https?:\/\/(m\.media-amazon\.com|images-na\.ssl-images-amazon\.com)/i.test(url)) {
    return '';
  }

  return url;
}

/**
 * Fallback: first Amazon image in the whole HTML (last resort).
 */
function extractFirstAmazonImage_(html) {
  if (!html) return '';
  const m = String(html).match(/<img[^>]+src="([^"]+(?:m\.media-amazon\.com|images-na\.ssl-images-amazon\.com)[^"]+)"[^>]*>/i);
  return m ? m[1] : '';
}
/**
 * Convenience: given content.{plainText,htmlText} and a product_id (ASIN),
 * return {url, thumbFormula}. Thumb is a 60x60 kept inside cell.
 */
function getReturnItemImage_(content, productId) {
  let url = extractItemImageForAsinFromHtml_(content.htmlText, productId);
  if (!url && content.htmlText) url = extractFirstAmazonImage_(content.htmlText);
  const thumbFormula = url ? `=IMAGE("${url.replace(/"/g,'""')}",4,60,60)` : '';
  return { url, thumbFormula };
}

/**
 * Extracts the Return QR image URL from the message and builds a Sheets thumbnail.
 * Priority: plain text "Download QR Code (URL)" → HTML <a> "Download QR Code" → any QR-like <img>.
 * 
 * @param {{plainText:string, htmlText:string}} content
 * @returns {{ return_qr_url: string, return_qr_thumb: string }}
 */
function extractReturnQr_(content) {
  const plain = String(content.plainText || '');
  const html  = String(content.htmlText || '');

  // 1) Plain text:  Download QR Code (https://....)
  let url = _extractQrFromPlain_(plain);

  // 2) HTML anchor: <a href="...">Download QR Code</a>
  if (!url) url = _extractQrFromHtmlLink_(html);

  // 3) Fallback: any obvious QR-like <img> (Amazon S3 presigned JPEG)
  if (!url) url = _extractQrFromHtmlImg_(html);

  const return_qr_url = url || '';
  const return_qr_thumb = return_qr_url
    ? `=IMAGE("${return_qr_url.replace(/"/g, '""')}",4,90,90)`
    : '';

  return { return_qr_url, return_qr_thumb };
}
/* ---------- internals ---------- */
// Plain text pattern: “Download QR Code (https://… )”
function _extractQrFromPlain_(plain) {
  const m = String(plain).match(/Download\s+QR\s+Code\s*\((https?:\/\/[^\s)]+)\)/i);
  return m ? m[1].trim() : '';
}
// HTML anchor with the visible text “Download QR Code”
function _extractQrFromHtmlLink_(html) {
  const H = String(html || '');
  const re = /<a[^>]+href="([^"]+)"[^>]*>\s*Download\s+QR\s+Code\s*<\/a>/i;
  const m = H.match(re);
  return m ? m[1].trim() : '';
}
// Last-resort: look for an <img> that likely is the QR (often S3 presigned .JPEG)
function _extractQrFromHtmlImg_(html) {
  const H = String(html || '');
  // Prefer S3 “trans-returnsummarycard-images-*” host; then any JPG-ish candidate
  let m = H.match(/<img[^>]+src="([^"]+trans-returnsummarycard-images[^"]+)"[^>]*>/i);
  if (m) return m[1].trim();

  m = H.match(/<img[^>]+src="([^"]+\.jpe?g[^"]*)"[^>]*>/i);
  return m ? m[1].trim() : '';
}



//----------------------------------------------------------------------------------------------------------------------------


//////////////////////////////
// Items upsert (item_key–keyed)
//////////////////////////////

/**
 * Upserts per-item rows with newest-first status_history.
 * Accepts an array of "item events" objects. Each object can include:
 * {
 *   order_id, product_id?, title, qty?, unit_price?, currency?,
 *   status, event_time, log_time, gmail_message_id,
 *   seller?, purchase_channel?,
 *   item_image_url?, item_image_thumb?,
 *   return_qr_url?, return_qr_thumb?, invoice_link?,
 *   last4?, refund_subtotal?, total_estimated_refund?, shipping_amount?,
 *   dropoff_by?, dropoff_location?
 * }
 */
function upsertItems_(itemEvents) {
  if (!Array.isArray(itemEvents) || !itemEvents.length) return;

  const { sh: sheet, headers: existingHeaders } = getHeaders_(SHEETS.ITEMS);
  // Normalize header list (don’t reorder, but remove null/undefined; keep empty strings as empty slots)
  // let hdrs = _filterUsableHeaders_(existingHeaders);

  // Ensure required columns exist (don’t reorder what’s already there)
  const REQUIRED = [
    'item_key',
    'order_id','product_id','item_title',
    'qty','unit_price','currency',
    'seller','purchase_channel','invoice_link','last4',
    'refund_subtotal','total_estimated_refund','shipping_amount',
    'dropoff_by','dropoff_location',
    'item_image_url','item_image_thumb','return_qr_url','return_qr_thumb',
    'status','event_time','log_time','status_history',
    'gmail_message_id'
  ];

  let hdrs = ensureHeadersFromA_(sheet, REQUIRED);


  // If the header row is effectively empty (no labeled columns yet), write all headers at once from col A.
  const hasAnyLabeledHeader = hdrs.some(h => h && h.length);
  if (!hasAnyLabeledHeader && sheet.getLastColumn() === 1 && String(existingHeaders[0] || '') === '') {
    sheet.getRange(1, 1, 1, REQUIRED.length).setValues([REQUIRED]);
    hdrs = REQUIRED.slice();
  } else {
    for (const col of REQUIRED) {
      if (!hdrs.includes(col)) {
        hdrs.push(col);
        sheet.getRange(1, hdrs.length, 1, 1).setValue(col);
      }
    }
  }
  SpreadsheetApp.flush();

  // Find item_key column (1-based for Sheets)
  const keyCol0 = hdrs.indexOf('item_key');
  if (keyCol0 === -1) throw new Error('Items sheet missing item_key header');
  const keyCol1 = keyCol0 + 1;

  // Build index: item_key -> row number
  const rowByKey = new Map();
  const lastRow = sheet.getLastRow();
  if (lastRow > 1) {
    const keys = sheet.getRange(2, keyCol1, lastRow - 1, 1).getValues();
    for (let i = 0; i < keys.length; i++) {
      const k = String(keys[i][0] || '').trim();
      if (k && !rowByKey.has(k)) rowByKey.set(k, i + 2);
    }
  }

  const rowCache = new Map(); // row -> obj

  for (const ev of itemEvents) {
    const orderId = String(ev.order_id || '').trim();
    if (!orderId) continue;

    // Compute stable per-item key
    const itemKey = computeItemKey_(orderId, ev.product_id, ev.title);
    const incomingRaw = Object.assign({ item_key: itemKey }, ev);

    // Only keep scalar, writeable values for the headers we actually have
    const incoming = _sanitizeRowObjectForHeaders_(hdrs, incomingRaw);

    let row = rowByKey.get(itemKey);
    if (!row) {
      const seededRaw = mergeItemRow_({}, incoming);              // may add fields
      const seeded = _sanitizeRowObjectForHeaders_(hdrs, seededRaw);
      const writeRow = [objToRow_(hdrs, seeded)];
      // sanity check
      if (writeRow[0].length !== hdrs.length) {
        LOG.error('Items insert width mismatch', { expected: hdrs.length, got: writeRow[0].length, itemKey });
        continue;
      }
      row = sheet.getLastRow() + 1;
      sheet.getRange(row, 1, 1, hdrs.length).setValues(writeRow);
      rowByKey.set(itemKey, row);
      rowCache.set(row, seeded);
      Logger.log('[Items] + Insert (key=%s, order=%s, title=%s)', itemKey, orderId, (incoming.item_title||incoming.title||'').slice(0,60));
      continue;
    }

    // Merge into existing row
    let current = rowCache.get(row);
    if (!current) {
      const values = sheet.getRange(row, 1, 1, hdrs.length).getValues()[0];
      current = rowToObj_(hdrs, values);
    }

    const mergedRaw = mergeItemRow_(current, incoming);
    const merged = _sanitizeRowObjectForHeaders_(hdrs, mergedRaw);
    const writeRow = [objToRow_(hdrs, merged)];

    if (writeRow[0].length !== hdrs.length) {
      LOG.error('Items update width mismatch', { expected: hdrs.length, got: writeRow[0].length, itemKey });
      continue;
    }

    sheet.getRange(row, 1, 1, hdrs.length).setValues(writeRow);
    rowCache.set(row, merged);
    Logger.log('[Items] ~ Update (key=%s, status=%s)', itemKey, merged.status || '');
  }
}


/** Create a stable per-item key from order_id + (product_id OR normalized title) */
function computeItemKey_(order_id, product_id, title) {
  const oid = String(order_id || '').trim();
  if (!oid) return '';
  const pid = String(product_id || '').trim();
  if (pid) return `${oid}|asin:${pid}`;
  const t   = _normTitle_(title || '');
  return `${oid}|t:${t}`;
}

/** Normalize title similarly to your other helpers */
function _normTitle_(t) {
  return String(t || '')
    .toLowerCase()
    .replace(/&amp;/g, '&')
    .replace(/[^a-z0-9$€£.\-\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * Merge an incoming per-item event into a row.
 * - Copies non-empty scalars (never overwrite with '', null, undefined)
 * - status/event_time go through status_history (newest-first, de-duped)
 * - Chooses current status by rank across orders + returns
 */
function mergeItemRow_(existing, incoming) {
  const out = Object.assign({}, existing);

  // Copy non-empty scalars EXCEPT status/event_time
  for (const [k, v] of Object.entries(incoming)) {
    if (k === 'status' || k === 'event_time') continue;
    if (v !== '' && v !== null && v !== undefined) out[k] = v;
  }

  // Parse current history (safe)
  let history = [];
  try { history = existing.status_history ? JSON.parse(existing.status_history) : []; }
  catch (_) { history = []; }

  // Prepend newest status event (de-dupe by status+time)
  const st = String(incoming.status || '').trim();
  const at = String(incoming.event_time || '');
  if (st && at) {
    const newest = { status: st, at: at };
    const key = h => `${String(h.status||'').trim()}::${String(h.at||'').trim()}`;
    const newestKey = key(newest);
    const deduped = [newest];
    for (const h of history) if (key(h) !== newestKey) deduped.push(h);
    history = deduped;
  }

  // Pick current status by rank (Orders + Returns)
  const { status, at: chosenAt } = pickCurrentItemStatus_(history);
  if (status) {
    out.status = status;
    if (chosenAt) out.event_time = chosenAt;
  }
  out.status_history = JSON.stringify(history);

  return out;
}

/** Status ranking across the full lifecycle */
function pickCurrentItemStatus_(history) {
  if (!Array.isArray(history) || !history.length) return { status: '', at: '' };

  const RANK = {
    'Ordered': 1,
    'Shipped': 2,
    'Delivered': 3,
    'ReturnRequested': 4,
    'ReturnDropoffConfirmed': 5,
    'RefundIssued': 6
  };

  let best = { status: '', at: '', rank: -1 };
  for (const h of history) {
    const st = String(h.status || '').trim();
    const at = String(h.at || '');
    const r  = RANK[st] || 0;
    if (r > best.rank) {
      best = { status: st, at, rank: r };
    } else if (r === best.rank && at && (!best.at || at > best.at)) {
      best = { status: st, at, rank: r };
    }
  }
  return best;
}
// Coerce a single cell value into something Sheets accepts.
function _coerceCell_(v) {
  if (v === '' || v === null || v === undefined) return '';
  const t = typeof v;
  if (t === 'string' || t === 'number' || t === 'boolean') return v;
  if (v instanceof Date) return v;
  // Functions / objects / arrays -> JSON (or string) so setValues won't crash
  try { return JSON.stringify(v); } catch (_) { return String(v); }
}

// Strip truly-empty header names to avoid creating properties named "".
// NOTE: we do NOT reorder columns; we only ignore nameless headers in obj mapping/writing.
function _filterUsableHeaders_(headers) {
  return headers.map(h => String(h || '').trim());
}

// Ensure the object has only scalar, writeable values (coerced), keyed by real headers
function _sanitizeRowObjectForHeaders_(headers, obj) {
  const out = {};
  for (const h of headers) {
    if (!h) continue;                 // skip blank header names
    out[h] = _coerceCell_(obj[h]);    // coerce everything to a Sheets-safe value
  }
  return out;
}

/**
 * Ensure a sheet has the given headers.
 * - If row 1 is effectively empty (no non-empty labels), write ALL headers at A1..A1+N-1
 * - Else, append any missing headers to the end (no reordering).
 * Returns the final header list (array of strings).
 */
function ensureHeadersFromA_(sheet, requiredHeaders) {
  // Read current header row
  const lastCol = Math.max(1, sheet.getLastColumn());
  const current = sheet.getRange(1, 1, 1, lastCol).getValues()[0]
    .map(h => String(h || '').trim());

  const hasAnyLabel = current.some(h => h.length > 0);

  if (!hasAnyLabel) {
    // Entire header row is blank -> write all required headers at A1
    sheet.getRange(1, 1, 1, requiredHeaders.length).setValues([requiredHeaders]);
    SpreadsheetApp.flush();
    return requiredHeaders.slice();
  }

  // Otherwise, append any missing headers to the end
  const final = current.slice();
  for (const col of requiredHeaders) {
    if (!final.includes(col)) {
      final.push(col);
      sheet.getRange(1, final.length, 1, 1).setValue(col);
    }
  }
  SpreadsheetApp.flush();
  return final;
}

/**
 * Get (or create) a sheet by name.
 * If it doesn't exist, create it and optionally seed headers in row 1 (from col A).
 *
 * @param {string} name - sheet name
 * @param {string[]=} seedHeaders - optional headers to write into row 1
 * @return {GoogleAppsScript.Spreadsheet.Sheet}
 */
function getSheet_(name, seedHeaders) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(name);

  if (!sh) {
    sh = ss.insertSheet(name);

    // Log creation (use your LOG utility if present; fallback to Logger)
    if (typeof LOG !== 'undefined' && LOG && typeof LOG.info === 'function') {
      LOG.info('Created missing sheet', { sheet: name });
    } else {
      Logger.log(`[Sheets] Created missing sheet: ${name}`);
    }

    // Seed headers if provided
    if (Array.isArray(seedHeaders) && seedHeaders.length > 0) {
      sh.getRange(1, 1, 1, seedHeaders.length).setValues([seedHeaders.map(h => String(h || ''))]);

      if (typeof LOG !== 'undefined' && LOG && typeof LOG.debug === 'function') {
        LOG.debug('Seeded headers for new sheet', { sheet: name, headers: seedHeaders });
      } else {
        Logger.log(`[Sheets] Seeded headers for ${name}: ${seedHeaders.join(', ')}`);
      }
    }
  }

  return sh;
}

