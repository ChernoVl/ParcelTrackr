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
const SHEETS = { SETTINGS:'Settings', ORDERS:'Orders', ITEMS:'Items', RETURNS:'Returns', REFUNDS:'Refunds', EMAIL_LOG:'EmailLog' };

// Tuning knobs
const MAX_THREADS_PER_RUN  = 100;
const MAX_MESSAGES_PER_RUN = 300;

// Order status rank (anti-downgrade)
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
  order_id: '',
  // status core
  status: '',
  status_event_at: '',
  status_changed_at: '',
  status_history: '',
  ordered_at: '',
  shipped_at: '',
  delivered_at: '',

  // dates & ids
  order_date_utc: '',
  order_date_local: '',
  gmail_message_id: '',
  last_updated_at: '',

  // parties & channel
  buyer_email: '',
  seller: '',
  purchase_channel: '',

  // money & first item
  order_total: '',
  shipping: '',
  currency: '',
  payment_method: '',
  first_item_title: '',
  first_item_image_url: '',
  first_item_image_thumb: '',

  // items summary (computed)
  items_count: '',
  items_total: '',
  items_json: '',
  items_summary: ''
};

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
    const ordersUpserts = [], returnsUpserts = [], emailLogs = [];
    const itemsByOrder = new Map(); // order_id -> merged items

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
      //LOG.debug(content.plainText);
      const { type } = classifyEmail_(content.subject);
      counts[type] = (counts[type] || 0) + 1;

      try {
        orderId = extractOrderId_(content.plainText) || '';
        if (!orderId) throw new Error('No order_id found (Ordered)');

        switch (type) {
          case 'Ordered': {
            const o = parseOrdered_(orderId, content, cfg.tz);
            ordersUpserts.push(ensureOrderShape_(o));
            break;
          }
          case 'Shipped': {
            const o = parseShipped_(content, cfg.tz);
            ordersUpserts.push(ensureOrderShape_(o));
            break;
          }
          case 'Delivered': {
            const o = parseDelivered_(content, cfg.tz);
            ordersUpserts.push(ensureOrderShape_(o));
            break;
          }
          case 'Cancelled': {
            const o = parseCancelled_(content, cfg.tz);
            ordersUpserts.push(ensureOrderShape_(o));
            break;
          }
          case 'ReturnRequested': {
            const rr = parseReturnRequested_(content, cfg.tz);
            returnsUpserts.push(rr);
            break;
          }
          case 'ReturnDropoffConfirmed': {
            const rdc = parseReturnDropoffConfirmed_(content, cfg.tz);
            returnsUpserts.push(rdc);
            break;
          }
          case 'RefundIssued': {
            const rf = parseRefundIssued_(content, cfg.tz);
            returnsUpserts.push(rf);
            break;
          }
          default:
            parseResult = 'Partial';
            notes = 'Unrecognized type';
        }

        // Label move happens at thread level; if you re-enable, keep try/catch.
        // try { if (parseResult !== 'Failed') markParsed_(msg); } catch (e2) { notes += (notes ? ' | ' : '') + 'Label move failed: ' + e2; }

      } catch (e) {
        parseResult = 'Failed';
        notes = String(e && e.message ? e.message : e);
        LOG.warn('Parse failed for message', { mid, type, notes });
      }

      emailLogs.push(buildEmailLogRow_(msg, parseResult, type, orderId, notes));
    }

    // Writes
    if (ordersUpserts.length) {
      LOG.info('Upserting Orders', { count: ordersUpserts.length });
      upsertOrdersWithStatus_(ordersUpserts);
    }
    if (returnsUpserts.length) {
      LOG.info('Upserting Returns', { count: returnsUpserts.length });
      upsertMany_(SHEETS.RETURNS, 'return_id', returnsUpserts);
    }
    if (emailLogs.length) {
      LOG.info('Appending EmailLog', { count: emailLogs.length });
      appendRows_(SHEETS.EMAIL_LOG, emailLogs);
    }

    // Items → Items sheet + summarize back onto Orders
    if (itemsByOrder.size) {
      LOG.info('Writing Items + Order summaries', { ordersWithItems: itemsByOrder.size });
      const orderItemSummaries = [];
      itemsByOrder.forEach((items, oid) => {
        replaceItemsForOrder_(oid, items);
        const summary = summarizeItemsForOrderRow_(items);
        orderItemSummaries.push(Object.assign({ order_id: oid }, summary));
      });
      upsertMany_(SHEETS.ORDERS, 'order_id', orderItemSummaries);
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
  const order_date_local = iso_(content.date, tz || 'Etc/UTC');
  const currency = inferCurrency_(content.plainText);
  let order_total = extractTotalAmount_(content.plainText);
  const purchase_channel = inferPurchaseChannel_(content.from, content.htmlText);
  const seller = content.from;
  const items = extractLineItems_(content.plainText); // Parse all items (title, qty, unit price)
  const view_order_link = extractOrderViewLink_(content.plainText);

  const o = {
    order_id,
    order_date_local,
    buyer_email: (content.to || '').split(',')[0] || '',
    seller,
    order_total: order_total != null ? order_total : '',
    currency: currency || '',
    purchase_channel,
    status: 'Ordered',
    gmail_message_id: content.id,
    last_updated_at: isoNow_(tz),
    view_order_link,
    _items: items // carried to runner; not stored directly in Orders
  };

  return o;
}

function parseShipped_(content, tz) {
  return { 
    order_id: extractOrderId_(content.plainText), 
    status: 'Shipped', 
    status_event_at: iso_(content.date, tz) ,
    last_updated_at: isoNow_(tz),
  };
}
function parseDelivered_(content, tz) {
  return { 
    order_id: extractOrderId_(content.plainText),
    status: 'Delivered', 
    status_event_at: iso_(content.date, tz),
    last_updated_at: isoNow_(tz),
  };
}
function parseCancelled_(content, tz) {
  return { 
    order_id: extractOrderId_(content.plainText),
    status: 'Cancelled', 
    status_event_at: iso_(content.date, tz),
    last_updated_at: isoNow_(tz),
  };
}

/**
 * ReturnRequested_
 */
/** Parse a "ReturnRequested" email (PLAIN TEXT ONLY)
 * Returns only the fields you asked for:
 * - order_id
 * - request_at            (ISO, from email date)
 * - gmail_message_id
 * - last_updated_at       (ISO now)
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

  const order_id  = extractOrderId_(plain);
  const request_at = iso_(content.date, tzUse);
  const gmail_message_id = content.id;
  const last_updated_at  = isoNow_(tzUse);

  const refund_subtotal        = _findAmountAfterLabel_(plain, /refund subtotal/i);
  const total_estimated_refund = _findAmountAfterLabel_(plain, /total estimated refund/i);
  const shipping_amount        = _findAmountAfterLabel_(plain, /shipping/i);

  const dropoff_by_date   = _extractDropoffByDate_(plain, tzUse, content.date);
  const dropoff_location  = _extractDropoffLocation_(plain);

  const { items } = _extractBracketedItemsWithQuantities_(plain);

  return {
    order_id,
    request_at,
    gmail_message_id,
    last_updated_at,
    refund_subtotal,
    total_estimated_refund,
    shipping_amount,
    dropoff_by_date,
    dropoff_location,
    items
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
function _extractBracketedItemsWithQuantities_(s) {
  const lines = String(s || '').split('\n');
  const items = [];
  let title = '';

  for (let i = 0; i < lines.length; i++) {
    const L = lines[i].trim();
    if (!L) continue;

    // [Title] or [Title](...)
    const mt = L.match(/^\[([^\]]{4,})\](?:\([^)]+\))?/);
    if (mt) { title = mt[1].trim(); continue; }

    if (title) {
      const mq = L.match(/^quantity\s*:\s*(\d+)/i);
      if (mq) {
        items.push({ title, qty: Math.max(1, Number(mq[1])) });
        title = '';
      }
    }
  }
  return { items };
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
  const dropoff_at = iso_(content.date, tzUse);
  const refund_subtotal        = _findAmountAfterLabel_(plain, /refund subtotal/i);
  const shipping_amount        = _findAmountAfterLabel_(plain, /shipping/i);
  const total_estimated_refund = _findAmountAfterLabel_(plain, /total estimated refund/i);
  const refund_card_last4 = _extractCardLast4_(plain);
  const items = _extractBracketedItemsWithQuantities_(plain);

  LOG.debug('[ReturnDropoffConfirmed]', {order_id}, {total_estimated_refund});

  return {
    order_id,
    dropoff_at,
    refund_subtotal,
    shipping_amount,
    total_estimated_refund,
    refund_card_last4,
    items
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
function _extractBracketedItemsWithQuantities_(s) {
  const lines = String(s || '').split('\n');
  const out = [];
  let currentTitle = '';

  for (let i = 0; i < lines.length; i++) {
    const L = lines[i].trim();
    if (!L) continue;

    // [Title] or [Title](...)
    const mt = L.match(/^\[([^\]]{4,})\](?:\([^)]+\))?/);
    if (mt) { currentTitle = mt[1].trim(); continue; }

    // Quantity after a captured title
    if (currentTitle) {
      const mq = L.match(/^quantity\s*:\s*(\d+)/i);
      if (mq) {
        out.push({ title: currentTitle, qty: Math.max(1, Number(mq[1])) });
        currentTitle = '';
      }
    }
  }
  return out;
}

/**
 * RefundIssued_
 */
function parseRefundIssued_(content, tz) {
  const src = String(content.plainText || '');
  const refundText = _cutRefundMainSection_(src);

  const order_id          = extractOrderId_(refundText);
  const { refund_subtotal, total_refund } = _extractRefundAmounts_(refundText);
  const refund_card_last4 = _extractCardLast4_(refundText);
  const invoice_link      = _extractInvoiceLink_(refundText);
  const { items, items_count } = _extractItemsFromRefund_(refundText);

  const row = {
    order_id,
    refund_subtotal: refund_subtotal != null ? refund_subtotal : '',
    total_refund:    total_refund    != null ? total_refund    : '',
    refund_card_last4: refund_card_last4 || '',
    items_json: JSON.stringify(items),
    items_count,
    invoice_link: invoice_link || '',
    refund_issued_at: iso_(content.date, tz),
    status: "RefundIssued"
  };

  Logger.log(
    '[RefundIssued] order=%s subtotal=%s total=%s last4=%s items=%s invoice=%s',
    row.order_id, row.refund_subtotal, row.total_refund, row.refund_card_last4, row.items_count, row.invoice_link
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
function _extractItemsFromRefund_(s) {
  const lines = String(s || '').split('\n');
  const items = [];
  let pendingTitle = '';

  for (let i = 0; i < lines.length; i++) {
    const L = lines[i].trim();
    if (!L) continue;

    // Title: [Some Item ...] OR [Some Item ...](https://...)
    const mTitle = L.match(/^\[([^\]]{4,})\](?:\([^)]+\))?/);
    if (mTitle) { 
      pendingTitle = mTitle[1].trim();
      continue;
    }

    // Quantity line comes after the title
    if (pendingTitle) {
      const mQty = L.match(/^quantity\s*:\s*(\d+)/i);
      if (mQty) {
        const qty = Math.max(1, Number(mQty[1]));
        items.push({ title: pendingTitle, qty });
        pendingTitle = '';
      }
    }
  }

  const items_count = items.reduce((sum, it) => sum + (it.qty || 0), 0);
  return { items, items_count };
}
function _amountAfterLabel_(s, labelRe) {
  const re = new RegExp(labelRe.source + '\\s*[:\\-]?\\s*\\$?\\s*(\\d{1,3}(?:,\\d{3})*(?:\\.\\d{2}))', 'i');
  const m = s.match(re);
  return m ? normalizeAmount_(m[1]) : null;
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

function getSheet_(name) {
  const sh = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(name);
  if (!sh) throw new Error(`Missing sheet: ${name}`);
  return sh;
}
function getHeaders_(sheetName) {
  const sh = getSheet_(sheetName);
  const headers = sh.getRange(1, 1, 1, Math.max(1, sh.getLastColumn())).getValues()[0].map(h => String(h||'').trim());
  return { sh, headers };
}
function ensureColumns_(sheetName, headers, obj) {
  const sh = getSheet_(sheetName);
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
function upsertMany_(sheetName, keyCol, objs) {
  if (!objs.length) return;
  const sc = LOG.scope(`upsertMany_${sheetName}`);
  try {
    const { sh, headers } = getHeaders_(sheetName);
    let hdrs = headers.slice();
    for (const o of objs) hdrs = ensureColumns_(sheetName, hdrs, o);

    const index = buildIndex_(sheetName, keyCol);
    const writes = [];
    for (const o of objs) {
      const key = String(o[keyCol] || '').trim();
      if (!key) continue;
      const row = index.get(key);
      const arr = hdrs.map(h => o.hasOwnProperty(h) ? o[h] : '');
      if (row) sh.getRange(row, 1, 1, hdrs.length).setValues([arr]);
      else writes.push(arr);
    }
    if (writes.length) sh.getRange(sh.getLastRow() + 1, 1, writes.length, hdrs.length).setValues(writes);
  } finally {
    sc.end({ count: objs.length });
  }
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
function objToRow_(headers, obj) {
  return headers.map(h => obj.hasOwnProperty(h) ? obj[h] : '');
}

//////////////////////////////
// Merge Orders (status + history)
//////////////////////////////

function mergeOrder_(existing, incoming) {
  const out = Object.assign({}, existing);

  // copy non-empty scalars (never wipe with '')
  for (const [k, v] of Object.entries(incoming)) {
    if (k === 'status' || k === 'status_event_at') continue; // handled below

    if (k === 'order_total') {
      // set-once only
      if (!existing.order_total && v !== '' && v !== null && v !== undefined) {
        out.order_total = v;
      }
      continue;
    }

    if (v !== '' && v !== null && v !== undefined) out[k] = v;
  }

  // --- status + history (newest first) ---
  const newStatus = String(incoming.status || '').trim();
  const eventAt   = incoming.status_event_at || incoming.last_updated_at || '';

  // if incoming has a status, adopt it as current
  if (newStatus) {
    out.status = newStatus;
    if (eventAt) out.status_changed_at = eventAt;
  }

  // parse existing history (if any)
  let history = [];
  try { history = existing.status_history ? JSON.parse(existing.status_history) : []; }
  catch (_) { history = []; }

  // build key for dedupe
  const key = (h) => `${String(h.status||'').trim()}::${String(h.at||'').trim()}`;

  // prepend newest
  if (newStatus && eventAt) {
    const newest = { status: newStatus, at: eventAt };
    const newestKey = key(newest);
    const deduped = [newest];

    for (const h of history) {
      if (key(h) !== newestKey) deduped.push(h);
    }
    history = deduped; // newest-first
  }

  out.status_history = JSON.stringify(history);
  return out;
}

//////////////////////////////
// Upsert Orders with status merge + history tracking
//////////////////////////////

function upsertOrdersWithStatus_(objs) {
  const { sh, headers } = getHeaders_(SHEETS.ORDERS);
  let hdrs = headers.slice();

  // Ensure only the columns we still care about (kept your item summaries)
  [
    'status','status_changed_at','status_history',
    'ordered_at','shipped_at','delivered_at', // keep if you still want these timestamps (optional)
    'order_date_utc','order_date_local','buyer_email','seller','order_total','shipping',
    'currency','payment_method','purchase_channel',
    'last_updated_at','first_item_title','first_item_image_url','first_item_image_thumb',
    'items_count','items_total','items_json','items_summary'
  ].forEach(c => { if (!hdrs.includes(c)) { hdrs.push(c); sh.getRange(1, hdrs.length, 1, 1).setValue(c); } });

  SpreadsheetApp.flush();

  // index existing rows by order_id (col A)
  const lastRow = sh.getLastRow();
  const index = new Map();
  if (lastRow > 1) {
    const ids = sh.getRange(2, 1, lastRow - 1, 1).getValues();
    for (let i = 0; i < ids.length; i++) {
      const id = String(ids[i][0] || '').trim();
      if (id && !index.has(id)) index.set(id, i + 2);
    }
  }

  // cache (row -> obj) to avoid rereads
  const cache = new Map();

  for (const incoming of objs) {
    const id = String(incoming.order_id || '').trim();
    if (!id) continue;

    // fold parsed items into row summary fields (if present)
    if (incoming._items && incoming._items.length) {
      const itemFields = summarizeItemsForOrderRow_(incoming._items);
      Object.assign(incoming, itemFields);
    }

    let row = index.get(id);
    if (!row) {
      // brand new row → write-through append
      const seeded = mergeOrder_({}, incoming);
      row = sh.getLastRow() + 1;
      sh.getRange(row, 1, 1, hdrs.length).setValues([objToRow_(hdrs, seeded)]);
      index.set(id, row);
      cache.set(row, seeded);
      continue;
    }

    // existing row → merge and update
    let current = cache.get(row);
    if (!current) {
      current = rowToObj_(hdrs, sh.getRange(row, 1, 1, hdrs.length).getValues()[0]);
    }

    // if you still want to stamp ordered/shipped/delivered once, do it here (optional):
    // when adopting incoming status as current, set the per-status field if empty.
    const merged = mergeOrder_(current, incoming);
    const st     = String(merged.status || '').trim();
    const at     = merged.status_changed_at || incoming.status_event_at || incoming.last_updated_at || '';

    const setOnce = (field, want) => {
      if (st === want && at && !current[field]) merged[field] = at;
    };
    setOnce('ordered_at',   'Ordered');
    setOnce('shipped_at',   'Shipped');
    setOnce('delivered_at', 'Delivered');

    sh.getRange(row, 1, 1, hdrs.length).setValues([objToRow_(hdrs, merged)]);
    cache.set(row, merged);
  }
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
  const compact = [];
  let itemsCount = 0;
  let itemsTotal = 0;
  let currency = '';

  items.forEach(it => {
    const qty = Math.max(1, Number(it.qty || 1));
    const unit = (it.item_price !== '' && it.item_price != null) ? Number(it.item_price) : NaN;
    const line = isFinite(unit) ? +(unit * qty).toFixed(2) : '';
    if (isFinite(unit) && isFinite(line)) itemsTotal += line;
    itemsCount += qty;
    if (!currency && it.currency) currency = it.currency;

    compact.push({
      title: (it.item_title || '').slice(0, 500),
      qty,
      unit_price: isFinite(unit) ? +unit.toFixed(2) : '',
      line_total: line
    });
  });

  const pretty = compact.map(c => {
    const up = (c.unit_price !== '' ? `${currency} ${c.unit_price.toFixed(2)}` : '').trim();
    const lt = (c.line_total !== '' ? `${currency} ${c.line_total.toFixed(2)}` : '').trim();
    return `${c.qty} × ${c.title}${up ? ` — ${up}` : ''}${lt ? ` (${lt})` : ''}`;
  }).join('\n');

  return {
    items_count: itemsCount || '',
    items_total: itemsTotal ? +itemsTotal.toFixed(2) : '',
    items_json: JSON.stringify(compact),
    items_summary: pretty
  };
}

function ensureOrderShape_(partial) {
  const out = Object.assign({}, ORDER_DEFAULTS, partial || {});
  // never let undefined/null leak to Sheets – coerce to '' for scalars
  for (const k in out) if (out[k] == null) out[k] = '';
  return out;
}
