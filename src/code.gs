/**
 * Amazon → Sheets (orders/returns/refunds) — v0.5 (refactor + logs)
 * - Structured logs (LOG.debug/info/warn/error with scopes + timing)
 * - Clear function docs + guard clauses
 * - Safe wrappers for Gmail/Sheets interactions
 * - Same functionality as v0.4, just easier to read/diagnose
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
    .addItem('Dry Run (no writes)', 'runSyncDryRun')
    .addToUi();
}

//////////////////////////////
// Entrypoints
//////////////////////////////

/** Dry run: classify + log only (no writes). */
function runSyncDryRun() {
  const sc = LOG.scope('DryRun');
  try {
    const cfg = readSettings_();
    const messages = fetchCandidateEmails_(cfg.runWindowDays);
    LOG.info('DryRun: messages fetched', { count: messages.length });
    const rows = [];
    for (const m of messages) {
      const { type } = classifyEmail_(m);
      rows.push(buildEmailLogRow_(m, 'DryRun', type, extractOrderId_(m.getSubject())));
    }
    if (rows.length) appendRows_(SHEETS.EMAIL_LOG, rows);
    LOG.info('DryRun complete', { emailLogs: rows.length });
  } catch (e) {
    LOG.error('DryRun failure', { err: String(e) });
    throw e;
  } finally {
    sc.end();
  }
}

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
    const ordersUpserts = [], returnsUpserts = [], refundsAppends = [], emailLogs = [];
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
      const { type } = classifyEmail_(msg);
      counts[type] = (counts[type] || 0) + 1;

      try {
        const content = getMessageContent_(msg);
        const combined  = content.subject + ' ' + content.plainText + ' ' + content.htmlText;
        orderId = extractOrderId_(combined) || '';

        switch (type) {
          case 'OrderConfirm': {
            const o = parseOrderConfirm_(content, cfg.tz);
            if (!o.order_id && orderId) o.order_id = orderId;
            if (!o.order_id) throw new Error('No order_id found (OrderConfirm)');
            o.last_updated_at = isoNow_(cfg.tz);
            ordersUpserts.push(o);

            // Merge items per order_id across any emails
            if (o._items && o._items.length) {
              const prev = itemsByOrder.get(o.order_id) || [];
              itemsByOrder.set(o.order_id, mergeItemArrays_(prev, o._items));
              LOG.debug('Items merged', { order_id: o.order_id, items: itemsByOrder.get(o.order_id).length });
            }
            break;
          }
          case 'Shipment': {
            const o = parseShipment_(content, cfg.tz);
            if (!o.order_id && orderId) o.order_id = orderId;
            if (o.order_id) { o.last_updated_at = isoNow_(cfg.tz); ordersUpserts.push(o); }
            break;
          }
          case 'Delivery': {
            const o = parseDelivery_(content, cfg.tz);
            if (!o.order_id && orderId) o.order_id = orderId;
            if (o.order_id) { o.last_updated_at = isoNow_(cfg.tz); ordersUpserts.push(o); }
            break;
          }
          case 'Return': {
            const r = parseReturn_(content, cfg.tz);
            if (!r.order_id && orderId) r.order_id = orderId;
            if (!r.order_id) throw new Error('No order_id found (Return)');
            returnsUpserts.push(r);
            break;
          }
          case 'Refund': {
            const rf = parseRefund_(content, cfg.tz);
            if (!rf.order_id && orderId) rf.order_id = orderId;
            if (!rf.order_id) throw new Error('No order_id found (Refund)');
            refundsAppends.push(rf);
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
    if (refundsAppends.length) {
      LOG.info('Appending Refunds', { count: refundsAppends.length });
      appendRows_(SHEETS.REFUNDS, refundsAppends);
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
function classifyEmail_(msg) {
  const s = (msg.getSubject() || '').toLowerCase().trim();

  if (
    /^ordered[:\-]/i.test(s) ||
    /(order\s+placed|order\s+confirmed|order\s+confirmation)/i.test(s) ||
    /(your order|thanks for your order)/i.test(s)
  ) return { type: 'OrderConfirm' };

  if (/refund/i.test(s)) return { type: 'Refund' };
  if (/return/i.test(s)) return { type: 'Return' };

  if (/(delivered|your package was delivered)/i.test(s)) return { type: 'Delivery' };
  if (/(shipped|has shipped|out for delivery|on the way|package was shipped)/i.test(s)) return { type: 'Shipment' };

  return { type: 'Other' };
}

//////////////////////////////
// Parsers
//////////////////////////////

function parseOrderConfirm_(content, tz) {
  const order_id = extractOrderId_(content.subject + ' ' + content.plainText + ' ' + content.htmlText);

  const order_date_utc   = iso_(content.date, 'Etc/UTC');
  const order_date_local = iso_(content.date, tz);

  const currency = inferCurrency_(content.plainText, content.htmlText);

  let order_total = extractLabeledAmount_(content.plainText, ['Order Total','Grand Total','Total']);
  if (order_total == null) order_total = extractLabeledAmountHTML_(content.htmlText, ['Order Total','Grand Total','Total']);

  let shipping = extractLabeledAmount_(content.plainText, ['Shipping','Delivery']);
  if (shipping == null) shipping = extractLabeledAmountHTML_(content.htmlText, ['Shipping','Delivery']);

  const pay = extractPayment_(content.plainText + ' ' + content.htmlText);
  const purchase_channel = inferPurchaseChannel_(content.from, content.htmlText);
  const seller = extractSeller_(content.htmlText, content.plainText) || 'Amazon';
  const product = extractFirstProduct_(content.htmlText, content.plainText);

  // Parse all items (title, qty, unit price)
  const items = extractLineItems_(content.htmlText, content.plainText);

  const o = {
    order_id,
    order_date_utc,
    order_date_local,
    buyer_email: (content.to || '').split(',')[0] || '',
    seller,
    order_total: order_total != null ? order_total : '',
    shipping: shipping != null ? shipping : '',
    currency: currency || '',
    payment_method: pay.method,
    purchase_channel,
    status: 'Ordered',
    status_event_at: iso_(content.date, tz),

    gmail_message_id: content.id,
    first_seen_email_id: content.id,
    last_updated_at: isoNow_(tz),

    _items: items // carried to runner; not stored directly in Orders
  };

  if (product.title) o.first_item_title = product.title;
  if (product.imageUrl) {
    o.first_item_image_url = product.imageUrl;
    o.first_item_image_thumb = `=IMAGE("${product.imageUrl.replace(/"/g,'""')}",4,60,60)`;
  }
  return o;
}

function parseShipment_(content, tz) {
  const order_id = extractOrderId_(content.subject + ' ' + content.plainText + ' ' + content.htmlText);
  return { order_id, status: 'Shipped', status_event_at: iso_(content.date, tz) };
}
function parseDelivery_(content, tz) {
  const order_id = extractOrderId_(content.subject + ' ' + content.plainText + ' ' + content.htmlText);
  return { order_id, status: 'Delivered', status_event_at: iso_(content.date, tz) };
}

function parseReturn_(content, tz) {
  const combined = content.subject + ' ' + content.plainText;
  const order_id = extractOrderId_(combined);
  let status = 'Requested';
  if (/drop\s*off confirmation|was dropped off/i.test(combined)) status = 'Shipped Back';
  if (/return (received|processed)/i.test(combined)) status = 'Received';

  const qrLink = extractHrefByText_(content.htmlText, ['Download QR Code','QR code','Return code']);
  const qrImg  = extractImageByAltOrNearText_(content.htmlText, ['Return code','QR']);

  const return_id = order_id ? `${order_id}-ret-${formatDateId_(content.date, tz)}` : `ret-${content.id}`;
  const r = {
    return_id,
    order_id,
    qty_returned: '',
    return_requested_at: status === 'Requested' ? iso_(content.date, tz) : '',
    return_approved_at: status === 'Received' ? iso_(content.date, tz) : '',
    return_carrier: '',
    rma_label_id: '',
    return_reason: '',
    status
  };
  if (qrLink) r.return_qr_link = qrLink;
  if (qrImg)  r.return_qr_image_url = qrImg;
  return r;
}

function parseRefund_(content, tz) {
  const order_id = extractOrderId_(content.subject + ' ' + content.plainText + ' ' + content.htmlText);
  const refund_total = extractLabeledAmount_(content.plainText, ['Total refund','Refund subtotal','Refund total','Refund','Refunded']) ??
                       largestAmount_(content.plainText);
  const currency = inferCurrency_(content.plainText, content.htmlText);
  const refund_id = order_id ? `${order_id}-rf-${formatDateId_(content.date, tz)}` : `rf-${content.id}`;
  const reason = extractAfterLabel_(content.plainText, ['Reason','Reason for refund']);

  return {
    refund_id,
    order_id,
    item_id: '',
    refund_email_type: 'Issued',
    amount_items: '',
    amount_tax: '',
    amount_shipping: '',
    amount_other: '',
    currency: currency || '',
    refund_total: refund_total != null ? refund_total : '',
    refund_initiated_at: '',
    refund_issued_at: iso_(content.date, tz),
    reason: reason || ''
  };
}

//////////////////////////////
// Helpers: message, text, regex
//////////////////////////////

function getMessageContent_(msg) {
  const subject = msg.getSubject() || '';
  const htmlText = (msg.getBody() || '').replace(/\r/g, '\n');
  const plainText = (msg.getPlainBody() || '').replace(/\r/g, '\n');
  return {
    id: msg.getId(),
    threadId: msg.getThread().getId(),
    subject, htmlText, plainText, date: msg.getDate(),
    from: msg.getFrom() || '', to: msg.getTo() || '', label: LABELS.TO_PARSE
  };
}
function extractOrderId_(text) {
  const m = String(text || '').match(/(\d{3}-\d{7}-\d{7})/);
  return m ? m[1] : '';
}
function inferCurrency_(plain, html) {
  if (/\$\s?\d/.test(plain+html)) return 'USD';
  if (/€\s?\d/.test(plain+html))  return 'EUR';
  if (/£\s?\d/.test(plain+html))  return 'GBP';
  return '';
}
function extractLabeledAmount_(text, labels) {
  if (!text) return null;
  for (const label of labels) {
    const re = new RegExp(label + '\\s*[:\\-]?\\s*([\\$€£]?\\s?\\d{1,3}(?:,\\d{3})*(?:\\.\\d{2})?)', 'i');
    const m = text.match(re);
    if (m) return normalizeAmount_(m[1]);
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

function extractFirstProduct_(html, plain) {
  const out = { title:'', imageUrl:'' };
  try {
    const titles = [];
    const reA = /<a\b[^>]*>([^<]{8,120})<\/a>/gi;
    let m;
    while ((m = reA.exec(html))) {
      const t = m[1].replace(/\s+/g,' ').trim();
      if (!t) continue;
      if (/your orders|your account|buy again|track package/i.test(t)) continue;
      titles.push(t);
    }
    titles.sort((a,b)=>b.length-a.length);
    if (titles.length) out.title = titles[0];

    const reImg = /<img\b[^>]*src="([^"]+m\.media-amazon\.com[^"]+)"[^>]*>/gi;
    const im = reImg.exec(html);
    if (im) out.imageUrl = im[1];
  } catch(_) {}

  if (!out.title && /quantity\s*:\s*\d/i.test(plain)) {
    const lines = plain.split('\n').map(s=>s.trim()).filter(Boolean);
    const qix = lines.findIndex(l=>/quantity\s*:\s*\d/i.test(l));
    if (qix>0) out.title = lines[qix-1].slice(0,140);
  }
  return out;
}
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

function extractPayment_(plainPlusHtml) {
  const t = String(plainPlusHtml || '').toLowerCase();
  if (/monthly payments?/.test(t)) return { method: 'Amazon Monthly Payments', last4: '' };

  const cards = ['visa','mastercard','master card','american express','amex','discover','amazon store card','amazon secured card','gift card'];
  let method = '';
  for (const c of cards) {
    const re = new RegExp(c.replace(' ', '\\s+'), 'i');
    if (re.test(t)) { method = c.replace(/\b\w/g, s => s.toUpperCase()); break; }
  }
  let last4 = '';
  const m = t.match(/ending(?:\s+in|\s+with)?\s*(\d{4})|last\s*4\s*digits\s*(\d{4})/i);
  if (m) last4 = (m[1] || m[2] || '').trim();

  if (!method && last4) method = 'Card';
  if (method === 'Amex') method = 'American Express';
  if (method) return { method, last4 };
  return { method: '', last4: '' };
}
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

  // Copy non-empty scalars (never wipe with '')
  for (const [k, v] of Object.entries(incoming)) {
    if (k === 'status' || k === 'status_event_at') continue; // handled below
    if (k === 'order_total' && existing.order_total) {
      // protect order_total from being overwritten by empties/smaller shipment subtotals
      if (v !== '' && v !== null && v !== undefined && Number(v) > Number(existing.order_total)) {
        out[k] = v; // accept upgrade
      }
      continue;
    }
    if (v !== '' && v !== null && v !== undefined) out[k] = v;
  }

  // seed first_seen_* once
  if (!existing.first_seen_email_id && (incoming.gmail_message_id || incoming.first_seen_email_id)) {
    out.first_seen_email_id = incoming.gmail_message_id || incoming.first_seen_email_id;
  }
  if (!existing.first_seen_at && incoming.first_seen_at) out.first_seen_at = incoming.first_seen_at;

  // Status history
  let history = [];
  try { history = existing.status_history ? JSON.parse(existing.status_history) : []; }
  catch (_) { history = []; }

  const addHist = (st, at) => {
    if (!st || !at) return;
    if (history.some(h => String(h.status) === st && String(h.at) === at)) return;
    history.push({ status: st, at: at });
  };
  const sortHist = () => history.sort((a,b) => String(a.at).localeCompare(String(b.at)));

  // Anti-downgrade
  const oldStatus = String(existing.status || '').trim();
  const newStatus = String(incoming.status || '').trim();
  const eventAt   = incoming.status_event_at || incoming.last_updated_at || '';

  const rank = s => STATUS_ORDER[s] || 0;
  const oldRank = rank(oldStatus);
  const newRank = rank(newStatus);

  if (newStatus && eventAt) addHist(newStatus, eventAt);

  if (!oldStatus && newStatus) {
    out.status = newStatus;
    out.status_changed_at = eventAt || out.status_changed_at || '';
  } else if (newStatus) {
    if (newRank > oldRank) {
      out.status = newStatus;
      out.status_changed_at = eventAt || out.status_changed_at || '';
    } else if (newRank === oldRank) {
      if (eventAt && (!existing.status_changed_at || String(eventAt) > String(existing.status_changed_at))) {
        out.status_changed_at = eventAt;
      }
    }
  }

  const setOnce = (field, st) => {
    if (newStatus === st && eventAt && !existing[field]) out[field] = eventAt;
  };
  setOnce('ordered_at',   'Ordered');
  setOnce('shipped_at',   'Shipped');
  setOnce('delivered_at', 'Delivered');

  sortHist();
  out.status_history = JSON.stringify(history);
  return out;
}

//////////////////////////////
// Upsert Orders with status merge + history tracking
//////////////////////////////

function upsertOrdersWithStatus_(objs) {
  const { sh, headers } = getHeaders_(SHEETS.ORDERS);
  let hdrs = headers.slice();

  [
    'status','status_changed_at','status_history','ordered_at','shipped_at','delivered_at',
    'order_date_utc','order_date_local','buyer_email','seller','order_total','shipping',
    'currency','payment_method','purchase_channel','first_seen_email_id',
    'last_updated_at','first_item_title','first_item_image_url','first_item_image_thumb',
    'items_count','items_total','items_json','items_summary'
  ].forEach(c => { if (!hdrs.includes(c)) { hdrs.push(c); sh.getRange(1, hdrs.length, 1, 1).setValue(c); } });

  SpreadsheetApp.flush();

  // Build index of existing rows
  const lastRow = sh.getLastRow();
  const index = new Map(); // order_id -> row
  if (lastRow > 1) {
    const ids = sh.getRange(2, 1, lastRow - 1, 1).getValues(); // column A = order_id
    for (let i = 0; i < ids.length; i++) {
      const id = String(ids[i][0] || '').trim();
      if (id && !index.has(id)) index.set(id, i + 2);
    }
  }

  // Cache row obj to avoid rereads
  const cache = new Map(); // row -> obj

  for (const incoming of objs) {
    const id = String(incoming.order_id || '').trim();
    if (!id) continue;

    // If incoming carries parsed items, fold into row fields
    if (incoming._items && incoming._items.length) {
      const itemFields = summarizeItemsForOrderRow_(incoming._items);
      Object.assign(incoming, itemFields);
    }

    let row = index.get(id);
    if (!row) {
      const seeded = mergeOrder_({}, incoming);
      row = sh.getLastRow() + 1;
      sh.getRange(row, 1, 1, hdrs.length).setValues([objToRow_(hdrs, seeded)]);
      index.set(id, row);
      cache.set(row, seeded);
      continue;
    }

    let current = cache.get(row);
    if (!current) {
      current = rowToObj_(hdrs, sh.getRange(row, 1, 1, hdrs.length).getValues()[0]);
    }
    const merged = mergeOrder_(current, incoming);
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

function extractLineItems_(html, plain) {
  const items = [];
  const seen = new Set();
  const H = String(html || '').replace(/\s+/g, ' '); // normalize

  // --- split into order sections (robust to tags/RTL markers) ---
  const sections = [];
  const orderRe = /Order\s*#(?:[^0-9]{0,80}?)(\d{3}-\d{7}-\d{7})/gi;
  let m;
  const starts = [];
  while ((m = orderRe.exec(H))) starts.push(m.index);
  if (starts.length === 0) starts.push(0);
  for (let i = 0; i < starts.length; i++) {
    const start = starts[i];
    let end = i + 1 < starts.length ? starts[i + 1] : H.length;
    const cut = H.slice(start, end).search(/(?:Continue shopping|Keep shopping|By placing your order|Privacy Notice|Conditions of Use|tax and seller information)/i);
    if (cut !== -1) end = start + cut;
    sections.push(H.slice(start, end));
  }

  const blacklist = /your orders|your account|buy again|track package|view (?:or )?edit order|view and manage|order details|sign in|help|privacy notice|conditions of use/i;

  const pickPrice = (s) => {
    let mm = s.match(/([\$€£]\s?\d{1,3}(?:,\d{3})*(?:\.\d{2}))/);
    if (mm) return mm[1];
    mm = s.match(/([\$€£]\s?\d{1,3}(?:,\d{3})*)(?:\s*<\/?[^>]*sup[^>]*>\s*(\d{2})\s*<\/?[^>]*sup[^>]*>)/i);
    if (mm) return mm[1] + '.' + mm[2];
    mm = s.match(/([\$€£]\s?\d{1,3}(?:,\d{3})*)\s*(?:<\/?[^>]+>|\s){0,30}(\d{2})(?!\d)/i);
    if (mm) return mm[1] + '.' + mm[2];
    return '';
  };

  const pushItem = (title, qty, price) => {
    title = (title || '').trim();
    if (!title || title.length < 4 || blacklist.test(title)) return;
    if (!price) return;
    qty = Math.max(1, Number(qty || 1));
    const key = `${title.toLowerCase()}::${qty}::${price}`;
    if (seen.has(key)) return;
    seen.add(key);
    items.push({
      item_title: title.slice(0, 500),
      qty,
      item_price: normalizeAmount_(price),
      currency: /\$/.test(price) ? 'USD' : /€/.test(price) ? 'EUR' : /£/.test(price) ? 'GBP' : ''
    });
  };

  // --- parse each section ---
  const aRe = /<a\b[^>]*href="([^"]+)"[^>]*>([^<]{6,220})<\/a>/gi;
  for (const region of sections) {
    let ma;
    while ((ma = aRe.exec(region))) {
      const href = ma[1] || '';
      let title = (ma[2] || '').replace(/\s+/g, ' ').trim();
      if (!/\/(?:dp|gp\/product)\//i.test(href)) continue; // product-like link
      if (!title || blacklist.test(title)) continue;

      // look around the anchor for qty + price
      const at = ma.index;
      const win = region.slice(Math.max(0, at - 700), at + 1200);

      // Quantity nearby
      let qty = 1;
      const mq = win.match(/\b(?:qty|quantity)\b\s*[:\-]?\s*(\d+)/i);
      if (mq) qty = Number(mq[1]) || 1;

      // Price nearby
      const price = pickPrice(win) || pickPrice(region.slice(at, at + 1500));
      if (!price) continue;

      pushItem(title, qty, price);
    }
  }

  // --- plaintext fallback ("1 x Title — $9.99") ---
  if (!items.length && plain) {
    const lines = String(plain).split('\n').map(s => s.trim()).filter(Boolean);
    for (let i = 0; i < lines.length; i++) {
      let t = lines[i];
      if (/^(order|arriving|delivered|tracking|ship|estimate|keep shopping|privacy|conditions)/i.test(t)) continue;

      let qty = 1, price = '';
      const mLead = t.match(/^(\d+)\s*x\s+(.{6,})/i);
      if (mLead) { qty = Number(mLead[1]) || 1; t = mLead[2].trim(); }
      const mpHere = t.match(/([\$€£]\s?\d{1,3}(?:,\d{3})*(?:\.\d{2}))/);
      if (mpHere) price = mpHere[1];
      if (!price) {
        for (let j = 1; j <= 3 && i + j < lines.length; j++) {
          const L = lines[i + j];
          const mq2 = L.match(/\b(?:qty|quantity)\b\s*[:\-]?\s*(\d+)/i); if (mq2) qty = Number(mq2[1]) || qty;
          const mp2 = L.match(/([\$€£]\s?\d{1,3}(?:,\d{3})*(?:\.\d{2}))/); if (mp2) { price = mp2[1]; break; }
        }
      }
      if (price && t.length >= 8 && !blacklist.test(t)) pushItem(t, qty, price);
    }
  }

  return items;
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

//////////////////////////////
// Misc helpers for items merge keys
//////////////////////////////

function _normTitle_(t) {
  return String(t || '')
    .toLowerCase()
    .replace(/&amp;/g, '&')
    .replace(/[^a-z0-9$€£.\-\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}
function _itemKey_(title, unitPrice) {
  const p = (unitPrice != null && isFinite(Number(unitPrice))) ? Number(unitPrice).toFixed(2) : '';
  return _normTitle_(title) + ' :: ' + p;
}
function mergeItemArrays_(existing = [], incoming = []) {
  const out = [];
  const idx = new Map(); // key -> index
  const add = (it) => {
    const qty  = Math.max(1, Number(it.qty || 1));
    const unit = (it.item_price !== '' && it.item_price != null) ? Number(it.item_price) : NaN;
    const key  = _itemKey_(it.item_title, unit);
    if (!idx.has(key)) {
      const row = {
        item_title: it.item_title || '',
        qty,
        item_price: isFinite(unit) ? +unit.toFixed(2) : '',
        currency: it.currency || ''
      };
      idx.set(key, out.length);
      out.push(row);
    } else {
      out[idx.get(key)].qty += qty; // increment qty when same item shows up again
    }
  };
  existing.forEach(add);
  incoming.forEach(add);
  return out;
}

//////////////////////////////
// Legacy helpers used by extractLineItems_
//////////////////////////////

function _cleanHtml_(html) {
  return String(html || '')
    .replace(/\s+/g, ' ')
    .replace(/<img[^>]+?amazon\.(?:com|ca|co\.uk|de|fr|it|es|com\.au)[^>]*?>/gi, m => m);
}
function _pickPriceFromWindow_(win) {
  let m = win.match(/([\$€£]\s?\d{1,3}(?:,\d{3})*(?:\.\d{2}))/);
  if (m) return m[1];
  m = win.match(/([\$€£]\s?\d{1,3}(?:,\d{3})*)(?:\s*<\/?[^>]*sup[^>]*>\s*(\d{2})\s*<\/?[^>]*sup[^>]*>)/i);
  if (m) return m[1] + '.' + m[2];
  return '';
}
