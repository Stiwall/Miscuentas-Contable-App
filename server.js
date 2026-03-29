/**
 * MisCuentas RD — Bot Server v2
 * Stack : Express + Telegram Webhooks + PostgreSQL (Railway) + Groq Vision + Gemini
 * Host  : Railway
 */

'use strict';

const express  = require('express');
const { Pool } = require('pg');

const app = express();
app.use(express.json());
// No public/ folder - frontend served as single HTML at root
// app.use(express.static('public')); // no public folder in this app

// ─── ENV ──────────────────────────────────────────────────────────────────────
const {
  TELEGRAM_BOT_TOKEN,
  DATABASE_URL,
  GEMINI_API_KEY,
  GROQ_API_KEY,
  CRON_SECRET,
  WEBHOOK_SECRET,          // token aleatorio para validar llamadas de Telegram
  SESSION_SECRET = 'miscuentas_secret_change_me', // secreto para firmar tokens de sesión
  PORT = 3000,
  API_BASE = '',
} = process.env;

['DATABASE_URL'].forEach(k => {
  if (!process.env[k]) { console.error(`❌ Missing env: ${k}`); process.exit(1); }
});
const HAS_TELEGRAM = !!TELEGRAM_BOT_TOKEN;

// ─── POSTGRES ─────────────────────────────────────────────────────────────────
const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 10,
  idleTimeoutMillis: 30000,
});

pool.on('error', err => console.error('PG pool error:', err.message));

// ─── SESSION TOKENS ───────────────────────────────────────────────────────────
const crypto = require('crypto');

function generateToken(userId) {
  const payload = `${userId}:${Date.now()}`;
  const sig = crypto.createHmac('sha256', SESSION_SECRET).update(payload).digest('hex');
  return Buffer.from(`${payload}:${sig}`).toString('base64url');
}

function verifyToken(token) {
  try {
    const decoded = Buffer.from(token, 'base64url').toString();
    const lastColon = decoded.lastIndexOf(':');
    const payload = decoded.substring(0, lastColon);
    const sig = decoded.substring(lastColon + 1);
    const expected = crypto.createHmac('sha256', SESSION_SECRET).update(payload).digest('hex');
    if (sig !== expected) return null;
    const colonIdx = payload.indexOf(':');
    return payload.substring(0, colonIdx); // userId
  } catch {
    return null;
  }
}

// ─── TELEGRAM OAUTH TOKENS (DB-backed) ─────────────────────────────────────────

// Create a pending auth token
async function createAuthToken(token, telegramId) {
  const sessionToken = generateToken(telegramId);
  await query(
    `INSERT INTO auth_tokens (token, telegram_id, session_token, created_at)
     VALUES($1, $2, $3, NOW())
     ON CONFLICT (token) DO UPDATE SET
       telegram_id = EXCLUDED.telegram_id,
       session_token = EXCLUDED.session_token,
       created_at = NOW()`,
    [token, telegramId, sessionToken]
  );
  return sessionToken;
}

// Get and delete an auth token (one-time use)
async function consumeAuthToken(token) {
  const r = await query(
    `SELECT telegram_id, session_token FROM auth_tokens
     WHERE token = $1 AND created_at > NOW() - INTERVAL '30 minutes'`,
    [token]
  );
  if (!r.rows[0]) return null;
  await query('DELETE FROM auth_tokens WHERE token = $1', [token]);
  return r.rows[0];
}

function authMiddleware(req, res, next) {
  const token = req.headers['x-session-token'];
  if (!token) return res.status(401).json({ error: 'unauthorized' });
  const userId = verifyToken(token);
  if (!userId) return res.status(401).json({ error: 'invalid token' });
  req.userId = userId;
  next();
}

async function adminMiddleware(req, res, next) {
  const token = req.headers['x-session-token'];
  if (!token) return res.status(401).json({ error: 'unauthorized' });
  const userId = verifyToken(token);
  if (!userId) return res.status(401).json({ error: 'invalid token' });
  const r = await query(`SELECT is_admin FROM users WHERE id=$1`, [userId]);
  if (!r.rows[0]?.is_admin) return res.status(403).json({ error: 'admin only' });
  req.userId = userId;
  next();
}

async function query(sql, params = []) {
  const client = await pool.connect();
  try {
    return await client.query(sql, params);
  } finally {
    client.release();
  }
}

// ─── TELEGRAM API ─────────────────────────────────────────────────────────────
const TG = HAS_TELEGRAM ? `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}` : '';

async function tgCall(method, body) {
  const res = await fetch(`${TG}/${method}`, {
    method : 'POST',
    headers: { 'Content-Type': 'application/json' },
    body   : JSON.stringify(body),
    signal : AbortSignal.timeout(15000),
  });
  return res.json();
}

async function sendMessage(chatId, text, extra = {}) {
  return tgCall('sendMessage', { chat_id: chatId, text, parse_mode: 'Markdown', ...extra });
}

async function getFileLink(fileId) {
  const r = await tgCall('getFile', { file_id: fileId });
  if (!r.ok) throw new Error('getFile failed');
  return `https://api.telegram.org/file/bot${TELEGRAM_BOT_TOKEN}/${r.result.file_path}`;
}

// ─── WEBHOOK SETUP ────────────────────────────────────────────────────────────
async function setWebhook(baseUrl) {
  const url = `${baseUrl}/webhook/${WEBHOOK_SECRET || 'tg'}`;
  const r   = await tgCall('setWebhook', { url, drop_pending_updates: true });
  console.log('Webhook set:', r.ok ? '✅' : '❌', r.description || '');
  return r;
}

// ─── DB HELPERS ───────────────────────────────────────────────────────────────
async function ensureUser(id, lang = 'es') {
  await query(
    `INSERT INTO users(id, lang) VALUES($1,$2)
     ON CONFLICT(id) DO NOTHING`,
    [id, lang]
  );
  // Auto-create system accounts for new user
  await createSystemAccounts(id);
}

// System accounts to create for each new user (classes 1-6)
const SYSTEM_ACCOUNTS = [
  { code: '1.1.01', name: 'Caja',            type: 'asset',     class: 1 },
  { code: '1.1.02', name: 'Banco',           type: 'asset',     class: 1 },
  { code: '1.2.01', name: 'Cuentas por Cobrar', type: 'asset', class: 1 },
  { code: '2.1.01', name: 'Cuentas por Pagar', type: 'liability', class: 2 },
  { code: '2.2.01', name: 'Tarjetas de Crédito', type: 'liability', class: 2 },
  { code: '3.1.01', name: 'Capital',          type: 'equity',    class: 3 },
  { code: '4.1.01', name: 'Ingresos',         type: 'income',    class: 4 },
  { code: '5.1.01', name: 'Costo de Ventas',  type: 'cost',      class: 5 },
  { code: '6.1.01', name: 'Gastos Operativos', type: 'expense', class: 6 },
];

async function createSystemAccounts(userId) {
  for (const acc of SYSTEM_ACCOUNTS) {
    const id = `sys_${userId}_${acc.code.replace('.', '_')}`;
    try {
      await query(
        `INSERT INTO accounts(id, user_id, code, name, type, class, is_system)
         VALUES($1,$2,$3,$4,$5,$6,TRUE)
         ON CONFLICT(user_id, code) DO NOTHING`,
        [id, userId, acc.code, acc.name, acc.type, acc.class]
      );
      // Insert initial balance
      await query(
        `INSERT INTO account_balances(account_id, balance)
         VALUES($1, 0) ON CONFLICT(account_id) DO NOTHING`,
        [id]
      );
    } catch(e) { /* ignore dup */ }
  }
}

async function getUser(id) {
  const r = await query('SELECT * FROM users WHERE id=$1', [id]);
  return r.rows[0] || null;
}

async function getUserLang(id) {
  const r = await query('SELECT lang FROM users WHERE id=$1', [id]);
  return r.rows[0]?.lang || 'es';
}

async function setUserLang(id, lang) {
  await query('UPDATE users SET lang=$2 WHERE id=$1', [id, lang]);
}

async function getMonthTxs(userId, month, year) {
  const r = await query(
    `SELECT * FROM transactions
     WHERE user_id=$1
       AND EXTRACT(MONTH FROM tx_date)=$2
       AND EXTRACT(YEAR  FROM tx_date)=$3
     ORDER BY created_at ASC`,
    [userId, month + 1, year]          // month es 0-based en JS
  );
  return r.rows;
}

async function getAllTxs(userId) {
  const r = await query(
    `SELECT * FROM transactions WHERE user_id=$1 ORDER BY created_at ASC`,
    [userId]
  );
  return r.rows;
}

async function insertTx(tx) {
  await query(
    `INSERT INTO transactions(id, user_id, type, amount, description, category, account, tx_date)
     VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
    [tx.id, tx.userId, tx.type, tx.amount, tx.description, tx.category, tx.account, tx.date]
  );
}

async function deleteTxById(txId, userId) {
  await query('DELETE FROM transactions WHERE id=$1 AND user_id=$2', [txId, userId]);
}

async function getBudgets(userId) {
  const r = await query('SELECT category, amount FROM budgets WHERE user_id=$1', [userId]);
  const obj = {};
  r.rows.forEach(row => { obj[row.category] = parseFloat(row.amount); });
  return obj;
}

async function setBudget(userId, category, amount) {
  await query(
    `INSERT INTO budgets(user_id, category, amount) VALUES($1,$2,$3)
     ON CONFLICT(user_id, category) DO UPDATE SET amount=$3`,
    [userId, category, amount]
  );
}

async function getPending(userId) {
  const r = await query('SELECT tx_data FROM pending_tx WHERE user_id=$1', [userId]);
  return r.rows[0]?.tx_data || null;
}

async function setPending(userId, txData) {
  await query(
    `INSERT INTO pending_tx(user_id, tx_data) VALUES($1,$2)
     ON CONFLICT(user_id) DO UPDATE SET tx_data=$2, created_at=NOW()`,
    [userId, JSON.stringify(txData)]
  );
}

async function clearPending(userId) {
  await query('DELETE FROM pending_tx WHERE user_id=$1', [userId]);
}

// ─── UTILS ────────────────────────────────────────────────────────────────────
function uid() {
  return `tx_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
}

function fmt(n) {
  return Number(n).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

const MONTHS_ES = ['Enero','Febrero','Marzo','Abril','Mayo','Junio',
                   'Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre'];
const MONTHS_EN = ['January','February','March','April','May','June',
                   'July','August','September','October','November','December'];

const CAT_EMOJI = {
  comida:'🍽️', transporte:'🚗', servicios:'💡', salud:'🏥',
  entretenimiento:'🎬', ropa:'👕', educacion:'📚', salario:'💼',
  negocio:'🏪', inversion:'📈', prestamo:'🤝', ahorro:'💰', otro:'📦',
  food:'🍽️', transport:'🚗', health:'🏥', entertainment:'🎬',
  clothes:'👕', education:'📚', salary:'💼', business:'🏪', savings:'💰',
};
const ACC_EMOJI = { efectivo:'💵', banco:'🏦', tarjeta:'💳' };

function detectLang(msg = '') {
  const t = msg.toLowerCase();
  const esWords = ['gasté','gaste','pagué','pague','compré','compre','deposité','deposite',
    'cobré','cobre','recibí','recibi','ingresé','ingrese','sueldo','quincena','resumen',
    'cuentas','alertas','historial','presupuesto','ayuda','hola','gracias','si','sí','buenos'];
  return esWords.some(w => t.includes(w)) ? 'es' : 'en';
}

// ─── MESSAGES ─────────────────────────────────────────────────────────────────
const MSG = {
  welcome: (id, lang) => lang === 'es'
    ? `👋 *¡Bienvenido a MisCuentas!*\n\n🎉 Ya puedes registrar tus finanzas.\n\nTu Telegram ID: \`${id}\`\nÚsalo para entrar al panel web.\n\nEnvía *ayuda* para ver los comandos.`
    : `👋 *Welcome to MisCuentas!*\n\n🎉 Start tracking your finances now.\n\nYour Telegram ID: \`${id}\`\nUse it to log in to the web panel.\n\nSend *help* for all commands.`,

  miid: (id, lang) => lang === 'es'
    ? `🪪 *Tu Telegram ID:*\n\n\`${id}\`\n\nÚsalo para entrar al panel web.`
    : `🪪 *Your Telegram ID:*\n\n\`${id}\`\n\nUse it to log in to the web panel.`,

  recorded: (tx, lang) => {
    const catE  = CAT_EMOJI[tx.category] || '📦';
    const accE  = ACC_EMOJI[tx.account]  || '💵';
    const arrow = tx.type === 'ingreso' ? '▲' : '▼';
    return lang === 'es'
      ? `✅ *Registrado*\n\n${arrow} ${catE} ${tx.description}\n💰 ${fmt(tx.amount)}\n${accE} ${tx.account}`
      : `✅ *Recorded*\n\n${arrow} ${catE} ${tx.description}\n💰 ${fmt(tx.amount)}\n${accE} ${tx.account}`;
  },

  receiptPreview: (tx, lang) => lang === 'es'
    ? `🧾 *Factura detectada*\n\n📍 ${tx.description}\n💰 ${fmt(tx.amount)}\n${CAT_EMOJI[tx.category]||'📦'} ${tx.category}\n\n✅ Responde *si* para confirmar\n❌ Responde *no* para cancelar\n💡 Para cambiar cuenta: *si banco* o *si tarjeta*`
    : `🧾 *Receipt detected*\n\n📍 ${tx.description}\n💰 ${fmt(tx.amount)}\n${CAT_EMOJI[tx.category]||'📦'} ${tx.category}\n\n✅ Reply *yes* to confirm\n❌ Reply *no* to cancel\n💡 To change account: *yes bank* or *yes card*`,

  noPending  : (lang) => lang === 'es' ? '❌ No hay transacción pendiente.'  : '❌ No pending transaction.',
  cancelled  : (lang) => lang === 'es' ? '❌ Cancelado.'                     : '❌ Cancelled.',
  notUnderstood: (lang) => lang === 'es'
    ? `🤔 No entendí ese mensaje.\n\nEnvía *ayuda* para ver los comandos.\n\nEjemplos:\n• gasté 350 en comida\n• pagué la luz 1200 con banco\n• deposité el sueldo 28000\n• 📷 Envía una foto de factura`
    : `🤔 I didn't understand that.\n\nSend *help* to see commands.\n\nExamples:\n• spent 50 on food\n• paid rent 800 with bank\n• received salary 2000\n• 📷 Send a receipt photo`,

  help: (lang) => lang === 'es'
    ? `📖 *MisCuentas — Comandos*\n\n` +
      `━━━━━ 💰 FINANZAS PERSONALES ━━━━━\n\n` +
      `📊 *Consultas:*\n• resumen — Balance del mes\n• cuentas — Por cuenta (efectivo/banco/tarjeta)\n• alertas — Alertas financieras\n• historial — Últimos movimientos\n• presupuesto — Ver límites de gastos\n\n` +
      `📝 *Registrar movimientos:*\n• gasté 350 en comida\n• pagué la luz 1200 con banco\n• deposité el sueldo 28000\n• compré zapatos 2500 con tarjeta\n\n` +
      `📷 *Facturas:*\n• Envía una foto de factura para registrar automáticamente\n\n` +
      `📊 *Presupuestos:*\n• presupuesto comida 5000\n• presupuesto transporte 3000\n\n` +
      `━━━━━ 📋 CONTABILIDAD ━━━━━\n\n` +
      `📚 *Plan de Cuentas:*\n• /plan — Ver plan de cuentas con saldos\n• /nuevacuenta [código] [nombre] [tipo] — Crear cuenta\n  Tipos: asset, liability, equity, income, cost, expense\n  Ej: /nuevacuenta 1.3.05 Inventario asset\n\n` +
      `👥 *Clientes y Cobros:*\n• /clientes — Ver clientes con deudas pendientes\n• /ccobrar — Resumen cuentas por cobrar\n• /agregarcliente — Agregar nuevo cliente\n• /nuevacobranza [clientId] [monto] [desc] — Crear cobranza\n• /registrarpago [recId] [monto] — Registrar pago recibido\n\n` +
      `🏪 *Proveedores y Pagos:*\n• /cpagar — Resumen cuentas por pagar\n• /agregarproveedor — Agregar nuevo proveedor\n\n` +
      `━━━━━ 🔐 CUENTA WEB ━━━━━\n\n` +
      `• /setpassword — Crear usuario y contraseña para la app web\n• /linkaccount — Vincular cuenta web existente\n• /miid — Ver tu Telegram ID\n\n` +
      `💡 _La web incluye gráficas, reportes financieros y más_`
    : `📖 *MisCuentas — Commands*\n\n` +
      `━━━━━ 💰 PERSONAL FINANCE ━━━━━\n\n` +
      `📊 *Queries:*\n• resumen / summary — Monthly balance\n• cuentas / accounts — By account\n• alertas / alerts — Financial alerts\n• historial / history — Recent transactions\n• presupuesto / budget — Spending limits\n\n` +
      `📝 *Record transactions:*\n• spent 50 on food\n• paid rent 800 with bank\n• received salary 2000\n• bought shoes 80 with card\n\n` +
      `📷 *Receipts:*\n• Send a photo to auto-register\n\n` +
      `📊 *Budgets:*\n• budget food 500\n\n` +
      `━━━━━ 📋 ACCOUNTING ━━━━━\n\n` +
      `📚 *Chart of Accounts:*\n• /plan — View accounts with balances\n• /nuevacuenta [code] [name] [type] — Create account\n  Types: asset, liability, equity, income, cost, expense\n\n` +
      `👥 *Clients & Receivables:*\n• /clientes — Clients with outstanding balances\n• /ccobrar — Accounts receivable summary\n• /agregarcliente — Add new client\n• /nuevacobranza [clientId] [amount] [desc] — Create receivable\n• /registrarpago [recId] [amount] — Register payment received\n\n` +
      `🏪 *Vendors & Payables:*\n• /cpagar — Accounts payable summary\n• /agregarproveedor — Add new vendor\n\n` +
      `━━━━━ 🔐 WEB ACCOUNT ━━━━━\n\n` +
      `• /setpassword — Create credentials for the web app\n• /linkaccount — Link existing web account\n• /miid — Your Telegram ID\n\n` +
      `💡 _The web includes charts, financial reports and more_`,

  noGroq   : (lang) => lang === 'es' ? '❌ El análisis de fotos no está configurado.' : '❌ Photo analysis is not configured.',
  analyzing: (lang) => lang === 'es' ? '🔄 *Analizando factura...*'                  : '🔄 *Analyzing receipt...*',
  photoError: (lang) => lang === 'es' ? '❌ No pude analizar la imagen. Intenta con una foto más clara.' : '❌ Could not analyze the image. Try a clearer photo.',
  generalError: (lang) => lang === 'es' ? '❌ Ocurrió un error. Intenta de nuevo.' : '❌ An error occurred. Please try again.',
};

// ─── GROQ VISION ──────────────────────────────────────────────────────────────
async function analyzeReceipt(base64, mimeType) {
  if (!GROQ_API_KEY) return null;
  try {
    const r = await fetch('https://api.groq.com/openai/v1/chat/completions', {
      method : 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${GROQ_API_KEY}` },
      body   : JSON.stringify({
        model   : 'meta-llama/llama-4-scout-17b-16e-instruct',
        messages: [{ role: 'user', content: [
          { type: 'text', text: 'Analyze this receipt. Reply ONLY with valid JSON on one line, no markdown:\n{"success":true,"amount":NUMBER,"description":"STORE_NAME","category":"CATEGORY"}\nCATEGORY must be one of: comida,transporte,servicios,salud,entretenimiento,ropa,educacion,negocio,otro\nIf not a receipt reply: {"success":false}' },
          { type: 'image_url', image_url: { url: `data:${mimeType};base64,${base64}` } },
        ]}],
        temperature: 0,
        max_tokens : 150,
      }),
      signal: AbortSignal.timeout(30000),
    });
    const d   = await r.json();
    if (d.error) { console.error('Groq error:', d.error.message); return null; }
    const raw = d.choices?.[0]?.message?.content?.trim() || '';
    const m   = raw.match(/\{[^{}]*\}/);
    if (!m) return null;
    return JSON.parse(m[0]);
  } catch (e) { console.error('analyzeReceipt:', e.message); return null; }
}

// ─── GEMINI AI PARSER ─────────────────────────────────────────────────────────
async function parseWithAI(message) {
  if (!GEMINI_API_KEY) return null;
  const prompt = `You are a personal finance assistant. Parse this message and respond ONLY with valid JSON on one line, no markdown.

Message: "${message}"

Format: {"type":"ingreso|egreso|comando","amount":number_or_null,"desc":"text","cat":"category","account":"efectivo|banco|tarjeta","cmd":null,"budget_cat":null,"budget_amount":null}

Categories: comida, transporte, servicios, salud, entretenimiento, ropa, educacion, salario, negocio, inversion, prestamo, ahorro, otro

Rules:
- tarjeta/card/credit → account: tarjeta
- banco/bank/transfer/deposito → account: banco
- no mention → account: efectivo
- income words (received,earned,deposited,salary,cobré,ingresé,recibí,deposité,sueldo,quincena) → type:ingreso
- expense words (spent,paid,bought,gasté,pagué,compré) → type:egreso
- commands: resumen/summary, cuentas/accounts, alertas/alerts, historial/history, presupuesto/budget, ayuda/help, miid, plan/cuentas, clientes, ccobrar/cxc, cpagar/cxp, agregarcliente, agregarproveedor, nuevacobranza, registrarpago, nuevacuenta → type:comando, cmd:command_name
- "nuevacobranza clientId 500 description" → type:comando, cmd:nuevacobranza, client_id:clientId, amount:500, description:"description"
- "registrarpago receivableId 200" → type:comando, cmd:registrarpago, receivable_id:receivableId, amount:200
- "nuevacuenta 1.3.05 Name asset" → type:comando, cmd:nuevacuenta, code:"1.3.05", name:"Name", type:"asset"
- "budget/presupuesto X 500" → type:comando, cmd:set_budget, budget_cat:X, budget_amount:500
- yes/si/confirm + optional account → type:comando, cmd:confirmar, account: parsed_account_or_efectivo
- no/cancel → type:comando, cmd:cancelar`;

  try {
    const r = await fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash-latest:generateContent?key=${GEMINI_API_KEY}`,
      {
        method : 'POST',
        headers: { 'Content-Type': 'application/json' },
        body   : JSON.stringify({
          contents         : [{ parts: [{ text: prompt }] }],
          generationConfig : { temperature: 0.1, maxOutputTokens: 200 },
        }),
        signal: AbortSignal.timeout(15000),
      }
    );
    const d    = await r.json();
    const text = d.candidates?.[0]?.content?.parts?.[0]?.text?.trim()
                   .replace(/```json|```/g, '').trim();
    if (!text) return null;
    const m = text.match(/\{[\s\S]*?\}/);
    if (!m) return null;
    return JSON.parse(m[0]);
  } catch { return null; }
}

// ─── FALLBACK PARSER ──────────────────────────────────────────────────────────
const CAT_KW = {
  comida         : ['food','comida','almuerzo','desayuno','cena','restaurant','mercado','colmado','pizza','pollo','supermercado','grocery','lunch','dinner','breakfast'],
  transporte     : ['transport','transporte','gas','gasolina','taxi','uber','carro','bus','car','fuel','metro'],
  servicios      : ['luz','agua','internet','telefono','phone','netflix','spotify','cable','electric','water','service'],
  salud          : ['salud','health','medico','doctor','farmacia','pharmacy','medicina','hospital','dentista'],
  entretenimiento: ['entertainment','entretenimiento','cine','movie','fiesta','party','bar','viaje','hotel','travel'],
  ropa           : ['ropa','clothes','zapatos','shoes','camisa','shirt','tienda','store'],
  educacion      : ['school','escuela','universidad','university','libro','book','curso','course'],
  salario        : ['salary','salario','sueldo','quincena','nomina','payroll'],
  negocio        : ['business','negocio','venta','sale','cliente','client'],
  inversion      : ['investment','inversion','dividendo','dividend','stocks'],
  ahorro         : ['savings','ahorro','fondo','fund'],
  prestamo       : ['loan','prestamo','deuda','debt'],
};
const INC_VERBS = ['ingresé','ingrese','recibí','recibi','gané','gane','cobré','cobre',
                   'deposité','deposite','entró','entro','quincena','sueldo','salario',
                   'received','earned','got paid','deposited','salary','income'];
const EXP_VERBS = ['gasté','gaste','pagué','pague','compré','compre',
                   'spent','paid','bought','me costó','me costo','purchased'];

function detectCat(t) {
  for (const [c, kws] of Object.entries(CAT_KW)) if (kws.some(k => t.includes(k))) return c;
  return 'otro';
}
function detectAcc(t) {
  if (['tarjeta','card','credit','debit','credito','debito'].some(k => t.includes(k))) return 'tarjeta';
  if (['banco','bank','transfer','transferencia','deposito','deposit'].some(k => t.includes(k))) return 'banco';
  return 'efectivo';
}

function fallbackParse(msg) {
  const t = msg.trim().toLowerCase().replace(/^\//, '');
  const CMDS = {
    resumen:'resumen', balance:'resumen', summary:'resumen', hoy:'resumen',
    alertas:'alertas', alerts:'alertas',
    ayuda:'ayuda', help:'ayuda', start:'ayuda',
    'ver cuentas':'ver_cuentas', cuentas:'ver_cuentas', accounts:'ver_cuentas',
    presupuesto:'presupuesto', budget:'presupuesto',
    historial:'historial', history:'historial',
    miid:'miid',
    // Accounting
    plan:'plan', ver_plan:'plan',
    clientes:'clientes',
    ccobrar:'ccobrar', cxc:'ccobrar',
    cpagar:'cpagar', cxp:'cpagar',
    agregarcliente:'agregarcliente', add_client:'agregarcliente',
    agregarproveedor:'agregarproveedor', add_vendor:'agregarproveedor',
    setpassword:'setpassword', linkaccount:'linkaccount',
    nuevacobranza:'nuevacobranza',
    registrarpago:'registrarpago',
    nuevacuenta:'nuevacuenta',
    // Confirm/cancel
    si:'confirmar', sí:'confirmar', yes:'confirmar', confirm:'confirmar',
    no:'cancelar', cancel:'cancelar',
  };
  if (CMDS[t]) return { type: 'comando', cmd: CMDS[t] };

  // presupuesto comida 5000
  const bm = t.match(/(?:presupuesto|budget)\s+(\w+)\s+(\d+(?:[.,]\d+)?)/);
  if (bm) return { type:'comando', cmd:'set_budget', budget_cat:bm[1], budget_amount:parseFloat(bm[2].replace(',','.')) };

  // si banco / yes card
  const confirmAcc = t.match(/^(?:si|sí|yes|confirm)\s+(banco|bank|tarjeta|card|efectivo|cash)$/);
  if (confirmAcc) return { type:'comando', cmd:'confirmar', account: detectAcc(confirmAcc[1]) };

  const am = t.match(/(\d+(?:[.,]\d+)?)/);
  if (!am) return null;
  const amount = parseFloat(am[1].replace(',', '.'));
  if (!amount || amount <= 0) return null;

  const hasInc = INC_VERBS.some(v => t.includes(v));
  const hasExp = EXP_VERBS.some(v => t.includes(v));
  let type;
  if (hasInc && !hasExp) type = 'ingreso';
  else if (hasExp) type = 'egreso';
  else {
    const np = t.match(/\d+(?:[.,]\d+)?\s+(?:en|de|para|for|on)\s+(.+)/i);
    if (np) return { type:'egreso', amount, desc:np[1].trim(), cat:detectCat(np[1]+' '+t), account:detectAcc(t) };
    return null;
  }

  let desc = t
    .replace(/\d+(?:[.,]\d+)?/g, '')
    .replace(/\b(el|la|los|las|un|una|de|del|con|al|en|por|para|a|mi|the|a|an|for|on|at|in|with|from)\b/gi, ' ')
    .replace(/\s+/g, ' ').trim();
  if (!desc || desc.length < 2) {
    if (t.includes('quincena')) desc = 'Quincena';
    else if (t.includes('sueldo') || t.includes('salary')) desc = 'Salary';
    else desc = type === 'ingreso' ? 'Income' : 'Expense';
  }
  desc = desc.charAt(0).toUpperCase() + desc.slice(1);
  return { type, amount, desc, cat: detectCat(t), account: detectAcc(t) };
}

// ─── MESSAGE HANDLER ──────────────────────────────────────────────────────────
async function handleText(msgText, chatId) {
  const id  = String(chatId);
  const msg = msgText.trim();
  const now = new Date();

  // Obtener o crear usuario
  let user = await getUser(id);
  if (!user) {
    const lang = detectLang(msg);
    await ensureUser(id, lang);
    user = { id, lang };
    await sendMessage(chatId, MSG.welcome(id, lang));
    return;
  }

  const lang = user.lang || 'es';

  // /miid o miid
  if (/^\/miid$|^miid$/i.test(msg)) {
    await sendMessage(chatId, MSG.miid(id, lang));
    return;
  }

  const parsed = await parseWithAI(msg) || fallbackParse(msg);

  // ── CONFIRMAR (foto pendiente) ──
  if (parsed?.cmd === 'confirmar') {
    const pending = await getPending(id);
    if (!pending) { await sendMessage(chatId, MSG.noPending(lang)); return; }
    // Cambiar cuenta si se especificó
    if (parsed.account && parsed.account !== 'efectivo') pending.account = parsed.account;
    const tx = { ...pending, userId: id };
    await insertTx(tx);
    await clearPending(id);
    await sendMessage(chatId, MSG.recorded(tx, lang));
    return;
  }

  // ── CANCELAR ──
  if (parsed?.cmd === 'cancelar') {
    const pending = await getPending(id);
    if (pending) { await clearPending(id); await sendMessage(chatId, MSG.cancelled(lang)); }
    else { await sendMessage(chatId, MSG.noPending(lang)); }
    return;
  }

  // ── ACCOUNTING MULTI-STEP: handle pending state responses ──
  const pending = await getPending(id);
  if (pending && pending.step) {
    // await_client_name → create client with just name
    if (pending.step === 'await_client_name') {
      const name = msg.trim();
      if (!name) { await sendMessage(chatId, lang === 'es' ? '❌ Envía un nombre válido.' : '❌ Send a valid name.'); return; }
      const clientId = `cli_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
      try {
        await query(`INSERT INTO clients(id, user_id, name) VALUES($1,$2,$3)`, [clientId, id, name]);
        await clearPending(id);
        await sendMessage(chatId, lang === 'es'
          ? `✅ Cliente creado:\n\n👤 *${name}*\n   ID: \`${clientId}\``
          : `✅ Client created:\n\n👤 *${name}*\n   ID: \`${clientId}\``);
      } catch(e) { await sendMessage(chatId, MSG.generalError(lang)); }
      return;
    }
    // await_vendor_name → create vendor with just name
    if (pending.step === 'await_vendor_name') {
      const name = msg.trim();
      if (!name) { await sendMessage(chatId, lang === 'es' ? '❌ Envía un nombre válido.' : '❌ Send a valid name.'); return; }
      const vendorId = `ven_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
      try {
        await query(`INSERT INTO vendors(id, user_id, name) VALUES($1,$2,$3)`, [vendorId, id, name]);
        await clearPending(id);
        await sendMessage(chatId, lang === 'es'
          ? `✅ Proveedor creado:\n\n🏪 *${name}*\n   ID: \`${vendorId}\``
          : `✅ Vendor created:\n\n🏪 *${name}*\n   ID: \`${vendorId}\``);
      } catch(e) { await sendMessage(chatId, MSG.generalError(lang)); }
      return;
    }
    // await_setpassword_username → ask for password
    if (pending.step === 'await_setpassword_username') {
      const username = msg.trim().toLowerCase();
      if (!/^[a-z0-9_]{3,30}$/.test(username)) {
        await sendMessage(chatId, lang === 'es'
          ? '❌ Usuario inválido. Solo letras, números y guiones bajos. 3-30 caracteres.'
          : '❌ Invalid username. Only letters, numbers and underscores. 3-30 characters.');
        return;
      }
      // Check if username taken by another user
      const existing = await query('SELECT user_id FROM user_credentials WHERE username=$1', [username]);
      if (existing.rows[0] && existing.rows[0].user_id !== id) {
        await sendMessage(chatId, lang === 'es'
          ? '❌ Ese nombre de usuario ya está tomado. Elige otro.'
          : '❌ That username is already taken. Choose another.');
        return;
      }
      await setPending(id, { step: 'await_setpassword_password', username, lang });
      await sendMessage(chatId, lang === 'es'
        ? `✅ Usuario: *${username}*\n\nAhora envúa tu contraseña (mínimo 6 caracteres):`
        : `✅ Username: *${username}*\n\nNow send your password (min 6 characters):`);
      return;
    }
    // await_setpassword_password → ask for confirmation
    if (pending.step === 'await_setpassword_password') {
      const password = msg.trim();
      if (password.length < 6) {
        await sendMessage(chatId, lang === 'es'
          ? '❌ La contraseña debe tener al menos 6 caracteres. Intenta de nuevo:'
          : '❌ Password must be at least 6 characters. Try again:');
        return;
      }
      const username = pending.username;
      const hash = crypto.pbkdf2Sync(password, username, 100000, 64, 'sha512').toString('hex');
      try {
        await query(
          `INSERT INTO user_credentials(user_id, username, password_hash) VALUES($1,$2,$3)
           ON CONFLICT (user_id) DO UPDATE SET username=$2, password_hash=$3`,
          [id, username, hash]
        );
        await clearPending(id);
        await sendMessage(chatId, lang === 'es'
          ? `✅ *¡Cuenta vinculada!*\n\n👤 Usuario: *${username}*\n\nYa puedes entrar a la web con estas credenciales.`
          : `✅ *Account linked!*\n\n👤 Username: *${username}*\n\nYou can now log in to the web with these credentials.`);
      } catch(e) {
        await sendMessage(chatId, MSG.generalError(lang));
      }
      return;
    }
    // await_link_username → ask for password
    if (pending.step === 'await_link_username') {
      const username = msg.trim().toLowerCase();
      if (!/^[a-z0-9_]{3,30}$/.test(username)) {
        await sendMessage(chatId, lang === 'es'
          ? '❌ Usuario inválido. Solo letras, números y guiones bajos. 3-30 caracteres.'
          : '❌ Invalid username. Only letters, numbers and underscores. 3-30 characters.');
        return;
      }
      await setPending(id, { step: 'await_link_password', username, lang });
      await sendMessage(chatId, lang === 'es'
        ? `✅ Usuario: *${username}*\n\nAhora envúa tu contraseña:`
        : `✅ Username: *${username}*\n\nNow send your password:`);
      return;
    }
    // await_link_password → verify and link
    if (pending.step === 'await_link_password') {
      const password = msg.trim();
      const username = pending.username;
      const hash = crypto.pbkdf2Sync(password, username, 100000, 64, 'sha512').toString('hex');
      // Verify credentials
      const cred = await query(
        'SELECT user_id, password_hash FROM user_credentials WHERE username=$1',
        [username]
      );
      if (!cred.rows[0]) {
        await sendMessage(chatId, lang === 'es'
          ? '❌ Usuario no encontrado. Verifica tu usuario e intenta de nuevo:'
          : '❌ Username not found. Verify your username and try again:');
        await setPending(id, { step: 'await_link_username', lang });
        return;
      }
      if (cred.rows[0].password_hash !== hash) {
        await sendMessage(chatId, lang === 'es'
          ? '❌ Contraseña incorrecta. Intenta de nuevo:'
          : '❌ Wrong password. Try again:');
        await setPending(id, { step: 'await_link_password', username, lang });
        return;
      }
      // User logged in via web wants to also use Telegram
      // Update their user_credentials to also have this Telegram chat_id
      const webUserId = cred.rows[0].user_id;
      // If web user_id !== this Telegram chat_id, we need to migrate data
      // For now: just add this Telegram as an additional user_id (not possible with FK)
      // Solution: update the web account to use this Telegram chat_id as the canonical user_id
      // First check if this Telegram already has data
      const tgTxs = await query('SELECT COUNT(*) FROM transactions WHERE user_id=$1', [id]);
      const tgHasData = parseInt(tgTxs.rows[0].count) > 0;
      if (webUserId !== id) {
        if (tgHasData) {
          // Both have data — ask user which to keep
          await clearPending(id);
          await sendMessage(chatId, lang === 'es'
            ? `⚠️ *Conflicto de cuentas*\n\nTienes datos tanto en Telegram como en la web con ese usuario.\n\nPara unir ambas cuentas manualmente, contacta al desarrollador o crea un nuevo usuario web.`
            : `⚠️ *Account conflict*\n\nYou have data in both Telegram and web with that username.\n\nTo merge both accounts manually, contact the developer or create a new web user.`
          );
          return;
        }
        // Telegram has no data — migrate: update all Telegram records to use web userId
        await query('UPDATE transactions SET user_id=$1 WHERE user_id=$2', [webUserId, id]);
        await query('UPDATE budgets SET user_id=$1 WHERE user_id=$2', [webUserId, id]);
        await query('UPDATE clients SET user_id=$1 WHERE user_id=$2', [webUserId, id]);
        await query('UPDATE receivables SET user_id=$1 WHERE user_id=$2', [webUserId, id]);
        await query('UPDATE vendors SET user_id=$1 WHERE user_id=$2', [webUserId, id]);
        await query('UPDATE payables SET user_id=$1 WHERE user_id=$2', [webUserId, id]);
        await query('UPDATE accounts SET user_id=$1 WHERE user_id=$2', [webUserId, id]);
        await query('UPDATE users SET id=$1 WHERE id=$2', [webUserId, id]);
        await query('UPDATE user_credentials SET user_id=$1 WHERE user_id=$2', [webUserId, id]);
        await clearPending(id);
        await sendMessage(chatId, lang === 'es'
          ? `✅ *¡Cuentas unidas!*\n\nTus datos de la web ahora están en este Telegram. 👇\n\nPara entrar a la web, usa:\n👤 Usuario: *${username}*\n🔑 Contraseña: *(la que usaste)*`
          : `✅ *Accounts merged!*\n\nYour web data is now available in this Telegram. 👇\n\nTo log in to the web:\n👤 Username: *${username}*\n🔑 Password: *(the one you used)*`
        );
        return;
      }
      // userId is already the same (user created web account with same id as Telegram)
      await clearPending(id);
      await sendMessage(chatId, lang === 'es'
        ? `✅ *¡Ya estás vinculado!*\n\nEste Telegram ya está conectado a la cuenta web.`
        : `✅ *Already linked!*\n\nThis Telegram is already connected to the web account.`
      );
      return;
    }
    // Unknown pending step — clear and fall through
    await clearPending(id);
  }

  if (!parsed) { await sendMessage(chatId, MSG.notUnderstood(lang)); return; }

  const { cmd } = parsed;
  const month = now.getMonth();
  const year  = now.getFullYear();

  // ── MIID ──
  if (cmd === 'miid') { await sendMessage(chatId, MSG.miid(id, lang)); return; }

  // ── RESUMEN ──
  if (cmd === 'resumen') {
    const txs = await getMonthTxs(id, month, year);
    const inc = txs.filter(t => t.type === 'ingreso').reduce((s, t) => s + parseFloat(t.amount), 0);
    const exp = txs.filter(t => t.type === 'egreso').reduce((s, t)  => s + parseFloat(t.amount), 0);
    const bal = inc - exp;
    const MN  = lang === 'es' ? MONTHS_ES : MONTHS_EN;
    const text = lang === 'es'
      ? `💰 *Resumen — ${MN[month]} ${year}*\n\n▲ Ingresos: *${fmt(inc)}*\n▼ Egresos: *${fmt(exp)}*\n\n${bal >= 0 ? '✅' : '🚨'} Balance: *${fmt(bal)}*\n\n_${txs.length} movimiento(s)_`
      : `💰 *Summary — ${MN[month]} ${year}*\n\n▲ Income: *${fmt(inc)}*\n▼ Expenses: *${fmt(exp)}*\n\n${bal >= 0 ? '✅' : '🚨'} Balance: *${fmt(bal)}*\n\n_${txs.length} transaction(s)_`;
    await sendMessage(chatId, text);
    return;
  }

  // ── CUENTAS ──
  if (cmd === 'ver_cuentas') {
    const txs  = await getMonthTxs(id, month, year);
    const MN   = lang === 'es' ? MONTHS_ES : MONTHS_EN;
    const lines = ['efectivo','banco','tarjeta'].map(acc => {
      const inc = txs.filter(t => t.type==='ingreso' && t.account===acc).reduce((s,t) => s+parseFloat(t.amount), 0);
      const exp = txs.filter(t => t.type==='egreso'  && t.account===acc).reduce((s,t) => s+parseFloat(t.amount), 0);
      return `${ACC_EMOJI[acc]} *${acc}*\n   ▲ ${fmt(inc)}  ▼ ${fmt(exp)}\n   Balance: ${fmt(inc-exp)}`;
    });
    await sendMessage(chatId, lang === 'es'
      ? `🏦 *Cuentas — ${MN[month]}*\n\n${lines.join('\n\n')}`
      : `🏦 *Accounts — ${MN[month]}*\n\n${lines.join('\n\n')}`);
    return;
  }

  // ── ALERTAS ──
  if (cmd === 'alertas') {
    const txs     = await getMonthTxs(id, month, year);
    const budgets = await getBudgets(id);
    const inc = txs.filter(t => t.type==='ingreso').reduce((s,t) => s+parseFloat(t.amount), 0);
    const exp = txs.filter(t => t.type==='egreso').reduce((s,t)  => s+parseFloat(t.amount), 0);
    const alerts = [];
    if (inc > 0) {
      const pct = (exp / inc) * 100;
      if (pct >= 100) alerts.push(`🚨 Egresos superaron ingresos (${pct.toFixed(0)}%)`);
      else if (pct >= 80) alerts.push(`⚠️ Gastaste el ${pct.toFixed(0)}% de tus ingresos`);
      else alerts.push(`✅ Finanzas saludables (${pct.toFixed(0)}% gastado)`);
    }
    for (const [cat, limit] of Object.entries(budgets)) {
      const spent = txs.filter(t => t.type==='egreso' && t.category===cat).reduce((s,t) => s+parseFloat(t.amount), 0);
      const pct   = (spent / limit) * 100;
      const e     = CAT_EMOJI[cat] || '📦';
      if (pct >= 100) alerts.push(`🚨 ${e} ${cat}: SUPERADO (${fmt(spent)})`);
      else if (pct >= 80) alerts.push(`⚠️ ${e} ${cat}: ${pct.toFixed(0)}% usado`);
    }
    const MN = lang === 'es' ? MONTHS_ES : MONTHS_EN;
    await sendMessage(chatId, lang === 'es'
      ? `🔔 *Alertas — ${MN[month]}*\n\n${alerts.join('\n') || 'Sin alertas ✅'}`
      : `🔔 *Alerts — ${MN[month]}*\n\n${alerts.join('\n') || 'No alerts ✅'}`);
    return;
  }

  // ── HISTORIAL ──
  if (cmd === 'historial') {
    const txs = await getMonthTxs(id, month, year);
    const MN  = lang === 'es' ? MONTHS_ES : MONTHS_EN;
    const last5 = [...txs].reverse().slice(0, 5);
    if (!last5.length) {
      await sendMessage(chatId, lang === 'es' ? `📭 Sin movimientos en ${MN[month]}` : `📭 No transactions in ${MN[month]}`);
      return;
    }
    const lines = last5.map(t =>
      `${t.type==='ingreso'?'▲':'▼'} ${CAT_EMOJI[t.category]||'📦'} ${t.description} — ${fmt(t.amount)}`
    );
    await sendMessage(chatId, lang === 'es'
      ? `📋 *Recientes — ${MN[month]}*\n\n${lines.join('\n')}`
      : `📋 *Recent — ${MN[month]}*\n\n${lines.join('\n')}`);
    return;
  }

  // ── PRESUPUESTO ──
  if (cmd === 'presupuesto') {
    const budgets = await getBudgets(id);
    const txs     = await getMonthTxs(id, month, year);
    const MN      = lang === 'es' ? MONTHS_ES : MONTHS_EN;
    if (!Object.keys(budgets).length) {
      await sendMessage(chatId, lang === 'es'
        ? `📊 *Sin presupuestos.*\n\nCrea uno:\n• presupuesto comida 5000`
        : `📊 *No budgets set.*\n\nCreate one:\n• budget food 500`);
      return;
    }
    const lines = Object.entries(budgets).map(([cat, limit]) => {
      const spent = txs.filter(t => t.type==='egreso' && t.category===cat).reduce((s,t) => s+parseFloat(t.amount), 0);
      const pct   = Math.min(100, (spent / limit) * 100);
      const bar   = '█'.repeat(Math.floor(pct / 10)) + '░'.repeat(10 - Math.floor(pct / 10));
      return `${CAT_EMOJI[cat]||'📦'} ${cat}\n   ${bar} ${pct.toFixed(0)}%\n   ${fmt(spent)} / ${fmt(limit)}`;
    });
    await sendMessage(chatId, lang === 'es'
      ? `📊 *Presupuestos — ${MN[month]}*\n\n${lines.join('\n\n')}`
      : `📊 *Budgets — ${MN[month]}*\n\n${lines.join('\n\n')}`);
    return;
  }

  // ── SET_BUDGET ──
  if (cmd === 'set_budget') {
    if (!parsed.budget_cat || !parsed.budget_amount || parsed.budget_amount <= 0) {
      await sendMessage(chatId, lang === 'es' ? '❌ Ejemplo: presupuesto comida 5000' : '❌ Example: budget food 500');
      return;
    }
    await setBudget(id, parsed.budget_cat, parsed.budget_amount);
    await sendMessage(chatId, lang === 'es'
      ? `✅ Presupuesto:\n\n${CAT_EMOJI[parsed.budget_cat]||'📦'} *${parsed.budget_cat}*: ${fmt(parsed.budget_amount)}/mes`
      : `✅ Budget set:\n\n${CAT_EMOJI[parsed.budget_cat]||'📦'} *${parsed.budget_cat}*: ${fmt(parsed.budget_amount)}/month`);
    return;
  }

  // ── AYUDA ──
  if (cmd === 'ayuda') { await sendMessage(chatId, MSG.help(lang)); return; }

  // ── ACCOUNTING COMMANDS ──

  // /plan or /cuentas — chart of accounts with balances
  if (cmd === 'plan' || cmd === 'ver_plan') {
    const r = await query(
      `SELECT a.code, a.name, a.type, a.class, COALESCE(ab.balance,0) as balance
       FROM accounts a LEFT JOIN account_balances ab ON ab.account_id=a.id
       WHERE a.user_id=$1 AND a.is_active=TRUE ORDER BY a.class, a.code`,
      [id]
    );
    if (!r.rows.length) { await sendMessage(chatId, lang === 'es' ? '📋 No hay cuentas registradas.' : '📋 No accounts found.'); return; }
    const lines = r.rows.map(row => {
      const typeLabel = { asset:'🏦', liability:'📜', equity:'🏛️', income:'💵', cost:'📉', expense:'📤' }[row.type] || '📦';
      return `${typeLabel} \`${row.code}\` ${row.name}\n   Balance: ${fmt(row.balance)}`;
    });
    const chunkSize = 3000;
    const chunks = [];
    for (let i = 0; i < lines.length; i += chunkSize) {
      chunks.push(lines.slice(i, i + chunkSize).join('\n'));
    }
    const header = lang === 'es' ? '📋 *Plan de Cuentas*\n\n' : '📋 *Chart of Accounts*\n\n';
    for (const chunk of chunks) {
      await sendMessage(chatId, header + chunk);
    }
    return;
  }

  // /clientes — list clients with outstanding receivables
  if (cmd === 'clientes') {
    const r = await query(
      `SELECT c.id, c.name, c.phone, c.email,
              COALESCE(SUM(rec.total_amount - rec.paid_amount), 0) as outstanding
       FROM clients c
       LEFT JOIN receivables rec ON rec.client_id=c.id AND rec.status IN ('pending','partial')
       WHERE c.user_id=$1
       GROUP BY c.id
       HAVING COALESCE(SUM(rec.total_amount - rec.paid_amount),0) > 0
       ORDER BY outstanding DESC`,
      [id]
    );
    if (!r.rows.length) {
      await sendMessage(chatId, lang === 'es' ? '📋 No hay clientes con deudas pendientes.' : '📋 No clients with outstanding balances.');
      return;
    }
    const lines = r.rows.map(row =>
      `👤 *${row.name}*\n   ID: \`${row.id}\`\n   Debe: *${fmt(row.outstanding)}*\n   📞 ${row.phone || 'N/A'}`
    );
    const header = lang === 'es' ? '👥 *Clientes — Cuentas por Cobrar*\n\n' : '👥 *Clients — Accounts Receivable*\n\n';
    await sendMessage(chatId, header + lines.join('\n\n'));
    return;
  }

  // /ccobrar or /cxc — accounts receivable summary
  if (cmd === 'ccobrar' || cmd === 'cxc') {
    const r = await query(
      `SELECT c.name, rec.description, rec.total_amount, rec.paid_amount,
              rec.total_amount - rec.paid_amount as outstanding, rec.due_date, rec.status
       FROM receivables rec
       JOIN clients c ON c.id=rec.client_id
       WHERE rec.user_id=$1 AND rec.status IN ('pending','partial')
       ORDER BY rec.due_date NULLS LAST`,
      [id]
    );
    if (!r.rows.length) {
      await sendMessage(chatId, lang === 'es' ? '✅ No hay cuentas por cobrar.' : '✅ No accounts receivable.');
      return;
    }
    const total = r.rows.reduce((s, row) => s + parseFloat(row.outstanding), 0);
    const lines = r.rows.slice(0, 10).map(row =>
      `👤 ${row.name}\n   ${row.description}\n   💰 ${fmt(row.outstanding)} pendiente`
    );
    const msg = (lang === 'es' ? '📋 *Cuentas por Cobrar*\n\n' : '📋 *Accounts Receivable*\n\n')
      + lines.join('\n\n')
      + (r.rows.length > 10 ? `\n\n...y ${r.rows.length - 10} más` : '')
      + `\n\n💰 *Total: ${fmt(total)}*`;
    await sendMessage(chatId, msg);
    return;
  }

  // /cpagar or /cxp — accounts payable summary
  if (cmd === 'cpagar' || cmd === 'cxp') {
    const r = await query(
      `SELECT v.name, v.vendor_type, p.description, p.total_amount, p.paid_amount,
              p.total_amount - p.paid_amount as outstanding, p.due_date, p.status
       FROM payables p
       JOIN vendors v ON v.id=p.vendor_id
       WHERE p.user_id=$1 AND p.status IN ('pending','partial')
       ORDER BY p.due_date NULLS LAST`,
      [id]
    );
    if (!r.rows.length) {
      await sendMessage(chatId, lang === 'es' ? '✅ No hay cuentas por pagar.' : '✅ No accounts payable.');
      return;
    }
    const total = r.rows.reduce((s, row) => s + parseFloat(row.outstanding), 0);
    const lines = r.rows.slice(0, 10).map(row =>
      `🏪 ${row.name}\n   ${row.description}\n   💰 ${fmt(row.outstanding)} pendiente`
    );
    const msg = (lang === 'es' ? '📋 *Cuentas por Pagar*\n\n' : '📋 *Accounts Payable*\n\n')
      + lines.join('\n\n')
      + (r.rows.length > 10 ? `\n\n...y ${r.rows.length - 10} más` : '')
      + `\n\n💰 *Total: ${fmt(total)}*`;
    await sendMessage(chatId, msg);
    return;
  }

  // Multi-step: /agregarcliente — start multi-step client creation
  if (cmd === 'agregarcliente' || cmd === 'add_client') {
    await setPending(id, { step: 'await_client_name', lang });
    await sendMessage(chatId, lang === 'es'
      ? `👤 *Agregar Cliente*\n\nEnvía el nombre del cliente:`
      : `👤 *Add Client*\n\nSend the client name:`);
    return;
  }

  // Multi-step: /agregarproveedor — start multi-step vendor creation
  if (cmd === 'agregarproveedor' || cmd === 'add_vendor') {
    await setPending(id, { step: 'await_vendor_name', lang });
    await sendMessage(chatId, lang === 'es'
      ? `🏪 *Agregar Proveedor*\n\nEnvía el nombre del proveedor:`
      : `🏪 *Add Vendor*\n\nSend the vendor name:`);
    return;
  }

  // ── SETPASSWORD ──
  if (cmd === 'setpassword') {
    // Check if already has credentials
    const existingCred = await query('SELECT username FROM user_credentials WHERE user_id=$1', [id]);
    if (existingCred.rows[0]) {
      await sendMessage(chatId, lang === 'es'
        ? `🔒 Ya tienes credenciales:\n\n👤 *${existingCred.rows[0].username}*\n\nSi quieres cambiar la contraseña, primero elimina tu cuenta con /borrarcreds y vuelve a registrarte.`
        : `🔒 You already have credentials:\n\n👤 *${existingCred.rows[0].username}*\n\nTo change password, delete your credentials with /deletecreds and register again.`);
      return;
    }
    await setPending(id, { step: 'await_setpassword_username', lang });
    await sendMessage(chatId, lang === 'es'
      ? `🔐 *Crear contraseña para la web*\n\nEnvía el nombre de usuario que quieres usar (solo letras, números y _, 3-30 caracteres):`
      : `🔐 *Create password for the web*\n\nSend the username you want to use (letters, numbers and _, 3-30 characters):`);
    return;
  }

  // ── LINKACCOUNT ── Vincula cuenta web existente a este Telegram
  if (cmd === 'linkaccount') {
    // Check if already linked
    const existingCred = await query('SELECT username FROM user_credentials WHERE user_id=$1', [id]);
    if (existingCred.rows[0]) {
      await sendMessage(chatId, lang === 'es'
        ? `🔒 Ya tienes credenciales vinculadas:\n\n👤 *${existingCred.rows[0].username}*\n\nEste Telegram ya está conectado a la web.`
        : `🔒 You already have linked credentials:\n\n👤 *${existingCred.rows[0].username}*\n\nThis Telegram is already connected to the web.`);
      return;
    }
    await setPending(id, { step: 'await_link_username', lang });
    await sendMessage(chatId, lang === 'es'
      ? `🔗 *Vincular cuenta web*\n\nSi ya tienes cuenta en la web (creada desde el navegador), envía tu *usuario*:`
      : `🔗 *Link web account*\n\nIf you already have a web account (created from the browser), send your *username*:`
    );
    return;
  }

  // /nuevacobranza [client_id] [monto] [descripción]
  if (cmd === 'nuevacobranza') {
    const r = await query(`SELECT name FROM clients WHERE id=$1 AND user_id=$2`, [parsed?.client_id, id]);
    if (!r.rows[0]) { await sendMessage(chatId, lang === 'es' ? '❌ Cliente no encontrado.' : '❌ Client not found.'); return; }
    const desc = parsed?.description || (lang === 'es' ? 'Cobranza' : 'Charge');
    const amt  = parsed?.amount || parsed?.total_amount;
    if (!amt || amt <= 0) { await sendMessage(chatId, lang === 'es' ? '❌ Monto inválido.' : '❌ Invalid amount.'); return; }
    const recId = `rec_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
    try {
      await query(
        `INSERT INTO receivables(id, user_id, client_id, description, total_amount)
         VALUES($1,$2,$3,$4,$5)`,
        [recId, id, parsed.client_id, desc, amt]
      );
      await sendMessage(chatId, lang === 'es'
        ? `✅ *Cuentas por Cobrar creada*\n\n👤 Cliente: ${r.rows[0].name}\n📝 ${desc}\n💰 ${fmt(amt)}\n   ID: \`${recId}\``
        : `✅ *Receivable created*\n\n👤 Client: ${r.rows[0].name}\n📝 ${desc}\n💰 ${fmt(amt)}\n   ID: \`${recId}\``);
    } catch(e) { await sendMessage(chatId, MSG.generalError(lang)); }
    return;
  }

  // /registrarpago [receivable_id] [monto]
  if (cmd === 'registrarpago') {
    if (!parsed?.receivable_id || !parsed?.amount) {
      await sendMessage(chatId, lang === 'es'
        ? `❌ Formato: /registrarpago [receivable_id] [monto]`
        : `❌ Format: /registrarpago [receivable_id] [amount]`);
      return;
    }
    const rec = await query(
      `SELECT r.*, c.name as client_name FROM receivables r JOIN clients c ON c.id=r.client_id
       WHERE r.id=$1 AND r.user_id=$2`, [parsed.receivable_id, id]
    );
    if (!rec.rows[0]) { await sendMessage(chatId, lang === 'es' ? '❌ Cuenta por cobrar no encontrada.' : '❌ Receivable not found.'); return; }
    const payId = `rpay_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
    try {
      await query(
        `INSERT INTO receivable_payments(id, receivable_id, amount) VALUES($1,$2,$3)`,
        [payId, parsed.receivable_id, parsed.amount]
      );
      await query(
        `UPDATE receivables SET paid_amount = paid_amount + $1,
         status = CASE WHEN paid_amount + $1 >= total_amount THEN 'paid' WHEN paid_amount + $1 > 0 THEN 'partial' ELSE status END
         WHERE id=$2`,
        [parsed.amount, parsed.receivable_id]
      );
      await sendMessage(chatId, lang === 'es'
        ? `✅ *Pago registrado*\n\n👤 Cliente: ${rec.rows[0].client_name}\n💰 ${fmt(parsed.amount)}\n   Quedan: ${fmt(parseFloat(rec.rows[0].total_amount) - parseFloat(rec.rows[0].paid_amount) - parsed.amount)}`
        : `✅ *Payment registered*\n\n👤 Client: ${rec.rows[0].client_name}\n💰 ${fmt(parsed.amount)}\n   Remaining: ${fmt(parseFloat(rec.rows[0].total_amount) - parseFloat(rec.rows[0].paid_amount) - parsed.amount)}`);
    } catch(e) { await sendMessage(chatId, MSG.generalError(lang)); }
    return;
  }

  // /nuevacuenta [code] [name] [tipo] — create new account
  if (cmd === 'nuevacuenta') {
    const code = parsed?.code;
    const name = parsed?.name;
    const type = parsed?.type;
    if (!code || !name) {
      await sendMessage(chatId, lang === 'es'
        ? `❌ Formato: /nuevacuenta [código] [nombre] [tipo]\nTipos: asset, liability, equity, income, cost, expense`
        : `❌ Format: /nuevacuenta [code] [name] [type]\nTypes: asset, liability, equity, income, cost, expense`);
      return;
    }
    const accClass = parseInt(code.charAt(0));
    if (!accClass || accClass < 1 || accClass > 6) {
      await sendMessage(chatId, lang === 'es' ? '❌ Código debe empezar con clase 1-6.' : '❌ Code must start with class 1-6.');
      return;
    }
    const accId = `acc_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
    try {
      await query(
        `INSERT INTO accounts(id, user_id, code, name, type, class) VALUES($1,$2,$3,$4,$5,$6)`,
        [accId, id, code, name, type || 'asset', accClass]
      );
      await query(`INSERT INTO account_balances(account_id, balance) VALUES($1,0) ON CONFLICT DO NOTHING`, [accId]);
      await sendMessage(chatId, lang === 'es'
        ? `✅ *Cuenta creada*\n\n\`${code}\` ${name}\nTipo: ${type || 'asset'}\n   ID: \`${accId}\``
        : `✅ *Account created*\n\n\`${code}\` ${name}\nType: ${type || 'asset'}\n   ID: \`${accId}\``);
    } catch(e) { await sendMessage(chatId, e.message.includes('unique') ? (lang === 'es' ? '❌ Ya existe una cuenta con ese código.' : '❌ An account with that code already exists.') : MSG.generalError(lang)); }
    return;
  }

  // ── TRANSACCIÓN ──
  if (parsed.type === 'ingreso' || parsed.type === 'egreso') {
    const tx = {
      id         : uid(),
      userId     : id,
      type       : parsed.type,
      amount     : parsed.amount,
      description: parsed.desc || (parsed.type === 'ingreso' ? 'Income' : 'Expense'),
      category   : parsed.cat  || 'otro',
      account    : parsed.account || 'efectivo',
      date       : now.toISOString().split('T')[0],
    };
    await insertTx(tx);
    // Update lang detection
    const detectedLang = detectLang(msg);
    if (detectedLang !== lang) await setUserLang(id, detectedLang);
    await sendMessage(chatId, MSG.recorded(tx, lang));
    return;
  }

  await sendMessage(chatId, MSG.notUnderstood(lang));
}

async function handlePhoto(msg, chatId) {
  const id   = String(chatId);
  const user = await getUser(id);
  const lang = user?.lang || 'es';

  if (!GROQ_API_KEY) { await sendMessage(chatId, MSG.noGroq(lang)); return; }

  try {
    const photo = msg.photo?.[msg.photo.length - 1];
    if (!photo) { await sendMessage(chatId, MSG.photoError(lang)); return; }

    await sendMessage(chatId, MSG.analyzing(lang));

    const link = await getFileLink(photo.file_id);
    const res  = await fetch(link, { signal: AbortSignal.timeout(15000) });
    const buf  = await res.arrayBuffer();
    const b64  = Buffer.from(buf).toString('base64');
    const mime = link.endsWith('.png') ? 'image/png' : 'image/jpeg';

    const result = await analyzeReceipt(b64, mime);
    if (!result?.success) { await sendMessage(chatId, MSG.photoError(lang)); return; }

    const now = new Date();
    const tx  = {
      id         : uid(),
      type       : 'egreso',
      amount     : result.amount,
      description: result.description || 'Receipt',
      category   : result.category    || 'otro',
      account    : 'efectivo',
      date       : now.toISOString().split('T')[0],
    };
    await ensureUser(id, lang);
    await setPending(id, tx);
    await sendMessage(chatId, MSG.receiptPreview(tx, lang));
  } catch (e) {
    console.error('handlePhoto:', e.message);
    await sendMessage(chatId, MSG.photoError(lang));
  }
}

// ─── RESUMEN SEMANAL ──────────────────────────────────────────────────────────
async function sendWeeklySummaries() {
  const now   = new Date();
  const month = now.getMonth();
  const year  = now.getFullYear();

  const usersRes = await query('SELECT id, lang FROM users');
  let sent = 0;

  for (const user of usersRes.rows) {
    try {
      const txs = await getMonthTxs(user.id, month, year);
      if (!txs.length) continue;

      const inc = txs.filter(t => t.type==='ingreso').reduce((s,t) => s+parseFloat(t.amount), 0);
      const exp = txs.filter(t => t.type==='egreso').reduce((s,t)  => s+parseFloat(t.amount), 0);
      const bal = inc - exp;
      const MN  = user.lang === 'es' ? MONTHS_ES : MONTHS_EN;

      // Top 3 categorías de gasto
      const byCat = {};
      txs.filter(t => t.type==='egreso').forEach(t => {
        byCat[t.category] = (byCat[t.category] || 0) + parseFloat(t.amount);
      });
      const top3 = Object.entries(byCat)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 3)
        .map(([cat, amt]) => `  ${CAT_EMOJI[cat]||'📦'} ${cat}: ${fmt(amt)}`)
        .join('\n');

      const msg = user.lang === 'es'
        ? `📊 *Resumen Semanal — ${MN[month]} ${year}*\n\n▲ Ingresos: *${fmt(inc)}*\n▼ Egresos: *${fmt(exp)}*\n${bal>=0?'✅':'🚨'} Balance: *${fmt(bal)}*\n\n🏆 *Top gastos:*\n${top3||'  Sin gastos'}\n\n_${txs.length} movimiento(s) este mes_`
        : `📊 *Weekly Summary — ${MN[month]} ${year}*\n\n▲ Income: *${fmt(inc)}*\n▼ Expenses: *${fmt(exp)}*\n${bal>=0?'✅':'🚨'} Balance: *${fmt(bal)}*\n\n🏆 *Top expenses:*\n${top3||'  No expenses'}\n\n_${txs.length} transaction(s) this month_`;

      await sendMessage(user.id, msg);
      sent++;
      await new Promise(r => setTimeout(r, 50)); // rate-limit amigable
    } catch (e) {
      console.error(`Weekly summary error for ${user.id}:`, e.message);
    }
  }
  return sent;
}

// ─── CORS & ALLOWED_ORIGINS (siempre fuera del if para que esté disponible) ──
const ALLOWED_ORIGINS = [
  'https://stiwall.github.io',
  'https://stiwall.github.io/miscuentas-bot',
  'http://localhost:3000',
  'http://127.0.0.1:3000',
  'https://miscuentas-contable-app-production.up.railway.app',
  'https://miscuentas-contable-production-34aa.up.railway.app',
];

// ─── WEBHOOK HANDLER ──────────────────────────────────────────────────────────
if (HAS_TELEGRAM) {
  app.post(`/webhook/:secret`, async (req, res) => {
    // Validar secret para evitar llamadas no autorizadas
    if (req.params.secret !== (WEBHOOK_SECRET || 'tg')) {
      return res.sendStatus(403);
    }
    res.sendStatus(200); // Responder inmediatamente a Telegram

    const update = req.body;

    // Log para debug — ver qué llega de Telegram
    console.log('Webhook received:', JSON.stringify(update).substring(0, 300));

    const msg    = update?.message;
    if (!msg) {
      //可能是callback_query或其他类型的update
      console.log('No message in update, type:', update.update_id ? 'id:' + update.update_id : 'unknown');
      return;
    }

    // ── Handle deep link: t.me/Miscuentasrdbot/miscuentas?start=TOKEN ────────────
    // When user clicks START, Telegram sends a callback_query with data containing the start parameter
    const cq = update?.callback_query;
    if (cq) {
      const chatId = String(cq.from.id);
      const data   = cq.data || '';

      // data looks like "start=TG_TOKEN" or just "start" — extract the token
      let authToken = null;
      let isDeepLink = false;
      if (data.startsWith('start=')) {
        authToken = data.replace('start=', '').trim();
        isDeepLink = true;
      } else if (data.startsWith('tg_')) {
        authToken = data;
        isDeepLink = true;
      }

      // Answer the callback query to remove loading state in Telegram
      try {
        await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/answerCallbackQuery`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ callback_query_id: cq.id }),
        }).catch(() => {});
      } catch(e) { /* ignore */ }

      if (isDeepLink && authToken) {
        // Deep link with token — save to DB
        try {
          await ensureUser(chatId, 'es');
          await createAuthToken(authToken, chatId);
          const lang = await getUserLang(chatId);
          await sendMessage(chatId, lang === 'es'
            ? `✅ ¡Cuenta conectada! Tu ID es:\n\n${chatId}\n\n📋 Cópialo y pégalo en la web.`
            : `✅ Account connected! Your ID:\n\n${chatId}\n\n📋 Copy and paste on the web.`
          );
        } catch(e) {
          console.error('Telegram OAuth callback error:', e.message);
        }
      } else {
        // Plain START click — just send welcome message with ID
        try {
          await ensureUser(chatId, 'es');
          const lang = await getUserLang(chatId);
          await sendMessage(chatId, lang === 'es'
            ? `👋 ¡Bienvenido a MisCuentas!\n\nTu Telegram ID:\n\n${chatId}\n\n📋 Cópialo y pégalo en la web para iniciar sesión.\n\n💰 MisCuentas — Finanzas Personales 💰`
            : `👋 Welcome to MisCuentas!\n\nYour Telegram ID:\n\n${chatId}\n\n📋 Copy and paste on the web to log in.\n\n💰 MisCuentas — Personal Finance 💰`
          );
        } catch(e) {
          console.error('Telegram welcome error:', e.message);
        }
      }
      return;
    }

    const chatId = msg.chat.id;
    const text   = msg.text || '';

    // ── TELEGRAM OAUTH: /start tg_xxx or /start miscuentas?start=tg_xxx ────────
    // Handles both: t.me/Miscuentasrdbot?start=tg_xxx  AND  t.me/Miscuentasrdbot/miscuentas?start=tg_xxx
    // Also handles: /tg_xxx (token enviado como comando directo desde la web)
    let authToken = null;
    if (text.startsWith('/start tg_')) {
      authToken = text.replace('/start tg_', '').trim();
    } else if (text.startsWith('/start miscuentas?start=')) {
      authToken = text.replace('/start miscuentas?start=', '').trim();
    } else if (/^\/tg_[a-z0-9]+$/i.test(text)) {
      // Token enviado como /tg_xxx desde la web (el frontend pre-llena el mensaje)
      authToken = text.replace('/tg_', '').trim();
    } else if (/^\/start miscuentas$/.test(text)) {
      // Bot was opened from t.me/Miscuentasrdbot/miscuentas without a token — redirect to bot
      await sendMessage(chatId, '👋 Usa el botón de "Iniciar con Telegram" en la web para conectar tu cuenta.\n\nO envía /start nuevamente con un token válido.');
      return;
    }
    if (authToken) {
      try {
        // Generar session token para el usuario
        await ensureUser(String(chatId), 'es');

        // Guardar token en DB (persiste aunque Railway se reinicie)
        await createAuthToken(authToken, String(chatId));

        // Responder al usuario
        const lang = await getUserLang(String(chatId));
        await sendMessage(chatId, lang === 'es'
          ? '✅ ¡Cuenta conectada! Puedes volver a la web. Bienvenido a MisCuentas 💰'
          : '✅ Account connected! You can go back to the web. Welcome to MisCuentas 💰'
        );
      } catch(e) {
        console.error('Telegram OAuth error:', e.message);
      }
      return;
    }

    try {
      if (msg.photo?.length > 0) {
        await handlePhoto(msg, chatId);
      } else if (msg.text) {
        await handleText(msg.text, chatId);
      }
    } catch (e) {
      console.error('Webhook handler error:', e);
      try { await sendMessage(chatId, MSG.generalError('es')); } catch {}
    }
  });
} // end HAS_TELEGRAM

app.use((req, res, next) => {
  if (req.method === 'OPTIONS') {
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, x-session-token');
    res.setHeader('Access-Control-Max-Age', '86400');
    const origin = req.headers.origin;
    if (origin && ALLOWED_ORIGINS.includes(origin)) {
      res.setHeader('Access-Control-Allow-Origin', origin);
      res.setHeader('Vary', 'Origin');
    }
    return res.sendStatus(200);
  }
  const origin = req.headers.origin;
  if (origin && ALLOWED_ORIGINS.includes(origin)) {
    res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Vary', 'Origin');
  }
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, x-session-token');
  next();
});

// ─── TELEGRAM OAUTH ────────────────────────────────────────────────────────────

// Página que ve el usuario al abrir el deep link desde Telegram
app.get('/miscuentas', (req, res) => {
  const { start } = req.query;
  if (!start || !start.startsWith('tg_')) {
    return res.redirect('https://t.me/Miscuentasrdbot');
  }
  const base = API_BASE || `https://${req.headers.host}`;
  res.send(`<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>Conectando...</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #080d1a; color: #eeeef8; display: flex; align-items: center; justify-content: center; min-height: 100vh; margin: 0; text-align: center; }
    .container { padding: 24px; }
    h2 { color: #00e5a0; font-size: 24px; margin-bottom: 12px; }
    p  { color: #a0a0c0; font-size: 16px; }
    .spinner { font-size: 48px; animation: spin 1s linear infinite; }
    @keyframes spin { to { transform: rotate(360deg); } }
  </style>
</head>
<body>
  <div class="container">
    <div class="spinner">⏳</div>
    <h2>Conectando tu cuenta...</h2>
    <p>Espera un momento</p>
  </div>
  <script>
    // Notify server we're here, then poll until auth is confirmed or timeout
    const token = '${start}';
    let attempts = 0;
    const maxAttempts = 15;

    function updateStatus(msg) {
      const p = document.querySelector('p');
      if (p) p.textContent = msg;
    }

    function poll() {
      attempts++;
      fetch('${base}/auth-status?token=' + token)
        .then(r => r.json())
        .then(d => {
          if (d.ok) {
            document.querySelector('h2').textContent = '✅ ¡Conectado!';
            updateStatus('Ya puedes cerrar esta ventana');
            setTimeout(() => window.close(), 1500);
          } else if (attempts < maxAttempts) {
            updateStatus('Esperando... (' + attempts + '/' + maxAttempts + ')');
            setTimeout(poll, 1000);
          } else {
            document.querySelector('h2').textContent = '⏳ Procesando';
            updateStatus('El servidor está procesando. Puedes cerrar esta ventana.');
            setTimeout(() => window.close(), 2000);
          }
        })
        .catch(() => {
          if (attempts < maxAttempts) {
            setTimeout(poll, 1000);
          } else {
            document.querySelector('h2').textContent = '⚠️ Listo';
            updateStatus('Cierra esta ventana y vuelve a la app');
            setTimeout(() => window.close(), 2000);
          }
        });
    }

    // Initial notification
    fetch('${base}/api/telegram-auth', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ token: token })
    }).catch(() => {});

    // Start polling after a short delay (give Telegram time to send webhook)
    setTimeout(poll, 2000);
  </script>
</body>
</html>`);
});

// Inicia flujo Telegram OAuth — crea token pendiente y retorna el deep link
if (HAS_TELEGRAM) {
app.post('/api/telegram-auth/init', async (req, res) => {
  const token = `tg_${crypto.randomBytes(16).toString('hex')}`;
  // Guardar token pendiente SIN telegram_id (se asigna cuando el webhook recibe el callback)
  await query(
    `INSERT INTO auth_tokens(token, telegram_id, session_token, created_at)
     VALUES($1, $2, $3, NOW())`,
    [token, 'pending', '']
  );
  const deepLink = `https://t.me/Miscuentasrdbot?start=${token}`;
  res.json({ ok: true, token, deepLink });
});

// Endpoint que la página miscuentas llama al abrirse ( Deep link mini-app )
// El token ya fue guardado por el webhook cuando el usuario clickeó START
app.post('/api/telegram-auth', async (req, res) => {
  const { token } = req.body;
  if (!token) return res.status(400).json({ error: 'Missing token' });
  res.json({ ok: true, pending: true });
});
} // end HAS_TELEGRAM

// Polling endpoint — la web consulta si el token fue completado
app.get('/auth-status', async (req, res) => {
  const { token } = req.query;
  if (!token) return res.status(400).json({ error: 'Missing token' });

  const result = await consumeAuthToken(token);

  if (!result) {
    // Token no existe o expiró
    return res.json({ pending: false, ok: false, expired: true });
  }

  // Token consumido y eliminado (de DB) — retornamos los datos
  res.json({
    ok: true,
    telegram_id: result.telegram_id,
    token: result.session_token
  });
});

// ─── USERNAME/PASSWORD AUTH ───────────────────────────────────────────────────

// POST /api/auth/register — body: { username, password, phone? }
app.post('/api/auth/register', async (req, res) => {
  try {
    const { username, password, phone } = req.body;
    if (!username || !password) return res.status(400).json({ error: 'Username and password required' });
    if (username.length < 3) return res.status(400).json({ error: 'Username must be at least 3 characters' });
    if (password.length < 6) return res.status(400).json({ error: 'Password must be at least 6 characters' });
    // Only allow alphanumeric + underscore, 3-30 chars
    if (!/^[a-zA-Z0-9_]{3,30}$/.test(username)) {
      return res.status(400).json({ error: 'Username: letters, numbers, underscore only, 3-30 chars' });
    }

    // Check username not taken
    const existing = await query('SELECT user_id FROM user_credentials WHERE username=$1', [username.toLowerCase()]);
    if (existing.rows[0]) return res.status(409).json({ error: 'Username already taken' });

    // Generate userId
    const userId = phone ? String(phone) : crypto.randomUUID();

    // Hash password with PBKDF2
    const salt = username.toLowerCase();
    const hash = crypto.pbkdf2Sync(password, salt, 100000, 64, 'sha512').toString('hex');

    // Upsert user (creates if not exists)
    // First user ever becomes admin
    const userCount = await query(`SELECT COUNT(*) as cnt FROM users`);
    const isFirstUser = Number(userCount.rows[0].cnt) === 0;
    await query(
      `INSERT INTO users(id, lang, is_admin) VALUES($1, 'es', $2)
       ON CONFLICT(id) DO UPDATE SET is_admin = EXCLUDED.is_admin`,
      [userId, isFirstUser]
    );
    if (isFirstUser) console.log('👑 First user registered as admin:', username);
    await createSystemAccounts(userId);

    // Insert credentials
    await query(
      `INSERT INTO user_credentials(user_id, username, password_hash)
       VALUES($1, $2, $3)`,
      [userId, username.toLowerCase(), hash]
    );

    const token = generateToken(userId);
    res.json({ ok: true, token, userId, isAdmin: isFirstUser });
  } catch (e) {
    console.error('Register error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// POST /api/auth/login — body: { username, password }
app.post('/api/auth/login', async (req, res) => {
  try {
    const { username, password } = req.body;
    if (!username || !password) return res.status(400).json({ error: 'Username and password required' });

    const creds = await query(
      'SELECT user_id, password_hash FROM user_credentials WHERE username=$1',
      [username.toLowerCase()]
    );
    if (!creds.rows[0]) return res.status(401).json({ error: 'Invalid username or password' });

    const { user_id: userId, password_hash: storedHash } = creds.rows[0];
    const salt = username.toLowerCase();
    const hash = crypto.pbkdf2Sync(password, salt, 100000, 64, 'sha512').toString('hex');

    if (hash !== storedHash) return res.status(401).json({ error: 'Invalid username or password' });

    const token = generateToken(userId);
    const adminR = await query(`SELECT is_admin FROM users WHERE id=$1`, [userId]);
    res.json({ ok: true, token, userId, isAdmin: adminR.rows[0]?.is_admin || false });
  } catch (e) {
    console.error('Login error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// POST /api/auth/change-password — body: { currentPassword, newPassword }
app.post('/api/auth/change-password', authMiddleware, async (req, res) => {
  try {
    const { currentPassword, newPassword } = req.body;
    if (!currentPassword || !newPassword) return res.status(400).json({ error: 'Both passwords required' });
    if (newPassword.length < 6) return res.status(400).json({ error: 'New password must be at least 6 characters' });

    // Get user's credentials
    const creds = await query('SELECT username, password_hash FROM user_credentials WHERE user_id=$1', [req.userId]);
    if (!creds.rows[0]) return res.status(404).json({ error: 'No credentials found. Use /setpassword in Telegram to create a password.' });

    const { username, password_hash: storedHash } = creds.rows[0];
    const salt = username.toLowerCase();
    const hash = crypto.pbkdf2Sync(currentPassword, salt, 100000, 64, 'sha512').toString('hex');

    if (hash !== storedHash) return res.status(401).json({ error: 'Current password is incorrect' });

    const newHash = crypto.pbkdf2Sync(newPassword, salt, 100000, 64, 'sha512').toString('hex');
    await query('UPDATE user_credentials SET password_hash=$1 WHERE user_id=$2', [newHash, req.userId]);

    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/admin/bootstrap — make first user admin (one-time use, no auth needed)
app.get('/api/admin/bootstrap', async (req, res) => {
  try {
    const r = await query(`SELECT id FROM users ORDER BY created_at ASC LIMIT 1`);
    if (!r.rows[0]) return res.status(404).json({ error: 'No users found' });
    await query(`UPDATE users SET is_admin=TRUE WHERE id=$1`, [r.rows[0].id]);
    res.json({ ok: true, message: 'User promoted to admin', userId: r.rows[0].id });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/admin/promote/:userId — make any user admin (temp endpoint for setup)
app.get('/api/admin/promote/:userId', async (req, res) => {
  try {
    await query(`UPDATE users SET is_admin=TRUE WHERE id=$1`, [req.params.userId]);
    res.json({ ok: true, message: 'User promoted to admin', userId: req.params.userId });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/admin/users — list all users (admin only)
app.get('/api/admin/users', adminMiddleware, async (req, res) => {
  try {
    const r = await query(`
      SELECT u.id, u.is_admin, u.created_at, u.registered,
             uc.username
      FROM users u
      LEFT JOIN user_credentials uc ON uc.user_id = u.id
      ORDER BY u.created_at DESC
    `);
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// PUT /api/admin/users/:id/admin — promote to admin
app.put('/api/admin/users/:id/admin', adminMiddleware, async (req, res) => {
  try {
    if (req.params.id === req.userId) return res.status(400).json({ error: 'Cannot modify yourself' });
    await query(`UPDATE users SET is_admin=TRUE WHERE id=$1`, [req.params.id]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// DELETE /api/admin/users/:id/admin — demote from admin
app.delete('/api/admin/users/:id/admin', adminMiddleware, async (req, res) => {
  try {
    if (req.params.id === req.userId) return res.status(400).json({ error: 'Cannot modify yourself' });
    await query(`UPDATE users SET is_admin=FALSE WHERE id=$1`, [req.params.id]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// DELETE /api/admin/users/:id — delete a user (admin only)
app.delete('/api/admin/users/:id', adminMiddleware, async (req, res) => {
  try {
    if (req.params.id === req.userId) return res.status(400).json({ error: 'Cannot delete yourself' });
    // Delete in order: journal_lines -> journal_entries -> account_balances -> accounts -> clients/vendors/receivables/payables -> user_credentials -> users
    await query(`DELETE FROM journal_lines WHERE account_id IN (SELECT id FROM accounts WHERE user_id=$1)`, [req.params.id]);
    await query(`DELETE FROM journal_entries WHERE user_id=$1`, [req.params.id]);
    await query(`DELETE FROM account_balances WHERE account_id IN (SELECT id FROM accounts WHERE user_id=$1)`, [req.params.id]);
    await query(`DELETE FROM accounts WHERE user_id=$1`, [req.params.id]);
    await query(`DELETE FROM clients WHERE user_id=$1`, [req.params.id]);
    await query(`DELETE FROM vendors WHERE user_id=$1`, [req.params.id]);
    await query(`DELETE FROM receivables WHERE user_id=$1`, [req.params.id]);
    await query(`DELETE FROM payable_payments WHERE payable_id IN (SELECT id FROM payables WHERE user_id=$1)`, [req.params.id]);
    await query(`DELETE FROM payables WHERE user_id=$1`, [req.params.id]);
    await query(`DELETE FROM receivable_payments WHERE receivable_id IN (SELECT id FROM receivables WHERE user_id=$1)`, [req.params.id]);
    await query(`DELETE FROM user_credentials WHERE user_id=$1`, [req.params.id]);
    await query(`DELETE FROM users WHERE id=$1`, [req.params.id]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/', (_, res) => res.sendFile(__dirname + '/contabilidad.html'));
app.get('/health', (_, res) => res.json({ status: 'healthy', groq: !!GROQ_API_KEY, gemini: !!GEMINI_API_KEY }));

// ─── PRODUCTS (catalog, separate from inventory) ───────────────────────────────
app.get('/api/products', authMiddleware, async (req, res) => {
  try {
    const r = await query(`SELECT * FROM products WHERE user_id=$1 ORDER BY name`, [req.userId]);
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/products', authMiddleware, async (req, res) => {
  try {
    const { code, name, description, category, unit, cost_price, sale_price, stock_minimum, stock_current } = req.body;
    if (!name) return res.status(400).json({ error: 'Name required' });
    const id = `prod_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
    await query(
      `INSERT INTO products(id,user_id,code,name,description,category,unit,cost_price,sale_price,stock_minimum,stock_current)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
      [id, req.userId, code||null, name, description||null, category||'General', unit||'unidad', cost_price||0, sale_price||0, stock_minimum||0, stock_current||0]
    );
    res.json({ ok: true, id });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/products/:id', authMiddleware, async (req, res) => {
  try {
    const { code, name, description, category, unit, cost_price, sale_price, stock_minimum } = req.body;
    await query(
      `UPDATE products SET code=$1,name=$2,description=$3,category=$4,unit=$5,cost_price=$6,sale_price=$7,stock_minimum=$8 WHERE id=$9 AND user_id=$10`,
      [code||null, name, description||null, category||'General', unit||'unidad', cost_price||0, sale_price||0, stock_minimum||0, req.params.id, req.userId]
    );
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/products/:id', authMiddleware, async (req, res) => {
  try {
    await query(`DELETE FROM products WHERE id=$1 AND user_id=$2`, [req.params.id, req.userId]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── INVOICES ─────────────────────────────────────────────────────────────────
app.get('/api/invoices', authMiddleware, async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 100;
    const r = await query(`SELECT * FROM invoices WHERE user_id=$1 ORDER BY date DESC, created_at DESC LIMIT $2`, [req.userId, limit]);
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/invoices/next-number', authMiddleware, async (req, res) => {
  try {
    const r = await query(`SELECT last_number FROM invoice_counter WHERE user_id=$1`, [req.userId]);
    const next = (r.rows[0]?.last_number || 0) + 1;
    res.json({ invoice_number: String(next).padStart(6, '0') });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/invoices', authMiddleware, async (req, res) => {
  try {
    const { invoice_number, client_name, client_rnc, client_address, subtotal, tax, total, status, date, due_date, notes, items } = req.body;
    if (!invoice_number || !total) return res.status(400).json({ error: 'invoice_number and total required' });
    const id = `inv_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
    await query(
      `INSERT INTO invoices(id,user_id,invoice_number,client_name,client_rnc,client_address,subtotal,tax,total,status,date,due_date,notes)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
      [id, req.userId, invoice_number, client_name||null, client_rnc||null, client_address||null, subtotal||0, tax||0, total, status||'pending', date||null, due_date||null, notes||null]
    );
    // Update counter
    const num = parseInt(invoice_number) || 1;
    await query(`INSERT INTO invoice_counter(user_id,last_number) VALUES($1,$2) ON CONFLICT(user_id) DO UPDATE SET last_number=$2`, [req.userId, num]);
    // Insert items (frontend sends 'lines' array)
    const rawItems = req.body.lines || items || [];
    if (rawItems.length > 0) {
      for (const item of rawItems) {
        const itemId = `item_${Date.now()}_${Math.random().toString(36).substr(2,4)}`;
        const qty = parseFloat(item.quantity || item.qty || 1);
        const price = parseFloat(item.unit_price || item.price || 0);
        const disc = parseFloat(item.discount_pct || 0);
        const lineTotal = qty * price * (1 - disc / 100);
        await query(`INSERT INTO invoice_items(id,invoice_id,description,qty,price,total) VALUES($1,$2,$3,$4,$5,$6)`,
          [itemId, id, item.description || '', qty, price, lineTotal]);
      }
    }
    res.json({ ok: true, id });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/invoices/:id', authMiddleware, async (req, res) => {
  try {
    const inv = await query(`SELECT * FROM invoices WHERE id=$1 AND user_id=$2`, [req.params.id, req.userId]);
    if (!inv.rows[0]) return res.status(404).json({ error: 'Not found' });
    const items = await query(`SELECT * FROM invoice_items WHERE invoice_id=$1`, [req.params.id]);
    res.json({ ...inv.rows[0], items: items.rows });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/invoices/:id/status', authMiddleware, async (req, res) => {
  try {
    const { status } = req.body;
    await query(`UPDATE invoices SET status=$1 WHERE id=$2 AND user_id=$3`, [status, req.params.id, req.userId]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/invoices/:id', authMiddleware, async (req, res) => {
  try {
    await query(`DELETE FROM invoices WHERE id=$1 AND user_id=$2`, [req.params.id, req.userId]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── FIXED ASSETS ─────────────────────────────────────────────────────────────
app.get('/api/assets', authMiddleware, async (req, res) => {
  try {
    const r = await query(`SELECT * FROM fixed_assets WHERE user_id=$1 ORDER BY name`, [req.userId]);
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/assets', authMiddleware, async (req, res) => {
  try {
    const { name, description, category, purchase_date, purchase_value, useful_life_years, salvage_value } = req.body;
    if (!name || purchase_value == null) return res.status(400).json({ error: 'name and purchase_value required' });
    const id = `asset_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
    await query(
      `INSERT INTO fixed_assets(id,user_id,name,description,category,purchase_date,purchase_value,useful_life_years,salvage_value)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
      [id, req.userId, name, description||null, category||'General', purchase_date||null, purchase_value, useful_life_years||5, salvage_value||0]
    );
    res.json({ ok: true, id });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/assets/:id', authMiddleware, async (req, res) => {
  try {
    const { name, description, category, purchase_date, purchase_value, useful_life_years, salvage_value } = req.body;
    await query(
      `UPDATE fixed_assets SET name=$1,description=$2,category=$3,purchase_date=$4,purchase_value=$5,useful_life_years=$6,salvage_value=$7 WHERE id=$8 AND user_id=$9`,
      [name, description||null, category||'General', purchase_date||null, purchase_value, useful_life_years||5, salvage_value||0, req.params.id, req.userId]
    );
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/assets/:id', authMiddleware, async (req, res) => {
  try {
    await query(`DELETE FROM fixed_assets WHERE id=$1 AND user_id=$2`, [req.params.id, req.userId]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/assets/:id/depreciation', authMiddleware, async (req, res) => {
  try {
    const r = await query(`SELECT * FROM asset_depreciation WHERE asset_id=$1 ORDER BY period`, [req.params.id]);
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── INCOME TYPES ─────────────────────────────────────────────────────────────
app.get('/api/income-types', authMiddleware, async (req, res) => {
  try {
    const r = await query(`SELECT * FROM income_types WHERE user_id=$1 ORDER BY name`, [req.userId]);
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/income-types', authMiddleware, async (req, res) => {
  try {
    const { name, description, icon, color } = req.body;
    if (!name) return res.status(400).json({ error: 'Name required' });
    const id = `inctype_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
    await query(`INSERT INTO income_types(id,user_id,name,description,icon,color) VALUES($1,$2,$3,$4,$5,$6)`, [id, req.userId, name, description||null, icon||'💰', color||'#00e5a0']);
    res.json({ ok: true, id });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/income-types/:id', authMiddleware, async (req, res) => {
  try {
    const { name, description, icon, color } = req.body;
    await query(`UPDATE income_types SET name=$1,description=$2,icon=$3,color=$4 WHERE id=$5 AND user_id=$6`, [name, description||null, icon||'💰', color||'#00e5a0', req.params.id, req.userId]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/income-types/:id', authMiddleware, async (req, res) => {
  try {
    await query(`DELETE FROM income_types WHERE id=$1 AND user_id=$2`, [req.params.id, req.userId]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── INCOME RECORDS ───────────────────────────────────────────────────────────
app.get('/api/income-records', authMiddleware, async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 100;
    const r = await query(`
      SELECT ir.*, it.name as type_name, it.icon as type_icon, it.color as type_color
      FROM income_records ir
      LEFT JOIN income_types it ON it.id = ir.type_id
      WHERE ir.user_id=$1
      ORDER BY ir.date DESC, ir.created_at DESC
      LIMIT $2`, [req.userId, limit]);
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/income-records', authMiddleware, async (req, res) => {
  try {
    const { type_id, amount, description, date, reference } = req.body;
    if (!type_id || !amount) return res.status(400).json({ error: 'type_id and amount required' });
    const id = `inc_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
    await query(
      `INSERT INTO income_records(id,user_id,type_id,amount,description,date,reference)
       VALUES($1,$2,$3,$4,$5,$6,$7)`,
      [id, req.userId, type_id, amount, description||null, date||null, reference||null]
    );
    res.json({ ok: true, id });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/income-records/:id', authMiddleware, async (req, res) => {
  try {
    await query(`DELETE FROM income_records WHERE id=$1 AND user_id=$2`, [req.params.id, req.userId]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── INVENTORY ───────────────────────────────────────────────────────────────
// GET /api/inventory/stock
app.get('/api/inventory/stock', authMiddleware, async (req, res) => {
  try {
    const r = await query(`
      SELECT p.id, p.code, p.name, p.category, p.unit,
             COALESCE(p.cost_price,0) as cost_price, COALESCE(p.sell_price,0) as sell_price,
             COALESCE(p.min_stock,0) as min_stock,
             COALESCE(SUM(CASE WHEN m.type='entry' THEN m.quantity WHEN m.type IN ('exit','adjustment') THEN -m.quantity ELSE 0 END),0) as stock
      FROM inventory_products p
      LEFT JOIN inventory_movements m ON m.product_id = p.id AND m.user_id = p.user_id
      WHERE p.user_id=$1
      GROUP BY p.id
      ORDER BY p.name`, [req.userId]);
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/inventory/products
app.get('/api/inventory/products', authMiddleware, async (req, res) => {
  try {
    const r = await query(`SELECT * FROM inventory_products WHERE user_id=$1 ORDER BY name`, [req.userId]);
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/inventory/products
app.post('/api/inventory/products', authMiddleware, async (req, res) => {
  try {
    const { code, name, category, unit, cost_price, sell_price, min_stock } = req.body;
    if (!name) return res.status(400).json({ error: 'Name required' });
    const id = `prod_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
    await query(
      `INSERT INTO inventory_products(id,user_id,code,name,category,unit,cost_price,sell_price,min_stock)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
      [id, req.userId, code||null, name, category||'General', unit||'unidad', cost_price||0, sell_price||0, min_stock||0]
    );
    res.json({ ok: true, id });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// DELETE /api/inventory/products/:id
app.delete('/api/inventory/products/:id', authMiddleware, async (req, res) => {
  try {
    await query(`DELETE FROM inventory_products WHERE id=$1 AND user_id=$2`, [req.params.id, req.userId]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/inventory/movements
app.get('/api/inventory/movements', authMiddleware, async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 80;
    const r = await query(`
      SELECT m.*, p.name as product_name
      FROM inventory_movements m
      JOIN inventory_products p ON p.id = m.product_id
      WHERE m.user_id=$1
      ORDER BY m.created_at DESC
      LIMIT $2`, [req.userId, limit]);
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/inventory/entry
app.post('/api/inventory/entry', authMiddleware, async (req, res) => {
  try {
    const { product_id, quantity, unit_cost, reference, notes, mov_date } = req.body;
    if (!product_id || !quantity) return res.status(400).json({ error: 'product_id and quantity required' });
    const id = `mov_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
    await query(
      `INSERT INTO inventory_movements(id,user_id,product_id,type,quantity,unit_cost,reference,notes,mov_date)
       VALUES($1,$2,$3,'entry',$4,$5,$6,$7,$8)`,
      [id, req.userId, product_id, quantity, unit_cost||null, reference||null, notes||null, mov_date||null]
    );
    res.json({ ok: true, id });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/inventory/exit
app.post('/api/inventory/exit', authMiddleware, async (req, res) => {
  try {
    const { product_id, quantity, unit_price, reference, notes, mov_date } = req.body;
    if (!product_id || !quantity) return res.status(400).json({ error: 'product_id and quantity required' });
    const id = `mov_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
    await query(
      `INSERT INTO inventory_movements(id,user_id,product_id,type,quantity,unit_cost,reference,notes,mov_date)
       VALUES($1,$2,$3,'exit',$4,$5,$6,$7,$8)`,
      [id, req.userId, product_id, quantity, unit_price||null, reference||null, notes||null, mov_date||null]
    );
    res.json({ ok: true, id });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/inventory/adjustment
app.post('/api/inventory/adjustment', authMiddleware, async (req, res) => {
  try {
    const { product_id, new_quantity, notes, mov_date } = req.body;
    if (!product_id || new_quantity == null) return res.status(400).json({ error: 'product_id and new_quantity required' });
    const id = `mov_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
    await query(
      `INSERT INTO inventory_movements(id,user_id,product_id,type,quantity,notes,mov_date)
       VALUES($1,$2,$3,'adjustment',$4,$5,$6)`,
      [id, req.userId, product_id, new_quantity, notes||'Ajuste de inventario', mov_date||null]
    );
    res.json({ ok: true, id });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/inventory/kardex/:productId
app.get('/api/inventory/kardex/:productId', authMiddleware, async (req, res) => {
  try {
    const r = await query(`
      SELECT m.*, p.name as product_name
      FROM inventory_movements m
      JOIN inventory_products p ON p.id = m.product_id
      WHERE m.product_id=$1 AND m.user_id=$2
      ORDER BY m.mov_date ASC, m.created_at ASC`,
      [req.params.productId, req.userId]);
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Login / upsert de usuario — devuelve token de sesión
app.post('/api/login', async (req, res) => {
  try {
    const { phone } = req.body;
    if (!phone) return res.status(400).json({ error: 'id required' });
    const id = String(phone);
    await ensureUser(id, 'es');
    const token = generateToken(id);
    res.json({ ok: true, token, userId: id });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// GET datos del usuario — requiere token válido
app.get('/api/data/:id', authMiddleware, async (req, res) => {
  try {
    const id = decodeURIComponent(req.params.id);
    // Solo puede ver sus propios datos
    if (req.userId !== id) return res.status(403).json({ error: 'forbidden' });
    await ensureUser(id);
    const txs     = await getAllTxs(id);
    const budgets = await getBudgets(id);
    const normalized = txs.map(t => ({
      id       : t.id,
      type     : t.type,
      amount   : parseFloat(t.amount),
      desc     : t.description,
      cat      : t.category,
      account  : t.account,
      date     : t.tx_date instanceof Date ? t.tx_date.toISOString().split('T')[0] : t.tx_date,
      timestamp: t.created_at,
    }));
    res.json({ transactions: normalized, budgets });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// POST — requiere token válido
app.post('/api/data/:id', authMiddleware, async (req, res) => {
  try {
    const id = decodeURIComponent(req.params.id);
    if (req.userId !== id) return res.status(403).json({ error: 'forbidden' });
    const { transactions, budgets } = req.body;
    await ensureUser(id);

    if (Array.isArray(transactions)) {
      const existing = await query('SELECT id FROM transactions WHERE user_id=$1', [id]);
      const existingIds = new Set(existing.rows.map(r => String(r.id)));

      for (const t of transactions) {
        const txId = String(t.id || t.timestamp || '');
        if (!txId || existingIds.has(txId)) continue;
        await query(
          `INSERT INTO transactions(id, user_id, type, amount, description, category, account, tx_date)
           VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(id) DO NOTHING`,
          [txId, id, t.type, t.amount, t.desc || t.description || 'Transaction',
           t.cat || t.category || 'otro', t.account || 'efectivo',
           t.date || new Date().toISOString().split('T')[0]]
        );
      }
    }

    if (budgets && typeof budgets === 'object') {
      for (const [cat, amount] of Object.entries(budgets)) {
        if (amount > 0) await setBudget(id, cat, amount);
      }
    }

    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// DELETE — requiere token válido
app.delete('/api/data/:id/tx/:txId', authMiddleware, async (req, res) => {
  try {
    const id = decodeURIComponent(req.params.id);
    if (req.userId !== id) return res.status(403).json({ error: 'forbidden' });
    const txId = req.params.txId;
    await deleteTxById(txId, id);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Resumen semanal (llamado por cron-job.org los lunes 7am RD)
app.post('/send-weekly', async (req, res) => {
  const secret = req.headers['x-cron-secret'] || req.query.secret;
  if (CRON_SECRET && secret !== CRON_SECRET) return res.status(403).json({ error: 'forbidden' });
  try {
    const sent = await sendWeeklySummaries();
    res.json({ ok: true, sent });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── ACCOUNTING API ───────────────────────────────────────────────────────────

// GET /api/accounts — list all accounts with balances
app.get('/api/accounts', authMiddleware, async (req, res) => {
  try {
    const r = await query(
      `SELECT a.id, a.code, a.name, a.type, a.class, a.currency, a.is_system, a.is_active,
              COALESCE(ab.balance, 0) as balance
       FROM accounts a
       LEFT JOIN account_balances ab ON ab.account_id = a.id
       WHERE a.user_id=$1 AND a.is_active=TRUE
       ORDER BY a.class, a.code`,
      [req.userId]
    );
    res.json(r.rows);  // plain array — frontend expects this
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── WIZARD PRESETS ────────────────────────────────────────────────────────────
const WIZARD_PRESETS = {
  restaurant: [
    // Activos
    {code:'1101',name:'Bancos',              type:'asset',     class:1},
    {code:'1102',name:'Caja Chica',           type:'asset',     class:1},
    {code:'1103',name:'Inventario Comida',   type:'asset',     class:1},
    {code:'1104',name:'Combustibles',        type:'asset',     class:1},
    {code:'1201',name:'Clientes',            type:'asset',     class:1},
    {code:'1501',name:'Equipos de Cocina',   type:'asset',     class:1},
    {code:'1502',name:'Mobiliario y Equipos',type:'asset',     class:1},
    {code:'1503',name:'Vehículos',           type:'asset',     class:1},
    {code:'1504',name:'(+) Depreciación Acum.',type:'asset',   class:1},
    // Pasivos
    {code:'2101',name:'Proveedores',         type:'liability', class:2},
    {code:'2201',name:'ITBIS por Pagar',     type:'liability', class:2},
    {code:'2301',name:'Cuentas por Pagar',   type:'liability', class:2},
    {code:'2401',name:'Obligaciones Bancarias',type:'liability',class:2},
    // Patrimonio
    {code:'3101',name:'Capital Social',      type:'equity',    class:3},
    {code:'3201',name:'Utilidades Retenidas', type:'equity',    class:3},
    // Ingresos
    {code:'4101',name:'Ventas de Comida',    type:'income',     class:4},
    {code:'4102',name:'Ventas de Bebidas',   type:'income',     class:4},
    {code:'4201',name:'Otros Ingresos',      type:'income',     class:4},
    // Costos
    {code:'5101',name:'CMV',                 type:'cost',       class:5},
    {code:'5102',name:'Costo Bebidas',       type:'cost',       class:5},
    // Gastos
    {code:'6101',name:'Alquiler',            type:'expense',    class:6},
    {code:'6102',name:'Sueldos y Salarios',  type:'expense',    class:6},
    {code:'6103',name:'Gastos de Luz',       type:'expense',    class:6},
    {code:'6104',name:'Gastos de Agua',      type:'expense',    class:6},
    {code:'6105',name:'Gastos de Internet',  type:'expense',    class:6},
    {code:'6106',name:'Marketing y Publicidad',type:'expense',  class:6},
    {code:'6107',name:'Gastos Bancarios',    type:'expense',    class:6},
    {code:'6108',name:'Depreciación',        type:'expense',    class:6},
    {code:'6109',name:'Suministros de Limpieza',type:'expense', class:6},
    {code:'6110',name:'Seguros',             type:'expense',    class:6},
    {code:'6111',name:'Mantenimiento',        type:'expense',    class:6},
    {code:'6112',name:'Gastos Varios',       type:'expense',    class:6},
  ],
  tienda: [
    {code:'1101',name:'Bancos',              type:'asset',     class:1},
    {code:'1102',name:'Caja',                type:'asset',     class:1},
    {code:'1103',name:'Inventario Mercancías',type:'asset',   class:1},
    {code:'1201',name:'Clientes',            type:'asset',     class:1},
    {code:'1501',name:'Mobiliario y Equipos',type:'asset',     class:1},
    {code:'1502',name:'Vehículos',           type:'asset',     class:1},
    {code:'1503',name:'(+) Depreciación Acum.',type:'asset',  class:1},
    {code:'2101',name:'Proveedores',         type:'liability', class:2},
    {code:'2102',name:'ITBIS por Pagar',     type:'liability', class:2},
    {code:'2301',name:'Cuentas por Pagar',   type:'liability', class:2},
    {code:'3101',name:'Capital Social',      type:'equity',    class:3},
    {code:'4101',name:'Ventas',              type:'income',    class:4},
    {code:'4102',name:'Otros Ingresos',      type:'income',    class:4},
    {code:'5101',name:'Costo de Ventas',     type:'cost',      class:5},
    {code:'6101',name:'Alquiler',            type:'expense',   class:6},
    {code:'6102',name:'Sueldos y Salarios',  type:'expense',   class:6},
    {code:'6103',name:'Gastos de Operación', type:'expense',   class:6},
    {code:'6104',name:'Gastos Bancarios',    type:'expense',   class:6},
    {code:'6105',name:'Depreciación',        type:'expense',   class:6},
    {code:'6106',name:'Marketing',           type:'expense',   class:6},
    {code:'6107',name:'Impuestos y Tasas',   type:'expense',   class:6},
    {code:'6108',name:'Mantenimiento',       type:'expense',   class:6},
    {code:'6109',name:'Gastos Varios',       type:'expense',   class:6},
  ],
  servicios: [
    {code:'1101',name:'Bancos',              type:'asset',     class:1},
    {code:'1102',name:'Caja',                type:'asset',     class:1},
    {code:'1201',name:'Cuentas por Cobrar',  type:'asset',     class:1},
    {code:'1301',name:'Gastos Pagados x Adelantado',type:'asset',class:1},
    {code:'1501',name:'Equipos de Oficina',  type:'asset',     class:1},
    {code:'1502',name:'Equipos de Cómputos', type:'asset',     class:1},
    {code:'1503',name:'(+) Depreciación Acum.',type:'asset',   class:1},
    {code:'2101',name:'Cuentas por Pagar',   type:'liability', class:2},
    {code:'2102',name:'ITBIS por Pagar',     type:'liability', class:2},
    {code:'2201',name:'Impuestos Acumulados',type:'liability', class:2},
    {code:'3101',name:'Capital Social',      type:'equity',    class:3},
    {code:'3201',name:'Utilidades Retenidas',type:'equity',    class:3},
    {code:'4101',name:'Ingresos por Servicios',type:'income',   class:4},
    {code:'4102',name:'Ingresos por Consultorías',type:'income',class:4},
    {code:'4103',name:'Otros Ingresos',      type:'income',    class:4},
    {code:'6101',name:'Alquiler',            type:'expense',   class:6},
    {code:'6102',name:'Sueldos y Salarios',  type:'expense',   class:6},
    {code:'6103',name:'Gastos de Luz',       type:'expense',   class:6},
    {code:'6104',name:'Internet y Telefonía',type:'expense',   class:6},
    {code:'6105',name:'Gastos Bancarios',    type:'expense',   class:6},
    {code:'6106',name:'Depreciación',        type:'expense',   class:6},
    {code:'6107',name:'Marketing',           type:'expense',   class:6},
    {code:'6108',name:'Seguro Social',       type:'expense',   class:6},
    {code:'6109',name:'Herramientas y Suministros',type:'expense',class:6},
    {code:'6110',name:'Gastos Varios',       type:'expense',   class:6},
  ],
  general: [
    {code:'1101',name:'Bancos',              type:'asset',     class:1},
    {code:'1102',name:'Caja',                type:'asset',     class:1},
    {code:'1103',name:'Inventario',          type:'asset',     class:1},
    {code:'1201',name:'Cuentas por Cobrar',  type:'asset',     class:1},
    {code:'1501',name:'Activos Fijos',       type:'asset',     class:1},
    {code:'1502',name:'(+) Depreciación Acum.',type:'asset',  class:1},
    {code:'2101',name:'Proveedores',         type:'liability',class:2},
    {code:'2102',name:'Cuentas por Pagar',   type:'liability', class:2},
    {code:'2103',name:'ITBIS por Pagar',     type:'liability', class:2},
    {code:'3101',name:'Capital Social',       type:'equity',    class:3},
    {code:'3201',name:'Utilidades Retenidas',type:'equity',    class:3},
    {code:'4101',name:'Ventas',              type:'income',    class:4},
    {code:'4102',name:'Otros Ingresos',      type:'income',    class:4},
    {code:'5101',name:'Costo de Ventas',     type:'cost',      class:5},
    {code:'6101',name:'Alquiler',            type:'expense',   class:6},
    {code:'6102',name:'Sueldos y Salarios',  type:'expense',   class:6},
    {code:'6103',name:'Gastos Generales',    type:'expense',   class:6},
    {code:'6104',name:'Gastos Bancarios',    type:'expense',   class:6},
    {code:'6105',name:'Depreciación',        type:'expense',   class:6},
    {code:'6106',name:'Impuestos y Tasas',   type:'expense',   class:6},
    {code:'6107',name:'Marketing',           type:'expense',   class:6},
    {code:'6108',name:'Mantenimiento',       type:'expense',   class:6},
    {code:'6109',name:'Gastos Varios',       type:'expense',   class:6},
  ],
};

const WIZARD_LABELS = {
  restaurant: 'Restaurante / Comida',
  tienda: 'Tienda / Comercio',
  servicios: 'Servicios',
  general: 'Negocios en General',
};

// GET /api/setup/status — check if user has accounts
app.get('/api/setup/status', authMiddleware, async (req, res) => {
  try {
    const r = await query(`SELECT COUNT(*) as cnt FROM accounts WHERE user_id=$1 AND is_active=TRUE`, [req.userId]);
    res.json({ hasAccounts: Number(r.rows[0].cnt) > 0 });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/setup/wizard — create preset accounts
app.post('/api/setup/wizard', authMiddleware, async (req, res) => {
  try {
    const { businessType } = req.body;
    const preset = WIZARD_PRESETS[businessType];
    if (!preset) return res.status(400).json({ error: 'Tipo de negocio no válido' });

    const label = WIZARD_LABELS[businessType] || businessType;
    let created = 0;

    for (const acc of preset) {
      const id = `acc_${Date.now()}_${Math.random().toString(36).substr(2, 6)}_${created}`;
      try {
        await query(
          `INSERT INTO accounts(id, user_id, code, name, type, class, currency, is_system)
           VALUES($1,$2,$3,$4,$5,$6,'DOP',FALSE)
           ON CONFLICT (id) DO NOTHING`,
          [id, req.userId, acc.code, acc.name, acc.type, acc.class]
        );
        await query(`INSERT INTO account_balances(account_id, balance) VALUES($1,0) ON CONFLICT DO NOTHING`, [id]);
      } catch(e) { console.warn('Wizard account insert warning:', e.message); }
      created++;
    }

    res.json({ ok: true, count: created, label });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/accounts', authMiddleware, async (req, res) => {
  try {
    const { code, name, type, accClass, currency = 'DOP' } = req.body;
    if (!code || !name || !type || !accClass) return res.status(400).json({ error: 'Missing required fields' });
    const id = `acc_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
    await query(
      `INSERT INTO accounts(id, user_id, code, name, type, class, currency)
       VALUES($1,$2,$3,$4,$5,$6,$7)`,
      [id, req.userId, code, name, type, accClass, currency]
    );
    await query(`INSERT INTO account_balances(account_id, balance) VALUES($1,0) ON CONFLICT DO NOTHING`, [id]);
    res.json({ ok: true, id });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// PUT /api/accounts/:id — update account
app.put('/api/accounts/:id', authMiddleware, async (req, res) => {
  try {
    const { name, code, type, accClass, currency, is_active } = req.body;
    const r = await query(
      `UPDATE accounts SET name=COALESCE($1,name), code=COALESCE($2,code),
       type=COALESCE($3,type), class=COALESCE($4,class), currency=COALESCE($5,currency),
       is_active=COALESCE($6,is_active)
       WHERE id=$7 AND user_id=$8 AND is_system=FALSE
       RETURNING id`,
      [name, code, type, accClass, currency, is_active, req.params.id, req.userId]
    );
    if (!r.rows[0]) return res.status(404).json({ error: 'Account not found or system account' });
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// DELETE /api/accounts/:id — soft delete
app.delete('/api/accounts/:id', authMiddleware, async (req, res) => {
  try {
    const r = await query(
      `UPDATE accounts SET is_active=FALSE WHERE id=$1 AND user_id=$2 AND is_system=FALSE RETURNING id`,
      [req.params.id, req.userId]
    );
    if (!r.rows[0]) return res.status(404).json({ error: 'Account not found or system account' });
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/journal — list journal entries
app.get('/api/journal', authMiddleware, async (req, res) => {
  try {
    const { from, to, account_id } = req.query;
    let sql = `SELECT je.id, je.date, je.description, je.ref_type, je.ref_id, je.created_at,
                      jl.id as line_id, jl.account_id, a.name as account_name, a.code as account_code,
                      jl.debit, jl.credit,
                      jl.auxiliary_type, jl.auxiliary_id, jl.auxiliary_name
               FROM journal_entries je
               JOIN journal_lines jl ON jl.journal_entry_id = je.id
               JOIN accounts a ON a.id = jl.account_id
               WHERE je.user_id=$1`;
    const params = [req.userId];
    let p = 2;
    if (from) { sql += ` AND je.date >= $${p++}`; params.push(from); }
    if (to)   { sql += ` AND je.date <= $${p++}`; params.push(to); }
    if (account_id) { sql += ` AND jl.account_id = $${p++}`; params.push(account_id); }
    sql += ` ORDER BY je.date DESC, je.created_at DESC`;
    const r = await query(sql, params);

    // Group by entry
    const entries = {};
    for (const row of r.rows) {
      if (!entries[row.id]) {
        entries[row.id] = { id: row.id, date: row.date, description: row.description,
          ref_type: row.ref_type, ref_id: row.ref_id, created_at: row.created_at, lines: [] };
      }
      entries[row.id].lines.push({
        id: row.line_id, account_id: row.account_id,
        account_name: row.account_name, account_code: row.account_code,
        debit: parseFloat(row.debit), credit: parseFloat(row.credit),
        auxiliary_type: row.auxiliary_type, auxiliary_id: row.auxiliary_id, auxiliary_name: row.auxiliary_name
      });
    }
    res.json({ entries: Object.values(entries) });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/journal — create journal entry
app.post('/api/journal', authMiddleware, async (req, res) => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { date, description, ref_type, ref_id, lines } = req.body;
    if (!lines || lines.length < 2) return res.status(400).json({ error: 'Need at least 2 lines' });

    const totalDebit  = lines.reduce((s, l) => s + parseFloat(l.debit || 0), 0);
    const totalCredit = lines.reduce((s, l) => s + parseFloat(l.credit || 0), 0);
    if (Math.abs(totalDebit - totalCredit) > 0.01) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'Debits must equal credits', totalDebit, totalCredit });
    }

    const jeId = `je_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
    await client.query(
      `INSERT INTO journal_entries(id, user_id, date, description, ref_type, ref_id)
       VALUES($1,$2,$3,$4,$5,$6)`,
      [jeId, req.userId, date || new Date().toISOString().split('T')[0], description || 'Journal entry', ref_type || 'manual', ref_id]
    );

    for (const line of lines) {
      const jlId = `jl_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
      await client.query(
        `INSERT INTO journal_lines(id, journal_entry_id, account_id, debit, credit, auxiliary_type, auxiliary_id, auxiliary_name)
         VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
        [jlId, jeId, line.account_id, line.debit || 0, line.credit || 0, line.auxiliary_type || null, line.auxiliary_id || null, line.auxiliary_name || null]
      );
      // Update account balance
      await client.query(
        `INSERT INTO account_balances(account_id, balance)
         VALUES($1, $2::numeric - $3::numeric)
         ON CONFLICT(account_id) DO UPDATE SET balance = account_balances.balance + $2::numeric - $3::numeric`,
        [line.account_id, line.debit || 0, line.credit || 0]
      );

      // Auto-create receivable: debit on 1.2.01 with client auxiliary
      if (line.auxiliary_type === 'client' && Number(line.debit) > 0) {
        const accCheck = await client.query(`SELECT code FROM accounts WHERE id=$1 AND user_id=$2`, [line.account_id, req.userId]);
        if (accCheck.rows[0]?.code === '1.2.01') {
          const rId = `rec_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
          await client.query(
            `INSERT INTO receivables(id, user_id, client_id, description, total_amount, paid_amount, status, due_date)
             VALUES($1,$2,$3,$4,$5,0,'pending',$6)`,
            [rId, req.userId, line.auxiliary_id, description, line.debit, date]
          );
        }
      }

      // Auto-create payable: credit on 2.1.01 with vendor auxiliary
      if (line.auxiliary_type === 'vendor' && Number(line.credit) > 0) {
        const accCheck = await client.query(`SELECT code FROM accounts WHERE id=$1 AND user_id=$2`, [line.account_id, req.userId]);
        if (accCheck.rows[0]?.code === '2.1.01') {
          const pId = `pay_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
          await client.query(
            `INSERT INTO payables(id, user_id, vendor_id, description, total_amount, paid_amount, status, due_date)
             VALUES($1,$2,$3,$4,$5,0,'pending',$6)`,
            [pId, req.userId, line.auxiliary_id, description, line.credit, date]
          );
        }
      }
    }

    await client.query('COMMIT');
    res.json({ ok: true, id: jeId });
  } catch(e) {
    await client.query('ROLLBACK');
    res.status(500).json({ error: e.message });
  } finally {
    client.release();
  }
});

// DELETE /api/journal/:id — delete and reverse balances
app.delete('/api/journal/:id', authMiddleware, async (req, res) => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // Get lines before deleting
    const lines = await client.query(
      `SELECT jl.account_id, jl.debit, jl.credit FROM journal_lines jl
       JOIN journal_entries je ON je.id = jl.journal_entry_id
       WHERE jl.journal_entry_id=$1 AND je.user_id=$2`,
      [req.params.id, req.userId]
    );
    if (!lines.rows.length) { await client.query('ROLLBACK'); return res.status(404).json({ error: 'Entry not found' }); }

    for (const line of lines.rows) {
      // Reverse: subtract debit, add credit
      await client.query(
        `UPDATE account_balances SET balance = balance - $1 + $2 WHERE account_id = $3`,
        [line.debit, line.credit, line.account_id]
      );
    }
    await client.query(`DELETE FROM journal_entries WHERE id=$1 AND user_id=$2`, [req.params.id, req.userId]);
    await client.query('COMMIT');
    res.json({ ok: true });
  } catch(e) {
    await client.query('ROLLBACK');
    res.status(500).json({ error: e.message });
  } finally {
    client.release();
  }
});

// GET /api/clients
app.get('/api/clients', authMiddleware, async (req, res) => {
  try {
    const r = await query(`SELECT * FROM clients WHERE user_id=$1 ORDER BY name`, [req.userId]);
    res.json(r.rows);  // plain array
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/clients
app.post('/api/clients', authMiddleware, async (req, res) => {
  try {
    const { name, phone, email, address, notes } = req.body;
    if (!name) return res.status(400).json({ error: 'Name required' });
    const id = `cli_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
    await query(
      `INSERT INTO clients(id, user_id, name, phone, email, address, notes)
       VALUES($1,$2,$3,$4,$5,$6,$7)`,
      [id, req.userId, name, phone, email, address, notes]
    );
    res.json({ ok: true, id });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// DELETE /api/clients/:id
app.delete('/api/clients/:id', authMiddleware, async (req, res) => {
  try {
    const r = await query(
      `DELETE FROM clients WHERE id=$1 AND user_id=$2 RETURNING id`,
      [req.params.id, req.userId]
    );
    if (!r.rows[0]) return res.status(404).json({ error: 'Client not found' });
    res.json({ ok: true });
  } catch(e) {
    // Foreign key constraint — client has receivables
    if (e.code === '23503') return res.status(409).json({ error: 'Este cliente tiene cuentas por cobrar. Elimínalas primero.' });
    res.status(500).json({ error: e.message });
  }
});

// GET /api/receivables
app.get('/api/receivables', authMiddleware, async (req, res) => {
  try {
    const r = await query(
      `SELECT r.id, r.client_id, c.name as client_name, r.description,
              r.total_amount, r.paid_amount, r.total_amount - r.paid_amount as outstanding,
              r.due_date, r.status, r.created_at
       FROM receivables r
       JOIN clients c ON c.id = r.client_id
       WHERE r.user_id=$1
       ORDER BY r.created_at DESC`,
      [req.userId]
    );
    res.json(r.rows.map(row => ({   // plain array
      ...row,
      total: parseFloat(row.total_amount),
      paid: parseFloat(row.paid_amount),
      outstanding: parseFloat(row.outstanding)
    })));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/receivables
app.post('/api/receivables', authMiddleware, async (req, res) => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { client_id, description, total_amount, due_date } = req.body;
    if (!client_id || !description || !total_amount) return res.status(400).json({ error: 'Missing fields' });
    const id = `rec_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
    await client.query(
      `INSERT INTO receivables(id, user_id, client_id, description, total_amount, due_date)
       VALUES($1,$2,$3,$4,$5,$6)`,
      [id, req.userId, client_id, description, total_amount, due_date]
    );
    // Auto-create journal entry: Debit "Cuentas por Cobrar" (asset), Credit "Ingresos" (income)
    const cxc = await client.query(
      `SELECT id FROM accounts WHERE code='1.2.01' AND user_id=$1`, [req.userId]
    );
    const ing = await client.query(
      `SELECT id FROM accounts WHERE code='4.1.01' AND user_id=$1`, [req.userId]
    );
    const clientInfo = await client.query(`SELECT name FROM clients WHERE id=$1`, [client_id]);
    const clientName = clientInfo.rows[0]?.name || 'Cliente';
    if (cxc.rows[0] && ing.rows[0]) {
      const jeId = `je_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
      await client.query(
        `INSERT INTO journal_entries(id, user_id, date, description, ref_type, ref_id)
         VALUES($1,$2,CURRENT_DATE,$3,'receivable',$4)`,
        [jeId, req.userId, description, id]
      );
      const debitLine  = `jl_${Date.now()}_${Math.random().toString(36).substr(2, 4)}`;
      const creditLine = `jl_${Date.now()}_${Math.random().toString(36).substr(2, 4)}`;
      await client.query(
        `INSERT INTO journal_lines(id, journal_entry_id, account_id, debit, credit, auxiliary_type, auxiliary_id, auxiliary_name)
         VALUES($1,$2,$3,$4,0,'client',$5,$6)`,
        [debitLine, jeId, cxc.rows[0].id, total_amount, client_id, clientName]
      );
      await client.query(
        `INSERT INTO journal_lines(id, journal_entry_id, account_id, debit, credit)
         VALUES($1,$2,$3,0,$4)`, [creditLine, jeId, ing.rows[0].id, total_amount]
      );
      await client.query(
        `INSERT INTO account_balances(account_id, balance)
         VALUES($1, $2) ON CONFLICT(account_id) DO UPDATE SET balance = account_balances.balance + $2`,
        [cxc.rows[0].id, total_amount]
      );
      await client.query(
        `INSERT INTO account_balances(account_id, balance)
         VALUES($1, $2) ON CONFLICT(account_id) DO UPDATE SET balance = account_balances.balance - $2`,
        [ing.rows[0].id, total_amount]
      );
    }
    await client.query('COMMIT');
    res.json({ ok: true, id });
  } catch(e) {
    await client.query('ROLLBACK');
    res.status(500).json({ error: e.message });
  } finally {
    client.release();
  }
});

// POST /api/receivables/:id/payments
app.post('/api/receivables/:id/payments', authMiddleware, async (req, res) => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { amount, payment_date, notes } = req.body;
    if (!amount) return res.status(400).json({ error: 'Amount required' });
    const rec = await client.query(
      `SELECT * FROM receivables WHERE id=$1 AND user_id=$2`, [req.params.id, req.userId]
    );
    if (!rec.rows[0]) { await client.query('ROLLBACK'); return res.status(404).json({ error: 'Not found' }); }

    const payId = `rpay_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
    await client.query(
      `INSERT INTO receivable_payments(id, receivable_id, amount, payment_date, notes)
       VALUES($1,$2,$3,$4,$5)`,
      [payId, req.params.id, amount, payment_date || new Date().toISOString().split('T')[0], notes]
    );
    await client.query(
      `UPDATE receivables SET paid_amount = paid_amount + $1,
       status = CASE WHEN paid_amount + $1 >= total_amount THEN 'paid'
                     WHEN paid_amount + $1 > 0 THEN 'partial' ELSE status END
       WHERE id=$2`,
      [amount, req.params.id]
    );
    // Reverse the receivable journal: Debit "Caja/Banco", Credit "Cuentas por Cobrar"
    const caja = await client.query(`SELECT id FROM accounts WHERE code='1.1.01' AND user_id=$1`, [req.userId]);
    const banco = await client.query(`SELECT id FROM accounts WHERE code='1.1.02' AND user_id=$1`, [req.userId]);
    const cxc = await client.query(`SELECT id FROM accounts WHERE code='1.2.01' AND user_id=$1`, [req.userId]);
    const cashAcc = banco.rows[0]?.id || caja.rows[0]?.id;
    if (cashAcc && cxc.rows[0]) {
      const jeId = `je_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
      await client.query(
        `INSERT INTO journal_entries(id, user_id, date, description, ref_type, ref_id)
         VALUES($1,$2,CURRENT_DATE,$3,'receivable',$4)`,
        [jeId, req.userId, `Pago de cliente: ${rec.rows[0].description}`, req.params.id]
      );
      const debitLine  = `jl_${Date.now()}_${Math.random().toString(36).substr(2, 4)}`;
      const creditLine = `jl_${Date.now()}_${Math.random().toString(36).substr(2, 4)}`;
      await client.query(
        `INSERT INTO journal_lines(id, journal_entry_id, account_id, debit, credit)
         VALUES($1,$2,$3,$4,0)`, [debitLine, jeId, cashAcc, amount]
      );
      await client.query(
        `INSERT INTO journal_lines(id, journal_entry_id, account_id, debit, credit)
         VALUES($1,$2,$3,0,$4)`, [creditLine, jeId, cxc.rows[0].id, amount]
      );
      await client.query(
        `INSERT INTO account_balances(account_id, balance)
         VALUES($1, $2) ON CONFLICT(account_id) DO UPDATE SET balance = account_balances.balance + $2`,
        [cashAcc, amount]
      );
      await client.query(
        `INSERT INTO account_balances(account_id, balance)
         VALUES($1, $2) ON CONFLICT(account_id) DO UPDATE SET balance = account_balances.balance - $2`,
        [cxc.rows[0].id, amount]
      );
    }
    await client.query('COMMIT');
    res.json({ ok: true });
  } catch(e) {
    await client.query('ROLLBACK');
    res.status(500).json({ error: e.message });
  } finally {
    client.release();
  }
});

// GET /api/vendors
app.get('/api/vendors', authMiddleware, async (req, res) => {
  try {
    const r = await query(`SELECT * FROM vendors WHERE user_id=$1 ORDER BY name`, [req.userId]);
    res.json(r.rows);  // plain array
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/vendors
app.post('/api/vendors', authMiddleware, async (req, res) => {
  try {
    const { name, vendor_type = 'vendor', phone, email, address, notes } = req.body;
    if (!name) return res.status(400).json({ error: 'Name required' });
    const id = `ven_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
    await query(
      `INSERT INTO vendors(id, user_id, name, vendor_type, phone, email, address, notes)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
      [id, req.userId, name, vendor_type, phone, email, address, notes]
    );
    res.json({ ok: true, id });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// DELETE /api/vendors/:id
app.delete('/api/vendors/:id', authMiddleware, async (req, res) => {
  try {
    const r = await query(
      `DELETE FROM vendors WHERE id=$1 AND user_id=$2 RETURNING id`,
      [req.params.id, req.userId]
    );
    if (!r.rows[0]) return res.status(404).json({ error: 'Vendor not found' });
    res.json({ ok: true });
  } catch(e) {
    if (e.code === '23503') return res.status(409).json({ error: 'Este proveedor tiene cuentas por pagar. Elimínalas primero.' });
    res.status(500).json({ error: e.message });
  }
});

// DELETE /api/receivables/:id
app.delete('/api/receivables/:id', authMiddleware, async (req, res) => {
  try {
    const r = await query(
      `DELETE FROM receivables WHERE id=$1 AND user_id=$2 RETURNING id`,
      [req.params.id, req.userId]
    );
    if (!r.rows[0]) return res.status(404).json({ error: 'Receivable not found' });
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// DELETE /api/payables/:id
app.delete('/api/payables/:id', authMiddleware, async (req, res) => {
  try {
    const r = await query(
      `DELETE FROM payables WHERE id=$1 AND user_id=$2 RETURNING id`,
      [req.params.id, req.userId]
    );
    if (!r.rows[0]) return res.status(404).json({ error: 'Payable not found' });
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/payables
app.get('/api/payables', authMiddleware, async (req, res) => {
  try {
    const r = await query(
      `SELECT p.id, p.vendor_id, v.name as vendor_name, v.vendor_type, p.description,
              p.total_amount, p.paid_amount, p.total_amount - p.paid_amount as outstanding,
              p.due_date, p.status, p.created_at
       FROM payables p
       JOIN vendors v ON v.id = p.vendor_id
       WHERE p.user_id=$1
       ORDER BY p.created_at DESC`,
      [req.userId]
    );
    res.json(r.rows.map(row => ({   // plain array
      ...row,
      total: parseFloat(row.total_amount),
      paid: parseFloat(row.paid_amount),
      outstanding: parseFloat(row.outstanding)
    })));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/payables
app.post('/api/payables', authMiddleware, async (req, res) => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { vendor_id, description, total_amount, due_date } = req.body;
    if (!vendor_id || !description || !total_amount) return res.status(400).json({ error: 'Missing fields' });
    const id = `pay_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
    await client.query(
      `INSERT INTO payables(id, user_id, vendor_id, description, total_amount, due_date)
       VALUES($1,$2,$3,$4,$5,$6)`,
      [id, req.userId, vendor_id, description, total_amount, due_date]
    );
    // Auto-journal: Debit "Gastos Operativos" (expense), Credit "Cuentas por Pagar" (liability)
    const exp = await client.query(`SELECT id FROM accounts WHERE code='6.1.01' AND user_id=$1`, [req.userId]);
    const cxp = await client.query(`SELECT id FROM accounts WHERE code='2.1.01' AND user_id=$1`, [req.userId]);
    const vendorInfo = await client.query(`SELECT name FROM vendors WHERE id=$1`, [vendor_id]);
    const vendorName = vendorInfo.rows[0]?.name || 'Proveedor';
    if (exp.rows[0] && cxp.rows[0]) {
      const jeId = `je_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
      await client.query(
        `INSERT INTO journal_entries(id, user_id, date, description, ref_type, ref_id)
         VALUES($1,$2,CURRENT_DATE,$3,'payable',$4)`,
        [jeId, req.userId, description, id]
      );
      const debitLine  = `jl_${Date.now()}_${Math.random().toString(36).substr(2, 4)}`;
      const creditLine = `jl_${Date.now()}_${Math.random().toString(36).substr(2, 4)}`;
      await client.query(
        `INSERT INTO journal_lines(id, journal_entry_id, account_id, debit, credit)
         VALUES($1,$2,$3,$4,0)`, [debitLine, jeId, exp.rows[0].id, total_amount]
      );
      await client.query(
        `INSERT INTO journal_lines(id, journal_entry_id, account_id, debit, credit, auxiliary_type, auxiliary_id, auxiliary_name)
         VALUES($1,$2,$3,0,$4,'vendor',$5,$6)`,
        [creditLine, jeId, cxp.rows[0].id, total_amount, vendor_id, vendorName]
      );
      await client.query(
        `INSERT INTO account_balances(account_id, balance)
         VALUES($1, $2) ON CONFLICT(account_id) DO UPDATE SET balance = account_balances.balance + $2`,
        [exp.rows[0].id, total_amount]
      );
      await client.query(
        `INSERT INTO account_balances(account_id, balance)
         VALUES($1, $2) ON CONFLICT(account_id) DO UPDATE SET balance = account_balances.balance - $2`,
        [cxp.rows[0].id, total_amount]
      );
    }
    await client.query('COMMIT');
    res.json({ ok: true, id });
  } catch(e) {
    await client.query('ROLLBACK');
    res.status(500).json({ error: e.message });
  } finally {
    client.release();
  }
});

// POST /api/payables/:id/payments
app.post('/api/payables/:id/payments', authMiddleware, async (req, res) => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { amount, payment_date, notes } = req.body;
    if (!amount) return res.status(400).json({ error: 'Amount required' });
    const pay = await client.query(
      `SELECT * FROM payables WHERE id=$1 AND user_id=$2`, [req.params.id, req.userId]
    );
    if (!pay.rows[0]) { await client.query('ROLLBACK'); return res.status(404).json({ error: 'Not found' }); }

    const payId = `ppay_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
    await client.query(
      `INSERT INTO payable_payments(id, payable_id, amount, payment_date, notes)
       VALUES($1,$2,$3,$4,$5)`,
      [payId, req.params.id, amount, payment_date || new Date().toISOString().split('T')[0], notes]
    );
    await client.query(
      `UPDATE payables SET paid_amount = paid_amount + $1,
       status = CASE WHEN paid_amount + $1 >= total_amount THEN 'paid'
                     WHEN paid_amount + $1 > 0 THEN 'partial' ELSE status END
       WHERE id=$2`,
      [amount, req.params.id]
    );
    // Journal: Debit "Cuentas por Pagar", Credit "Caja/Banco"
    const cxp = await client.query(`SELECT id FROM accounts WHERE code='2.1.01' AND user_id=$1`, [req.userId]);
    const caja = await client.query(`SELECT id FROM accounts WHERE code='1.1.01' AND user_id=$1`, [req.userId]);
    const banco = await client.query(`SELECT id FROM accounts WHERE code='1.1.02' AND user_id=$1`, [req.userId]);
    const cashAcc = banco.rows[0]?.id || caja.rows[0]?.id;
    const vendorInfo = await client.query(`SELECT v.name FROM vendors v JOIN payables p ON p.vendor_id=v.id WHERE p.id=$1`, [req.params.id]);
    const vendorName = vendorInfo.rows[0]?.name || 'Proveedor';
    if (cxp.rows[0] && cashAcc) {
      const jeId = `je_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
      await client.query(
        `INSERT INTO journal_entries(id, user_id, date, description, ref_type, ref_id)
         VALUES($1,$2,CURRENT_DATE,$3,'payable',$4)`,
        [jeId, req.userId, `Pago a proveedor: ${pay.rows[0].description}`, req.params.id]
      );
      const debitLine  = `jl_${Date.now()}_${Math.random().toString(36).substr(2, 4)}`;
      const creditLine = `jl_${Date.now()}_${Math.random().toString(36).substr(2, 4)}`;
      await client.query(
        `INSERT INTO journal_lines(id, journal_entry_id, account_id, debit, credit, auxiliary_type, auxiliary_id, auxiliary_name)
         VALUES($1,$2,$3,$4,0,'vendor',$5,$6)`, [debitLine, jeId, cxp.rows[0].id, amount, pay.rows[0].vendor_id, vendorName]
      );
      await client.query(
        `INSERT INTO journal_lines(id, journal_entry_id, account_id, debit, credit)
         VALUES($1,$2,$3,0,$4)`, [creditLine, jeId, cashAcc, amount]
      );
      await client.query(
        `INSERT INTO account_balances(account_id, balance)
         VALUES($1, $2) ON CONFLICT(account_id) DO UPDATE SET balance = account_balances.balance - $2`,
        [cxp.rows[0].id, amount]
      );
      await client.query(
        `INSERT INTO account_balances(account_id, balance)
         VALUES($1, $2) ON CONFLICT(account_id) DO UPDATE SET balance = account_balances.balance - $2`,
        [cashAcc, amount]
      );
    }
    await client.query('COMMIT');
    res.json({ ok: true });
  } catch(e) {
    await client.query('ROLLBACK');
    res.status(500).json({ error: e.message });
  } finally {
    client.release();
  }
});

// GET /api/cashflow — cash in/out by period
app.get('/api/cashflow', authMiddleware, async (req, res) => {
  try {
    let { from, to, month, year } = req.query;
    // Support ?month=0-based&year=YYYY  (sent by frontend)
    if (!from && month !== undefined && year !== undefined) {
      const m = parseInt(month) + 1;  // frontend sends 0-based
      const y = parseInt(year);
      from = `${y}-${String(m).padStart(2,'0')}-01`;
      const lastDay = new Date(y, m, 0).getDate();
      to   = `${y}-${String(m).padStart(2,'0')}-${lastDay}`;
    }
    // Cash accounts: 1.1.01 (Caja), 1.1.02 (Banco)
    const cashAccounts = await query(
      `SELECT id FROM accounts WHERE user_id=$1 AND code IN ('1.1.01','1.1.02')`, [req.userId]
    );
    // If no accounts yet, fall back to transaction totals for that month
    if (!cashAccounts.rows.length) {
      let txWhere = `WHERE user_id=$1`;
      const txParams = [req.userId];
      let p = 2;
      if (from) { txWhere += ` AND tx_date >= $${p++}`; txParams.push(from); }
      if (to)   { txWhere += ` AND tx_date <= $${p++}`; txParams.push(to); }
      const txr = await query(
        `SELECT type, SUM(amount) as total FROM transactions ${txWhere} GROUP BY type`, txParams
      );
      const cashIn  = parseFloat(txr.rows.find(r => r.type === 'ingreso')?.total || 0);
      const cashOut = parseFloat(txr.rows.find(r => r.type === 'egreso')?.total  || 0);
      return res.json({ cashIn, cashOut, cash_in: cashIn, cash_out: cashOut, net: cashIn - cashOut, periods: [] });
    }
    const ids = cashAccounts.rows.map(r => r.id);
    const placeholders = ids.map((_, i) => `$${i + 2}`).join(',');

    let sql = `SELECT je.date, SUM(jl.debit) as cash_in, SUM(jl.credit) as cash_out
               FROM journal_lines jl
               JOIN journal_entries je ON je.id = jl.journal_entry_id
               JOIN accounts a ON a.id = jl.account_id
               WHERE a.user_id = $1::text AND jl.account_id IN (${placeholders})`;
    const params = [req.userId, ...ids];
    let p = params.length + 1;
    if (from) { sql += ` AND je.date >= $${p++}`; params.push(from); }
    if (to)   { sql += ` AND je.date <= $${p++}`; params.push(to); }
    sql += ` GROUP BY je.date ORDER BY je.date`;
    const r = await query(sql, params);
    const periods = r.rows.map(row => ({
      date: row.date, cash_in: parseFloat(row.cash_in || 0), cash_out: parseFloat(row.cash_out || 0)
    }));
    const cashIn  = periods.reduce((s, p) => s + p.cash_in, 0);
    const cashOut = periods.reduce((s, p) => s + p.cash_out, 0);
    res.json({ cashIn, cashOut, cash_in: cashIn, cash_out: cashOut, net: cashIn - cashOut, periods });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/income-statement — revenues - costs - expenses by period
app.get('/api/income-statement', authMiddleware, async (req, res) => {
  try {
    let { from, to, month, year } = req.query;
    // Support ?month=0-based&year=YYYY
    if (!from && month !== undefined && year !== undefined) {
      const m = parseInt(month) + 1;
      const y = parseInt(year);
      from = `${y}-${String(m).padStart(2,'0')}-01`;
      const lastDay = new Date(y, m, 0).getDate();
      to   = `${y}-${String(m).padStart(2,'0')}-${lastDay}`;
    }
    let params = [req.userId];
    let p = 2;
    let where = `WHERE a.user_id=$1`;
    if (from) { where += ` AND je.date >= $${p++}`; params.push(from); }
    if (to)   { where += ` AND je.date <= $${p++}`; params.push(to); }

    const r = await query(
      `SELECT a.type, a.class, a.name, a.code,
              SUM(jl.debit) as debit, SUM(jl.credit) as credit
       FROM journal_lines jl
       JOIN journal_entries je ON je.id = jl.journal_entry_id
       JOIN accounts a ON a.id = jl.account_id
       ${where}
       GROUP BY a.type, a.class, a.name, a.code
       ORDER BY a.class, a.code`,
      params
    );

    let income = 0, costs = 0, expenses = 0;

    if (r.rows.length > 0) {
      const byClass = {};
      for (const row of r.rows) {
        if (!byClass[row.class]) byClass[row.class] = { debit: 0, credit: 0 };
        byClass[row.class].debit  += parseFloat(row.debit || 0);
        byClass[row.class].credit += parseFloat(row.credit || 0);
      }
      income   = (byClass[4]?.credit || 0) - (byClass[4]?.debit || 0);
      costs    = (byClass[5]?.debit || 0) - (byClass[5]?.credit || 0);
      expenses = (byClass[6]?.debit || 0) - (byClass[6]?.credit || 0);
    } else {
      // Fallback: use transactions table when no journal entries exist
      let txWhere = `WHERE user_id=$1`;
      const txParams = [req.userId];
      let tp = 2;
      if (from) { txWhere += ` AND tx_date >= $${tp++}`; txParams.push(from); }
      if (to)   { txWhere += ` AND tx_date <= $${tp++}`; txParams.push(to); }
      const txr = await query(
        `SELECT type, SUM(amount) as total FROM transactions ${txWhere} GROUP BY type`, txParams
      );
      income   = parseFloat(txr.rows.find(rr => rr.type === 'ingreso')?.total || 0);
      expenses = parseFloat(txr.rows.find(rr => rr.type === 'egreso')?.total  || 0);
    }

    res.json({
      revenues: income,   // alias used by some frontend versions
      income,             // primary key
      costs,
      expenses,
      gastos: expenses,   // Spanish alias
      costos: costs,
      ingresos: income,
      net_income: income - costs - expenses,
      detail: r.rows.map(row => ({
        code: row.code, name: row.name, type: row.type, class: row.class,
        debit: parseFloat(row.debit), credit: parseFloat(row.credit)
      }))
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/balance — balance sheet (assets = liabilities + equity)
app.get('/api/balance', authMiddleware, async (req, res) => {
  try {
    const r = await query(
      `SELECT a.type, a.class, a.code, a.name, COALESCE(ab.balance, 0) as balance
       FROM accounts a
       LEFT JOIN account_balances ab ON ab.account_id = a.id
       WHERE a.user_id=$1 AND a.is_active=TRUE
       ORDER BY a.class, a.code`,
      [req.userId]
    );
    let assets = 0, liabilities = 0, equity = 0, income = 0, costs = 0, expenses = 0;
    const detail = [], assetList = [], liabilityList = [];
    for (const row of r.rows) {
      const bal = parseFloat(row.balance);
      const item = { code: row.code, name: row.name, type: row.type, balance: bal };
      detail.push(item);
      if (row.type === 'asset')     { assets      += bal; assetList.push(item); }
      if (row.type === 'liability') { liabilities += bal; liabilityList.push(item); }
      if (row.type === 'equity')    equity      += bal;
      if (row.type === 'income')    income      += bal;
      if (row.type === 'cost')      costs       += bal;
      if (row.type === 'expense')   expenses    += bal;
    }
    equity += income - costs - expenses;
    res.json({
      assets, liabilities, equity,
      assetList, liabilityList,
      balanced: Math.abs(assets - (liabilities + equity)) < 0.01,
      detail
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── EXPORT endpoints (CSV) ──────────────────────────────────────────────────
function toCSV(headers, rows) {
  const escape = (v) => {
    if (v == null) return '';
    const s = String(v).replace(/"/g, '""');
    return s.includes(',') || s.includes('"') || s.includes('\n') ? `"${s}"` : s;
  };
  const h = headers.map(escape).join(',');
  const lines = rows.map(r => headers.map(col => escape(r[col])).join(','));
  return [h, ...lines].join('\n');
}

function csvHeaders(filename) {
  return {
    'Content-Type': 'text/csv; charset=utf-8',
    'Content-Disposition': `attachment; filename="${filename}"`,
  };
}

// GET /api/export/accounts
app.get('/api/export/accounts', authMiddleware, async (req, res) => {
  try {
    const r = await query(
      `SELECT code, name, type, class, currency, is_system,
              COALESCE(ab.balance, 0) as balance
       FROM accounts a
       LEFT JOIN account_balances ab ON ab.account_id = a.id
       WHERE a.user_id=$1 AND a.is_active=TRUE
       ORDER BY a.class, a.code`,
      [req.userId]
    );
    const tipoMap = { asset: 'Activo', liability: 'Pasivo', equity: 'Patrimonio', income: 'Ingreso', cost: 'Costo', expense: 'Gasto' };
    const rows = r.rows.map(row => ({ ...row, type: tipoMap[row.type] || row.type, balance: Number(row.balance).toFixed(2) }));
    res.set(csvHeaders('plan_cuentas.csv'));
    res.send(toCSV(['code', 'name', 'type', 'class', 'currency', 'balance'], rows));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/export/clients
app.get('/api/export/clients', authMiddleware, async (req, res) => {
  try {
    const r = await query(`SELECT name, phone, email, address, notes, created_at FROM clients WHERE user_id=$1 ORDER BY name`, [req.userId]);
    const rows = r.rows.map(row => ({ ...row, created_at: row.created_at ? new Date(row.created_at).toLocaleDateString('es-DO') : '' }));
    res.set(csvHeaders('clientes.csv'));
    res.send(toCSV(['name', 'phone', 'email', 'address', 'notes', 'created_at'], rows));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/export/vendors
app.get('/api/export/vendors', authMiddleware, async (req, res) => {
  try {
    const r = await query(`SELECT name, vendor_type, phone, email, address, notes, created_at FROM vendors WHERE user_id=$1 ORDER BY name`, [req.userId]);
    const tipoMap = { vendor: 'Proveedor', credit_card: 'Tarjeta de Crédito', loan: 'Préstamo', other: 'Otro' };
    const rows = r.rows.map(row => ({ ...row, vendor_type: tipoMap[row.vendor_type] || row.vendor_type, created_at: row.created_at ? new Date(row.created_at).toLocaleDateString('es-DO') : '' }));
    res.set(csvHeaders('proveedores.csv'));
    res.send(toCSV(['name', 'vendor_type', 'phone', 'email', 'address', 'notes', 'created_at'], rows));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/export/receivables
app.get('/api/export/receivables', authMiddleware, async (req, res) => {
  try {
    const r = await query(
      `SELECT c.name as client, r.description, r.total_amount, r.paid_amount, r.total_amount - r.paid_amount as pending, r.due_date, r.status, r.created_at
       FROM receivables r JOIN clients c ON c.id = r.client_id
       WHERE r.user_id=$1 ORDER BY r.due_date NULLS LAST`,
      [req.userId]
    );
    const rows = r.rows.map(row => ({
      ...row,
      total_amount: Number(row.total_amount).toFixed(2),
      paid_amount: Number(row.paid_amount).toFixed(2),
      pending: Number(row.pending).toFixed(2),
      due_date: row.due_date ? new Date(row.due_date).toLocaleDateString('es-DO') : '',
      created_at: row.created_at ? new Date(row.created_at).toLocaleDateString('es-DO') : ''
    }));
    const statusMap = { pending: 'Pendiente', partial: 'Parcial', paid: 'Pagado', cancelled: 'Cancelado' };
    rows.forEach(row => { row.status = statusMap[row.status] || row.status; });
    res.set(csvHeaders('cuentas_cobrar.csv'));
    res.send(toCSV(['client', 'description', 'total_amount', 'paid_amount', 'pending', 'due_date', 'status', 'created_at'], rows));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/export/payables
app.get('/api/export/payables', authMiddleware, async (req, res) => {
  try {
    const r = await query(
      `SELECT v.name as vendor, p.description, p.total_amount, p.paid_amount, p.total_amount - p.paid_amount as pending, p.due_date, p.status, p.created_at
       FROM payables p JOIN vendors v ON v.id = p.vendor_id
       WHERE p.user_id=$1 ORDER BY p.due_date NULLS LAST`,
      [req.userId]
    );
    const rows = r.rows.map(row => ({
      ...row,
      total_amount: Number(row.total_amount).toFixed(2),
      paid_amount: Number(row.paid_amount).toFixed(2),
      pending: Number(row.pending).toFixed(2),
      due_date: row.due_date ? new Date(row.due_date).toLocaleDateString('es-DO') : '',
      created_at: row.created_at ? new Date(row.created_at).toLocaleDateString('es-DO') : ''
    }));
    const statusMap = { pending: 'Pendiente', partial: 'Parcial', paid: 'Pagado', cancelled: 'Cancelado' };
    rows.forEach(row => { row.status = statusMap[row.status] || row.status; });
    res.set(csvHeaders('cuentas_pagar.csv'));
    res.send(toCSV(['vendor', 'description', 'total_amount', 'paid_amount', 'pending', 'due_date', 'status', 'created_at'], rows));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/export/journal
app.get('/api/export/journal', authMiddleware, async (req, res) => {
  try {
    const r = await query(
      `SELECT je.date, je.description as entry_desc, je.ref_type, je.ref_id,
              jl.account_code, jl.account_name, jl.debit, jl.credit, jl.memo
       FROM journal_entries je
       JOIN journal_lines jl ON jl.journal_entry_id = je.id
       WHERE je.user_id=$1
       ORDER BY je.date DESC, je.created_at DESC`,
      [req.userId]
    );
    const rows = r.rows.map(row => ({
      ...row,
      date: new Date(row.date).toLocaleDateString('es-DO'),
      debit: Number(row.debit || 0).toFixed(2),
      credit: Number(row.credit || 0).toFixed(2)
    }));
    res.set(csvHeaders('diario_contable.csv'));
    res.send(toCSV(['date', 'entry_desc', 'ref_type', 'ref_id', 'account_code', 'account_name', 'debit', 'credit', 'memo'], rows));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/export/transactions
app.get('/api/export/transactions', authMiddleware, async (req, res) => {
  try {
    const r = await query(
      `SELECT tx_date, type, category, description, amount, account, created_at
       FROM transactions WHERE user_id=$1 ORDER BY tx_date DESC`,
      [req.userId]
    );
    const tipoMap = { ingreso: 'Ingreso', egreso: 'Egreso' };
    const accountMap = { efectivo: 'Efectivo', banco: 'Banco', tarjeta: 'Tarjeta' };
    const rows = r.rows.map(row => ({
      ...row,
      type: tipoMap[row.type] || row.type,
      account: accountMap[row.account] || row.account,
      amount: Number(row.amount).toFixed(2),
      tx_date: new Date(row.tx_date).toLocaleDateString('es-DO'),
      created_at: row.created_at ? new Date(row.created_at).toLocaleDateString('es-DO') : ''
    }));
    res.set(csvHeaders('transacciones.csv'));
    res.send(toCSV(['tx_date', 'type', 'category', 'description', 'amount', 'account', 'created_at'], rows));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Registro del webhook (llamar una vez desde Railway shell o al startup)
app.post('/setup-webhook', async (req, res) => {
  const secret = req.headers['x-setup-secret'] || req.query.secret;
  if (CRON_SECRET && secret !== CRON_SECRET) return res.status(403).json({ error: 'forbidden' });
  try {
    const base = req.body.base_url || `https://${req.headers.host}`;
    const r    = await setWebhook(base);
    res.json(r);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── INIT DB — crea tablas automáticamente si no existen ──────────────────────
async function initDB() {
  console.log('🗄️  Initializing database schema...');

  // Eliminar índice problemático si existe (de versiones anteriores)
  try {
    await query(`DROP INDEX IF EXISTS idx_tx_user_month`);
  } catch(e) { /* ignorar */ }

  // Tablas — cada una en try/catch para no fallar si ya existen con diferencias
  const tables = [
    `CREATE TABLE IF NOT EXISTS users (
      id          TEXT PRIMARY KEY,
      registered  BOOLEAN NOT NULL DEFAULT TRUE,
      lang        TEXT NOT NULL DEFAULT 'es',
      is_admin    BOOLEAN NOT NULL DEFAULT FALSE,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS transactions (
      id          TEXT PRIMARY KEY,
      user_id     TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      type        TEXT NOT NULL CHECK (type IN ('ingreso','egreso')),
      amount      NUMERIC(12,2) NOT NULL CHECK (amount > 0),
      description TEXT NOT NULL,
      category    TEXT NOT NULL DEFAULT 'otro',
      account     TEXT NOT NULL DEFAULT 'efectivo'
                  CHECK (account IN ('efectivo','banco','tarjeta')),
      tx_date     DATE NOT NULL,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS budgets (
      user_id     TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      category    TEXT NOT NULL,
      amount      NUMERIC(12,2) NOT NULL CHECK (amount > 0),
      PRIMARY KEY (user_id, category)
    )`,
    `CREATE TABLE IF NOT EXISTS pending_tx (
      user_id     TEXT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
      tx_data     JSONB NOT NULL,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`,

    // Auth tokens para Telegram OAuth (tabla persistente, no se pierde en restart)
    `CREATE TABLE IF NOT EXISTS auth_tokens (
      token        TEXT PRIMARY KEY,
      telegram_id  TEXT NOT NULL,
      session_token TEXT NOT NULL,
      created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`,

    // ── Chart of accounts (clases 1-6) ──
    `CREATE TABLE IF NOT EXISTS accounts (
      id          TEXT PRIMARY KEY,
      user_id     TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      code        TEXT NOT NULL,
      name        TEXT NOT NULL,
      type        TEXT NOT NULL CHECK (type IN ('asset','liability','equity','income','cost','expense')),
      class       INTEGER NOT NULL CHECK (class BETWEEN 1 AND 6),
      currency    TEXT NOT NULL DEFAULT 'DOP',
      is_system   BOOLEAN NOT NULL DEFAULT FALSE,
      is_active   BOOLEAN NOT NULL DEFAULT TRUE,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE(user_id, code)
    )`,

    // ── Journal entries (asientos contables) ──
    `CREATE TABLE IF NOT EXISTS journal_entries (
      id          TEXT PRIMARY KEY,
      user_id     TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      date        DATE NOT NULL DEFAULT CURRENT_DATE,
      description TEXT NOT NULL,
      ref_type    TEXT,
      ref_id      TEXT,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`,

    // ── Journal lines (partidas doble) ──
    `CREATE TABLE IF NOT EXISTS journal_lines (
      id              TEXT PRIMARY KEY,
      journal_entry_id TEXT NOT NULL REFERENCES journal_entries(id) ON DELETE CASCADE,
      account_id      TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
      debit           NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (debit >= 0),
      credit          NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (credit >= 0),
      auxiliary_type  TEXT,
      auxiliary_id    TEXT,
      auxiliary_name  TEXT,
      CHECK (debit > 0 OR credit > 0),
      CHECK (debit = 0 OR credit = 0)
    )`,

    // ── Running balances on accounts ──
    `CREATE TABLE IF NOT EXISTS account_balances (
      account_id  TEXT PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
      balance     NUMERIC(14,2) NOT NULL DEFAULT 0
    )`,

    // ── Clients (clientes para cuentas por cobrar) ──
    `CREATE TABLE IF NOT EXISTS clients (
      id          TEXT PRIMARY KEY,
      user_id     TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      name        TEXT NOT NULL,
      phone       TEXT,
      email       TEXT,
      address     TEXT,
      notes       TEXT,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`,

    // ── Receivables (cuentas por cobrar) ──
    `CREATE TABLE IF NOT EXISTS receivables (
      id              TEXT PRIMARY KEY,
      user_id         TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      client_id       TEXT NOT NULL REFERENCES clients(id) ON DELETE RESTRICT,
      description     TEXT NOT NULL,
      total_amount    NUMERIC(12,2) NOT NULL CHECK (total_amount > 0),
      paid_amount     NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (paid_amount >= 0),
      due_date        DATE,
      status          TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','partial','paid','cancelled')),
      created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`,

    // ── Receivable payments (pagos de clientes) ──
    `CREATE TABLE IF NOT EXISTS receivable_payments (
      id            TEXT PRIMARY KEY,
      receivable_id TEXT NOT NULL REFERENCES receivables(id) ON DELETE CASCADE,
      amount        NUMERIC(12,2) NOT NULL CHECK (amount > 0),
      payment_date  DATE NOT NULL DEFAULT CURRENT_DATE,
      notes         TEXT,
      created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`,

    // ── Vendors (proveedores + tarjetas de crédito) ──
    `CREATE TABLE IF NOT EXISTS vendors (
      id          TEXT PRIMARY KEY,
      user_id     TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      name        TEXT NOT NULL,
      vendor_type TEXT NOT NULL DEFAULT 'vendor' CHECK (vendor_type IN ('vendor','credit_card','loan','other')),
      phone       TEXT,
      email       TEXT,
      address     TEXT,
      notes       TEXT,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`,

    // ── Payables (cuentas por pagar) ──
    `CREATE TABLE IF NOT EXISTS payables (
      id              TEXT PRIMARY KEY,
      user_id         TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      vendor_id       TEXT NOT NULL REFERENCES vendors(id) ON DELETE RESTRICT,
      description     TEXT NOT NULL,
      total_amount    NUMERIC(12,2) NOT NULL CHECK (total_amount > 0),
      paid_amount     NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (paid_amount >= 0),
      due_date        DATE,
      status          TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','partial','paid','cancelled')),
      created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`,

    // ── Payable payments (pagos a proveedores) ──
    `CREATE TABLE IF NOT EXISTS payable_payments (
      id            TEXT PRIMARY KEY,
      payable_id    TEXT NOT NULL REFERENCES payables(id) ON DELETE CASCADE,
      amount        NUMERIC(12,2) NOT NULL CHECK (amount > 0),
      payment_date  DATE NOT NULL DEFAULT CURRENT_DATE,
      notes         TEXT,
      created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`,

    // ── Plain-text credentials auth (username + PBKDF2 password) ──
    `CREATE TABLE IF NOT EXISTS user_credentials (
      user_id       TEXT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
      username     TEXT UNIQUE NOT NULL,
      password_hash TEXT NOT NULL,
      created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`,

    // ── Income types ──
    `CREATE TABLE IF NOT EXISTS income_types (
      id          TEXT PRIMARY KEY,
      user_id     TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      name        TEXT NOT NULL,
      description TEXT,
      icon        TEXT DEFAULT '💰',
      color       TEXT DEFAULT '#00e5a0',
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS income_records (
      id          TEXT PRIMARY KEY,
      user_id     TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      type_id     TEXT REFERENCES income_types(id),
      amount      NUMERIC(12,2) NOT NULL CHECK (amount > 0),
      description TEXT,
      date        DATE NOT NULL DEFAULT CURRENT_DATE,
      reference   TEXT,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`,

    // ── Products (product catalog, separate from inventory movements) ──
    `CREATE TABLE IF NOT EXISTS products (
      id          TEXT PRIMARY KEY,
      user_id     TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      code        TEXT,
      name        TEXT NOT NULL,
      description TEXT,
      category    TEXT DEFAULT 'General',
      unit        TEXT DEFAULT 'unidad',
      cost_price  NUMERIC(12,2) DEFAULT 0,
      sale_price  NUMERIC(12,2) DEFAULT 0,
      stock_minimum NUMERIC(12,2) DEFAULT 0,
      stock_current NUMERIC(12,2) DEFAULT 0,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`,

    // ── Invoices ──
    `CREATE TABLE IF NOT EXISTS invoices (
      id              TEXT PRIMARY KEY,
      user_id         TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      invoice_number  TEXT NOT NULL,
      client_name     TEXT,
      client_rnc      TEXT,
      client_address  TEXT,
      subtotal        NUMERIC(12,2) DEFAULT 0,
      tax             NUMERIC(12,2) DEFAULT 0,
      total           NUMERIC(12,2) DEFAULT 0,
      status          TEXT DEFAULT 'pending' CHECK (status IN ('pending','paid','cancelled')),
      date            DATE NOT NULL DEFAULT CURRENT_DATE,
      due_date        DATE,
      notes           TEXT,
      created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS invoice_items (
      id          TEXT PRIMARY KEY,
      invoice_id  TEXT NOT NULL REFERENCES invoices(id) ON DELETE CASCADE,
      description TEXT NOT NULL,
      qty         NUMERIC(12,2) NOT NULL DEFAULT 1,
      price       NUMERIC(12,2) NOT NULL,
      total       NUMERIC(12,2) NOT NULL
    )`,
    `CREATE TABLE IF NOT EXISTS invoice_counter (
      user_id     TEXT PRIMARY KEY,
      last_number INTEGER NOT NULL DEFAULT 0
    )`,

    // ── Fixed Assets ──
    `CREATE TABLE IF NOT EXISTS fixed_assets (
      id            TEXT PRIMARY KEY,
      user_id       TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      name          TEXT NOT NULL,
      description   TEXT,
      category      TEXT DEFAULT 'General',
      purchase_date DATE,
      purchase_value NUMERIC(12,2) NOT NULL DEFAULT 0,
      useful_life_years INTEGER DEFAULT 5,
      salvage_value  NUMERIC(12,2) DEFAULT 0,
      depreciacion_metodo TEXT DEFAULT 'linea_recta',
      created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS asset_depreciation (
      id          TEXT PRIMARY KEY,
      asset_id    TEXT NOT NULL REFERENCES fixed_assets(id) ON DELETE CASCADE,
      period      TEXT NOT NULL,
      amount      NUMERIC(12,2) NOT NULL,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`,

    // ── Inventory ──
    `CREATE TABLE IF NOT EXISTS inventory_products (
      id          TEXT PRIMARY KEY,
      user_id     TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      code        TEXT,
      name        TEXT NOT NULL,
      category    TEXT DEFAULT 'General',
      unit        TEXT DEFAULT 'unidad',
      cost_price  NUMERIC(12,2) DEFAULT 0,
      sell_price  NUMERIC(12,2) DEFAULT 0,
      min_stock   NUMERIC(12,2) DEFAULT 0,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS inventory_movements (
      id          TEXT PRIMARY KEY,
      user_id     TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      product_id  TEXT NOT NULL REFERENCES inventory_products(id) ON DELETE CASCADE,
      type        TEXT NOT NULL CHECK (type IN ('entry','exit','adjustment')),
      quantity    NUMERIC(12,2) NOT NULL,
      unit_cost   NUMERIC(12,2),
      reference   TEXT,
      notes       TEXT,
      mov_date    DATE NOT NULL DEFAULT CURRENT_DATE,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`,
  ];

  for (const sql of tables) {
    try { await query(sql); }
    catch(e) { console.warn('Table warning (ignored):', e.message); }
  }

  // Índice simple — sin funciones, siempre IMMUTABLE
  try {
    await query(`CREATE INDEX IF NOT EXISTS idx_tx_user_date ON transactions(user_id, tx_date)`);
  } catch(e) { console.warn('Index warning (ignored):', e.message); }

  // Indices para accounting
  try { await query(`CREATE INDEX IF NOT EXISTS idx_journal_user_date ON journal_entries(user_id, date)`); } catch(e) {}
  try { await query(`CREATE INDEX IF NOT EXISTS idx_journal_lines_entry ON journal_lines(journal_entry_id)`); } catch(e) {}
  try { await query(`CREATE INDEX IF NOT EXISTS idx_receivables_user ON receivables(user_id)`); } catch(e) {}
  try { await query(`CREATE INDEX IF NOT EXISTS idx_payables_user ON payables(user_id)`); } catch(e) {}

  // Migration: add is_admin column if not exists
  try { await query(`ALTER TABLE users ADD COLUMN is_admin BOOLEAN NOT NULL DEFAULT FALSE`); } catch(e) {}
  try { await query(`ALTER TABLE journal_lines ADD COLUMN IF NOT EXISTS auxiliary_type TEXT`); } catch(e) {}
  try { await query(`ALTER TABLE journal_lines ADD COLUMN IF NOT EXISTS auxiliary_id TEXT`); } catch(e) {}
  try { await query(`ALTER TABLE journal_lines ADD COLUMN IF NOT EXISTS auxiliary_name TEXT`); } catch(e) {}
  // Migrations for invoices
  try { await query(`ALTER TABLE invoices ADD COLUMN IF NOT EXISTS date DATE NOT NULL DEFAULT CURRENT_DATE`); } catch(e) {}
  try { await query(`ALTER TABLE invoices ADD COLUMN IF NOT EXISTS subtotal NUMERIC(12,2) DEFAULT 0`); } catch(e) {}
  try { await query(`ALTER TABLE invoices ADD COLUMN IF NOT EXISTS tax NUMERIC(12,2) DEFAULT 0`); } catch(e) {}
  try { await query(`ALTER TABLE invoices ADD COLUMN IF NOT EXISTS client_rnc TEXT`); } catch(e) {}
  try { await query(`ALTER TABLE invoices ADD COLUMN IF NOT EXISTS client_address TEXT`); } catch(e) {}
  try { await query(`ALTER TABLE invoice_items ADD COLUMN IF NOT EXISTS qty NUMERIC(12,2) NOT NULL DEFAULT 1`); } catch(e) {}
  // Migrations for products
  try { await query(`ALTER TABLE products ADD COLUMN IF NOT EXISTS stock_current NUMERIC(12,2) DEFAULT 0`); } catch(e) {}
  // Migrations for fixed_assets
  try { await query(`ALTER TABLE fixed_assets ADD COLUMN IF NOT EXISTS depreciacion_metodo TEXT DEFAULT 'linea_recta'`); } catch(e) {}

  // Make first registered user an admin
  try {
    const r = await query(`SELECT id FROM users ORDER BY created_at ASC LIMIT 1`);
    if (r.rows.length > 0) {
      await query(`UPDATE users SET is_admin=TRUE WHERE id=$1`, [r.rows[0].id]);
    }
  } catch(e) {}

  console.log('✅  Database schema ready');
}

// ─── START ────────────────────────────────────────────────────────────────────
async function start() {
  try {
    await initDB();
  } catch (e) {
    console.error('❌  initDB failed:', e.message);
    process.exit(1);
  }

  app.listen(PORT, '0.0.0.0', () => {
    console.log(`\n========================================`);
    console.log(`✅  MisCuentas v2 started`);
    console.log(`🌐  Port       : ${PORT}`);
    console.log(`🗄️  Database   : PostgreSQL (Railway)`);
    console.log(`📸  Vision     : ${GROQ_API_KEY   ? 'Groq ✅'   : '❌ GROQ_API_KEY missing'}`);
    console.log(`🧠  AI Parser  : ${GEMINI_API_KEY ? 'Gemini ✅' : 'Fallback only'}`);
    console.log(`🔔  Webhook    : POST /webhook/${WEBHOOK_SECRET || 'tg'}`);
    console.log(`========================================\n`);
  });
}

// Error handler
app.use((err, req, res, next) => {
  console.error('Server error:', err.message);
  res.status(500).json({ error: 'Internal server error' });
});

start();

process.on('SIGTERM', () => { pool.end(); process.exit(0); });
process.on('SIGINT',  () => { pool.end(); process.exit(0); });

