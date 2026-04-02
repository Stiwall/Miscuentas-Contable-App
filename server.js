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
// Serve static PWA files
const path = require('path');
app.use(express.static(__dirname, {
  index: false,
  setHeaders: (res, filepath) => {
    const ext = path.extname(filepath);
    if (['.png','.json','.js'].includes(ext)) {
      res.setHeader('Content-Security-Policy', "default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline' 'unsafe-eval'; img-src 'self' data: blob:; font-src 'self'; connect-src 'self' https://miscuentas-contable-app-production.up.railway.app https://api.minimax.io https://cdnjs.cloudflare.com https://openapi.baidu.com;");
    }
  }
}));

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
      const hash = crypto.pbkdf2Sync(password, username.toLowerCase(), 100000, 64, 'sha512').toString('hex');
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
      const hash = crypto.pbkdf2Sync(password, username.toLowerCase(), 100000, 64, 'sha512').toString('hex');
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
    // Try lowercase first, then original (for backward compat)
    let hash = crypto.pbkdf2Sync(password, username.toLowerCase(), 100000, 64, 'sha512').toString('hex');
    if (hash !== storedHash) {
      hash = crypto.pbkdf2Sync(password, username, 100000, 64, 'sha512').toString('hex');
    }
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
    const { status } = req.query;
    let sql = `SELECT * FROM invoices WHERE user_id=$1`;
    const params = [req.userId];
    if (status) { sql += ` AND status=$2`; params.push(status); }
    sql += ` ORDER BY date DESC, created_at DESC LIMIT $${params.length+1}`;
    params.push(limit);
    const r = await query(sql, params);
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/invoices/next-number', authMiddleware, async (req, res) => {
  try {
    // Use MAX of actual invoices to avoid duplicates
    const r = await query(
      `SELECT GREATEST(
         COALESCE((SELECT last_number FROM invoice_counter WHERE user_id=$1), 0),
         COALESCE((SELECT MAX(CAST(REGEXP_REPLACE(invoice_number,'[^0-9]','','g') AS INTEGER)) FROM invoices WHERE user_id=$1 AND invoice_number ~ '^[0-9]+$'), 0)
       ) AS last_num`,
      [req.userId]
    );
    const next = (parseInt(r.rows[0]?.last_num) || 0) + 1;
    // Update counter to stay in sync
    await query(`INSERT INTO invoice_counter(user_id,last_number) VALUES($1,$2) ON CONFLICT(user_id) DO UPDATE SET last_number=$2`,
      [req.userId, next]);
    res.json({ invoice_number: String(next).padStart(6, '0') });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── HELPER: registrar asiento contable dentro de una transacción PG ─────────
async function insertJournalEntry(pgClient, userId, date, description, refType, refId, lines) {
  const q = pgClient
    ? (sql, params) => pgClient.query(sql, params)
    : (sql, params) => query(sql, params);

  const jeId = `je_${Date.now()}_${Math.random().toString(36).substr(2,8)}`;
  await q(
    `INSERT INTO journal_entries(id,user_id,date,description,ref_type,ref_id) VALUES($1,$2,$3,$4,$5,$6)`,
    [jeId, userId, date, description, refType, refId]
  );
  for (const ln of lines) {
    const lnId = `jl_${Date.now()}_${Math.random().toString(36).substr(2,8)}`;
    await q(
      `INSERT INTO journal_lines(id,journal_entry_id,account_id,debit,credit,auxiliary_type,auxiliary_id,auxiliary_name)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
      [lnId, jeId, ln.acct, ln.d||0, ln.c||0, ln.auxType||null, ln.auxId||null, ln.auxName||null]
    );
    await q(
      `INSERT INTO account_balances(account_id,balance) VALUES($1,$2)
       ON CONFLICT(account_id) DO UPDATE SET balance=account_balances.balance+$2`,
      [ln.acct, (ln.d||0)-(ln.c||0)]
    );
  }
  return jeId;
}

// ─── HELPER: calcular y registrar CMV de los ítems de una factura ──────────
async function processCMVForInvoice(pgClient, userId, invoiceId, invoiceNumber, rawItems, invoiceDate) {
  const q = pgClient
    ? (sql, params) => pgClient.query(sql, params)
    : (sql, params) => query(sql, params);

  // Buscar cuentas CMV e Inventario (con múltiples códigos posibles según plan)
  const acctR = await q(
    `SELECT id, code, name FROM accounts
     WHERE user_id=$1 AND code IN ('5101','5.1.01','5102','5.1.02','1103','1.1.03','1301','1.3.01')
     ORDER BY code`,
    [userId]
  );
  const acctMap = {};
  acctR.rows.forEach(a => { acctMap[a.code] = { id: a.id, name: a.name }; });

  // CMV: preferir 5101 o 5.1.01
  const cmvAcct = acctMap['5101'] || acctMap['5.1.01'] || acctMap['5102'] || acctMap['5.1.02'];
  // Inventario: preferir 1103 o 1.1.03 o 1301
  const invAcct = acctMap['1103'] || acctMap['1.1.03'] || acctMap['1301'] || acctMap['1.3.01'];

  let totalCMV = 0;
  const cmvDetails = [];

  for (const item of rawItems) {
    if (!item.product_id) continue;
    const qty = parseFloat(item.quantity || item.qty || 1);
    if (qty <= 0) continue;

    const prodR = await q(
      `SELECT id, name, cost_price, stock_current FROM products WHERE id=$1 AND user_id=$2`,
      [item.product_id, userId]
    );
    if (!prodR.rows[0]) continue;
    const prod = prodR.rows[0];
    const costPrice = parseFloat(prod.cost_price || 0);
    if (costPrice <= 0) continue;

    const stockActual = parseFloat(prod.stock_current || 0);
    const lineCMV = Math.round(qty * costPrice * 100) / 100;
    totalCMV += lineCMV;

    // Reducir stock en tabla products
    const newStock = Math.max(0, stockActual - qty);
    await q(
      `UPDATE products SET stock_current=$1 WHERE id=$2 AND user_id=$3`,
      [newStock, item.product_id, userId]
    );

    // Registrar salida en inventory_movements con el product_id de la tabla products
    const movId = `mov_inv_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
    await q(
      `INSERT INTO inventory_movements(id,user_id,product_id,type,quantity,unit_cost,reference,notes,mov_date,reason)
       VALUES($1,$2,$3,'exit',$4,$5,$6,$7,$8,'sale')`,
      [movId, userId, item.product_id, qty, costPrice,
       `FAC-${invoiceNumber}`, `CMV automático Factura ${invoiceNumber}`, invoiceDate]
    );

    cmvDetails.push({ product: prod.name, qty, cost: costPrice, cmv: lineCMV, stockBefore: stockActual, stockAfter: newStock });
  }

  // Asiento CMV: Débito CMV / Crédito Inventario
  if (totalCMV > 0 && cmvAcct && invAcct) {
    await insertJournalEntry(pgClient, userId, invoiceDate,
      `CMV Factura ${invoiceNumber} — Costo de mercancía vendida`,
      'cmv', invoiceId,
      [
        { acct: cmvAcct.id, d: totalCMV, c: 0 },   // Débito CMV (costo)
        { acct: invAcct.id, d: 0, c: totalCMV },    // Crédito Inventario (activo baja)
      ]
    );
  }

  return { totalCMV, cmvDetails, hasCMVAccounts: !!(cmvAcct && invAcct) };
}

app.post('/api/invoices', authMiddleware, async (req, res) => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const {
      invoice_number, client_name, client_rnc, client_address,
      subtotal, tax, total, discount_amount, discount_pct,
      status, date, due_date, notes, items
    } = req.body;

    if (!total) { await client.query('ROLLBACK'); return res.status(400).json({ error: 'total es requerido' }); }

    // ── Resolver número de factura ──
    let resolvedNum = invoice_number;
    if (!resolvedNum) {
      const cntR = await client.query(`SELECT last_number FROM invoice_counter WHERE user_id=$1`, [req.userId]);
      resolvedNum = String((parseInt(cntR.rows[0]?.last_number) || 0) + 1).padStart(6, '0');
    } else {
      const dup = await client.query(`SELECT id FROM invoices WHERE user_id=$1 AND invoice_number=$2`, [req.userId, invoice_number]);
      if (dup.rows[0]) {
        const cntR = await client.query(`SELECT last_number FROM invoice_counter WHERE user_id=$1`, [req.userId]);
        resolvedNum = String((parseInt(cntR.rows[0]?.last_number) || 0) + 1).padStart(6, '0');
      }
    }

    const invId = `inv_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
    const invDate = date || new Date().toISOString().split('T')[0];
    const invStatus = status || 'draft';

    await client.query(
      `INSERT INTO invoices(id,user_id,invoice_number,client_name,client_rnc,client_address,
        subtotal,tax,total,discount_amount,discount_pct,status,date,due_date,notes,payment_method)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
      [invId, req.userId, resolvedNum, client_name||null, client_rnc||null, client_address||null,
       subtotal||0, tax||0, total, discount_amount||0, discount_pct||0,
       invStatus, invDate, due_date||null, notes||null, req.body.payment_method||'credit']
    );

    // Actualizar contador
    await client.query(
      `INSERT INTO invoice_counter(user_id,last_number) VALUES($1,$2)
       ON CONFLICT(user_id) DO UPDATE SET last_number=$2`,
      [req.userId, parseInt(resolvedNum) || 1]
    );

    // ── Insertar ítems ──
    const rawItems = req.body.lines || items || [];
    for (const item of rawItems) {
      const itemId = `item_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
      const qty      = parseFloat(item.quantity || item.qty || 1);
      const price    = parseFloat(item.unit_price || item.price || 0);
      const disc     = parseFloat(item.discount_pct || 0);
      const unitPrice = price * (1 - disc / 100);
      const lineTotal = Math.round(qty * unitPrice * 100) / 100;
      await client.query(
        `INSERT INTO invoice_items(id,invoice_id,description,qty,price,total,discount_pct,product_id)
         VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
        [itemId, invId, item.description||'', qty, unitPrice, lineTotal, disc, item.product_id||null]
      );
    }

    // ══════════════════════════════════════════════════════════════════
    // ── LÓGICA CONTABLE — Solo cuando se emite (status = 'issued') ──
    // ══════════════════════════════════════════════════════════════════
    if (invStatus === 'issued') {
      const totalNum = parseFloat(total);
      const taxNum   = parseFloat(tax || 0);
      const subNum   = parseFloat(subtotal || totalNum);
      const pmeth    = req.body.payment_method || 'credit';
      const payDescs = { cash:'Efectivo', bank:'Transferencia/Banco', card:'Tarjeta', credit:'Crédito' };
      const payDesc  = payDescs[pmeth] || 'Crédito';

      // Buscar cuentas contables (busca códigos del plan de cuentas del usuario)
      const acctR = await client.query(
        `SELECT id, code FROM accounts WHERE user_id=$1
         AND code IN ('1101','1.1.01','1102','1.1.02','1201','1.2.01',
                      '4101','4.1.01','4102','4.1.02','2101','2.1.01',
                      '2201','2.2.01','2102','2.1.02')`,
        [req.userId]
      );
      const am = {}; acctR.rows.forEach(a => { am[a.code] = a.id; });

      // Caja / Banco / CxC / Ventas / ITBIS Cobrado
      const cajaAcct  = am['1101'] || am['1.1.01'];
      const bancoAcct = am['1102'] || am['1.1.02'];
      const cxcAcct   = am['1201'] || am['1.2.01'];
      const salesAcct = am['4101'] || am['4.1.01'] || am['4102'] || am['4.1.02'];
      const itbisAcct = am['2201'] || am['2.2.01'] || am['2102'] || am['2.1.02'];

      if (!salesAcct) {
        await client.query('ROLLBACK');
        return res.status(400).json({
          error: 'Cuenta de Ventas (4101) no encontrada. Ve a Plan de Cuentas → Inicio Rápido para configurarla.'
        });
      }

      // ── Cuenta débito según método de pago ──
      let debitAcct = null;
      if (pmeth === 'cash')              debitAcct = cajaAcct || bancoAcct;
      else if (pmeth === 'bank' || pmeth === 'card') debitAcct = bancoAcct || cajaAcct;
      else                               debitAcct = cxcAcct;  // crédito → CxC

      // ── Asiento #1: Ingreso por venta ──
      if (debitAcct) {
        const jLines = [
          { acct: debitAcct, d: totalNum,  c: 0 },      // Débito: cobro (caja/banco/cxc)
          { acct: salesAcct, d: 0, c: subNum },          // Crédito: ventas (neto sin ITBIS)
        ];
        if (taxNum > 0 && itbisAcct) {
          jLines.push({ acct: itbisAcct, d: 0, c: taxNum }); // Crédito: ITBIS cobrado
        } else if (taxNum > 0) {
          // Si no hay cuenta ITBIS separada, suma a ventas
          jLines[1].c = totalNum;
        }
        await insertJournalEntry(client, req.userId, invDate,
          `Factura ${resolvedNum} — ${client_name||'Cliente'} [${payDesc}]`,
          'invoice', invId, jLines
        );
      }

      // ── CMV automático: Débito CMV / Crédito Inventario ──
      const cmvResult = await processCMVForInvoice(
        client, req.userId, invId, resolvedNum, rawItems, invDate
      );

      // ── CxC si es a crédito ──
      if (pmeth === 'credit') {
        let clientId = null;
        if (client_name) {
          const cl = await client.query(
            `SELECT id FROM clients WHERE user_id=$1 AND name ILIKE $2 LIMIT 1`,
            [req.userId, client_name]
          );
          if (cl.rows[0]) clientId = cl.rows[0].id;
        }
        const cxcRecId = `rec_inv_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
        await client.query(
          `INSERT INTO receivables(id,user_id,client_id,description,total_amount,paid_amount,status,due_date)
           VALUES($1,$2,$3,$4,$5,$6,'pending',$7) ON CONFLICT DO NOTHING`,
          [cxcRecId, req.userId, clientId||null,
           `Factura ${resolvedNum}${client_name?' — '+client_name:''}`,
           totalNum, 0, due_date||null]
        );
      } else {
        // Pago inmediato → marcar pagada
        await client.query(
          `UPDATE invoices SET status='paid', paid_amount=$1 WHERE id=$2`,
          [totalNum, invId]
        );

        // Registro de ingreso para pago inmediato
        const pmLabels = { cash:'💵 Ventas Efectivo', bank:'🏦 Ventas Transferencia', card:'💳 Ventas Tarjeta' };
        const pmLabel  = pmLabels[pmeth] || '💰 Ventas';
        let incTypeId  = null;
        const itR = await client.query(
          `SELECT id FROM income_types WHERE user_id=$1 AND name=$2 LIMIT 1`, [req.userId, pmLabel]
        );
        if (itR.rows[0]) {
          incTypeId = itR.rows[0].id;
        } else {
          incTypeId = `it_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
          const icons = { cash:'💵', bank:'🏦', card:'💳' };
          await client.query(
            `INSERT INTO income_types(id,user_id,name,description,icon,color) VALUES($1,$2,$3,$4,$5,$6)`,
            [incTypeId, req.userId, pmLabel, 'Auto desde facturas', icons[pmeth]||'💰', '#00e5a0']
          );
        }
        const incId = `inc_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
        await client.query(
          `INSERT INTO income_records(id,user_id,income_type_id,amount,description,date,reference)
           VALUES($1,$2,$3,$4,$5,$6,$7)`,
          [incId, req.userId, incTypeId, subNum,
           `Factura ${resolvedNum}${client_name?' — '+client_name:''}`,
           invDate, resolvedNum]
        );
      }

      // ── Guardar resumen de CMV en la factura (columna cmv_amount) ──
      if (cmvResult.totalCMV > 0) {
        await client.query(
          `UPDATE invoices SET cmv_amount=$1 WHERE id=$2`,
          [cmvResult.totalCMV, invId]
        ).catch(() => {}); // columna opcional, ignorar si no existe aún
      }
    }

    await client.query('COMMIT');
    res.json({ ok: true, id: invId, invoice_number: resolvedNum, total });
  } catch(e) {
    await client.query('ROLLBACK');
    console.error('Invoice POST error:', e.message);
    res.status(500).json({ error: e.message });
  } finally {
    client.release();
  }
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
    const prevR = await query(`SELECT * FROM invoices WHERE id=$1 AND user_id=$2`, [req.params.id, req.userId]);
    if (!prevR.rows[0]) return res.status(404).json({ error: 'Invoice not found' });
    const inv = prevR.rows[0];
    const prevStatus = inv.status;

    await query(`UPDATE invoices SET status=$1 WHERE id=$2 AND user_id=$3`, [status, req.params.id, req.userId]);

    // When invoice is ISSUED: create CxC + journal entry + CMV
    if (status === 'issued' && prevStatus === 'draft') {
      const total = parseFloat(inv.total || 0);
      const tax   = parseFloat(inv.tax || 0);
      const sub   = parseFloat(inv.subtotal || total);
      const pmeth = inv.payment_method || 'credit';
      const payDescs = { cash:'Efectivo', bank:'Transferencia', card:'Tarjeta', credit:'Crédito' };
      const payDesc  = payDescs[pmeth] || 'Crédito';

      // Buscar cuentas (soporta ambos formatos de código: 1101 y 1.1.01)
      const acctR = await query(
        `SELECT id, code FROM accounts WHERE user_id=$1
         AND code IN ('1101','1.1.01','1102','1.1.02','1201','1.2.01',
                      '4101','4.1.01','4102','4.1.02','2201','2.2.01','2102','2.1.02')`,
        [req.userId]
      );
      const am = {}; acctR.rows.forEach(a => { am[a.code] = a.id; });
      const cajaAcct  = am['1101'] || am['1.1.01'];
      const bancoAcct = am['1102'] || am['1.1.02'];
      const cxcAcct   = am['1201'] || am['1.2.01'];
      const salesAcct = am['4101'] || am['4.1.01'] || am['4102'] || am['4.1.02'];
      const itbisAcct = am['2201'] || am['2.2.01'] || am['2102'] || am['2.1.02'];

      let debitAcct = null;
      if (pmeth === 'cash')              debitAcct = cajaAcct || bancoAcct;
      else if (pmeth === 'bank' || pmeth === 'card') debitAcct = bancoAcct || cajaAcct;
      else                               debitAcct = cxcAcct;

      // Asiento #1: Ingreso por venta
      if (debitAcct && salesAcct) {
        const jLines = [
          { acct: debitAcct, d: total, c: 0 },
          { acct: salesAcct, d: 0, c: sub },
        ];
        if (tax > 0 && itbisAcct) jLines.push({ acct: itbisAcct, d: 0, c: tax });
        else if (tax > 0) jLines[1].c = total; // sin cuenta ITBIS: suma a ventas
        await insertJournalEntry(null, req.userId, inv.date||new Date().toISOString().split('T')[0],
          `Factura ${inv.invoice_number} — ${inv.client_name||'Cliente'} [${payDesc}]`,
          'invoice', inv.id, jLines
        );
      }

      // Asiento #2: CMV automático
      const invItems = await query(`SELECT * FROM invoice_items WHERE invoice_id=$1`, [inv.id]);
      const cmvResult = await processCMVForInvoice(
        null, req.userId, inv.id, inv.invoice_number,
        invItems.rows.map(r => ({ product_id: r.product_id, quantity: r.qty, unit_price: r.price, discount_pct: r.discount_pct })),
        inv.date || new Date().toISOString().split('T')[0]
      );
      if (cmvResult.totalCMV > 0) {
        await query(`UPDATE invoices SET cmv_amount=$1 WHERE id=$2`, [cmvResult.totalCMV, inv.id]).catch(()=>{});
      }

      // CxC si es a crédito
      if (pmeth === 'credit') {
        let clientId = null;
        if (inv.client_name) {
          const cl = await query(`SELECT id FROM clients WHERE user_id=$1 AND name ILIKE $2 LIMIT 1`, [req.userId, inv.client_name]);
          if (cl.rows[0]) clientId = cl.rows[0].id;
        }
        const cxcId = `rec_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
        if (clientId || inv.client_name) {
          await query(
            `INSERT INTO receivables(id,user_id,client_id,description,total_amount,paid_amount,status,due_date)
             VALUES($1,$2,$3,$4,$5,$6,'pending',$7) ON CONFLICT DO NOTHING`,
            [cxcId, req.userId, clientId||null,
             `Factura ${inv.invoice_number}${inv.client_name?' — '+inv.client_name:''}`,
             total, 0, inv.due_date||null]
          );
        }
      } else {
        // Pago inmediato → marcar pagada
        await query(`UPDATE invoices SET status='paid', paid_amount=$1 WHERE id=$2`, [total, inv.id]);
      }
    }

    // When invoice is PAID: update CxC to paid + create journal entry + income record
    if (status === 'paid') {
      await query(
        `UPDATE receivables SET status='paid', paid_amount=total_amount
         WHERE user_id=$1 AND description ILIKE $2`,
        [req.userId, `%Factura ${inv.invoice_number}%`]
      );
      await query(
        `UPDATE invoices SET paid_amount=total WHERE id=$1 AND user_id=$2`,
        [req.params.id, req.userId]
      );

      // Generate journal entry: Debit Caja/Banco, Credit Clientes (CxC)
      const total = parseFloat(inv.total || 0);
      const sub   = parseFloat(inv.subtotal || total);
      const pmeth = inv.payment_method || 'credit';
      const cajaAcct  = await query(`SELECT id FROM accounts WHERE user_id=$1 AND code='1.1.01' LIMIT 1`, [req.userId]);
      const bancoAcct = await query(`SELECT id FROM accounts WHERE user_id=$1 AND code='1.1.02' LIMIT 1`, [req.userId]);
      const clientAcct = await query(`SELECT id FROM accounts WHERE user_id=$1 AND code='1.2.01' LIMIT 1`, [req.userId]);
      // Pick cash account based on payment method
      const cashAcct = (pmeth === 'bank' || pmeth === 'card')
        ? (bancoAcct.rows[0]?.id || cajaAcct.rows[0]?.id)
        : (cajaAcct.rows[0]?.id || bancoAcct.rows[0]?.id);
      if (cashAcct && clientAcct.rows[0]) {
        const jeId = `je_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
        await query(
          `INSERT INTO journal_entries(id,user_id,date,description,ref_type,ref_id)
           VALUES($1,$2,$3,$4,'invoice',$5)`,
          [jeId, req.userId, new Date().toISOString().split('T')[0],
           `Cobro Factura ${inv.invoice_number}${inv.client_name?' — '+inv.client_name:''}`,
           inv.id]
        );
        const debitLine  = `jl_${Date.now()}_d_${Math.random().toString(36).substr(2,4)}`;
        const creditLine = `jl_${Date.now()}_c_${Math.random().toString(36).substr(2,4)}`;
        await query(
          `INSERT INTO journal_lines(id,journal_entry_id,account_id,debit,credit)
           VALUES($1,$2,$3,$4,0)`,
          [debitLine, jeId, cashAcct, total]
        );
        await query(
          `INSERT INTO journal_lines(id,journal_entry_id,account_id,debit,credit)
           VALUES($1,$2,$3,0,$4)`,
          [creditLine, jeId, clientAcct.rows[0].id, total]
        );
        // Update account balances
        await query(
          `INSERT INTO account_balances(account_id,balance) VALUES($1,$2)
           ON CONFLICT(account_id) DO UPDATE SET balance=account_balances.balance+$2`,
          [cashAcct, total]
        );
        await query(
          `INSERT INTO account_balances(account_id,balance) VALUES($1,$2)
           ON CONFLICT(account_id) DO UPDATE SET balance=account_balances.balance-$2`,
          [clientAcct.rows[0].id, total]
        );
      }

      // Auto-create income record on payment
      const pmLabels = { cash:'💵 Efectivo', bank:'🏦 Transferencia/Banco', card:'💳 Tarjeta', credit:'📋 Crédito/CxC' };
      const pmLabel = pmLabels[pmeth] || '📋 Crédito/CxC';
      const pmIcon = pmeth==='cash'?'💵':pmeth==='bank'?'🏦':pmeth==='card'?'💳':'📋';
      let incTypeId = null;
      const itR = await query(`SELECT id FROM income_types WHERE user_id=$1 AND (name=$2 OR icon=$3) LIMIT 1`, [req.userId, pmLabel, pmIcon]);
      if (itR.rows[0]) {
        incTypeId = itR.rows[0].id;
      } else {
        incTypeId = `it_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
        const icon = pmeth==='cash'?'💵':pmeth==='bank'?'🏦':pmeth==='card'?'💳':'📋';
        await query(`INSERT INTO income_types(id,user_id,name,description,icon,color) VALUES($1,$2,$3,$4,$5,$6)`,
          [incTypeId, req.userId, pmLabel, 'Generado automáticamente', icon, '#00e5a0']);
      }
      const incId = `inc_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
      await query(
        `INSERT INTO income_records(id,user_id,income_type_id,amount,description,date,reference)
         VALUES($1,$2,$3,$4,$5,$6,$7)`,
        [incId, req.userId, incTypeId, sub,
         `Factura ${inv.invoice_number}${inv.client_name?' — '+inv.client_name:''}`,
         inv.date||new Date().toISOString().split('T')[0],
         inv.invoice_number]
      );
    }

    // When invoice is CANCELLED: reverse journal entry + cancel CxC (works from any status)
    if (status === 'cancelled') {
      // Cancel CxC
      await query(
        `UPDATE receivables SET status='cancelled'
         WHERE user_id=$1 AND description ILIKE $2`,
        [req.userId, `%Factura ${inv.invoice_number}%`]
      );

      // Remove income record and reverse paid_amount if invoice was paid
      if (prevStatus === 'paid') {
        await query(
          `UPDATE invoices SET paid_amount=0 WHERE id=$1 AND user_id=$2`,
          [req.params.id, req.userId]
        );
        await query(
          `DELETE FROM income_records WHERE user_id=$1 AND reference=$2 AND description ILIKE $3`,
          [req.userId, inv.invoice_number, `%${inv.invoice_number}%`]
        );
      } else {
        // For non-paid invoices, just delete income record if any
        await query(
          `DELETE FROM income_records WHERE user_id=$1 AND reference=$2 AND description ILIKE $3`,
          [req.userId, inv.invoice_number, `%${inv.invoice_number}%`]
        );
      }

      // Find original journal entry for this invoice
      const jeR = await query(
        `SELECT id FROM journal_entries WHERE user_id=$1 AND ref_type='invoice' AND ref_id=$2`,
        [req.userId, inv.id]
      );

      for (const je of jeR.rows) {
        // Get lines to reverse
        const linesR = await query(`SELECT * FROM journal_lines WHERE journal_entry_id=$1`, [je.id]);

        // Create reversal entry
        const revId = `je_rev_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
        await query(
          `INSERT INTO journal_entries(id,user_id,date,description,ref_type,ref_id)
           VALUES($1,$2,$3,$4,$5,$6)`,
          [revId, req.userId, new Date().toISOString().split('T')[0],
           `ANULACIÓN Factura ${inv.invoice_number}`, 'reversal', inv.id]
        );

        for (const ln of linesR.rows) {
          const rlnId = `jl_rev_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
          // Swap debit/credit to reverse
          await query(
            `INSERT INTO journal_lines(id,journal_entry_id,account_id,debit,credit)
             VALUES($1,$2,$3,$4,$5)`,
            [rlnId, revId, ln.account_id, ln.credit, ln.debit]
          );
          // Reverse balance
          await query(
            `INSERT INTO account_balances(account_id,balance) VALUES($1,$2)
             ON CONFLICT(account_id) DO UPDATE SET balance=account_balances.balance+$2`,
            [ln.account_id, parseFloat(ln.credit) - parseFloat(ln.debit)]
          );
        }
      }
    }

    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/invoices/:id', authMiddleware, async (req, res) => {
  try {
    const inv = await query(`SELECT * FROM invoices WHERE id=$1 AND user_id=$2`, [req.params.id, req.userId]);
    if (!inv.rows[0]) return res.status(404).json({ error: 'Invoice not found' });
    const invoice = inv.rows[0];

    // Get journal entries for this invoice to reverse balances
    const jeR = await query(`SELECT id FROM journal_entries WHERE user_id=$1 AND ref_type='invoice' AND ref_id=$2`,
      [req.userId, req.params.id]);

    // For each journal entry, reverse the balances
    for (const je of jeR.rows) {
      const linesR = await query(`SELECT * FROM journal_lines WHERE journal_entry_id=$1`, [je.id]);
      for (const ln of linesR.rows) {
        await query(
          `INSERT INTO account_balances(account_id,balance) VALUES($1,$2)
           ON CONFLICT(account_id) DO UPDATE SET balance=account_balances.balance+$2`,
          [ln.account_id, parseFloat(ln.credit) - parseFloat(ln.debit)]
        );
      }
      // Delete journal lines and entry
      await query(`DELETE FROM journal_lines WHERE journal_entry_id=$1`, [je.id]);
      await query(`DELETE FROM journal_entries WHERE id=$1`, [je.id]);
    }

    // Delete related income records
    await query(`DELETE FROM income_records WHERE user_id=$1 AND reference=$2 AND description ILIKE $3`,
      [req.userId, invoice.invoice_number, `%${invoice.invoice_number}%`]);

    // Delete related receivables
    await query(`DELETE FROM receivables WHERE user_id=$1 AND description ILIKE $2`,
      [req.userId, `%Factura ${invoice.invoice_number}%`]);

    // Delete invoice items
    await query(`DELETE FROM invoice_items WHERE invoice_id=$1`, [req.params.id]);

    // Delete invoice
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

// GET /api/income-types/with-totals — income types with record counts and totals
app.get('/api/income-types/with-totals', authMiddleware, async (req, res) => {
  try {
    const r = await query(`
      SELECT it.id, it.name, it.icon, it.color,
             COALESCE(SUM(ir.amount), 0) as total_amount,
             COUNT(ir.id) as record_count
      FROM income_types it
      LEFT JOIN income_records ir ON ir.income_type_id = it.id
      WHERE it.user_id=$1
      GROUP BY it.id, it.name, it.icon, it.color
      ORDER BY it.name`, [req.userId]);
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
    await query(`DELETE FROM income_records WHERE income_type_id=$1`, [req.params.id]);
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
      LEFT JOIN income_types it ON it.id = ir.income_type_id
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
      `INSERT INTO income_records(id,user_id,income_type_id,amount,description,date,reference)
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
// GET /api/inventory/stock — usa tabla products (unificada)
app.get('/api/inventory/stock', authMiddleware, async (req, res) => {
  try {
    const r = await query(
      `SELECT p.id, p.code, p.name, p.category, p.unit,
              COALESCE(p.cost_price,0)   as cost_price,
              COALESCE(p.sale_price,0)   as sale_price,
              COALESCE(p.stock_minimum,0) as stock_minimum,
              COALESCE(p.stock_current,0) as stock_current,
              COALESCE(p.stock_current,0) as inventory_value_qty,
              COALESCE(p.stock_current,0) * COALESCE(p.cost_price,0) as inventory_value,
              CASE
                WHEN COALESCE(p.stock_current,0) <= 0 THEN 'out'
                WHEN COALESCE(p.stock_current,0) <= COALESCE(p.stock_minimum,0) THEN 'low'
                ELSE 'ok'
              END as stock_status
       FROM products p
       WHERE p.user_id=$1
       ORDER BY p.name`,
      [req.userId]
    );
    const prods = r.rows;
    const total_value     = prods.reduce((s,p) => s + parseFloat(p.inventory_value||0), 0);
    const low_stock       = prods.filter(p => p.stock_status === 'low').length;
    const out_of_stock    = prods.filter(p => p.stock_status === 'out').length;
    res.json({
      products: prods,
      total_products: prods.length,
      total_value, low_stock, out_of_stock,
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/inventory/movements — lee de inventory_movements unida a products
app.get('/api/inventory/movements', authMiddleware, async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 80;
    // Intentar con products primero, luego con inventory_products como fallback
    const r = await query(
      `SELECT m.*,
              COALESCE(p.name, ip.name, 'Desconocido') as product_name,
              COALESCE(p.code, ip.code, '')             as product_code,
              COALESCE(p.unit, ip.unit, 'unidad')       as unit,
              v.name as vendor_name,
              c.name as client_name
       FROM inventory_movements m
       LEFT JOIN products p          ON p.id = m.product_id AND p.user_id = m.user_id
       LEFT JOIN inventory_products ip ON ip.id = m.product_id AND ip.user_id = m.user_id
       LEFT JOIN vendors v            ON v.id = m.vendor_id
       LEFT JOIN clients c            ON c.id = m.client_id
       WHERE m.user_id=$1
       ORDER BY m.mov_date DESC, m.created_at DESC
       LIMIT $2`,
      [req.userId, limit]
    );
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/inventory/entry — entrada de mercancía, actualiza stock en products
app.post('/api/inventory/entry', authMiddleware, async (req, res) => {
  const pgClient = await pool.connect();
  try {
    await pgClient.query('BEGIN');
    const { product_id, quantity, unit_cost, reference, notes, mov_date, vendor_id } = req.body;
    if (!product_id || !quantity || parseFloat(quantity) <= 0) {
      await pgClient.query('ROLLBACK');
      return res.status(400).json({ error: 'Selecciona un producto y una cantidad válida' });
    }
    const qty  = parseFloat(quantity);
    const cost = parseFloat(unit_cost) || 0;

    // Verificar que el producto existe en tabla products
    const prodR = await pgClient.query(
      `SELECT id, stock_current, cost_price FROM products WHERE id=$1 AND user_id=$2`,
      [product_id, req.userId]
    );
    if (!prodR.rows[0]) {
      // Intentar con inventory_products (retrocompatibilidad)
      const ipR = await pgClient.query(
        `SELECT id FROM inventory_products WHERE id=$1 AND user_id=$2`,
        [product_id, req.userId]
      );
      if (!ipR.rows[0]) {
        await pgClient.query('ROLLBACK');
        return res.status(404).json({ error: 'Producto no encontrado' });
      }
      // Producto legacy en inventory_products — solo insertar movimiento
      const movId = `mov_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
      await pgClient.query(
        `INSERT INTO inventory_movements(id,user_id,product_id,type,quantity,unit_cost,reference,notes,mov_date,vendor_id)
         VALUES($1,$2,$3,'entry',$4,$5,$6,$7,$8,$9)`,
        [movId, req.userId, product_id, qty, cost||null, reference||null, notes||null, mov_date||null, vendor_id||null]
      );
      await pgClient.query('COMMIT');
      return res.json({ ok: true, id: movId });
    }

    const prod = prodR.rows[0];
    const stockActual = parseFloat(prod.stock_current || 0);
    const costoAnterior = parseFloat(prod.cost_price || 0);

    // Costo promedio ponderado (si hay stock y costo previo)
    let nuevoCosto = cost;
    if (cost > 0 && stockActual > 0 && costoAnterior > 0) {
      nuevoCosto = ((stockActual * costoAnterior) + (qty * cost)) / (stockActual + qty);
    } else if (cost > 0) {
      nuevoCosto = cost;
    } else {
      nuevoCosto = costoAnterior;
    }

    // Actualizar stock y costo promedio en products
    await pgClient.query(
      `UPDATE products SET
         stock_current = COALESCE(stock_current,0) + $1,
         cost_price    = CASE WHEN $2 > 0 THEN $2 ELSE cost_price END
       WHERE id=$3 AND user_id=$4`,
      [qty, Math.round(nuevoCosto * 100) / 100, product_id, req.userId]
    );

    // Registrar movimiento
    const movId = `mov_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
    await pgClient.query(
      `INSERT INTO inventory_movements(id,user_id,product_id,type,quantity,unit_cost,reference,notes,mov_date,vendor_id)
       VALUES($1,$2,$3,'entry',$4,$5,$6,$7,$8,$9)`,
      [movId, req.userId, product_id, qty, cost||null, reference||null, notes||null, mov_date||null, vendor_id||null]
    );

    // Asiento contable si hay cuentas configuradas
    const invAcctR = await pgClient.query(
      `SELECT id, code FROM accounts WHERE user_id=$1 AND code IN ('1103','1.1.03','1301','1.3.01') LIMIT 1`,
      [req.userId]
    );
    const cxpAcctR = await pgClient.query(
      `SELECT id FROM accounts WHERE user_id=$1 AND code IN ('2101','2.1.01') LIMIT 1`,
      [req.userId]
    );
    if (invAcctR.rows[0] && cxpAcctR.rows[0] && cost > 0) {
      const totalCosto = Math.round(qty * cost * 100) / 100;
      await insertJournalEntry(pgClient, req.userId, mov_date||new Date().toISOString().split('T')[0],
        `Entrada inventario — ${reference||'Compra de mercancía'}`,
        'inventory_entry', movId,
        [
          { acct: invAcctR.rows[0].id, d: totalCosto, c: 0 },
          { acct: cxpAcctR.rows[0].id, d: 0, c: totalCosto },
        ]
      );
    }

    await pgClient.query('COMMIT');
    res.json({ ok: true, id: movId, new_stock: stockActual + qty, new_cost: Math.round(nuevoCosto*100)/100 });
  } catch(e) {
    await pgClient.query('ROLLBACK');
    console.error('inventory entry error:', e.message);
    res.status(500).json({ error: e.message });
  } finally { pgClient.release(); }
});

// POST /api/inventory/exit — salida manual de inventario
app.post('/api/inventory/exit', authMiddleware, async (req, res) => {
  const pgClient = await pool.connect();
  try {
    await pgClient.query('BEGIN');
    const { product_id, quantity, unit_price, reason, reference, notes, mov_date, client_id } = req.body;
    if (!product_id || !quantity || parseFloat(quantity) <= 0) {
      await pgClient.query('ROLLBACK');
      return res.status(400).json({ error: 'Selecciona un producto y una cantidad válida' });
    }
    const qty = parseFloat(quantity);

    // Verificar stock
    const prodR = await pgClient.query(
      `SELECT id, stock_current, cost_price, name FROM products WHERE id=$1 AND user_id=$2`,
      [product_id, req.userId]
    );

    if (!prodR.rows[0]) {
      // Fallback inventory_products
      const movId = `mov_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
      await pgClient.query(
        `INSERT INTO inventory_movements(id,user_id,product_id,type,quantity,unit_cost,reference,notes,mov_date,client_id,reason)
         VALUES($1,$2,$3,'exit',$4,$5,$6,$7,$8,$9,$10)`,
        [movId, req.userId, product_id, qty, unit_price||null, reference||null, notes||null, mov_date||null, client_id||null, reason||'sale']
      );
      await pgClient.query('COMMIT');
      return res.json({ ok: true, id: movId });
    }

    const prod = prodR.rows[0];
    const stockActual = parseFloat(prod.stock_current || 0);
    if (stockActual < qty) {
      await pgClient.query('ROLLBACK');
      return res.status(400).json({ error: `Stock insuficiente. Disponible: ${stockActual}` });
    }

    // Reducir stock
    await pgClient.query(
      `UPDATE products SET stock_current = stock_current - $1 WHERE id=$2 AND user_id=$3`,
      [qty, product_id, req.userId]
    );

    const movId = `mov_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
    await pgClient.query(
      `INSERT INTO inventory_movements(id,user_id,product_id,type,quantity,unit_cost,unit_price,reference,notes,mov_date,client_id,reason)
       VALUES($1,$2,$3,'exit',$4,$5,$6,$7,$8,$9,$10,$11)`,
      [movId, req.userId, product_id, qty, prod.cost_price||null, unit_price||null, reference||null, notes||null, mov_date||null, client_id||null, reason||'sale']
    );

    await pgClient.query('COMMIT');
    res.json({ ok: true, id: movId, new_stock: stockActual - qty });
  } catch(e) {
    await pgClient.query('ROLLBACK');
    res.status(500).json({ error: e.message });
  } finally { pgClient.release(); }
});

// POST /api/inventory/adjustment — ajuste de inventario (conteo físico)
app.post('/api/inventory/adjustment', authMiddleware, async (req, res) => {
  const pgClient = await pool.connect();
  try {
    await pgClient.query('BEGIN');
    const { product_id, new_quantity, notes, mov_date } = req.body;
    if (!product_id || new_quantity == null || parseFloat(new_quantity) < 0) {
      await pgClient.query('ROLLBACK');
      return res.status(400).json({ error: 'product_id y new_quantity requeridos' });
    }
    const newQty = parseFloat(new_quantity);

    const prodR = await pgClient.query(
      `SELECT id, stock_current, cost_price FROM products WHERE id=$1 AND user_id=$2`,
      [product_id, req.userId]
    );

    let stockActual = 0;
    if (prodR.rows[0]) {
      stockActual = parseFloat(prodR.rows[0].stock_current || 0);
      await pgClient.query(
        `UPDATE products SET stock_current=$1 WHERE id=$2 AND user_id=$3`,
        [newQty, product_id, req.userId]
      );
    } else {
      // Fallback inventory_products
      const ipR = await pgClient.query(
        `SELECT id FROM inventory_products WHERE id=$1 AND user_id=$2`, [product_id, req.userId]
      );
      if (!ipR.rows[0]) { await pgClient.query('ROLLBACK'); return res.status(404).json({ error: 'Producto no encontrado' }); }
    }

    const diff = newQty - stockActual;
    const movId = `mov_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
    await pgClient.query(
      `INSERT INTO inventory_movements(id,user_id,product_id,type,quantity,notes,mov_date)
       VALUES($1,$2,$3,'adjustment',$4,$5,$6)`,
      [movId, req.userId, product_id, newQty, notes||`Ajuste inventario: ${stockActual} → ${newQty} (${diff>=0?'+':''}${diff})`, mov_date||null]
    );

    await pgClient.query('COMMIT');
    res.json({ ok: true, id: movId, previous: stockActual, new: newQty, diff });
  } catch(e) {
    await pgClient.query('ROLLBACK');
    res.status(500).json({ error: e.message });
  } finally { pgClient.release(); }
});

// GET /api/inventory/kardex/:productId — busca en products O inventory_products
app.get('/api/inventory/kardex/:productId', authMiddleware, async (req, res) => {
  try {
    // Buscar info del producto (intentar en products primero)
    let productName = 'Producto';
    const pR = await query(
      `SELECT name, unit FROM products WHERE id=$1 AND user_id=$2`, [req.params.productId, req.userId]
    );
    if (pR.rows[0]) productName = pR.rows[0].name;
    else {
      const ipR = await query(
        `SELECT name FROM inventory_products WHERE id=$1 AND user_id=$2`, [req.params.productId, req.userId]
      );
      if (ipR.rows[0]) productName = ipR.rows[0].name;
    }

    const r = await query(
      `SELECT m.*, $3 as product_name
       FROM inventory_movements m
       WHERE m.product_id=$1 AND m.user_id=$2
       ORDER BY m.mov_date ASC, m.created_at ASC`,
      [req.params.productId, req.userId, productName]
    );

    // Calcular saldo corrido
    let balance = 0;
    const rows = r.rows.map(row => {
      const qty = parseFloat(row.quantity || 0);
      if (row.type === 'entry') balance += qty;
      else if (row.type === 'exit') balance -= qty;
      else if (row.type === 'adjustment') balance = qty; // ajuste = nuevo saldo absoluto
      return {
        ...row,
        balance: Math.round(balance * 1000) / 1000,
        unit_cost: parseFloat(row.unit_cost || 0),
      };
    });
    res.json(rows);
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

      // Auto-create receivable: debit on any Clientes/CxC account with client auxiliary
      if (line.auxiliary_type === 'client' && Number(line.debit) > 0) {
        const accCheck = await client.query(`SELECT code, type FROM accounts WHERE id=$1 AND user_id=$2`, [line.account_id, req.userId]);
        const code = accCheck.rows[0]?.code || '';
        const isCxC = ['1.2.01','1201','1202','1200'].includes(code) || code.startsWith('12');
        if (isCxC) {
          const rId = `rec_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
          await client.query(
            `INSERT INTO receivables(id, user_id, client_id, description, total_amount, paid_amount, status, due_date)
             VALUES($1,$2,$3,$4,$5,0,'pending',$6) ON CONFLICT DO NOTHING`,
            [rId, req.userId, line.auxiliary_id, description, line.debit, date]
          );
        }
      }

      // Auto-create payable: credit on any Proveedores/CxP account with vendor auxiliary
      if (line.auxiliary_type === 'vendor' && Number(line.credit) > 0) {
        const accCheck = await client.query(`SELECT code, type FROM accounts WHERE id=$1 AND user_id=$2`, [line.account_id, req.userId]);
        const code = accCheck.rows[0]?.code || '';
        const isCxP = ['2.1.01','2101','2100','2201','2301'].includes(code) || code.startsWith('21') || code.startsWith('22');
        if (isCxP) {
          const pId = `pay_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
          await client.query(
            `INSERT INTO payables(id, user_id, vendor_id, description, total_amount, paid_amount, status, due_date)
             VALUES($1,$2,$3,$4,$5,0,'pending',$6) ON CONFLICT DO NOTHING`,
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
        `INSERT INTO journal_lines(id, journal_entry_id, account_id, debit, credit, auxiliary_type, auxiliary_id, auxiliary_name)
         VALUES($1,$2,$3,0,$4,'client',$5,$6)`, [creditLine, jeId, ing.rows[0].id, total_amount, client_id, clientName]
      );
      await client.query(
        `INSERT INTO account_balances(account_id, balance)
         VALUES($1, $2) ON CONFLICT(account_id) DO UPDATE SET balance = account_balances.balance + $2`,
        [cxc.rows[0].id, total_amount]
      );
      await client.query(
        `INSERT INTO account_balances(account_id, balance)
         VALUES($1, $2) ON CONFLICT(account_id) DO UPDATE SET balance = account_balances.balance + $2`,
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
// Body: { amount, payment_date, notes, payment_method: 'cash'|'bank'|'card' }
app.post('/api/receivables/:id/payments', authMiddleware, async (req, res) => {
  const pgClient = await pool.connect();
  try {
    await pgClient.query('BEGIN');
    const { amount, payment_date, notes, payment_method = 'bank' } = req.body;
    if (!amount || parseFloat(amount) <= 0) {
      await pgClient.query('ROLLBACK');
      return res.status(400).json({ error: 'Monto requerido y debe ser mayor a 0' });
    }

    // Cargar CxC con info del cliente
    const rec = await pgClient.query(
      `SELECT r.*, c.name as client_name
       FROM receivables r LEFT JOIN clients c ON c.id=r.client_id
       WHERE r.id=$1 AND r.user_id=$2`,
      [req.params.id, req.userId]
    );
    if (!rec.rows[0]) {
      await pgClient.query('ROLLBACK');
      return res.status(404).json({ error: 'Cuenta por cobrar no encontrada' });
    }
    const recRow    = rec.rows[0];
    const amtNum    = parseFloat(amount);
    const payDate   = payment_date || new Date().toISOString().split('T')[0];
    const outstanding = parseFloat(recRow.total_amount) - parseFloat(recRow.paid_amount);

    if (amtNum > outstanding + 0.01) {
      await pgClient.query('ROLLBACK');
      return res.status(400).json({ error: `Monto excede lo pendiente (RD$ ${outstanding.toFixed(2)})` });
    }

    // 1. Registrar pago
    const payId = `rpay_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
    await pgClient.query(
      `INSERT INTO receivable_payments(id,receivable_id,amount,payment_date,notes)
       VALUES($1,$2,$3,$4,$5)`,
      [payId, req.params.id, amtNum, payDate, notes||null]
    );

    // 2. Actualizar saldo CxC
    const newPaid = parseFloat(recRow.paid_amount) + amtNum;
    const newStatus = newPaid >= parseFloat(recRow.total_amount) - 0.01 ? 'paid' : 'partial';
    await pgClient.query(
      `UPDATE receivables SET paid_amount=$1, status=$2 WHERE id=$3`,
      [newPaid, newStatus, req.params.id]
    );

    // 3. Buscar cuentas contables (soporta ambos formatos de código)
    const acctR = await pgClient.query(
      `SELECT id, code FROM accounts WHERE user_id=$1
       AND code IN ('1101','1.1.01','1102','1.1.02','1201','1.2.01',
                    '4101','4.1.01','4102','4.1.02')`,
      [req.userId]
    );
    const am = {}; acctR.rows.forEach(a => { am[a.code] = a.id; });
    const cajaAcct   = am['1101'] || am['1.1.01'];
    const bancoAcct  = am['1102'] || am['1.1.02'];
    const cxcAcct    = am['1201'] || am['1.2.01'];
    const salesAcct  = am['4101'] || am['4.1.01'] || am['4102'] || am['4.1.02'];

    // Cuenta de destino según método de pago
    const cashAcct = payment_method === 'cash'
      ? (cajaAcct || bancoAcct)
      : (bancoAcct || cajaAcct);

    const pmLabels = { cash:'Efectivo', bank:'Banco/Transferencia', card:'Tarjeta' };
    const pmLabel  = pmLabels[payment_method] || 'Banco';

    // 4. Asiento contable: Débito Caja/Banco → Crédito CxC
    if (cashAcct && cxcAcct) {
      await insertJournalEntry(pgClient, req.userId, payDate,
        `Cobro CxC — ${recRow.client_name||recRow.description} [${pmLabel}]`,
        'receivable_payment', req.params.id,
        [
          { acct: cashAcct, d: amtNum, c: 0,
            auxType: 'client', auxId: recRow.client_id, auxName: recRow.client_name },
          { acct: cxcAcct,  d: 0, c: amtNum,
            auxType: 'client', auxId: recRow.client_id, auxName: recRow.client_name },
        ]
      );
    }

    // 5. ── INGRESO AUTOMÁTICO ──
    // Solo registrar ingreso si la CxC viene de una factura con cuenta de ventas
    // O si no tiene referencia a factura (CxC manual = servicio)
    const pmIncLabels = { cash:'💵 Cobros Efectivo', bank:'🏦 Cobros Banco', card:'💳 Cobros Tarjeta' };
    const pmIncLabel  = pmIncLabels[payment_method] || '🏦 Cobros Banco';
    const pmIncIcon   = payment_method === 'cash' ? '💵' : payment_method === 'card' ? '💳' : '🏦';

    // Buscar o crear tipo de ingreso para este método
    let incTypeId = null;
    const itR = await pgClient.query(
      `SELECT id FROM income_types WHERE user_id=$1 AND name=$2 LIMIT 1`,
      [req.userId, pmIncLabel]
    );
    if (itR.rows[0]) {
      incTypeId = itR.rows[0].id;
    } else {
      incTypeId = `it_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
      await pgClient.query(
        `INSERT INTO income_types(id,user_id,name,description,icon,color)
         VALUES($1,$2,$3,$4,$5,$6)`,
        [incTypeId, req.userId, pmIncLabel, 'Generado automáticamente al cobrar CxC',
         pmIncIcon, '#00e5a0']
      );
    }

    // Registrar ingreso por el monto cobrado
    const incId = `inc_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
    await pgClient.query(
      `INSERT INTO income_records(id,user_id,income_type_id,amount,description,date,reference)
       VALUES($1,$2,$3,$4,$5,$6,$7)`,
      [incId, req.userId, incTypeId, amtNum,
       `Cobro — ${recRow.client_name||recRow.description}`,
       payDate, recRow.description]
    );

    // 6. Si la CxC quedó pagada y viene de factura, marcar factura como pagada
    if (newStatus === 'paid') {
      const invMatch = recRow.description?.match(/Factura\s+(\S+)/i);
      if (invMatch) {
        await pgClient.query(
          `UPDATE invoices SET status='paid', paid_amount=total
           WHERE user_id=$1 AND invoice_number=$2 AND status IN ('issued','partial')`,
          [req.userId, invMatch[1]]
        );
      }
    }

    await pgClient.query('COMMIT');
    res.json({
      ok: true,
      new_status: newStatus,
      paid: newPaid,
      outstanding: parseFloat(recRow.total_amount) - newPaid,
      income_registered: true
    });
  } catch(e) {
    await pgClient.query('ROLLBACK');
    console.error('Receivable payment error:', e.message);
    res.status(500).json({ error: e.message });
  } finally {
    pgClient.release();
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
    const { vendor_id, description, total_amount, due_date, expense_account_code } = req.body;
    if (!vendor_id || !description || !total_amount) { await client.query('ROLLBACK'); return res.status(400).json({ error: 'Missing fields' }); }
    const id = `pay_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
    await client.query(
      `INSERT INTO payables(id, user_id, vendor_id, description, total_amount, due_date)
       VALUES($1,$2,$3,$4,$5,$6)`,
      [id, req.userId, vendor_id, description, total_amount, due_date]
    );
    // Auto-journal: Debit configurable expense account, Credit "Cuentas por Pagar" (liability)
    const expAcctCode = expense_account_code || '6.1.01';
    const exp = await client.query(`SELECT id FROM accounts WHERE code=$1 AND user_id=$2`, [expAcctCode, req.userId]);
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
         VALUES($1, $2) ON CONFLICT(account_id) DO UPDATE SET balance = account_balances.balance + $2`,
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
// ═══════════════════════════════════════════════════════════════════════════════
// ─── FASE 1.2: CIERRE DE PERÍODO MENSUAL ──────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

// GET /api/cierre/preview — vista previa antes de cerrar
app.get('/api/cierre/preview', authMiddleware, async (req, res) => {
  try {
    const { month, year } = req.query;
    if (!month || !year) return res.status(400).json({ error: 'month y year requeridos' });
    const m = parseInt(month); const y = parseInt(year);
    const from = `${y}-${String(m).padStart(2,'0')}-01`;
    const lastDay = new Date(y, m, 0).getDate();
    const to = `${y}-${String(m).padStart(2,'0')}-${lastDay}`;

    // Verificar si ya hay cierre para este período
    const existing = await query(
      `SELECT id FROM period_closings WHERE user_id=$1 AND month=$2 AND year=$3`,
      [req.userId, m, y]
    );
    if (existing.rows[0]) {
      return res.json({ already_closed: true, month: m, year: y });
    }

    // Saldos de cuentas de resultado (clase 4,5,6)
    const r = await query(
      `SELECT a.code, a.name, a.type, a.class,
              COALESCE(ab.balance, 0) as balance
       FROM accounts a
       LEFT JOIN account_balances ab ON ab.account_id = a.id
       WHERE a.user_id=$1 AND a.class IN (4,5,6) AND a.is_active=TRUE
       ORDER BY a.class, a.code`,
      [req.userId]
    );

    let ingresos = 0, costos = 0, gastos = 0;
    const lineas = r.rows.map(row => {
      const bal = parseFloat(row.balance || 0);
      if (row.class === 4) ingresos += bal;
      if (row.class === 5) costos   += bal;
      if (row.class === 6) gastos   += bal;
      return { code: row.code, name: row.name, type: row.type, class: row.class, balance: bal };
    });

    const utilidad_neta = ingresos - costos - gastos;

    // Buscar cuenta de Utilidades Retenidas (patrimonio clase 3)
    const utRet = await query(
      `SELECT id, code, name FROM accounts
       WHERE user_id=$1 AND class=3
       AND (code ILIKE '%utilidad%' OR code IN ('3201','3.2.01','3101','3.1.01') OR name ILIKE '%utilidad%' OR name ILIKE '%retenid%')
       LIMIT 1`,
      [req.userId]
    );

    res.json({
      already_closed: false, month: m, year: y,
      periodo: `${from} al ${to}`,
      ingresos, costos, gastos, utilidad_neta,
      lineas,
      utilidades_retenidas_cuenta: utRet.rows[0] || null,
      advertencias: utRet.rows.length === 0
        ? ['No se encontró cuenta de Utilidades Retenidas (clase 3). Se necesita para el asiento de cierre.']
        : []
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/cierre — ejecutar cierre mensual
app.post('/api/cierre', authMiddleware, async (req, res) => {
  const pgClient = await pool.connect();
  try {
    await pgClient.query('BEGIN');
    const { month, year } = req.body;
    if (!month || !year) { await pgClient.query('ROLLBACK'); return res.status(400).json({ error: 'month y year requeridos' }); }
    const m = parseInt(month); const y = parseInt(year);
    const lastDay = new Date(y, m, 0).getDate();
    const closeDate = `${y}-${String(m).padStart(2,'0')}-${lastDay}`;

    // Verificar duplicado
    const existing = await pgClient.query(
      `SELECT id FROM period_closings WHERE user_id=$1 AND month=$2 AND year=$3`,
      [req.userId, m, y]
    );
    if (existing.rows[0]) {
      await pgClient.query('ROLLBACK');
      return res.status(409).json({ error: `El período ${m}/${y} ya fue cerrado` });
    }

    // Obtener saldos de cuentas de resultado (4,5,6)
    const r = await pgClient.query(
      `SELECT a.id, a.code, a.name, a.class, COALESCE(ab.balance,0) as balance
       FROM accounts a LEFT JOIN account_balances ab ON ab.account_id=a.id
       WHERE a.user_id=$1 AND a.class IN (4,5,6) AND a.is_active=TRUE`,
      [req.userId]
    );

    let ingresos = 0, costos = 0, gastos = 0;
    r.rows.forEach(row => {
      const bal = parseFloat(row.balance || 0);
      if (row.class === 4) ingresos += bal;
      if (row.class === 5) costos   += bal;
      if (row.class === 6) gastos   += bal;
    });
    const utilidad_neta = ingresos - costos - gastos;

    // Buscar cuenta Utilidades Retenidas
    const utRet = await pgClient.query(
      `SELECT id, code, name FROM accounts
       WHERE user_id=$1 AND class=3
       AND (code IN ('3201','3.2.01') OR name ILIKE '%utilidad%' OR name ILIKE '%retenid%')
       LIMIT 1`,
      [req.userId]
    );

    // Si no existe cuenta de Utilidades Retenidas, crearla
    let utRetId = utRet.rows[0]?.id;
    if (!utRetId) {
      utRetId = `acc_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
      await pgClient.query(
        `INSERT INTO accounts(id,user_id,code,name,type,class,currency)
         VALUES($1,$2,'3201','Utilidades Retenidas','equity',3,'DOP')
         ON CONFLICT(user_id,code) DO NOTHING`,
        [utRetId, req.userId]
      );
      await pgClient.query(
        `INSERT INTO account_balances(account_id,balance) VALUES($1,0) ON CONFLICT DO NOTHING`,
        [utRetId]
      );
    }

    // ── Asiento de Cierre ──
    // Las cuentas de ingresos (clase 4) tienen saldo Crédito → se debitan para cerrar
    // Las cuentas de costos/gastos (clase 5,6) tienen saldo Débito → se acreditan para cerrar
    // La diferencia va a Utilidades Retenidas

    const jeLines = [];

    // Cerrar ingresos (debitar)
    for (const row of r.rows.filter(x => x.class === 4 && parseFloat(x.balance) !== 0)) {
      const bal = parseFloat(row.balance);
      jeLines.push({ acct: row.id, d: Math.abs(bal), c: 0 });
    }
    // Cerrar costos y gastos (acreditar)
    for (const row of r.rows.filter(x => (x.class === 5 || x.class === 6) && parseFloat(x.balance) !== 0)) {
      const bal = parseFloat(row.balance);
      jeLines.push({ acct: row.id, d: 0, c: Math.abs(bal) });
    }
    // Diferencia a Utilidades Retenidas
    if (utilidad_neta > 0) {
      jeLines.push({ acct: utRetId, d: 0, c: utilidad_neta });
    } else if (utilidad_neta < 0) {
      jeLines.push({ acct: utRetId, d: Math.abs(utilidad_neta), c: 0 });
    }

    if (jeLines.length > 0) {
      await insertJournalEntry(pgClient, req.userId, closeDate,
        `CIERRE DE PERÍODO — ${m}/${y}`,
        'period_close', `${m}-${y}`,
        jeLines
      );
    }

    // Resetear saldos de cuentas de resultado a 0
    for (const row of r.rows) {
      await pgClient.query(
        `UPDATE account_balances SET balance=0 WHERE account_id=$1`,
        [row.id]
      );
    }

    // Registrar cierre
    const cierreId = `close_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
    await pgClient.query(
      `INSERT INTO period_closings(id,user_id,month,year,closed_at,ingresos,costos,gastos,utilidad_neta)
       VALUES($1,$2,$3,$4,NOW(),$5,$6,$7,$8)`,
      [cierreId, req.userId, m, y, ingresos, costos, gastos, utilidad_neta]
    );

    await pgClient.query('COMMIT');
    res.json({ ok: true, month: m, year: y, ingresos, costos, gastos, utilidad_neta, cierreId });
  } catch(e) {
    await pgClient.query('ROLLBACK');
    console.error('Cierre error:', e.message);
    res.status(500).json({ error: e.message });
  } finally {
    pgClient.release();
  }
});

// GET /api/cierre/historial — lista de cierres realizados
app.get('/api/cierre/historial', authMiddleware, async (req, res) => {
  try {
    const r = await query(
      `SELECT * FROM period_closings WHERE user_id=$1 ORDER BY year DESC, month DESC`,
      [req.userId]
    );
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════════════════════
// ─── FASE 1.3: CONCILIACIÓN BANCARIA ──────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

// GET /api/conciliacion — movimientos de cuentas banco/caja para un período
app.get('/api/conciliacion', authMiddleware, async (req, res) => {
  try {
    const { month, year, account_code } = req.query;
    if (!month || !year) return res.status(400).json({ error: 'month y year requeridos' });
    const m  = parseInt(month); const y = parseInt(year);
    const from = `${y}-${String(m).padStart(2,'0')}-01`;
    const lastDay = new Date(y, m, 0).getDate();
    const to   = `${y}-${String(m).padStart(2,'0')}-${lastDay}`;

    // Cuentas de banco disponibles
    const bankAccts = await query(
      `SELECT a.id, a.code, a.name, COALESCE(ab.balance,0) as balance
       FROM accounts a LEFT JOIN account_balances ab ON ab.account_id=a.id
       WHERE a.user_id=$1 AND a.class=1 AND (a.code ILIKE '%banco%' OR a.code IN ('1101','1.1.01','1102','1.1.02') OR a.name ILIKE '%banco%' OR a.name ILIKE '%caja%')
       ORDER BY a.code`,
      [req.userId]
    );

    // Filtrar por cuenta específica si se pide
    const acctFilter = account_code
      ? bankAccts.rows.filter(a => a.code === account_code)
      : bankAccts.rows;

    if (!acctFilter.length) {
      return res.json({ accounts: bankAccts.rows, movements: [], summary: {} });
    }

    const acctIds = acctFilter.map(a => a.id);
    const placeholders = acctIds.map((_,i) => `$${i+4}`).join(',');

    // Movimientos del período para esas cuentas
    const movs = await query(
      `SELECT je.date, je.description, je.ref_type, je.ref_id,
              jl.debit, jl.credit, a.code as account_code, a.name as account_name,
              jl.auxiliary_name,
              jl.id as line_id, je.id as entry_id
       FROM journal_lines jl
       JOIN journal_entries je ON je.id=jl.journal_entry_id
       JOIN accounts a ON a.id=jl.account_id
       WHERE je.user_id=$1 AND je.date>=$2 AND je.date<=$3
         AND jl.account_id IN (${placeholders})
       ORDER BY je.date ASC, je.created_at ASC`,
      [req.userId, from, to, ...acctIds]
    );

    // Calcular saldo inicial (antes del período) para cada cuenta
    const openingBalances = {};
    for (const acct of acctFilter) {
      const ob = await query(
        `SELECT COALESCE(SUM(jl.debit - jl.credit), 0) as net
         FROM journal_lines jl
         JOIN journal_entries je ON je.id=jl.journal_entry_id
         WHERE jl.account_id=$1 AND je.date < $2`,
        [acct.id, from]
      );
      openingBalances[acct.id] = parseFloat(ob.rows[0]?.net || 0);
    }

    // Construir movimientos con saldo corrido
    let runningBalance = Object.values(openingBalances).reduce((s,v)=>s+v, 0);
    const movements = movs.rows.map(row => {
      const debit  = parseFloat(row.debit  || 0);
      const credit = parseFloat(row.credit || 0);
      runningBalance += debit - credit;
      return {
        ...row,
        debit, credit,
        balance: Math.round(runningBalance * 100) / 100,
        type: debit > 0 ? 'entrada' : 'salida',
        amount: debit > 0 ? debit : credit
      };
    });

    const totalEntradas  = movements.reduce((s,m) => s + m.debit, 0);
    const totalSalidas   = movements.reduce((s,m) => s + m.credit, 0);
    const saldoInicial   = Object.values(openingBalances).reduce((s,v)=>s+v, 0);
    const saldoFinal     = saldoInicial + totalEntradas - totalSalidas;

    res.json({
      accounts: bankAccts.rows,
      selected_accounts: acctFilter,
      movements,
      summary: {
        period: `${from} al ${to}`,
        saldo_inicial:  Math.round(saldoInicial  * 100) / 100,
        total_entradas: Math.round(totalEntradas * 100) / 100,
        total_salidas:  Math.round(totalSalidas  * 100) / 100,
        saldo_final:    Math.round(saldoFinal    * 100) / 100,
      }
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/conciliacion/mark — marcar movimiento como conciliado
app.post('/api/conciliacion/mark', authMiddleware, async (req, res) => {
  try {
    const { line_id, conciliado, bank_reference } = req.body;
    if (!line_id) return res.status(400).json({ error: 'line_id requerido' });
    await query(
      `UPDATE journal_lines SET conciliado=$1, bank_reference=$2
       WHERE id=$3
         AND journal_entry_id IN (SELECT id FROM journal_entries WHERE user_id=$4)`,
      [conciliado !== false, bank_reference||null, line_id, req.userId]
    );
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

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
              a.code as account_code, a.name as account_name, jl.debit, jl.credit
       FROM journal_entries je
       JOIN journal_lines jl ON jl.journal_entry_id = je.id
       JOIN accounts a ON a.id = jl.account_id
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
    res.send(toCSV(['date', 'entry_desc', 'ref_type', 'ref_id', 'account_code', 'account_name', 'debit', 'credit'], rows));
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

// ═══════════════════════════════════════════════════════════════════════════════
// ─── FASE 2.1: COTIZACIONES / PRESUPUESTOS ────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

// GET /api/quotes — listar cotizaciones
app.get('/api/quotes', authMiddleware, async (req, res) => {
  try {
    const { status } = req.query;
    let sql = `SELECT q.*, c.name as client_display
               FROM quotes q
               LEFT JOIN clients c ON c.id = q.client_id
               WHERE q.user_id=$1`;
    const params = [req.userId];
    if (status) { sql += ` AND q.status=$2`; params.push(status); }
    sql += ` ORDER BY q.date DESC, q.created_at DESC LIMIT 100`;
    const r = await query(sql, params);
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/quotes/next-number
app.get('/api/quotes/next-number', authMiddleware, async (req, res) => {
  try {
    const r = await query(
      `SELECT COALESCE(MAX(CAST(REGEXP_REPLACE(quote_number,'[^0-9]','','g') AS INTEGER)),0) as last
       FROM quotes WHERE user_id=$1`,
      [req.userId]
    );
    const next = (parseInt(r.rows[0]?.last) || 0) + 1;
    res.json({ quote_number: 'COT-' + String(next).padStart(5, '0') });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/quotes — crear cotización
app.post('/api/quotes', authMiddleware, async (req, res) => {
  const pgClient = await pool.connect();
  try {
    await pgClient.query('BEGIN');
    const {
      quote_number, client_id, client_name, client_rnc, client_address,
      date, valid_until, subtotal, tax, total: bodyTotal,
      discount_amount, discount_pct, notes, items, lines,
      payment_terms, delivery_terms, status = 'draft'
    } = req.body;

    if (!bodyTotal && bodyTotal !== 0) {
      await pgClient.query('ROLLBACK');
      return res.status(400).json({ error: 'total requerido' });
    }

    const id = `cot_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
    const quoteDate = date || new Date().toISOString().split('T')[0];

    await pgClient.query(
      `INSERT INTO quotes(id,user_id,quote_number,client_id,client_name,client_rnc,client_address,
         date,valid_until,subtotal,tax,total,discount_amount,discount_pct,
         notes,payment_terms,delivery_terms,status)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`,
      [id, req.userId, quote_number, client_id||null, client_name||null, client_rnc||null, client_address||null,
       quoteDate, valid_until||null, subtotal||0, tax||0, bodyTotal,
       discount_amount||0, discount_pct||0, notes||null,
       payment_terms||null, delivery_terms||null, status]
    );

    // Insertar ítems (usar variable local para no colisionar con bodyTotal)
    const rawItems = lines || items || [];
    for (const item of rawItems) {
      const itemId    = `qi_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
      const qty       = parseFloat(item.quantity || item.qty || 1);
      const price     = parseFloat(item.unit_price || item.price || 0);
      const disc      = parseFloat(item.discount_pct || 0);
      const lineTotal = Math.round(qty * price * (1 - disc/100) * 100) / 100;
      await pgClient.query(
        `INSERT INTO quote_items(id,quote_id,description,qty,price,total,discount_pct,product_id)
         VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
        [itemId, id, item.description||'', qty, price, lineTotal, disc, item.product_id||null]
      );
    }

    await pgClient.query('COMMIT');
    res.json({ ok: true, id, quote_number });
  } catch(e) {
    await pgClient.query('ROLLBACK');
    console.error('quotes POST error:', e.message);
    res.status(500).json({ error: e.message });
  } finally { pgClient.release(); }
});

// GET /api/quotes/:id — FIX: rowid no existe en PostgreSQL, usar created_at
app.get('/api/quotes/:id', authMiddleware, async (req, res) => {
  try {
    const q = await query(`SELECT * FROM quotes WHERE id=$1 AND user_id=$2`, [req.params.id, req.userId]);
    if (!q.rows[0]) return res.status(404).json({ error: 'No encontrada' });
    const items = await query(`SELECT * FROM quote_items WHERE quote_id=$1 ORDER BY created_at ASC`, [req.params.id]);
    res.json({ ...q.rows[0], items: items.rows });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// PUT /api/quotes/:id/status — cambiar estado
app.put('/api/quotes/:id/status', authMiddleware, async (req, res) => {
  try {
    const { status } = req.body;
    const valid = ['draft','sent','approved','rejected','expired','converted'];
    if (!valid.includes(status)) return res.status(400).json({ error: 'Estado inválido' });
    await query(`UPDATE quotes SET status=$1 WHERE id=$2 AND user_id=$3`, [status, req.params.id, req.userId]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// DELETE /api/quotes/:id
app.delete('/api/quotes/:id', authMiddleware, async (req, res) => {
  try {
    await query(`DELETE FROM quote_items WHERE quote_id=$1`, [req.params.id]);
    await query(`DELETE FROM quotes WHERE id=$1 AND user_id=$2`, [req.params.id, req.userId]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/quotes/:id/convert — convertir cotización en factura
app.post('/api/quotes/:id/convert', authMiddleware, async (req, res) => {
  const pgClient = await pool.connect();
  try {
    await pgClient.query('BEGIN');
    const qR = await pgClient.query(
      `SELECT * FROM quotes WHERE id=$1 AND user_id=$2`, [req.params.id, req.userId]
    );
    if (!qR.rows[0]) { await pgClient.query('ROLLBACK'); return res.status(404).json({ error: 'Cotización no encontrada' }); }
    const quote = qR.rows[0];
    if (quote.status === 'converted') { await pgClient.query('ROLLBACK'); return res.status(409).json({ error: 'Ya fue convertida en factura' }); }

    // Obtener ítems de la cotización
    const itemsR = await pgClient.query(`SELECT * FROM quote_items WHERE quote_id=$1 ORDER BY created_at ASC`, [req.params.id]);

    // Obtener siguiente número de factura
    const cntR = await pgClient.query(`SELECT last_number FROM invoice_counter WHERE user_id=$1`, [req.userId]);
    const nextNum = (parseInt(cntR.rows[0]?.last_number) || 0) + 1;
    const invNum  = String(nextNum).padStart(6, '0');
    const { payment_method = 'credit', due_date } = req.body;

    // Crear factura borrador
    const invId  = `inv_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
    const invDate = new Date().toISOString().split('T')[0];
    await pgClient.query(
      `INSERT INTO invoices(id,user_id,invoice_number,client_name,client_rnc,client_address,
         subtotal,tax,total,discount_amount,discount_pct,status,date,due_date,notes,
         payment_method,quote_id)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'draft',$12,$13,$14,$15,$16)`,
      [invId, req.userId, invNum, quote.client_name, quote.client_rnc, quote.client_address,
       quote.subtotal, quote.tax, quote.total, quote.discount_amount||0, quote.discount_pct||0,
       invDate, due_date||null, quote.notes, payment_method, req.params.id]
    );
    await pgClient.query(
      `INSERT INTO invoice_counter(user_id,last_number) VALUES($1,$2) ON CONFLICT(user_id) DO UPDATE SET last_number=$2`,
      [req.userId, nextNum]
    );

    // Copiar ítems
    for (const item of itemsR.rows) {
      const iId = `item_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
      await pgClient.query(
        `INSERT INTO invoice_items(id,invoice_id,description,qty,price,total,discount_pct,product_id)
         VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
        [iId, invId, item.description||'', item.qty||1, item.price||0, item.total||0, item.discount_pct||0, item.product_id||null]
      );
    }

    // Marcar cotización como convertida
    await pgClient.query(`UPDATE quotes SET status='converted', invoice_id=$1 WHERE id=$2`, [invId, req.params.id]);

    await pgClient.query('COMMIT');
    res.json({ ok: true, invoice_id: invId, invoice_number: invNum });
  } catch(e) {
    await pgClient.query('ROLLBACK');
    console.error('convert quote error:', e.message);
    res.status(500).json({ error: e.message });
  } finally { pgClient.release(); }
});

// ═══════════════════════════════════════════════════════════════════════════════
// ─── FASE 2.2: FACTURAS RECURRENTES ──────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

// GET /api/recurring — listar plantillas recurrentes
app.get('/api/recurring', authMiddleware, async (req, res) => {
  try {
    const r = await query(`SELECT * FROM recurring_invoices WHERE user_id=$1 ORDER BY next_date ASC`, [req.userId]);
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/recurring — crear plantilla
app.post('/api/recurring', authMiddleware, async (req, res) => {
  try {
    const {
      name, client_name, client_rnc, subtotal, tax, total,
      discount_amount, discount_pct, notes, payment_method,
      frequency, // 'monthly' | 'bimonthly' | 'quarterly' | 'weekly'
      start_date, end_date, items
    } = req.body;
    if (!name || !total || !frequency) return res.status(400).json({ error: 'name, total y frequency requeridos' });

    const id = `rec_tmpl_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
    const nextDate = start_date || new Date().toISOString().split('T')[0];

    await query(
      `INSERT INTO recurring_invoices(id,user_id,name,client_name,client_rnc,subtotal,tax,total,
         discount_amount,discount_pct,notes,payment_method,frequency,next_date,end_date,
         items_json,is_active)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,TRUE)`,
      [id, req.userId, name, client_name||null, client_rnc||null, subtotal||0, tax||0, total,
       discount_amount||0, discount_pct||0, notes||null, payment_method||'credit',
       frequency, nextDate, end_date||null, JSON.stringify(items||[])]
    );
    res.json({ ok: true, id });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// PUT /api/recurring/:id — activar/desactivar
app.put('/api/recurring/:id', authMiddleware, async (req, res) => {
  try {
    const { is_active, name, end_date } = req.body;
    await query(
      `UPDATE recurring_invoices SET is_active=COALESCE($1,is_active), name=COALESCE($2,name), end_date=COALESCE($3,end_date)
       WHERE id=$4 AND user_id=$5`,
      [is_active, name||null, end_date||null, req.params.id, req.userId]
    );
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// DELETE /api/recurring/:id
app.delete('/api/recurring/:id', authMiddleware, async (req, res) => {
  try {
    await query(`DELETE FROM recurring_invoices WHERE id=$1 AND user_id=$2`, [req.params.id, req.userId]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/recurring/:id/generate — generar factura ahora desde plantilla
app.post('/api/recurring/:id/generate', authMiddleware, async (req, res) => {
  const pgClient = await pool.connect();
  try {
    await pgClient.query('BEGIN');
    const tmpl = await pgClient.query(
      `SELECT * FROM recurring_invoices WHERE id=$1 AND user_id=$2`, [req.params.id, req.userId]
    );
    if (!tmpl.rows[0]) { await pgClient.query('ROLLBACK'); return res.status(404).json({ error: 'Plantilla no encontrada' }); }
    const t = tmpl.rows[0];

    // Número de factura
    const cntR = await pgClient.query(`SELECT last_number FROM invoice_counter WHERE user_id=$1`, [req.userId]);
    const nextNum = (parseInt(cntR.rows[0]?.last_number) || 0) + 1;
    const invNum  = String(nextNum).padStart(6, '0');
    const invDate = new Date().toISOString().split('T')[0];
    const invId   = `inv_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;

    await pgClient.query(
      `INSERT INTO invoices(id,user_id,invoice_number,client_name,client_rnc,subtotal,tax,total,
         discount_amount,discount_pct,status,date,notes,payment_method,recurring_id)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'draft',$11,$12,$13,$14)`,
      [invId, req.userId, invNum, t.client_name, t.client_rnc,
       t.subtotal, t.tax, t.total, t.discount_amount, t.discount_pct,
       invDate, t.notes, t.payment_method, t.id]
    );
    await pgClient.query(`INSERT INTO invoice_counter(user_id,last_number) VALUES($1,$2) ON CONFLICT(user_id) DO UPDATE SET last_number=$2`, [req.userId, nextNum]);

    // Ítems
    const items = typeof t.items_json === 'string' ? JSON.parse(t.items_json) : (t.items_json || []);
    for (const item of items) {
      const iId = `item_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
      await pgClient.query(
        `INSERT INTO invoice_items(id,invoice_id,description,qty,price,total,discount_pct,product_id)
         VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
        [iId, invId, item.description||'', item.qty||1, item.price||0, item.total||0, item.discount_pct||0, item.product_id||null]
      );
    }

    // Calcular próxima fecha
    const freqDays = { weekly:7, biweekly:14, monthly:30, bimonthly:60, quarterly:90, yearly:365 };
    const days = freqDays[t.frequency] || 30;
    const nextDate = new Date();
    nextDate.setDate(nextDate.getDate() + days);
    const nextDateStr = nextDate.toISOString().split('T')[0];

    await pgClient.query(
      `UPDATE recurring_invoices SET next_date=$1, generated_count=COALESCE(generated_count,0)+1 WHERE id=$2`,
      [nextDateStr, t.id]
    );

    await pgClient.query('COMMIT');
    res.json({ ok: true, invoice_id: invId, invoice_number: invNum, next_date: nextDateStr });
  } catch(e) {
    await pgClient.query('ROLLBACK');
    res.status(500).json({ error: e.message });
  } finally { pgClient.release(); }
});

// POST /api/recurring/process-due — generar todas las vencidas (cron o manual)
app.post('/api/recurring/process-due', authMiddleware, async (req, res) => {
  try {
    const today = new Date().toISOString().split('T')[0];
    const due = await query(
      `SELECT id FROM recurring_invoices
       WHERE user_id=$1 AND is_active=TRUE AND next_date<=$2
         AND (end_date IS NULL OR end_date>=$2)`,
      [req.userId, today]
    );
    const generated = [];
    for (const row of due.rows) {
      try {
        const r = await fetch(`http://localhost:${PORT}/api/recurring/${row.id}/generate`, {
          method:'POST', headers: { 'Content-Type':'application/json', 'x-session-token': req.headers['x-session-token'] }
        });
        const d = await r.json();
        if (d.ok) generated.push({ id: row.id, invoice_number: d.invoice_number });
      } catch(e) { console.error('Recurring generate error:', e.message); }
    }
    res.json({ ok: true, processed: generated.length, generated });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════════════════════
// ─── FASE 2.3: RETENCIONES ISR / ITBIS (RD) ──────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

// GET /api/retenciones — listar retenciones
app.get('/api/retenciones', authMiddleware, async (req, res) => {
  try {
    const { month, year } = req.query;
    let sql = `SELECT r.*, c.name as client_name, v.name as vendor_name
               FROM retenciones r
               LEFT JOIN clients c ON c.id=r.client_id
               LEFT JOIN vendors v ON v.id=r.vendor_id
               WHERE r.user_id=$1`;
    const params = [req.userId]; let p = 2;
    if (month && year) {
      sql += ` AND EXTRACT(MONTH FROM r.date)=$${p++} AND EXTRACT(YEAR FROM r.date)=$${p++}`;
      params.push(month, year);
    }
    sql += ` ORDER BY r.date DESC`;
    const result = await query(sql, params);
    res.json(result.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/retenciones — registrar retención
app.post('/api/retenciones', authMiddleware, async (req, res) => {
  const pgClient = await pool.connect();
  try {
    await pgClient.query('BEGIN');
    const {
      tipo,          // 'isr' | 'itbis'
      subtipo,       // 'servicios' | 'alquileres' | 'honorarios' | 'otros' (para ISR)
      entity_type,   // 'client' | 'vendor'
      client_id, vendor_id,
      invoice_id,
      base_amount,   // monto base sobre el que se aplica la retención
      retention_pct, // % de retención (ej: 10 para ISR servicios, 30 para ITBIS)
      date, ncf, notes
    } = req.body;

    if (!tipo || !base_amount || !retention_pct) {
      await pgClient.query('ROLLBACK');
      return res.status(400).json({ error: 'tipo, base_amount y retention_pct requeridos' });
    }

    const retention_amount = Math.round(parseFloat(base_amount) * parseFloat(retention_pct) / 100 * 100) / 100;
    const id = `ret_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
    const retDate = date || new Date().toISOString().split('T')[0];

    await pgClient.query(
      `INSERT INTO retenciones(id,user_id,tipo,subtipo,entity_type,client_id,vendor_id,
         invoice_id,base_amount,retention_pct,retention_amount,date,ncf,notes,status)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,'pending')`,
      [id, req.userId, tipo, subtipo||null, entity_type||'client',
       client_id||null, vendor_id||null, invoice_id||null,
       parseFloat(base_amount), parseFloat(retention_pct), retention_amount,
       retDate, ncf||null, notes||null]
    );

    // Asiento contable de la retención
    // ISR retenido: Débito Gasto ISR / Crédito ISR por Pagar al Estado
    // ITBIS retenido: Débito ITBIS Retenido (activo) / Crédito CxC (reduce lo cobrable)
    const acctR = await pgClient.query(
      `SELECT id, code FROM accounts WHERE user_id=$1
       AND code IN ('1101','1.1.01','1102','1.1.02','1201','1.2.01',
                    '2301','2.3.01','2302','2.3.02','2303',
                    '6201','6.2.01','6202','6.2.02','1401','1.4.01')`,
      [req.userId]
    );
    const am = {}; acctR.rows.forEach(a => { am[a.code] = a.id; });

    // Cuentas para retención ISR por pagar al estado (pasivo)
    const isrPagarAcct = am['2301'] || am['2.3.01'] || am['2302'] || am['2.3.02'] || am['2303'];
    // Gasto ISR
    const isrGastoAcct = am['6201'] || am['6.2.01'] || am['6202'] || am['6.2.02'];
    // ITBIS retenido por cobrar (activo — cuando somos nosotros quienes retenemos al proveedor)
    const itbisRetAcct = am['1401'] || am['1.4.01'];
    // CxC y Caja/Banco
    const cxcAcct  = am['1201'] || am['1.2.01'];
    const cashAcct = am['1102'] || am['1.1.02'] || am['1101'] || am['1.1.01'];

    if (tipo === 'isr' && isrGastoAcct && isrPagarAcct) {
      // El cliente nos retiene ISR: registramos el gasto y la cuenta por pagar al estado
      await insertJournalEntry(pgClient, req.userId, retDate,
        `Retención ISR ${retention_pct}% — ${subtipo||'servicios'} — RD$ ${retention_amount}`,
        'retencion', id,
        [
          { acct: isrGastoAcct,  d: retention_amount, c: 0 },
          { acct: isrPagarAcct,  d: 0, c: retention_amount },
        ]
      );
    } else if (tipo === 'itbis' && cxcAcct) {
      // El cliente retiene 30% del ITBIS: reduce lo que nos deben en CxC
      await insertJournalEntry(pgClient, req.userId, retDate,
        `Retención ITBIS ${retention_pct}% — RD$ ${retention_amount}`,
        'retencion', id,
        itbisRetAcct ? [
          { acct: itbisRetAcct, d: retention_amount, c: 0 },  // ITBIS retenido por cobrar
          { acct: cxcAcct,      d: 0, c: retention_amount },  // Reduce CxC
        ] : [
          { acct: cashAcct || cxcAcct, d: retention_amount, c: 0 },
          { acct: cxcAcct, d: 0, c: retention_amount },
        ]
      );
    }

    await pgClient.query('COMMIT');
    res.json({ ok: true, id, retention_amount });
  } catch(e) {
    await pgClient.query('ROLLBACK');
    res.status(500).json({ error: e.message });
  } finally { pgClient.release(); }
});

// DELETE /api/retenciones/:id
app.delete('/api/retenciones/:id', authMiddleware, async (req, res) => {
  try {
    await query(`DELETE FROM retenciones WHERE id=$1 AND user_id=$2`, [req.params.id, req.userId]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/retenciones/resumen — totales ISR e ITBIS por período
app.get('/api/retenciones/resumen', authMiddleware, async (req, res) => {
  try {
    const { year } = req.query;
    const y = parseInt(year) || new Date().getFullYear();
    const r = await query(
      `SELECT
         tipo,
         EXTRACT(MONTH FROM date) as month,
         SUM(base_amount) as base_total,
         SUM(retention_amount) as ret_total,
         COUNT(*) as count
       FROM retenciones
       WHERE user_id=$1 AND EXTRACT(YEAR FROM date)=$2
       GROUP BY tipo, EXTRACT(MONTH FROM date)
       ORDER BY month, tipo`,
      [req.userId, y]
    );
    // Totales anuales
    const totR = await query(
      `SELECT tipo, SUM(base_amount) as base_total, SUM(retention_amount) as ret_total
       FROM retenciones WHERE user_id=$1 AND EXTRACT(YEAR FROM date)=$2
       GROUP BY tipo`,
      [req.userId, y]
    );
    res.json({ monthly: r.rows, annual: totR.rows, year: y });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════════════════════
// ─── FASE 3.1: DASHBOARD INTELIGENTE ─────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

// GET /api/dashboard/stats — datos completos para dashboard inteligente
app.get('/api/dashboard/stats', authMiddleware, async (req, res) => {
  try {
    const today = new Date();
    const yr    = today.getFullYear();
    const mo    = today.getMonth() + 1; // 1-12

    // ── Ventas últimos 6 meses ──
    const salesHistory = await query(
      `SELECT
         EXTRACT(YEAR  FROM je.date)::int as year,
         EXTRACT(MONTH FROM je.date)::int as month,
         SUM(jl.credit) as ventas,
         SUM(CASE WHEN a.class=5 THEN jl.debit ELSE 0 END) as cmv
       FROM journal_lines jl
       JOIN journal_entries je ON je.id=jl.journal_entry_id
       JOIN accounts a ON a.id=jl.account_id
       WHERE je.user_id=$1
         AND a.class IN (4,5)
         AND je.date >= (CURRENT_DATE - INTERVAL '6 months')
       GROUP BY year, month
       ORDER BY year, month`,
      [req.userId]
    );

    // ── Top 5 productos más vendidos (por cantidad) ──
    const topProducts = await query(
      `SELECT p.name, p.code, p.sale_price, p.cost_price,
              COALESCE(SUM(ii.qty), 0) as qty_sold,
              COALESCE(SUM(ii.total), 0) as revenue,
              COALESCE(SUM(ii.qty * p.cost_price), 0) as cost_total
       FROM products p
       JOIN invoice_items ii ON ii.product_id = p.id
       JOIN invoices inv ON inv.id = ii.invoice_id
       WHERE p.user_id=$1
         AND inv.status IN ('issued','paid','partial')
         AND inv.date >= (CURRENT_DATE - INTERVAL '12 months')
       GROUP BY p.id, p.name, p.code, p.sale_price, p.cost_price
       ORDER BY revenue DESC
       LIMIT 5`,
      [req.userId]
    );

    // ── Top 5 clientes por facturación ──
    const topClients = await query(
      `SELECT
         COALESCE(inv.client_name, 'Sin nombre') as name,
         COUNT(inv.id) as invoice_count,
         SUM(inv.total) as total_billed,
         SUM(inv.paid_amount) as total_paid,
         SUM(inv.total - inv.paid_amount) as outstanding
       FROM invoices inv
       WHERE inv.user_id=$1
         AND inv.status IN ('issued','paid','partial')
         AND inv.date >= (CURRENT_DATE - INTERVAL '12 months')
       GROUP BY inv.client_name
       ORDER BY total_billed DESC
       LIMIT 5`,
      [req.userId]
    );

    // ── KPIs del mes actual ──
    const monthFrom = `${yr}-${String(mo).padStart(2,'0')}-01`;
    const monthTo   = `${yr}-${String(mo).padStart(2,'0')}-${new Date(yr,mo,0).getDate()}`;

    const monthSales = await query(
      `SELECT
         COALESCE(SUM(CASE WHEN a.class=4 THEN jl.credit-jl.debit ELSE 0 END),0) as ingresos,
         COALESCE(SUM(CASE WHEN a.class=5 THEN jl.debit-jl.credit ELSE 0 END),0) as cmv,
         COALESCE(SUM(CASE WHEN a.class=6 THEN jl.debit-jl.credit ELSE 0 END),0) as gastos
       FROM journal_lines jl
       JOIN journal_entries je ON je.id=jl.journal_entry_id
       JOIN accounts a ON a.id=jl.account_id
       WHERE je.user_id=$1 AND je.date>=$2 AND je.date<=$3 AND a.class IN (4,5,6)`,
      [req.userId, monthFrom, monthTo]
    );

    // ── Comparación mes anterior ──
    const prevMo    = mo === 1 ? 12 : mo - 1;
    const prevYr    = mo === 1 ? yr - 1 : yr;
    const prevFrom  = `${prevYr}-${String(prevMo).padStart(2,'0')}-01`;
    const prevTo    = `${prevYr}-${String(prevMo).padStart(2,'0')}-${new Date(prevYr,prevMo,0).getDate()}`;

    const prevSales = await query(
      `SELECT COALESCE(SUM(CASE WHEN a.class=4 THEN jl.credit-jl.debit ELSE 0 END),0) as ingresos
       FROM journal_lines jl
       JOIN journal_entries je ON je.id=jl.journal_entry_id
       JOIN accounts a ON a.id=jl.account_id
       WHERE je.user_id=$1 AND je.date>=$2 AND je.date<=$3 AND a.class=4`,
      [req.userId, prevFrom, prevTo]
    );

    // ── Facturas pendientes vencidas ──
    const overdueInvoices = await query(
      `SELECT COUNT(*) as count, COALESCE(SUM(total-paid_amount),0) as total
       FROM invoices
       WHERE user_id=$1 AND status IN ('issued','partial')
         AND due_date < CURRENT_DATE`,
      [req.userId]
    );

    // ── CxC vencidas ──
    const overdueCxC = await query(
      `SELECT COUNT(*) as count,
              COALESCE(SUM(total_amount-paid_amount),0) as total
       FROM receivables
       WHERE user_id=$1 AND status IN ('pending','partial')
         AND due_date < CURRENT_DATE`,
      [req.userId]
    );

    const ms = monthSales.rows[0];
    const ps = prevSales.rows[0];
    const ingAct  = parseFloat(ms.ingresos||0);
    const ingPrev = parseFloat(ps.ingresos||0);
    const varPct  = ingPrev > 0 ? Math.round((ingAct-ingPrev)/ingPrev*100) : null;

    res.json({
      current_month: { year: yr, month: mo },
      kpis: {
        ingresos:     ingAct,
        cmv:          parseFloat(ms.cmv||0),
        gastos:       parseFloat(ms.gastos||0),
        utilidad:     ingAct - parseFloat(ms.cmv||0) - parseFloat(ms.gastos||0),
        vs_prev_pct:  varPct,
      },
      sales_history: salesHistory.rows.map(r => ({
        year: r.year, month: r.month,
        ventas: parseFloat(r.ventas||0),
        cmv:    parseFloat(r.cmv||0),
        margen: parseFloat(r.ventas||0) - parseFloat(r.cmv||0),
      })),
      top_products: topProducts.rows.map(r => ({
        name:      r.name, code: r.code,
        qty_sold:  parseFloat(r.qty_sold||0),
        revenue:   parseFloat(r.revenue||0),
        margin:    parseFloat(r.revenue||0) - parseFloat(r.cost_total||0),
        margin_pct: parseFloat(r.revenue||0) > 0
          ? Math.round((parseFloat(r.revenue||0)-parseFloat(r.cost_total||0))/parseFloat(r.revenue||0)*100)
          : 0,
      })),
      top_clients: topClients.rows.map(r => ({
        name:          r.name,
        invoice_count: parseInt(r.invoice_count||0),
        total_billed:  parseFloat(r.total_billed||0),
        total_paid:    parseFloat(r.total_paid||0),
        outstanding:   parseFloat(r.outstanding||0),
      })),
      overdue: {
        invoices_count: parseInt(overdueInvoices.rows[0]?.count||0),
        invoices_total: parseFloat(overdueInvoices.rows[0]?.total||0),
        cxc_count:      parseInt(overdueCxC.rows[0]?.count||0),
        cxc_total:      parseFloat(overdueCxC.rows[0]?.total||0),
      }
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════════════════════
// ─── FASE 3.2: ALERTAS INTELIGENTES ──────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

// GET /api/alerts — retorna todas las alertas activas
app.get('/api/alerts', authMiddleware, async (req, res) => {
  try {
    const alerts = [];
    const today  = new Date().toISOString().split('T')[0];
    const in7    = new Date(Date.now() + 7*24*60*60*1000).toISOString().split('T')[0];
    const in30   = new Date(Date.now() + 30*24*60*60*1000).toISOString().split('T')[0];

    // 1. Stock bajo o agotado
    const stockAlerts = await query(
      `SELECT id, name, code, stock_current, stock_minimum, unit
       FROM products
       WHERE user_id=$1 AND stock_current <= stock_minimum AND stock_minimum > 0
       ORDER BY (stock_current/NULLIF(stock_minimum,0)) ASC
       LIMIT 10`,
      [req.userId]
    );
    stockAlerts.rows.forEach(p => {
      const isOut = parseFloat(p.stock_current) <= 0;
      alerts.push({
        type:     isOut ? 'danger' : 'warning',
        category: 'stock',
        icon:     isOut ? '🚨' : '⚡',
        title:    isOut ? `${p.name} — AGOTADO` : `${p.name} — Stock bajo`,
        detail:   `Stock: ${p.stock_current} ${p.unit||''} (mín: ${p.stock_minimum})`,
        action:   'Registrar entrada',
        action_page: 'movimientos-inv',
        product_id: p.id,
      });
    });

    // 2. Facturas vencidas
    const overdueInv = await query(
      `SELECT id, invoice_number, client_name, total, paid_amount, due_date
       FROM invoices
       WHERE user_id=$1 AND status IN ('issued','partial')
         AND due_date < $2
       ORDER BY due_date ASC LIMIT 10`,
      [req.userId, today]
    );
    overdueInv.rows.forEach(inv => {
      const pend = parseFloat(inv.total||0) - parseFloat(inv.paid_amount||0);
      alerts.push({
        type: 'danger', category: 'invoice_overdue',
        icon: '🧾', title: `Factura ${inv.invoice_number} vencida`,
        detail: `${inv.client_name||'Sin cliente'} — RD$ ${fmt(pend)} pendiente · Venció: ${inv.due_date}`,
        action: 'Ver facturas', action_page: 'facturas',
      });
    });

    // 3. Facturas por vencer en 7 días
    const soonInv = await query(
      `SELECT id, invoice_number, client_name, total, paid_amount, due_date
       FROM invoices
       WHERE user_id=$1 AND status IN ('issued','partial')
         AND due_date >= $2 AND due_date <= $3
       ORDER BY due_date ASC LIMIT 5`,
      [req.userId, today, in7]
    );
    soonInv.rows.forEach(inv => {
      const pend = parseFloat(inv.total||0) - parseFloat(inv.paid_amount||0);
      const dias = Math.ceil((new Date(inv.due_date)-new Date(today))/(1000*60*60*24));
      alerts.push({
        type: 'warning', category: 'invoice_soon',
        icon: '⏰', title: `Factura ${inv.invoice_number} vence en ${dias} día(s)`,
        detail: `${inv.client_name||'Sin cliente'} — RD$ ${fmt(pend)}`,
        action: 'Ver facturas', action_page: 'facturas',
      });
    });

    // 4. CxC vencidas
    const overdueCxC = await query(
      `SELECT r.id, c.name as client_name,
              r.total_amount-r.paid_amount as outstanding, r.due_date
       FROM receivables r LEFT JOIN clients c ON c.id=r.client_id
       WHERE r.user_id=$1 AND r.status IN ('pending','partial')
         AND r.due_date < $2
       ORDER BY r.due_date ASC LIMIT 5`,
      [req.userId, today]
    );
    overdueCxC.rows.forEach(r => {
      alerts.push({
        type: 'danger', category: 'cxc_overdue',
        icon: '💰', title: `CxC vencida — ${r.client_name||'Cliente'}`,
        detail: `RD$ ${fmt(r.outstanding)} pendiente · Venció: ${r.due_date}`,
        action: 'Ver CxC', action_page: 'cobrar',
      });
    });

    // 5. CxP por vencer en 7 días
    const soonCxP = await query(
      `SELECT p.id, v.name as vendor_name,
              p.total_amount-p.paid_amount as outstanding, p.due_date
       FROM payables p LEFT JOIN vendors v ON v.id=p.vendor_id
       WHERE p.user_id=$1 AND p.status IN ('pending','partial')
         AND p.due_date >= $2 AND p.due_date <= $3
       ORDER BY p.due_date ASC LIMIT 5`,
      [req.userId, today, in7]
    );
    soonCxP.rows.forEach(p => {
      const dias = Math.ceil((new Date(p.due_date)-new Date(today))/(1000*60*60*24));
      alerts.push({
        type: 'warning', category: 'cxp_soon',
        icon: '💳', title: `Pago a ${p.vendor_name||'Proveedor'} vence en ${dias} día(s)`,
        detail: `RD$ ${fmt(p.outstanding)} por pagar`,
        action: 'Ver CxP', action_page: 'pagar',
      });
    });

    // 6. Cotizaciones aprobadas sin convertir
    const pendingQuotes = await query(
      `SELECT id, quote_number, client_name, total, valid_until
       FROM quotes
       WHERE user_id=$1 AND status='approved'
       ORDER BY valid_until ASC NULLS LAST LIMIT 5`,
      [req.userId]
    ).catch(()=>({rows:[]}));
    pendingQuotes.rows.forEach(q => {
      const isExpiring = q.valid_until && q.valid_until <= in7;
      alerts.push({
        type: isExpiring ? 'warning' : 'info',
        category: 'quote_approved',
        icon: '📄', title: `Cotización ${q.quote_number} aprobada sin convertir`,
        detail: `${q.client_name||''} — RD$ ${fmt(q.total)}${isExpiring?' · ⚠️ Vence pronto':''}`,
        action: 'Convertir a Factura', action_page: 'cotizaciones',
      });
    });

    // 7. Facturas recurrentes vencidas
    const dueRecurring = await query(
      `SELECT id, name, next_date, total
       FROM recurring_invoices
       WHERE user_id=$1 AND is_active=TRUE AND next_date<=$2
         AND (end_date IS NULL OR end_date>=$2)
       LIMIT 5`,
      [req.userId, today]
    ).catch(()=>({rows:[]}));
    dueRecurring.rows.forEach(r => {
      alerts.push({
        type: 'warning', category: 'recurring_due',
        icon: '🔄', title: `Factura recurrente vencida: ${r.name}`,
        detail: `RD$ ${fmt(r.total)} · Fecha: ${r.next_date}`,
        action: 'Generar ahora', action_page: 'recurrentes',
      });
    });

    // Ordenar: danger primero, luego warning, luego info
    const priority = { danger:0, warning:1, info:2 };
    alerts.sort((a,b) => (priority[a.type]||2) - (priority[b.type]||2));

    res.json({ alerts, count: alerts.length, has_danger: alerts.some(a=>a.type==='danger') });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════════════════════
// ─── FASE 3.3: DGII 606 / 607 ────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

// GET /api/dgii/606 — Compras (para tu declaración de ITBIS)
// Formato: mes/año de compras a proveedores con ITBIS
app.get('/api/dgii/606', authMiddleware, async (req, res) => {
  try {
    const { month, year } = req.query;
    if (!month || !year) return res.status(400).json({ error: 'month y year requeridos' });
    const m = parseInt(month), y = parseInt(year);
    const from = `${y}-${String(m).padStart(2,'0')}-01`;
    const lastDay = new Date(y, m, 0).getDate();
    const to = `${y}-${String(m).padStart(2,'0')}-${lastDay}`;

    // Cuentas por pagar del período como base de compras
    const rows = await query(
      `SELECT
         p.id,
         v.name                        as proveedor,
         COALESCE(v.rnc,'')            as rnc_cedula,
         COALESCE(v.vendor_type,'vendor') as tipo_bienes,
         TO_CHAR(p.created_at,'YYYYMMDD') as fecha_comprobante,
         ''                            as ncf_comprobante,
         p.total_amount                as monto_facturado,
         0                             as itbis_facturado,
         COALESCE(ret.retention_amount,0) as itbis_retenido,
         COALESCE(ret.retention_amount,0) as isr_retenido
       FROM payables p
       JOIN vendors v ON v.id=p.vendor_id
       LEFT JOIN retenciones ret ON ret.invoice_id=p.id AND ret.user_id=p.user_id
       WHERE p.user_id=$1
         AND p.created_at::date>=$2 AND p.created_at::date<=$3
       ORDER BY p.created_at ASC`,
      [req.userId, from, to]
    );

    // Totales
    const totales = {
      cantidad_registros: rows.rows.length,
      total_monto:        rows.rows.reduce((s,r)=>s+parseFloat(r.monto_facturado||0),0),
      total_itbis:        rows.rows.reduce((s,r)=>s+parseFloat(r.itbis_facturado||0),0),
      total_ret_itbis:    rows.rows.reduce((s,r)=>s+parseFloat(r.itbis_retenido||0),0),
      total_ret_isr:      rows.rows.reduce((s,r)=>s+parseFloat(r.isr_retenido||0),0),
    };

    res.json({ period: `${m}/${y}`, from, to, rows: rows.rows, totales });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/dgii/607 — Ventas (para tu declaración de ITBIS)
app.get('/api/dgii/607', authMiddleware, async (req, res) => {
  try {
    const { month, year } = req.query;
    if (!month || !year) return res.status(400).json({ error: 'month y year requeridos' });
    const m = parseInt(month), y = parseInt(year);
    const from = `${y}-${String(m).padStart(2,'0')}-01`;
    const lastDay = new Date(y, m, 0).getDate();
    const to = `${y}-${String(m).padStart(2,'0')}-${lastDay}`;

    const rows = await query(
      `SELECT
         inv.id,
         COALESCE(inv.client_name,'Consumidor Final')  as nombre_cliente,
         COALESCE(inv.client_rnc,'')                   as rnc_cedula,
         TO_CHAR(inv.date,'YYYYMMDD')                  as fecha_comprobante,
         COALESCE(inv.invoice_number,'')               as ncf,
         inv.subtotal                                  as monto_facturado,
         inv.tax                                       as itbis_facturado,
         inv.total                                     as total,
         COALESCE(ret.retention_amount,0)              as itbis_retenido_cliente
       FROM invoices inv
       LEFT JOIN retenciones ret
         ON ret.invoice_id=inv.id AND ret.tipo='itbis' AND ret.user_id=inv.user_id
       WHERE inv.user_id=$1
         AND inv.date>=$2 AND inv.date<=$3
         AND inv.status IN ('issued','paid','partial')
       ORDER BY inv.date ASC`,
      [req.userId, from, to]
    );

    const totales = {
      cantidad_registros: rows.rows.length,
      total_monto:        rows.rows.reduce((s,r)=>s+parseFloat(r.monto_facturado||0),0),
      total_itbis:        rows.rows.reduce((s,r)=>s+parseFloat(r.itbis_facturado||0),0),
      total_ret_itbis:    rows.rows.reduce((s,r)=>s+parseFloat(r.itbis_retenido_cliente||0),0),
    };

    res.json({ period: `${m}/${y}`, from, to, rows: rows.rows, totales });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/dgii/itbis — resumen ITBIS para declaración IT-1
app.get('/api/dgii/itbis', authMiddleware, async (req, res) => {
  try {
    const { month, year } = req.query;
    if (!month || !year) return res.status(400).json({ error: 'month y year requeridos' });
    const m = parseInt(month), y = parseInt(year);
    const from = `${y}-${String(m).padStart(2,'0')}-01`;
    const lastDay = new Date(y, m, 0).getDate();
    const to = `${y}-${String(m).padStart(2,'0')}-${lastDay}`;

    // ITBIS cobrado en ventas
    const cobrado = await query(
      `SELECT COALESCE(SUM(tax),0) as total
       FROM invoices
       WHERE user_id=$1 AND date>=$2 AND date<=$3
         AND status IN ('issued','paid','partial')`,
      [req.userId, from, to]
    );

    // ITBIS pagado en compras (estimado de CxP)
    const pagado = await query(
      `SELECT COALESCE(SUM(total_amount*0.18),0) as total
       FROM payables
       WHERE user_id=$1 AND created_at::date>=$2 AND created_at::date<=$3`,
      [req.userId, from, to]
    );

    // ITBIS retenido por clientes
    const retenido = await query(
      `SELECT COALESCE(SUM(retention_amount),0) as total
       FROM retenciones
       WHERE user_id=$1 AND tipo='itbis' AND date>=$2 AND date<=$3`,
      [req.userId, from, to]
    );

    const itbisCobrado = parseFloat(cobrado.rows[0]?.total||0);
    const itbisPagado  = parseFloat(pagado.rows[0]?.total||0);
    const itbisRet     = parseFloat(retenido.rows[0]?.total||0);
    const saldo        = itbisCobrado - itbisPagado - itbisRet;

    res.json({
      period: `${m}/${y}`,
      itbis_cobrado:  itbisCobrado,
      itbis_pagado:   itbisPagado,
      itbis_retenido: itbisRet,
      saldo_a_pagar:  Math.max(0, saldo),
      saldo_a_favor:  Math.max(0, -saldo),
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════════════════════
// ─── FASE 4.1: MÚLTIPLES MONEDAS ─────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

// GET /api/exchange-rates — tasas de cambio actuales
app.get('/api/exchange-rates', authMiddleware, async (req, res) => {
  try {
    const r = await query(
      `SELECT * FROM exchange_rates WHERE user_id=$1 ORDER BY date DESC LIMIT 30`,
      [req.userId]
    );
    // Si no hay tasas registradas, devolver defaults
    if (!r.rows.length) {
      return res.json({
        rates: [{ currency:'USD', rate:60.00, date: new Date().toISOString().split('T')[0] }],
        current: { USD: 60.00 }
      });
    }
    // Tasa más reciente por moneda
    const current = {};
    const seen = new Set();
    r.rows.forEach(row => {
      if (!seen.has(row.currency)) { current[row.currency] = parseFloat(row.rate); seen.add(row.currency); }
    });
    res.json({ rates: r.rows, current });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/exchange-rates — registrar tasa de cambio
app.post('/api/exchange-rates', authMiddleware, async (req, res) => {
  try {
    const { currency, rate, date } = req.body;
    if (!currency || !rate) return res.status(400).json({ error: 'currency y rate requeridos' });
    const id = `exr_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
    await query(
      `INSERT INTO exchange_rates(id,user_id,currency,rate,date)
       VALUES($1,$2,$3,$4,$5)
       ON CONFLICT(user_id,currency,date) DO UPDATE SET rate=$4`,
      [id, req.userId, currency.toUpperCase(), parseFloat(rate), date||new Date().toISOString().split('T')[0]]
    );
    res.json({ ok: true, id });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// DELETE /api/exchange-rates/:id
app.delete('/api/exchange-rates/:id', authMiddleware, async (req, res) => {
  try {
    await query(`DELETE FROM exchange_rates WHERE id=$1 AND user_id=$2`, [req.params.id, req.userId]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/exchange-rates/convert — convertir monto entre monedas
app.get('/api/exchange-rates/convert', authMiddleware, async (req, res) => {
  try {
    const { amount, from, to } = req.query;
    if (!amount || !from || !to) return res.status(400).json({ error: 'amount, from y to requeridos' });
    if (from === to) return res.json({ result: parseFloat(amount), rate: 1 });

    // Obtener tasas vs DOP (moneda base)
    const getRate = async (currency) => {
      if (currency === 'DOP') return 1;
      const r = await query(
        `SELECT rate FROM exchange_rates WHERE user_id=$1 AND currency=$2 ORDER BY date DESC LIMIT 1`,
        [req.userId, currency]
      );
      return parseFloat(r.rows[0]?.rate || (currency==='USD'?60:1));
    };

    const fromRate = await getRate(from.toUpperCase());
    const toRate   = await getRate(to.toUpperCase());
    // Convertir: amount(from) → DOP → to
    const inDOP    = parseFloat(amount) * fromRate;
    const result   = inDOP / toRate;
    const rate     = fromRate / toRate;
    res.json({ result: Math.round(result*100)/100, rate: Math.round(rate*10000)/10000, from, to });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════════════════════
// ─── FASE 4.2: MULTI-SUCURSAL ─────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

// GET /api/branches
app.get('/api/branches', authMiddleware, async (req, res) => {
  try {
    const r = await query(
      `SELECT b.*, COUNT(DISTINCT inv.id) as invoice_count,
              COALESCE(SUM(inv.total),0) as total_sales
       FROM branches b
       LEFT JOIN invoices inv ON inv.branch_id=b.id AND inv.status IN ('issued','paid')
       WHERE b.user_id=$1
       GROUP BY b.id ORDER BY b.name`,
      [req.userId]
    );
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/branches
app.post('/api/branches', authMiddleware, async (req, res) => {
  try {
    const { name, address, phone, email, manager, rnc, is_active=true } = req.body;
    if (!name) return res.status(400).json({ error: 'name requerido' });
    const id = `branch_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
    await query(
      `INSERT INTO branches(id,user_id,name,address,phone,email,manager,rnc,is_active)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
      [id, req.userId, name, address||null, phone||null, email||null, manager||null, rnc||null, is_active]
    );
    res.json({ ok: true, id });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// PUT /api/branches/:id
app.put('/api/branches/:id', authMiddleware, async (req, res) => {
  try {
    const { name, address, phone, email, manager, rnc, is_active } = req.body;
    await query(
      `UPDATE branches SET name=COALESCE($1,name), address=COALESCE($2,address),
       phone=COALESCE($3,phone), email=COALESCE($4,email), manager=COALESCE($5,manager),
       rnc=COALESCE($6,rnc), is_active=COALESCE($7,is_active)
       WHERE id=$8 AND user_id=$9`,
      [name||null, address||null, phone||null, email||null, manager||null, rnc||null, is_active, req.params.id, req.userId]
    );
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// DELETE /api/branches/:id
app.delete('/api/branches/:id', authMiddleware, async (req, res) => {
  try {
    await query(`DELETE FROM branches WHERE id=$1 AND user_id=$2`, [req.params.id, req.userId]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/branches/:id/stats — estadísticas de una sucursal
app.get('/api/branches/:id/stats', authMiddleware, async (req, res) => {
  try {
    const { month, year } = req.query;
    const from = month && year ? `${year}-${String(month).padStart(2,'0')}-01` : null;
    const lastDay = month && year ? new Date(year, month, 0).getDate() : null;
    const to = month && year ? `${year}-${String(month).padStart(2,'0')}-${lastDay}` : null;

    let sql = `SELECT COUNT(*) as invoices, COALESCE(SUM(total),0) as ventas,
               COALESCE(SUM(paid_amount),0) as cobrado,
               COALESCE(SUM(total-paid_amount),0) as pendiente
               FROM invoices WHERE user_id=$1 AND branch_id=$2 AND status IN ('issued','paid','partial')`;
    const params = [req.userId, req.params.id];
    if (from) { sql += ` AND date>=$3 AND date<=$4`; params.push(from, to); }

    const r = await query(sql, params);
    res.json(r.rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════════════════════
// ─── FASE 4.3: ROLES Y PERMISOS DE USUARIOS ──────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

// Roles: admin (todo), contador (todo menos eliminar usuarios), cajero (solo facturas + cobros)
const ROLE_PERMISSIONS = {
  admin:    ['*'],
  contador: ['invoices','quotes','products','clients','vendors','receivables','payables','journal','accounts','reports','inventory','assets','income','retenciones','recurring','branches'],
  cajero:   ['invoices_read','invoices_create','clients_read','products_read','receivables_read','receivables_pay'],
};

// GET /api/user-roles — listar usuarios con roles
app.get('/api/user-roles', authMiddleware, async (req, res) => {
  try {
    // Solo admins pueden ver roles de otros usuarios
    const adminR = await query(`SELECT is_admin FROM users WHERE id=$1`, [req.userId]);
    if (!adminR.rows[0]?.is_admin) return res.status(403).json({ error: 'Solo admins' });

    const r = await query(
      `SELECT u.id, uc.username, ur.role, ur.branch_id, b.name as branch_name,
              ur.is_active, ur.created_at
       FROM users u
       LEFT JOIN user_credentials uc ON uc.user_id=u.id
       LEFT JOIN user_roles ur ON ur.user_id=u.id AND ur.owner_id=$1
       LEFT JOIN branches b ON b.id=ur.branch_id
       ORDER BY uc.username`,
      [req.userId]
    );
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/user-roles — asignar rol a usuario
app.post('/api/user-roles', authMiddleware, async (req, res) => {
  try {
    const adminR = await query(`SELECT is_admin FROM users WHERE id=$1`, [req.userId]);
    if (!adminR.rows[0]?.is_admin) return res.status(403).json({ error: 'Solo admins' });

    const { target_user_id, role, branch_id } = req.body;
    if (!target_user_id || !role) return res.status(400).json({ error: 'target_user_id y role requeridos' });
    if (!['admin','contador','cajero'].includes(role)) return res.status(400).json({ error: 'Rol inválido' });

    const id = `role_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
    await query(
      `INSERT INTO user_roles(id,user_id,owner_id,role,branch_id,is_active)
       VALUES($1,$2,$3,$4,$5,TRUE)
       ON CONFLICT(user_id,owner_id) DO UPDATE SET role=$4, branch_id=$5, is_active=TRUE`,
      [id, target_user_id, req.userId, role, branch_id||null]
    );
    res.json({ ok: true, id });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// DELETE /api/user-roles/:userId — revocar acceso
app.delete('/api/user-roles/:userId', authMiddleware, async (req, res) => {
  try {
    const adminR = await query(`SELECT is_admin FROM users WHERE id=$1`, [req.userId]);
    if (!adminR.rows[0]?.is_admin) return res.status(403).json({ error: 'Solo admins' });
    await query(`DELETE FROM user_roles WHERE user_id=$1 AND owner_id=$2`, [req.params.userId, req.userId]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════════════════════
// ─── FASE 4.4: LINKS DE PAGO (AZUL / PAGOFLASH / GENÉRICO) ───────────────────
// ═══════════════════════════════════════════════════════════════════════════════

// GET /api/payment-config — configuración de pasarelas
app.get('/api/payment-config', authMiddleware, async (req, res) => {
  try {
    const r = await query(
      `SELECT provider, is_active, config_public FROM payment_configs WHERE user_id=$1`,
      [req.userId]
    );
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/payment-config — guardar configuración de pasarela
app.post('/api/payment-config', authMiddleware, async (req, res) => {
  try {
    const { provider, api_key, merchant_id, config_public, is_active } = req.body;
    if (!provider) return res.status(400).json({ error: 'provider requerido' });
    const id = `pc_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
    await query(
      `INSERT INTO payment_configs(id,user_id,provider,api_key_enc,merchant_id,config_public,is_active)
       VALUES($1,$2,$3,$4,$5,$6,$7)
       ON CONFLICT(user_id,provider) DO UPDATE SET
         api_key_enc=COALESCE($4,api_key_enc), merchant_id=COALESCE($5,merchant_id),
         config_public=COALESCE($6,config_public), is_active=$7`,
      [id, req.userId, provider, api_key||null, merchant_id||null,
       config_public ? JSON.stringify(config_public) : null, is_active!==false]
    );
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/payment-links — generar link de pago para una factura
app.post('/api/payment-links', authMiddleware, async (req, res) => {
  try {
    const { invoice_id, provider = 'generic', expires_in_days = 3 } = req.body;
    if (!invoice_id) return res.status(400).json({ error: 'invoice_id requerido' });

    const inv = await query(`SELECT * FROM invoices WHERE id=$1 AND user_id=$2`, [invoice_id, req.userId]);
    if (!inv.rows[0]) return res.status(404).json({ error: 'Factura no encontrada' });
    const invoice = inv.rows[0];

    // Token único para el link
    const token     = crypto.randomBytes(20).toString('hex');
    const expiresAt = new Date();
    expiresAt.setDate(expiresAt.getDate() + parseInt(expires_in_days));
    const id = `pl_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;

    await query(
      `INSERT INTO payment_links(id,user_id,invoice_id,token,provider,amount,currency,expires_at,status)
       VALUES($1,$2,$3,$4,$5,$6,'DOP',$7,'active')`,
      [id, req.userId, invoice_id, token, provider,
       parseFloat(invoice.total)-parseFloat(invoice.paid_amount||0), expiresAt.toISOString()]
    );

    const baseUrl = process.env.API_BASE || `https://miscuentas-contable-app-production.up.railway.app`;
    const payUrl  = `${baseUrl}/pay/${token}`;

    res.json({ ok: true, id, token, url: payUrl, expires_at: expiresAt.toISOString(), amount: parseFloat(invoice.total)-parseFloat(invoice.paid_amount||0) });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/payment-links — listar links de pago
app.get('/api/payment-links', authMiddleware, async (req, res) => {
  try {
    const r = await query(
      `SELECT pl.*, inv.invoice_number, inv.client_name
       FROM payment_links pl
       LEFT JOIN invoices inv ON inv.id=pl.invoice_id
       WHERE pl.user_id=$1
       ORDER BY pl.created_at DESC LIMIT 50`,
      [req.userId]
    );
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /pay/:token — página pública de pago (sin auth)
app.get('/pay/:token', async (req, res) => {
  try {
    const pl = await query(
      `SELECT pl.*, inv.invoice_number, inv.client_name, inv.total, inv.paid_amount, u.id as owner_id
       FROM payment_links pl
       JOIN invoices inv ON inv.id=pl.invoice_id
       JOIN users u ON u.id=pl.user_id
       WHERE pl.token=$1`,
      [req.params.token]
    );
    if (!pl.rows[0]) return res.status(404).send('<h2>Link no encontrado o expirado</h2>');
    const link = pl.rows[0];
    if (link.status !== 'active') return res.status(410).send('<h2>Este link ya fue utilizado o cancelado</h2>');
    if (new Date(link.expires_at) < new Date()) {
      await query(`UPDATE payment_links SET status='expired' WHERE id=$1`, [link.id]);
      return res.status(410).send('<h2>Este link de pago ha expirado</h2>');
    }

    const pending = parseFloat(link.total||0) - parseFloat(link.paid_amount||0);
    // Obtener config pública de pasarela si existe
    const pcR = await query(
      `SELECT config_public FROM payment_configs WHERE user_id=$1 AND provider=$2 AND is_active=TRUE`,
      [link.owner_id, link.provider]
    );
    const config = pcR.rows[0]?.config_public ? JSON.parse(pcR.rows[0].config_public) : {};

    res.send(`<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Pagar Factura ${link.invoice_number}</title>
  <style>
    *{box-sizing:border-box;margin:0;padding:0}
    body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#09100f;color:#e8f0ee;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:20px}
    .card{background:#0f1a18;border:1px solid #1f3330;border-radius:16px;padding:28px;width:100%;max-width:420px}
    .logo{font-size:20px;font-weight:900;color:#ff7c2a;text-align:center;margin-bottom:20px}
    .amount{text-align:center;font-size:42px;font-weight:900;color:#00e5a0;margin:16px 0}
    .label{font-size:11px;text-transform:uppercase;letter-spacing:1px;color:#3d6660;margin-bottom:4px}
    .value{font-size:14px;font-weight:600;margin-bottom:12px}
    .btn{width:100%;padding:14px;border:none;border-radius:10px;font-size:16px;font-weight:700;cursor:pointer;margin-top:8px;transition:.2s}
    .btn-pay{background:#ff7c2a;color:#000}
    .btn-pay:hover{opacity:.9}
    .btn-azul{background:#0066cc;color:#fff}
    .info{font-size:11px;color:#3d6660;text-align:center;margin-top:14px}
    .badge{display:inline-flex;align-items:center;gap:4px;background:rgba(0,229,160,.1);border:1px solid rgba(0,229,160,.2);border-radius:50px;padding:4px 10px;font-size:11px;color:#00e5a0;margin-bottom:16px}
    .divider{height:1px;background:#1f3330;margin:16px 0}
    #paySuccess{display:none;text-align:center;padding:20px}
  </style>
</head>
<body>
  <div class="card">
    <div class="logo">mis<span style="color:#fff">cuentas</span></div>
    <div style="text-align:center"><span class="badge">🔒 Pago Seguro</span></div>
    <div class="label">Factura</div>
    <div class="value">${link.invoice_number}</div>
    <div class="label">Cliente</div>
    <div class="value">${link.client_name||'Sin nombre'}</div>
    <div class="divider"></div>
    <div class="label">Monto a Pagar</div>
    <div class="amount">RD$ ${Number(pending).toLocaleString('en-US',{minimumFractionDigits:2})}</div>
    <div class="divider"></div>
    <div id="payForms">
      ${link.provider === 'azul' && config.merchant_id ? `
      <div>
        <div class="label" style="margin-bottom:8px">Tarjeta de Crédito/Débito</div>
        <input type="text" id="cardNumber" placeholder="Número de tarjeta" style="width:100%;padding:10px 12px;background:#162220;border:1px solid #1f3330;border-radius:8px;color:#e8f0ee;font-size:14px;margin-bottom:8px">
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:8px">
          <input type="text" id="cardExp" placeholder="MM/AA" style="padding:10px 12px;background:#162220;border:1px solid #1f3330;border-radius:8px;color:#e8f0ee;font-size:14px">
          <input type="text" id="cardCVV" placeholder="CVV" style="padding:10px 12px;background:#162220;border:1px solid #1f3330;border-radius:8px;color:#e8f0ee;font-size:14px">
        </div>
        <button class="btn btn-azul" onclick="processAzul()">💳 Pagar con Azul</button>
      </div>` : `
      <div style="text-align:center;padding:12px;background:#162220;border-radius:10px;margin-bottom:12px">
        <div style="font-size:12px;color:#8aada8;margin-bottom:6px">Realiza tu pago por:</div>
        <div style="font-size:14px;font-weight:600">Transferencia Bancaria · Efectivo</div>
        <div style="font-size:11px;color:#3d6660;margin-top:4px">Contacta al emisor de la factura para coordinar el pago</div>
      </div>
      <button class="btn btn-pay" onclick="confirmPayment()">✅ Confirmar Pago Realizado</button>`}
    </div>
    <div id="paySuccess">
      <div style="font-size:48px;margin-bottom:12px">✅</div>
      <div style="font-size:18px;font-weight:700;color:#00e5a0">¡Pago Confirmado!</div>
      <div style="font-size:13px;color:#8aada8;margin-top:8px">Tu pago ha sido registrado. Gracias.</div>
    </div>
    <div class="info">Vence: ${new Date(link.expires_at).toLocaleDateString('es-DO')} · Generado por MisCuentas Contable</div>
  </div>
  <script>
    function confirmPayment() {
      fetch('/api/payment-links/${link.token}/confirm', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({method:'manual'})})
        .then(r=>r.json())
        .then(d=>{ if(d.ok){ document.getElementById('payForms').style.display='none'; document.getElementById('paySuccess').style.display='block'; } else alert('Error: '+d.error); })
        .catch(()=>alert('Error al procesar'));
    }
    function processAzul() { alert('Integración Azul en configuración. Contacta al emisor.'); }
  </script>
</body>
</html>`);
  } catch(e) { res.status(500).send('<h2>Error interno</h2>'); }
});

// POST /api/payment-links/:token/confirm — confirmar pago desde link público
app.post('/api/payment-links/:token/confirm', async (req, res) => {
  try {
    const { method = 'manual', reference } = req.body;
    const pl = await query(
      `SELECT pl.*, inv.user_id, inv.total, inv.paid_amount
       FROM payment_links pl JOIN invoices inv ON inv.id=pl.invoice_id
       WHERE pl.token=$1 AND pl.status='active' AND pl.expires_at>NOW()`,
      [req.params.token]
    );
    if (!pl.rows[0]) return res.status(404).json({ error: 'Link no válido o expirado' });
    const link   = pl.rows[0];
    const amount = parseFloat(link.total)-parseFloat(link.paid_amount||0);

    // Marcar link como usado
    await query(`UPDATE payment_links SET status='used', used_at=NOW() WHERE token=$1`, [req.params.token]);

    // Registrar pago en CxC/factura
    const payId = `rpay_${Date.now()}_${Math.random().toString(36).substr(2,6)}`;
    const today = new Date().toISOString().split('T')[0];
    // Actualizar factura
    await query(`UPDATE invoices SET paid_amount=total, status='paid' WHERE id=$1`, [link.invoice_id]);
    // Actualizar CxC relacionada
    await query(
      `UPDATE receivables SET paid_amount=total_amount, status='paid'
       WHERE user_id=$1 AND description ILIKE $2`,
      [link.user_id, `%${link.invoice_number}%`]
    );

    res.json({ ok: true, amount, method });
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
      product_id  TEXT NOT NULL,
      type        TEXT NOT NULL CHECK (type IN ('entry','exit','adjustment')),
      quantity    NUMERIC(12,3) NOT NULL,
      unit_cost   NUMERIC(12,4),
      unit_price  NUMERIC(12,4),
      vendor_id   TEXT,
      client_id   TEXT,
      reason      TEXT,
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
  // Fix invoice_counter - drop FK if exists, recreate without FK
  try { await query(`ALTER TABLE invoice_counter DROP CONSTRAINT IF EXISTS invoice_counter_user_id_fkey`); } catch(e) {}
  // Migrations for products
  try { await query(`ALTER TABLE products ADD COLUMN IF NOT EXISTS stock_current NUMERIC(12,2) DEFAULT 0`); } catch(e) {}
  // Migrations for fixed_assets
  try { await query(`ALTER TABLE fixed_assets ADD COLUMN IF NOT EXISTS depreciacion_metodo TEXT DEFAULT 'linea_recta'`); } catch(e) {}
  // Migration: income_records - add all missing columns
  try { await query(`ALTER TABLE income_records ADD COLUMN IF NOT EXISTS date DATE NOT NULL DEFAULT CURRENT_DATE`); } catch(e) {}
  try { await query(`ALTER TABLE income_records ADD COLUMN IF NOT EXISTS amount NUMERIC(12,2)`); } catch(e) {}
  try { await query(`ALTER TABLE income_records ADD COLUMN IF NOT EXISTS description TEXT`); } catch(e) {}
  try { await query(`ALTER TABLE income_records ADD COLUMN IF NOT EXISTS reference TEXT`); } catch(e) {}
  try { await query(`ALTER TABLE income_records ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`); } catch(e) {}
  // Ensure income_type_id column exists (not type_id)
  try { await query(`ALTER TABLE income_records ADD COLUMN IF NOT EXISTS income_type_id TEXT`); } catch(e) {}
  // If type_id exists but income_type_id doesn't, rename
  try { await query(`ALTER TABLE income_records DROP COLUMN IF EXISTS type_id`); } catch(e) {}
  // Migration: add payment_method to invoices
  try { await query(`ALTER TABLE invoices ADD COLUMN IF NOT EXISTS payment_method TEXT DEFAULT 'credit'`); } catch(e) {}
  try { await query(`ALTER TABLE invoices ADD COLUMN IF NOT EXISTS discount_amount NUMERIC(12,2) DEFAULT 0`); } catch(e) {}
  try { await query(`ALTER TABLE invoices ADD COLUMN IF NOT EXISTS discount_pct NUMERIC(5,2) DEFAULT 0`); } catch(e) {}
  try { await query(`ALTER TABLE invoices ADD COLUMN IF NOT EXISTS paid_amount NUMERIC(12,2) DEFAULT 0`); } catch(e) {}
  try { await query(`ALTER TABLE invoice_items ADD COLUMN IF NOT EXISTS discount_pct NUMERIC(5,2) DEFAULT 0`); } catch(e) {}
  // ── CMV: nuevas columnas ──
  try { await query(`ALTER TABLE invoices ADD COLUMN IF NOT EXISTS cmv_amount NUMERIC(12,2) DEFAULT 0`); } catch(e) {}
  try { await query(`ALTER TABLE invoice_items ADD COLUMN IF NOT EXISTS product_id TEXT`); } catch(e) {}
  try { await query(`ALTER TABLE invoice_items ADD COLUMN IF NOT EXISTS cost_price NUMERIC(12,2) DEFAULT 0`); } catch(e) {}
  try { await query(`ALTER TABLE invoice_items ADD COLUMN IF NOT EXISTS cmv_amount NUMERIC(12,2) DEFAULT 0`); } catch(e) {}
  // Actualizar status CHECK constraint para incluir 'issued'
  try { await query(`ALTER TABLE invoices DROP CONSTRAINT IF EXISTS invoices_status_check`); } catch(e) {}
  try { await query(`ALTER TABLE invoices ADD CONSTRAINT invoices_status_check CHECK (status IN ('draft','issued','pending','paid','partial','cancelled'))`); } catch(e) {}

  // ── inventory_movements: eliminar FK a inventory_products, agregar columnas ──
  try { await query(`ALTER TABLE inventory_movements DROP CONSTRAINT IF EXISTS inventory_movements_product_id_fkey`); } catch(e) {}
  try { await query(`ALTER TABLE inventory_movements ALTER COLUMN quantity TYPE NUMERIC(12,3)`); } catch(e) {}
  try { await query(`ALTER TABLE inventory_movements ALTER COLUMN unit_cost TYPE NUMERIC(12,4)`); } catch(e) {}
  try { await query(`ALTER TABLE inventory_movements ADD COLUMN IF NOT EXISTS unit_price NUMERIC(12,4)`); } catch(e) {}
  try { await query(`ALTER TABLE inventory_movements ADD COLUMN IF NOT EXISTS vendor_id TEXT`); } catch(e) {}
  try { await query(`ALTER TABLE inventory_movements ADD COLUMN IF NOT EXISTS client_id TEXT`); } catch(e) {}
  try { await query(`ALTER TABLE inventory_movements ADD COLUMN IF NOT EXISTS reason TEXT`); } catch(e) {}
  try { await query(`CREATE INDEX IF NOT EXISTS idx_inv_mov_product ON inventory_movements(product_id, user_id)`); } catch(e) {}

  // ── Fase 1.2: Cierre de Período ──
  try {
    await query(`CREATE TABLE IF NOT EXISTS period_closings (
      id           TEXT PRIMARY KEY,
      user_id      TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      month        INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
      year         INTEGER NOT NULL,
      closed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      ingresos     NUMERIC(14,2) NOT NULL DEFAULT 0,
      costos       NUMERIC(14,2) NOT NULL DEFAULT 0,
      gastos       NUMERIC(14,2) NOT NULL DEFAULT 0,
      utilidad_neta NUMERIC(14,2) NOT NULL DEFAULT 0,
      notes        TEXT,
      UNIQUE(user_id, month, year)
    )`);
  } catch(e) { console.warn('period_closings table:', e.message); }

  // ── Fase 1.3: Conciliación Bancaria ──
  try { await query(`ALTER TABLE journal_lines ADD COLUMN IF NOT EXISTS conciliado BOOLEAN DEFAULT FALSE`); } catch(e) {}
  try { await query(`ALTER TABLE journal_lines ADD COLUMN IF NOT EXISTS bank_reference TEXT`); } catch(e) {}
  try { await query(`CREATE INDEX IF NOT EXISTS idx_jl_conciliado ON journal_lines(conciliado) WHERE conciliado=FALSE`); } catch(e) {}

  // ── Fase 2.1: Cotizaciones ──
  try {
    await query(`CREATE TABLE IF NOT EXISTS quotes (
      id               TEXT PRIMARY KEY,
      user_id          TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      quote_number     TEXT NOT NULL,
      client_id        TEXT,
      client_name      TEXT,
      client_rnc       TEXT,
      client_address   TEXT,
      date             DATE NOT NULL DEFAULT CURRENT_DATE,
      valid_until      DATE,
      subtotal         NUMERIC(12,2) DEFAULT 0,
      tax              NUMERIC(12,2) DEFAULT 0,
      total            NUMERIC(12,2) NOT NULL,
      discount_amount  NUMERIC(12,2) DEFAULT 0,
      discount_pct     NUMERIC(5,2)  DEFAULT 0,
      notes            TEXT,
      payment_terms    TEXT,
      delivery_terms   TEXT,
      status           TEXT NOT NULL DEFAULT 'draft'
                       CHECK (status IN ('draft','sent','approved','rejected','expired','converted')),
      invoice_id       TEXT,
      created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`);
  } catch(e) { console.warn('quotes table:', e.message); }

  try {
    await query(`CREATE TABLE IF NOT EXISTS quote_items (
      id           TEXT PRIMARY KEY,
      quote_id     TEXT NOT NULL REFERENCES quotes(id) ON DELETE CASCADE,
      description  TEXT NOT NULL,
      qty          NUMERIC(12,3) NOT NULL DEFAULT 1,
      price        NUMERIC(12,2) NOT NULL DEFAULT 0,
      total        NUMERIC(12,2) NOT NULL DEFAULT 0,
      discount_pct NUMERIC(5,2)  DEFAULT 0,
      product_id   TEXT,
      created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`);
  } catch(e) { console.warn('quote_items table:', e.message); }

  // Migraciones de quotes
  try { await query(`ALTER TABLE quotes ADD COLUMN IF NOT EXISTS client_id TEXT`); } catch(e) {}
  try { await query(`ALTER TABLE quote_items ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`); } catch(e) {}

  // Columna quote_id en invoices
  try { await query(`ALTER TABLE invoices ADD COLUMN IF NOT EXISTS quote_id TEXT`); } catch(e) {}
  try { await query(`ALTER TABLE invoices ADD COLUMN IF NOT EXISTS recurring_id TEXT`); } catch(e) {}

  // ── Fase 2.2: Facturas Recurrentes ──
  try {
    await query(`CREATE TABLE IF NOT EXISTS recurring_invoices (
      id               TEXT PRIMARY KEY,
      user_id          TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      name             TEXT NOT NULL,
      client_name      TEXT,
      client_rnc       TEXT,
      subtotal         NUMERIC(12,2) DEFAULT 0,
      tax              NUMERIC(12,2) DEFAULT 0,
      total            NUMERIC(12,2) NOT NULL,
      discount_amount  NUMERIC(12,2) DEFAULT 0,
      discount_pct     NUMERIC(5,2)  DEFAULT 0,
      notes            TEXT,
      payment_method   TEXT DEFAULT 'credit',
      frequency        TEXT NOT NULL DEFAULT 'monthly'
                       CHECK (frequency IN ('weekly','biweekly','monthly','bimonthly','quarterly','yearly')),
      next_date        DATE NOT NULL,
      end_date         DATE,
      items_json       JSONB DEFAULT '[]',
      is_active        BOOLEAN NOT NULL DEFAULT TRUE,
      generated_count  INTEGER DEFAULT 0,
      created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`);
  } catch(e) { console.warn('recurring_invoices table:', e.message); }

  // ── Fase 2.3: Retenciones ──
  try {
    await query(`CREATE TABLE IF NOT EXISTS retenciones (
      id               TEXT PRIMARY KEY,
      user_id          TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      tipo             TEXT NOT NULL CHECK (tipo IN ('isr','itbis')),
      subtipo          TEXT,
      entity_type      TEXT DEFAULT 'client' CHECK (entity_type IN ('client','vendor')),
      client_id        TEXT REFERENCES clients(id),
      vendor_id        TEXT REFERENCES vendors(id),
      invoice_id       TEXT,
      base_amount      NUMERIC(12,2) NOT NULL,
      retention_pct    NUMERIC(5,2)  NOT NULL,
      retention_amount NUMERIC(12,2) NOT NULL,
      date             DATE NOT NULL DEFAULT CURRENT_DATE,
      ncf              TEXT,
      notes            TEXT,
      status           TEXT DEFAULT 'pending' CHECK (status IN ('pending','filed','paid')),
      created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`);
  } catch(e) { console.warn('retenciones table:', e.message); }

  // ── Fase 4.1: Tasas de Cambio ──
  try {
    await query(`CREATE TABLE IF NOT EXISTS exchange_rates (
      id         TEXT PRIMARY KEY,
      user_id    TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      currency   TEXT NOT NULL,
      rate       NUMERIC(12,6) NOT NULL,
      date       DATE NOT NULL DEFAULT CURRENT_DATE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE(user_id, currency, date)
    )`);
  } catch(e) { console.warn('exchange_rates:', e.message); }

  // ── Fase 4.2: Sucursales ──
  try {
    await query(`CREATE TABLE IF NOT EXISTS branches (
      id         TEXT PRIMARY KEY,
      user_id    TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      name       TEXT NOT NULL,
      address    TEXT,
      phone      TEXT,
      email      TEXT,
      manager    TEXT,
      rnc        TEXT,
      is_active  BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`);
  } catch(e) { console.warn('branches:', e.message); }

  // columna branch_id en invoices y quotes
  try { await query(`ALTER TABLE invoices ADD COLUMN IF NOT EXISTS branch_id TEXT`); } catch(e) {}
  try { await query(`ALTER TABLE quotes   ADD COLUMN IF NOT EXISTS branch_id TEXT`); } catch(e) {}

  // ── Fase 4.3: Roles ──
  try {
    await query(`CREATE TABLE IF NOT EXISTS user_roles (
      id         TEXT PRIMARY KEY,
      user_id    TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      owner_id   TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      role       TEXT NOT NULL CHECK (role IN ('admin','contador','cajero')),
      branch_id  TEXT,
      is_active  BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE(user_id, owner_id)
    )`);
  } catch(e) { console.warn('user_roles:', e.message); }

  // ── Fase 4.4: Pasarelas de Pago ──
  try {
    await query(`CREATE TABLE IF NOT EXISTS payment_configs (
      id             TEXT PRIMARY KEY,
      user_id        TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      provider       TEXT NOT NULL,
      api_key_enc    TEXT,
      merchant_id    TEXT,
      config_public  TEXT,
      is_active      BOOLEAN NOT NULL DEFAULT TRUE,
      created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE(user_id, provider)
    )`);
  } catch(e) { console.warn('payment_configs:', e.message); }

  try {
    await query(`CREATE TABLE IF NOT EXISTS payment_links (
      id          TEXT PRIMARY KEY,
      user_id     TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      invoice_id  TEXT NOT NULL,
      token       TEXT NOT NULL UNIQUE,
      provider    TEXT NOT NULL DEFAULT 'generic',
      amount      NUMERIC(12,2) NOT NULL,
      currency    TEXT NOT NULL DEFAULT 'DOP',
      expires_at  TIMESTAMPTZ NOT NULL,
      status      TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','used','expired','cancelled')),
      used_at     TIMESTAMPTZ,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`);
  } catch(e) { console.warn('payment_links:', e.message); }

  // invoice_number en payment_links (para referencia)
  try { await query(`ALTER TABLE payment_links ADD COLUMN IF NOT EXISTS invoice_number TEXT`); } catch(e) {}

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

