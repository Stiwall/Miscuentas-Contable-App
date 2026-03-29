# MisCuentas Contable

App de contabilidad para negocios dominicanos. Sistema completo de contabilidad con partida doble, plan de cuentas, diario contable, cuentas por cobrar/pagar, reportes financieros y exportación a PDF/CSV.

## Características

- **Plan de Cuentas** — Clasificación contable completa
- **Diario Contable** — Asientos de partida doble
- **Clientes y Proveedores** — Directorio y gestión
- **Cuentas por Cobrar/Pagar** — Seguimiento de deudas
- **Reportes Financieros** — Balance, Estado de Resultados, Flujo de Caja
- **Exportación** — PDF y CSV
- **Wizard de Configuración** — Plantillas para Restaurante, Tienda, Servicios

## Tech Stack

- Node.js + Express
- PostgreSQL
- PWA (Progressive Web App)
- jsPDF + autoTable para exportación

## Setup

```bash
npm install
npm start
```

## Variables de Entorno

- `DATABASE_URL` — Connection string de PostgreSQL
- `SESSION_SECRET` — Secret para sesiones
- `GROQ_API_KEY` — Para análisis de facturas (opcional)
- `GEMINI_API_KEY` — Para parsing de facturas (opcional)
- `TELEGRAM_BOT_TOKEN` — Token del bot de Telegram (opcional)
- `WEBHOOK_SECRET` — Secret para webhooks
- `CRON_SECRET` — Secret para tareas cron
- `PORT` — Puerto (default 3000)
