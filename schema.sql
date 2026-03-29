-- MisCuentas RD — PostgreSQL Schema
-- Ejecutar una vez en Railway PostgreSQL

CREATE TABLE IF NOT EXISTS users (
  id          TEXT PRIMARY KEY,          -- Telegram chat_id como string
  registered  BOOLEAN   NOT NULL DEFAULT TRUE,
  lang        TEXT      NOT NULL DEFAULT 'es',
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transactions (
  id          TEXT        PRIMARY KEY,   -- uid generado en servidor
  user_id     TEXT        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  type        TEXT        NOT NULL CHECK (type IN ('ingreso','egreso')),
  amount      NUMERIC(12,2) NOT NULL CHECK (amount > 0),
  description TEXT        NOT NULL,
  category    TEXT        NOT NULL DEFAULT 'otro',
  account     TEXT        NOT NULL DEFAULT 'efectivo' CHECK (account IN ('efectivo','banco','tarjeta')),
  tx_date     DATE        NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS budgets (
  user_id     TEXT        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  category    TEXT        NOT NULL,
  amount      NUMERIC(12,2) NOT NULL CHECK (amount > 0),
  PRIMARY KEY (user_id, category)
);

CREATE TABLE IF NOT EXISTS pending_tx (
  user_id     TEXT        PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
  tx_data     JSONB       NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Índices para consultas frecuentes
CREATE INDEX IF NOT EXISTS idx_tx_user_date  ON transactions(user_id, tx_date);
CREATE INDEX IF NOT EXISTS idx_tx_user_month ON transactions(user_id, DATE_TRUNC('month', tx_date));
