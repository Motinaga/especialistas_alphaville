// knexfile.cjs
'use strict';

const path = require('node:path');
require('dotenv').config({ path: path.resolve(__dirname, '.env') });

function connFromEnv() {
  const host = process.env.DB_HOST || '127.0.0.1';
  const port = +(process.env.DB_PORT || 3306);
  const user = process.env.DB_USER || 'root';
  const database = process.env.DB_NAME || 'scrapes';

  // aceite DB_PASS ou DB_PASSWORD
  const pass = (process.env.DB_PASS ?? process.env.DB_PASSWORD ?? '').trim();

  const conn = { host, port, user, database, charset: 'utf8mb4' };
  if (pass) conn.password = pass; // só define se não estiver vazio

  // Log de diagnóstico (sem expor a senha)
  console.log(`[knex] connecting to ${user}@${host}:${port}/${database} password: ${pass ? 'SET' : 'MISSING'}`);

  return conn;
}

module.exports = {
  client: 'mysql2',
  connection: connFromEnv(),
  pool: { min: 0, max: 10 },
  migrations: {
    directory: './migrations',
    tableName: 'knex_migrations'
  }
};
