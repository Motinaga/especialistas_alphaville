# Especialistas – Scrapers

## Requisitos
- Node 18+
- MySQL 8+
- Git

## Configuração
1. Copiar `.env.example` para `.env` e preencher.
2. `npm i`
3. Migrations: `npx knex migrate:latest`
4. Rodar API: `npm run start`

## Endpoints
- `POST /scrapes/diamantesB`  
  Body: `{ "otp":"123456", "full": false, "notionSync": true, "mysqlSave": true }`
