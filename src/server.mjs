import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import morgan from 'morgan';
import path from 'node:path';
import { createRequire } from 'node:module';

// consign é CommonJS; usamos createRequire
const require = createRequire(import.meta.url);
const consign = require('consign');

const app = express();
app.use(express.json());
app.use(cors());
app.use(helmet());
app.use(morgan('dev'));

// Autoload de rotas .cjs via consign
consign({
  cwd: path.resolve(process.cwd(), 'src'),
  extensions: ['.cjs'],
  verbose: true
})
  .include('routes')
  .into(app);

console.clear();
const PORT = process.env.PORT || 3000;
const server = app.listen(PORT, () => {
  console.log(`🚀 API rodando em http://localhost:${PORT}`);
});

// Sem timeout (scrapes longos)
server.setTimeout(0);
