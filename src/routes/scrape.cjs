// src/routes/scrapes.cjs
'use strict';

const path = require('node:path');
const { spawn } = require('node:child_process');

function requireApiKey(req, res, next) {
  const expected = process.env.API_KEY;
  if (!expected) return next();
  const got = req.get('x-api-key') || req.query.api_key;
  if (got === expected) return next();
  return res.status(401).json({ ok: false, error: 'unauthorized' });
}

// ===== Fila simples =====
const queue = [];
let running = null; // { name, args, startedAt, child, killTimer }

const DEFAULT_TIMEOUT = +(process.env.SCRAPER_TIMEOUT_MS || 12 * 60 * 1000); // 12 min

function runNext() {
  if (running || queue.length === 0) return;

  const job = queue.shift();
  running = job;

  const scriptPath = path.resolve(process.cwd(), job.script);
  const args = [scriptPath, ...job.args];

  console.log(
    `🚀 Iniciando scraper "${job.name}" ` +
    `(args: ${job.args.join(' ') || '(nenhum)'}). Jobs na fila: ${queue.length}`
  );

  const child = spawn(process.execPath, args, {
    stdio: ['ignore', 'inherit', 'inherit'],
    env: process.env
  });

  job.child = child;
  job.startedAt = Date.now();

  // ⏰ Watchdog — mata o processo se passar do tempo limite
  job.killTimer = setTimeout(() => {
    if (!child.killed) {
      console.error(
        `⏰ Timeout (${DEFAULT_TIMEOUT} ms) — matando "${job.name}" (pid ${child.pid})`
      );
      try { child.kill('SIGKILL'); } catch {}
    }
  }, DEFAULT_TIMEOUT);

  const finalize = (label, extra) => {
    clearTimeout(job.killTimer);
    const took = Date.now() - job.startedAt;
    console.log(
      `🏁 Scraper "${job.name}" ${label} em ${Math.round(took / 1000)}s` +
      (extra ? ` ${extra}` : '')
    );
    running = null;
    runNext();
  };

  child.on('exit', (code, signal) => {
    finalize(
      'finalizado',
      `(code=${code}, signal=${signal || 'none'})`
    );
  });

  child.on('error', (err) => {
    console.error(`❌ Erro ao iniciar "${job.name}":`, err);
    finalize('abortado por erro de spawn');
  });
}

function enqueue(job) {
  queue.push(job);
  runNext();
  const position = running ? queue.length : 0;
  return position;
}

module.exports = (app) => {
  app.use('/api/scrape', requireApiKey);

  app.get('/api/scrape/diamantes', (req, res) => {
    const otp  = String(req.query.otp || '').trim();
    const full = String(req.query.full || '').toLowerCase() === 'true';

    const job = {
      name: 'diamantes',
      script: 'scrapers/diamantes.scraper.mjs',
      args: [],
      startedAt: null,
      child: null,
      killTimer: null
    };

    if (otp)  job.args.push(`--otp=${otp}`);
    if (full) job.args.push('--full');

    const position = enqueue(job);

    if (position === 0) {
      return res.status(202).json({
        ok: true,
        started: true,
        queued: false,
        full,
        message: 'Scraper iniciado; acompanhe os logs no console.'
      });
    } else {
      return res.status(202).json({
        ok: true,
        started: false,
        queued: true,
        position,
        full,
        message: `Já existe um scraper em execução. Seu job foi enfileirado na posição ${position}.`
      });
    }
  });

  app.get('/api/scrape/status', (req, res) => {
    res.json({
      ok: true,
      running: !!running && {
        name: running?.name,
        pid: running?.child?.pid || null,
        args: running?.args || [],
        startedAt: running?.startedAt || null,
        timeoutMs: DEFAULT_TIMEOUT
      },
      queued: queue.map((j, idx) => ({
        position: idx + 1,
        name: j.name,
        args: j.args
      }))
    });
  });
};
