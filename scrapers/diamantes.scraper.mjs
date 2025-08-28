// scrapers/diamantes.scraper.mjs
import 'dotenv/config';
import fs from 'node:fs/promises';
import path from 'node:path';
import puppeteer from 'puppeteer';
import * as cheerio from 'cheerio';

// Se já tiver configurado Knex, descomente:
// import { upsertResumo, insertLeadsDetalhe, destroyDb } from '../lib/knex-client.mjs';

// =============== Utils tempo/formatos ===============
const __start = process.hrtime.bigint();
function formatDuration(ms) {
  const total = Math.max(0, Math.round(ms));
  const h = Math.floor(total / 3_600_000);
  const m = Math.floor((total % 3_600_000) / 60_000);
  const s = Math.floor((total % 60_000) / 1000);
  const msRest = total % 1000;
  const pad = (n, w = 2) => String(n).padStart(w, '0');
  return `${pad(h)}:${pad(m)}:${pad(s)}.${pad(msRest, 3)}`;
}
function nowBR() {
  return new Date().toLocaleString('pt-BR', { timeZone: 'America/Sao_Paulo' });
}
function todayISO_BR() {
  // YYYY-MM-DD no fuso de São Paulo
  const dt = new Date(
    new Date().toLocaleString('en-US', { timeZone: 'America/Sao_Paulo' })
  );
  const yyyy = dt.getFullYear();
  const mm = String(dt.getMonth() + 1).padStart(2, '0');
  const dd = String(dt.getDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}
function logTotal(prefix = '⏱️ Tempo total') {
  const ns = process.hrtime.bigint() - __start;
  const ms = Number(ns) / 1e6;
  console.log(`${prefix}: ${formatDuration(ms)} (${ms.toFixed(0)} ms)`);
}

// =============== Config/paths ===============
const OUT_DIR = path.resolve(process.cwd(), 'output');
const ESPECIALISTAS_FILE = path.resolve(process.cwd(), 'especialistas.json');
const {
  USER_EMAIL,
  USER_PASSWORD,
  USER_AUTH_CODE,          // pode ser vazia se a sessão já estiver salva
  USERDATA_DIR,            // ex: ".pupp_profile"
  HEADLESS = 'new',        // 'new' (recomendado) ou 'false'
  NO_SANDBOX = 'false'
} = process.env;

// =============== Helpers ===============
function getLaunchArgs() {
  const args = [];
  if (NO_SANDBOX === 'true') args.push('--no-sandbox', '--disable-setuid-sandbox');
  return args;
}
function corrigirEncoding(str) {
  if (!str) return '';
  if (/[ÃÂ]/.test(str)) {
    try { return Buffer.from(str, 'latin1').toString('utf8'); } catch { }
  }
  return str.normalize('NFC');
}
function norm(s) {
  return corrigirEncoding((s || '').replace(/\s+/g, ' ').trim());
}
async function readEspecialistas() {
  const raw = await fs.readFile(ESPECIALISTAS_FILE, 'utf-8');
  const data = JSON.parse(raw);
  if (!data?.especialistas || !Array.isArray(data.especialistas)) {
    throw new Error('especialistas.json inválido: esperado { especialistas: [...] }');
  }
  return data.especialistas;
}
function toCsv(rows) {
  if (!rows.length) return '';
  const headers = Object.keys(rows[0]);
  const esc = (v) => {
    if (v == null) return '';
    const s = String(v);
    return /[",\n;]/.test(s) ? `"${s.replaceAll('"', '""')}"` : s;
  };
  return [
    headers.join(';'),
    ...rows.map(r => headers.map(h => esc(r[h])).join(';'))
  ].join('\n');
}
async function saveCsvBOM(file, rows) {
  await fs.mkdir(path.dirname(file), { recursive: true });
  const payload = '\uFEFF' + toCsv(rows);
  await fs.writeFile(file, payload, 'utf-8');
}

// =============== Cheerio + HTTP (modo turbo) ===============
const fetchFn = globalThis.fetch ?? (await import('node-fetch')).default;

async function getCookieHeader(page, anyUrl) {
  const origin = new URL(anyUrl).origin;
  const cookies = await page.cookies(origin);
  return cookies.map(c => `${c.name}=${c.value}`).join('; ');
}
function abs(base, href) {
  try { return new URL(href, base).toString(); } catch { return null; }
}
function findNextHref($) {
  const byRel = $('a[rel="next"]').attr('href');
  if (byRel) return byRel;

  const candidates = $('a, button')
    .filter((_, el) => {
      const $el = $(el);
      const txt = norm($el.text()).toLowerCase();
      const aria = ($el.attr('aria-label') || '').toLowerCase();
      return (
        /próxima|proxima|next/.test(txt) ||
        /próxima|proxima|next/.test(aria) ||
        txt === '>' || txt === '»'
      );
    });

  if (candidates.length) {
    const href = candidates.first().attr('href');
    if (href && !href.startsWith('javascript')) return href;
  }

  const pagHref = $('.pagination a:contains("Próxima"), .pagination a:contains("Proxima")').attr('href');
  if (pagHref) return pagHref;

  return null;
}

/**
 * Parseia uma página de leads.
 * Retorna sempre: [{ id, corretor, situacao, nome?, email?, telefone?, produto? }]
 * Quando full=false, os campos opcionais podem vir vazios para economizar custo,
 * mas sempre extraímos id/corretor/situacao/nome para dedupe e contagem confiável.
 */
function parseLeadsFromHtml(html, { full }) {
  const $ = cheerio.load(html);
  const leads = [];

  $('tbody > tr').each((_, tr) => {
    const $tr = $(tr);
    if ($tr.attr('id') === 'buscaListagem') return;

    const id = norm($tr.find('span[data-col="id"]').text());
    const nome = norm($tr.find('span[data-col="name"]').text());

    let email = '';
    let telefone = '';
    let produto = '';

    if (full) {
      email = norm($tr.find('div[data-col="email"]').text());
      telefone = norm(
        $tr.find('div[data-col="phone"] .hidden-text-blur').text() ||
        $tr.find('div[data-col="phone"]').text()
      );
      // produto = primeiro .abrevia da 4ª coluna (Dados do Lead)
      produto = norm($tr.find('td').eq(3).find('.abrevia').first().text());
    }

    const corretor = norm(
      $tr.find('div[data-col="corretor"] .abrevia, div[data-col="corretor"] span').first().text()
    );

    const $badge = $tr.find('a.badge.aw-situacoes').first();
    const situacao = norm($badge.attr('data-bs-title') || $badge.text());

    if (id || nome) {
      leads.push({ id, nome, email, telefone, produto, corretor, situacao });
    }
  });

  const nextHref = findNextHref($);
  return { leads, nextHref };
}

/**
 * Coleta por HTTP (sem renderização) com contagem resumida
 * e, opcionalmente, detalhe completo (full).
 */
async function coletarViaHTTP(page, startUrl, { full = false, maxPages = 999 } = {}) {
  const cookie = await getCookieHeader(page, startUrl);
  const headers = {
    'Cookie': cookie,
    'Accept-Language': 'pt-BR,pt;q=0.9',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/125 Safari/537.36'
  };

  const seen = new Set(); // dedupe por id (ou nome|email|telefone)
  const resumo = new Map(); // key = `${corretor}||${situacao}` -> count
  const detalhes = [];      // só preenchido se full

  let url = startUrl;
  let pages = 0;

  while (url && pages < maxPages) {
    console.log(`  → baixando página ${pages + 1}`);
    const res = await fetchFn(url, { headers });
    if (!res.ok) throw new Error(`HTTP ${res.status} em ${url}`);
    const html = await res.text();

    const { leads, nextHref } = parseLeadsFromHtml(html, { full });

    for (const L of leads) {
      const key = L.id || `${L.nome}|${L.email}|${L.telefone}`;
      if (!key) continue;
      if (seen.has(key)) continue;
      seen.add(key);

      const k2 = `${L.corretor || ''}||${L.situacao || ''}`;
      resumo.set(k2, (resumo.get(k2) || 0) + 1);

      if (full) detalhes.push(L);
    }

    url = nextHref ? abs(url, nextHref) : null;
    pages++;
  }

  return { resumo, detalhes };
}

// =============== Login ===============
const LOGGED_SELECTOR = 'header .navbar, .sidebar, a[href*="sair"], #conteudo';
const OTP_ERROR_SEL = '.alert-danger, .alert-error, .text-danger, .validation-error, [role="alert"]';
const OTP_MAX_WAIT_MS = +(process.env.OTP_MAX_WAIT_MS || 10_000); // 90s padrão

async function waitForLoginOutcome(page, timeoutMs) {
  // Retorna 'OK' | 'OTP_ERROR' | 'TIMEOUT'
  try {
    const handle = await page.waitForFunction(
      (LOGGED_SELECTOR, OTP_ERROR_SEL) => {
        if (document.querySelector(LOGGED_SELECTOR)) return 'OK';
        if (document.querySelector(OTP_ERROR_SEL)) return 'OTP_ERROR';
        return false; // continue esperando
      },
      { timeout: timeoutMs, polling: 500 },
      LOGGED_SELECTOR,
      OTP_ERROR_SEL
    );
    return await handle.jsonValue();
  } catch {
    return 'TIMEOUT';
  }
}

async function loginIfNeeded(page, firstUrl, otpArg) {
  // bloqueia recursos pesados (mantém como você já tinha)
  await page.setRequestInterception(true);
  page.on('request', (req) => {
    const rt = req.resourceType();
    if (['image', 'stylesheet', 'font', 'media'].includes(rt)) return req.abort();
    if (/analytics|tagmanager|hotjar|intercom|fullstory/i.test(req.url())) return req.abort();
    req.continue();
  });

  await page.goto(firstUrl, { waitUntil: 'domcontentloaded', timeout: 120_000 });

  // já logado?
  if (await page.$(LOGGED_SELECTOR)) {
    console.log('✅ Sessão já ativa.');
    return;
  }

  // email/senha
  if (await page.$('#email')) {
    console.log('→ Digitando credenciais...');
    await page.type('#email', process.env.USER_EMAIL, { delay: 25 });
    await page.type('#senha', process.env.USER_PASSWORD, { delay: 25 });
    await Promise.all([
      page.keyboard.press('Enter'),
      page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 60_000 }).catch(() => null)
    ]);
  }

  // OTP
  if (await page.$('#chaveOtp')) {
    const code = (otpArg || process.env.USER_AUTH_CODE || '').trim();
    if (!code) {
      throw new Error('OTP requerido, mas nenhum código foi fornecido (--otp=123456 ou USER_AUTH_CODE).');
    }

    console.log('→ Inserindo OTP...');
    await page.type('#chaveOtp', code, { delay: 25 });
    await Promise.all([
      page.keyboard.press('Enter'),
      page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 30_000 }).catch(() => null)
    ]);

    const outcome = await waitForLoginOutcome(page, OTP_MAX_WAIT_MS);
    if (outcome !== 'OK') {
      throw new Error(
        outcome === 'OTP_ERROR'
          ? 'OTP inválido/expirado.'
          : 'Timeout aguardando validação do OTP.'
      );
    }
  }

  // checagem final
  await page.waitForSelector(LOGGED_SELECTOR, { visible: true, timeout: 60_000 });
  console.log('✅ Login concluído.');
}

// =============== MAIN ===============
function parseBoolFlag(argv, name) {
  // aceita: --full, --full=true, --full=1, --full=yes
  if (argv.includes(`--${name}`)) return true;
  const p = argv.find(a => a.startsWith(`--${name}=`));
  if (!p) return false;
  const v = p.split('=')[1]?.toLowerCase();
  return v === '1' || v === 'true' || v === 'yes';
}

async function main() {
  const argv = process.argv.slice(2);
  const otpArg = (argv.find(a => a.startsWith('--otp=')) || '').split('=')[1] || '';
  const FULL_MODE = parseBoolFlag(argv, 'full'); // default false

  console.log(`🔧 Modo: ${FULL_MODE ? 'FULL (detalhes + resumo)' : 'RESUMO (apenas contagem)'}`);

  await fs.mkdir(OUT_DIR, { recursive: true });
  const especialistas = await readEspecialistas();
  if (!especialistas.length) {
    console.log('Nenhum especialista em especialistas.json');
    return;
  }

  const browser = await puppeteer.launch({
    headless: (HEADLESS === 'true' || HEADLESS === 'new') ? 'new' : false,
    args: getLaunchArgs(),
    userDataDir: USERDATA_DIR ? path.resolve(USERDATA_DIR) : undefined
  });

  try {
    const page = await browser.newPage();
    await page.setViewport({ width: 1280, height: 900, deviceScaleFactor: 1 });

    const firstUrl = especialistas[0]?.diamantes;
    if (!firstUrl) throw new Error('Primeiro especialista sem URL "diamantes".');
    await loginIfNeeded(page, firstUrl, otpArg);

    const dataISO = todayISO_BR();

    const resumoRows = [];
    const leadsRows = []; // só será usado se FULL_MODE

    for (const esp of especialistas) {
      if (!esp?.diamantes) continue;

      const praca = esp['praça'] || esp.praca || '';
      console.log(`\n🧑‍💼 Coletando: ${esp.nome || '(sem nome)'} — ${praca || 'sem praça'}`);

      const { resumo, detalhes } = await coletarViaHTTP(page, esp.diamantes, { full: FULL_MODE, maxPages: 999 });

      // resumo -> linhas
      for (const [key, qtd] of resumo.entries()) {
        const [corretor, situacao] = key.split('||');
        resumoRows.push({
          data: dataISO,
          praça: praca,
          corretor,
          situacao,
          quantidade: qtd
        });
      }

      // detalhes (somente no full)
      if (FULL_MODE) {
        for (const L of detalhes) {
          leadsRows.push({
            data: dataISO,
            praça: praca,
            corretor: L.corretor || '',
            lead_id: L.id || '',
            nome: L.nome || '',
            email: L.email || '',
            telefone: L.telefone || '',
            produto: L.produto || '',
            situacao: L.situacao || ''
          });
        }
      }
    }

    // ===== CSVs =====
    const stamp = new Date().toISOString().slice(0, 19).replace(/[:T]/g, '-');
    const csvResumo = path.join(OUT_DIR, `diamantes-resumo-${stamp}.csv`);
    await saveCsvBOM(csvResumo, resumoRows);
    console.log(`\n🧾 CSV salvo (resumo): ${csvResumo}`);

    if (FULL_MODE) {
      const csvDetalhe = path.join(OUT_DIR, `diamantes-leads-${stamp}.csv`);
      await saveCsvBOM(csvDetalhe, leadsRows);
      console.log(`🧾 CSV salvo (detalhes): ${csvDetalhe}`);
    }

    // ===== MySQL/Knex (opcional) =====
    // try {
    //   if (FULL_MODE && leadsRows.length) {
    //     await insertLeadsDetalhe(leadsRows);
    //   }
    //   // upsert sempre do resumo
    //   await upsertResumo(resumoRows.map(r => ({
    //     data: r.data, praca: r['praça'], corretor: r.corretor, situacao: r.situacao, quantidade: r.quantidade
    //   })));
    //   console.log('🗄️  MySQL/Knex ok.');
    // } catch (e) {
    //   console.error('MySQL/Knex erro:', e?.message || e);
    // }

  } finally {
    // await destroyDb().catch(()=>{});
    await browser.close();
    logTotal();
  }
}

// Execução
console.clear();
console.log('🚀 Iniciando scraper diamantes (HTTP + Cheerio)...');
main().catch(err => {
  console.error('❌', err);
  logTotal('⏱️ Tempo até o erro');
  process.exit(1);
});
