// scrapers/diamantesB.scrapers.mjs
import 'dotenv/config';
import fs from 'node:fs/promises';
import path from 'node:path';
import puppeteer from 'puppeteer';
import { Client as NotionClient } from '@notionhq/client';
import { saveResumoMySQL } from '../src/lib/save-resumo.mjs';
import { db } from '../src/lib/db.mjs';

const OUT_DIR = path.resolve('./output');
const ESPECIALISTAS_FILE = path.resolve('./especialistas.json');
const USER_PROFILE_DIR = path.resolve('.pupp_profile'); // persiste sessão
const LOGGED_SELECTOR = 'header .navbar, .sidebar, a[href*="sair"], #conteudo';
const NAV_TIMEOUT_MS = 120_000;
const WAIT_2FA_MS = 45_000;
const MAX_PAGES = 500;

const NOTION_SYNC = String(process.env.NOTION_SYNC || 'false').toLowerCase() === 'true';
const notion = NOTION_SYNC ? new NotionClient({ auth: process.env.NOTION_TOKEN }) : null;

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const nowHR = () => process.hrtime.bigint();
const fmt = (n, w=2)=>String(n).padStart(w,'0');
const fmtDur = (ms)=>`${fmt(Math.floor(ms/3600000))}:${fmt(Math.floor(ms%3600000/60000))}:${fmt(Math.floor(ms%60000/1000))}.${String(Math.floor(ms%1000)).padStart(3,'0')}`;

function dataDiaBR() {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Sao_Paulo', year: 'numeric', month: '2-digit', day: '2-digit'
  }).format(new Date()); // YYYY-MM-DD
}
function normalizeText(s) {
  if (!s) return '';
  const fix = /[ÃÂ]/.test(s) ? (()=>{ try { return Buffer.from(s,'latin1').toString('utf8'); } catch { return s; } })() : s;
  return fix.replace(/\s+/g,' ').normalize('NFC').trim();
}
async function lerEspecialistas(){
  const raw = await fs.readFile(ESPECIALISTAS_FILE,'utf-8');
  const data = JSON.parse(raw);
  if (!Array.isArray(data?.especialistas)) throw new Error('especialistas.json inválido');
  return data.especialistas;
}
function getLaunchArgs(){
  const args=[]; if ((process.env.NO_SANDBOX||'').toLowerCase()==='true') args.push('--no-sandbox','--disable-setuid-sandbox'); return args;
}

// ===== Notion helpers (resumo) =====
function assertNotionEnv(){
  if (!NOTION_SYNC) return;
  if (!process.env.NOTION_TOKEN) throw new Error('NOTION_TOKEN ausente');
  if (!process.env.NOTION_DB_RESUMO_ID) throw new Error('NOTION_DB_RESUMO_ID ausente');
}
async function ensureSelectOptions(databaseId, propName, values) {
  const vals = [...new Set(values.filter(Boolean))];
  if (!vals.length) return;
  const dbMeta = await notion.databases.retrieve({ database_id: databaseId });
  const prop = dbMeta.properties[propName];
  if (!prop || !('select' in prop)) throw new Error(`Propriedade '${propName}' não é Select no Notion`);
  const existing = new Set((prop.select.options || []).map(o=>o.name));
  const missing = vals.filter(v=>!existing.has(v));
  if (!missing.length) return;
  await notion.databases.update({
    database_id: databaseId,
    properties: { [propName]: { select: { options: [...(prop.select.options||[]), ...missing.map(name=>({name}))] } } }
  });
}
async function notionFindByRegistro(databaseId, registro) {
  const q = await notion.databases.query({
    database_id: databaseId,
    filter: { property: 'Registro', title: { equals: registro } },
    page_size: 1
  });
  return q.results?.[0] || null;
}
function toNotionPropsResumo(row){
  return {
    'Registro':   { title:[{ text:{ content: row._registro } }] },
    'Data':       { date: { start: row.data } },
    'Praça':      { select: { name: row['praça'] || '—' } },
    'Corretor':   { select: { name: row.corretor || '—' } },
    'Situação':   { select: { name: row.situacao || '—' } },
    'Quantidade': { number: Number(row.quantidade)||0 }
  };
}
async function notionUpsertResumo(rows) {
  if (!NOTION_SYNC || !rows?.length) return { created:0, updated:0 };
  assertNotionEnv();
  const dbId = process.env.NOTION_DB_RESUMO_ID;
  rows.forEach(r => r._registro = `${r.data} | ${r['praça']||'—'} | ${r.corretor||'—'} | ${r.situacao||'—'}`);
  await ensureSelectOptions(dbId,'Praça',rows.map(r=>r['praça']));
  await ensureSelectOptions(dbId,'Corretor',rows.map(r=>r.corretor));
  await ensureSelectOptions(dbId,'Situação',rows.map(r=>r.situacao));

  let created=0, updated=0;
  for (const r of rows) {
    const existing = await notionFindByRegistro(dbId, r._registro);
    const properties = toNotionPropsResumo(r);
    if (existing) { await notion.pages.update({ page_id: existing.id, properties }); updated++; }
    else { await notion.pages.create({ parent:{ database_id: dbId }, properties }); created++; }
    await sleep(250);
  }
  return { created, updated };
}

// ===== Login =====
async function loginIfNeeded(page, firstUrl, { otp }) {
  await page.goto(firstUrl, { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT_MS });

  if (await page.$(LOGGED_SELECTOR)) { console.log('✅ Sessão ativa.'); return; }

  if (await page.$('#email')) {
    console.log('→ Digitando credenciais...');
    await page.type('#email', process.env.USER_EMAIL, { delay: 35 });
    await page.type('#senha', process.env.USER_PASSWORD, { delay: 35 });
    await Promise.all([
      page.waitForNavigation({ waitUntil: 'networkidle2', timeout: NAV_TIMEOUT_MS }).catch(()=>null),
      page.keyboard.press('Enter'),
    ]);
  }

  const otpField = await page.$('#chaveOtp');
  if (otpField) {
    if (!otp) throw new Error('Pediu OTP e nenhum foi informado (use body.otp ou param --otp)');
    console.log('→ Inserindo OTP...');
    await page.type('#chaveOtp', String(otp), { delay: 35 });
    await Promise.all([
      page.waitForNavigation({ waitUntil: 'networkidle2', timeout: NAV_TIMEOUT_MS }).catch(()=>null),
      page.keyboard.press('Enter'),
    ]);
  } else {
    console.log('⏳ Aguardando aprovação 2FA (push)...');
    const start = nowHR();
    while (Number(nowHR()-start)/1e6 < WAIT_2FA_MS) {
      if (await page.$(LOGGED_SELECTOR)) break;
      await sleep(500);
    }
  }

  if (!(await page.$(LOGGED_SELECTOR))) throw new Error('Falha no login/2FA (timeout).');
  console.log('✅ Login ok.');
}

// ===== Extração & paginação =====
async function extrairPaginaAtual(page, { praca, corretor }) {
  const list = await page.evaluate(() => {
    const norm = s => (s||'').replace(/\s+/g,' ').trim();
    const rows = Array.from(document.querySelectorAll('tbody tr'))
      .filter(tr => tr.querySelector('input.table-check-row'));
    return rows.map(tr => {
      const leadId = tr.querySelector('input.table-check-row')?.value || null;

      const badge = tr.querySelector('a.badge.aw-situacoes');
      const sitTitle = badge?.getAttribute('data-bs-title') || '';
      const sitText  = badge?.querySelector('.abrevia')?.textContent || '';
      const situacao = norm(sitTitle) || norm(sitText);

      // Coluna Responsável → Corretor (texto pode existir na célula)
      const corrEl = tr.querySelector('[data-col="corretor"]');
      const corr   = norm(corrEl?.textContent || '');

      // Produto: 1ª abrevia útil na coluna "Emp. / PDV"
      let produto = '';
      const tds = Array.from(tr.children);
      for (const td of tds) {
        const small = td.querySelector('.lighter.smaller');
        if (small && /Emp\.?\s*\/\s*PDV/i.test(small.textContent)) {
          const opts = Array.from(td.querySelectorAll('.abrevia')).map(el=>norm(el.textContent)).filter(Boolean);
          const cand = opts.find(t => !/^Sem\s+(PDV|M[ií]dia)$/i.test(t));
          produto = cand || opts[0] || '';
          break;
        }
      }

      // Contato
      const nome   = norm(tr.querySelector('[data-col="name"]')?.textContent || '');
      const emailE = tr.querySelector('div[data-col="email"]');
      const email  = norm(emailE?.getAttribute('data-bs-title') || emailE?.textContent || '');
      const telEl  = tr.querySelector('div[data-col="phone"] .hidden-text-blur');
      const telefone = norm((telEl?.textContent || '').replace(/[^\d+]/g,''));

      return { leadId, situacao, corretor: corr, produto, nome, email, telefone };
    });
  });

  return list.map(it => ({
    leadId: it.leadId,
    situacao: normalizeText(it.situacao),
    corretorDOM: normalizeText(it.corretor),
    produto: normalizeText(it.produto),
    nome: normalizeText(it.nome),
    email: normalizeText(it.email),
    telefone: normalizeText(it.telefone),
    praca,
    corretor // PADRÃO vindo do JSON
  })).filter(x => x.leadId);
}

async function tentarMaximizarItensPorPagina(page) {
  try {
    const changed = await page.evaluate(() => {
      const txt = el => (el?.innerText||el?.textContent||'').toLowerCase();
      for (const sel of Array.from(document.querySelectorAll('select'))) {
        const near = txt(sel.closest('label,.form-group,.dataTables_length,.buscaListagemColuna,td,th,div'));
        if (!near || !/(itens|items|por p[aá]gina|mostrar|exibir|results per page|length)/i.test(near)) continue;
        const opts = Array.from(sel.options);
        const nums = opts.map(o => parseInt((o.value||o.textContent||'').replace(/\D+/g,''),10)).filter(v=>!isNaN(v));
        if (!nums.length) continue;
        const max = Math.max(...nums);
        const opt = opts.find(o => (o.value||o.textContent||'').includes(String(max)));
        if (opt) { opt.selected=true; sel.value=opt.value; sel.dispatchEvent(new Event('change',{bubbles:true})); return true; }
      }
      return false;
    });
    if (changed) await sleep(900);
  } catch {}
}

async function clicarProximaPagina(page) {
  const nextSel = await page.evaluate(() => {
    const els = Array.from(document.querySelectorAll('a,button'));
    const disabled = el =>
      el.classList.contains('disabled') ||
      el.closest('.disabled') ||
      el.getAttribute?.('aria-disabled') === 'true';
    const cand = els.find(el => {
      const t = (el.innerText||el.textContent||'').trim().toLowerCase();
      if (disabled(el)) return false;
      return (
        t === 'próxima' || t === 'proxima' || t === 'próximo' || t === 'proximo' ||
        t === 'next' || t === '>' || t === '»' ||
        t.includes('próxima') || t.includes('proxima') || t.includes('next') ||
        (el.getAttribute?.('aria-label')||'').toLowerCase().includes('next')
      );
    });
    if (!cand) return null;
    cand.scrollIntoView({ block: 'center' });
    cand.setAttribute('data-next','1');
    return '[data-next="1"]';
  });

  if (!nextSel) return false;

  await Promise.all([
    page.click(nextSel).catch(()=>null),
    page.waitForNavigation({ waitUntil: 'networkidle2', timeout: NAV_TIMEOUT_MS }).catch(()=>sleep(900)),
  ]);
  await page.evaluate(()=>{ const el=document.querySelector('[data-next="1"]'); if (el) el.removeAttribute('data-next'); });
  await sleep(500);
  return true;
}

// ===== Coleta principal =====
async function coletarDiamantes(page, { url, praca, corretor, full=false }) {
  await page.goto(url, { waitUntil: 'networkidle2', timeout: NAV_TIMEOUT_MS });
  await tentarMaximizarItensPorPagina(page);

  const vistos = new Set();
  const contagem = new Map();
  const leads = [];

  for (let p=1; p<=MAX_PAGES; p++) {
    const itens = await extrairPaginaAtual(page, { praca, corretor });
    let novos = 0;
    for (const it of itens) {
      if (vistos.has(it.leadId)) continue;
      vistos.add(it.leadId);
      novos++;

      const situacao = it.situacao || '—';
      contagem.set(situacao, (contagem.get(situacao)||0) + 1);

      if (full) {
        leads.push({
          lead_id: it.leadId,
          nome: it.nome,
          email: it.email,
          telefone: it.telefone,
          produto: it.produto,
          corretor: corretor,
          situacao,
          'praça': praca
        });
      }
    }
    console.log(`   • Página ${p}: +${novos} (únicos: ${vistos.size})`);
    const next = await clicarProximaPagina(page);
    if (!next) break;
  }

  const data = dataDiaBR();
  const resumo = Array.from(contagem.entries()).map(([situacao, quantidade]) => ({
    data, 'praça': praca, corretor, situacao, quantidade
  }));

  return { resumo, leads };
}

// ======= Runner exportado =======
export async function runDiamantesBScraper({ otp='', full=false, notionSync=NOTION_SYNC, mysqlSave=true } = {}) {
  await fs.mkdir(OUT_DIR, { recursive: true });

  const especialistas = await lerEspecialistas();
  if (!especialistas.length) throw new Error('Sem especialistas no especialistas.json');

  const t0 = nowHR();
  const browser = await puppeteer.launch({
    headless: (process.env.HEADLESS || 'new').toLowerCase() === 'false' ? false : 'new',
    args: getLaunchArgs(),
    userDataDir: USER_PROFILE_DIR
  });

  let resumoAll = [];
  let leadsAll = [];

  try {
    const page = await browser.newPage();
    page.setDefaultTimeout(60_000);
    await page.setViewport({ width: 1400, height: 1000 });

    // Login usando o 1º link
    await loginIfNeeded(page, especialistas[0].diamantes, { otp });

    for (const esp of especialistas) {
      const praca = esp['praça'] || esp.praca || '';
      console.log(`\n🧑‍💼 ${esp.nome} — ${praca || 'sem praça'}`);

      const { resumo, leads } = await coletarDiamantes(page, {
        url: esp.diamantes, praca, corretor: esp.nome, full
      });

      resumo.forEach(r => console.log(`   - ${r.situacao}: ${r.quantidade}`));
      resumoAll.push(...resumo);
      if (full && leads?.length) leadsAll.push(...leads);
    }

    // MySQL
    if (mysqlSave && resumoAll.length) {
      console.log('\n💾 Gravando resumo no MySQL...');
      const rs = await saveResumoMySQL(resumoAll);
      console.log(`✅ MySQL ok (upserted ~${rs.upserted})`);
    }

    // Notion
    if (notionSync && NOTION_SYNC && resumoAll.length) {
      console.log('↗️  Enviando resumo ao Notion...');
      const { created, updated } = await notionUpsertResumo(resumoAll);
      console.log(`✅ Notion ok (created: ${created}, updated: ${updated})`);
    }

    // CSVs (apenas RESUMO por padrão)
    const stamp = new Date().toISOString().slice(0,19).replace(/[:T]/g,'-');
    await fs.writeFile(path.join(OUT_DIR, `diamantesB-resumo-${stamp}.json`), JSON.stringify(resumoAll, null, 2));
    if (full && leadsAll.length) {
      await fs.writeFile(path.join(OUT_DIR, `diamantesB-full-${stamp}.json`), JSON.stringify(leadsAll, null, 2));
    }

    const ms = Number(nowHR()-t0)/1e6;
    console.log(`\n⏱️ Tempo total: ${fmtDur(ms)} (${ms.toFixed(0)} ms)`);
  } finally {
    await browser.close();
    await db.destroy().catch(()=>{});
  }
}

// Execução direta (CLI)
if (import.meta.url === `file://${process.argv[1]}`) {
  const otp = (process.argv.find(a=>a.startsWith('--otp='))||'').slice(6);
  const full = process.argv.includes('--full');
  runDiamantesBScraper({ otp, full }).catch(e => { console.error('❌', e.message||e); process.exit(1); });
}
