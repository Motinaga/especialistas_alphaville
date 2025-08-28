import { knex } from './knex-client.mjs';

async function ensureDim(table, nome) {
  if (!nome) return null;
  await knex(table).insert({ nome }).onConflict('nome').ignore();
  const row = await knex(table).select('id').where({ nome }).first();
  return row?.id || null;
}

export async function upsertResumoRows(resumoRows = []) {
  for (const r of resumoRows) {
    const pracaId = await ensureDim('dim_praca', r['praça']);
    const corretorId = await ensureDim('dim_corretor', r.corretor);
    const situacaoId = await ensureDim('dim_situacao', r.situacao);

    await knex('fact_diamantes_resumo')
      .insert({
        data: r.data,
        praca_id: pracaId,
        corretor_id: corretorId,
        situacao_id: situacaoId,
        quantidade: r.quantidade
      })
      .onConflict(['data', 'praca_id', 'corretor_id', 'situacao_id'])
      .merge({ quantidade: r.quantidade });
  }
}

export async function upsertLeadsDetalhe(leads = []) {
  for (const L of leads) {
    const pracaId = await ensureDim('dim_praca', L['praça']);
    const corretorId = await ensureDim('dim_corretor', L.corretor);
    const situacaoId = await ensureDim('dim_situacao', L.situacao);
    const produtoId = await ensureDim('dim_produto', L.produto);

    await knex('lead_detalhe')
      .insert({
        lead_id: L.lead_id || L.id,
        nome: L.nome || null,
        email: L.email || null,
        telefone: L.telefone || null,
        produto_id: produtoId,
        corretor_id: corretorId,
        situacao_id: situacaoId,
        praca_id: pracaId
      })
      .onConflict('lead_id')
      .merge({
        nome: knex.raw('VALUES(nome)'),
        email: knex.raw('VALUES(email)'),
        telefone: knex.raw('VALUES(telefone)'),
        produto_id: knex.raw('VALUES(produto_id)'),
        corretor_id: knex.raw('VALUES(corretor_id)'),
        situacao_id: knex.raw('VALUES(situacao_id)'),
        praca_id: knex.raw('VALUES(praca_id)'),
        last_seen_at: knex.fn.now()
      });
  }
}
