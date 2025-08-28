import { db } from './db.mjs';

export async function saveResumoMySQL(rows) {
  if (!rows?.length) return { upserted: 0 };

  const key = (r) => `${r.data}||${r['praça'] || r.praca || ''}||${r.corretor || ''}`;
  const totalByKey = new Map();
  for (const r of rows) {
    const k = key(r);
    totalByKey.set(k, (totalByKey.get(k) || 0) + (Number(r.quantidade) || 0));
  }

  const toInsert = rows.map(r => ({
    data: r.data,
    praca: r['praça'] || r.praca || '',
    corretor: r.corretor || '',
    situacao: r.situacao || '—',
    quantidade: Number(r.quantidade) || 0,
    total_leads: totalByKey.get(key(r)) || 0,
  }));

  // Se seu MySQL/Knex suportar:
  try {
    await db('diamantes_resumo_diario')
      .insert(toInsert)
      .onConflict(['data','praca','corretor','situacao'])
      .merge({
        quantidade: db.raw('VALUES(quantidade)'),
        total_leads: db.raw('VALUES(total_leads)')
      });
  } catch {
    // fallback: raw com ON DUPLICATE KEY UPDATE
    const sql = db('diamantes_resumo_diario').insert(toInsert).toQuery()
      .replace(/^insert/i, 'INSERT') +
      ' ON DUPLICATE KEY UPDATE ' +
      'quantidade=VALUES(quantidade), total_leads=VALUES(total_leads), updated_at=CURRENT_TIMESTAMP';
    await db.raw(sql);
  }

  return { upserted: toInsert.length };
}
