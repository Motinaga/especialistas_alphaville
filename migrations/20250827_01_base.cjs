/** @param {import('knex').Knex} knex */
exports.up = async (knex) => {
  await knex.raw("SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci");

  await knex.schema.createTable('dim_praca', (t) => {
    t.increments('id').primary();
    t.string('nome', 191).notNullable().unique();
  });

  await knex.schema.createTable('dim_corretor', (t) => {
    t.increments('id').primary();
    t.string('nome', 191).notNullable().unique();
  });

  await knex.schema.createTable('dim_situacao', (t) => {
    t.increments('id').primary();
    t.string('nome', 191).notNullable().unique();
  });

  await knex.schema.createTable('fact_diamantes_resumo', (t) => {
    t.increments('id').primary();
    t.date('data').notNullable();
    t.integer('praca_id').unsigned().notNullable()
      .references('id').inTable('dim_praca').onDelete('RESTRICT');
    t.integer('corretor_id').unsigned().notNullable()
      .references('id').inTable('dim_corretor').onDelete('RESTRICT');
    t.integer('situacao_id').unsigned().notNullable()
      .references('id').inTable('dim_situacao').onDelete('RESTRICT');
    t.integer('quantidade').notNullable().defaultTo(0);

    t.unique(['data', 'praca_id', 'corretor_id', 'situacao_id'], 'uq_resumo_diario');
    t.index(['data', 'praca_id'], 'ix_resumo_data_praca');
  });
};

/** @param {import('knex').Knex} knex */
exports.down = async (knex) => {
  await knex.schema.dropTableIfExists('fact_diamantes_resumo');
  await knex.schema.dropTableIfExists('dim_situacao');
  await knex.schema.dropTableIfExists('dim_corretor');
  await knex.schema.dropTableIfExists('dim_praca');
};
