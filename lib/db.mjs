import knexPkg from 'knex';
export const knex = knexPkg({
    client: 'mysql2',
    connection: {
        host: process.env.DB_HOST,
        port: +(process.env.DB_PORT || 3306),
        user: process.env.DB_USER,
        password: process.env.DB_PASS,
        database: process.env.DB_NAME,
        charset: 'utf8mb4'
    },
    pool: { min: 0, max: 10 }
});

export async function destroyDb() {
    try { await knex.destroy(); } catch { }
}
