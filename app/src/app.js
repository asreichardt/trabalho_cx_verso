const express = require('express');
const mysql = require('mysql2/promise');
const path = require('path');
const app = express();
const port = 3000;

// Middleware
app.use(express.json());
app.use(express.static('public'));
app.use(express.urlencoded({ extended: true }));

// 🔧 CORREÇÃO: Configuração do MySQL usando nome do serviço

const dbConfig = {
    host: process.env.DB_HOST ,
    user: process.env.DB_USER ,
    password: process.env.DB_PASSWORD , 
    database: process.env.DB_NAME ,
    port: process.env.DB_PORT 
};

// Conexão com o banco - VERSÃO CORRIGIDA
let db;
async function connectDB() {
    try {
        console.log(`🔄 Tentando conectar no MySQL: ${dbConfig.host}:${dbConfig.port}`);
        db = await mysql.createConnection(dbConfig);
        console.log('✅ Conectado ao MySQL com sucesso!');
        
        // Testar se as tabelas existem
        const [tables] = await db.execute("SHOW TABLES");
        console.log(`📊 Tabelas encontradas: ${tables.length}`);
        
    } catch (error) {
        console.error('❌ Erro ao conectar com MySQL:', error.message);
        // Tentar reconectar após 5 segundos
        setTimeout(connectDB, 5000);
    }
}

// 🔧 CORREÇÃO: Adicionar tratamento de erro global para a conexão
process.on('unhandledRejection', (error) => {
    console.error('❌ Erro não tratado:', error);
});

// 🔧 CORREÇÃO: Melhorar o tratamento de erro das rotas
const handleDBError = (res, error) => {
    console.error('❌ Erro no banco de dados:', error);
    res.status(500).json({ 
        error: 'Erro interno do servidor',
        details: process.env.NODE_ENV === 'development' ? error.message : undefined
    });
};

// Rotas da API - VERSÃO CORRIGIDA
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Listar filmes - COM TRATAMENTO DE ERRO MELHORADO
app.get('/api/movies', async (req, res) => {
    try {
        if (!db) {
            return res.status(503).json({ error: 'Banco de dados não conectado' });
        }
        
        const [rows] = await db.execute(`
            SELECT m.*, AVG(r.rating) as avg_rating, COUNT(r.rating) as rating_count
            FROM movies m 
            LEFT JOIN ratings r ON m.id = r.movie_id 
            GROUP BY m.id 
            ORDER BY avg_rating DESC
        `);
        res.json(rows);
    } catch (error) {
        handleDBError(res, error);
    }
});

// 🔧 CORREÇÃO: Health check endpoint
app.get('/health', async (req, res) => {
    try {
        if (!db) {
            return res.status(503).json({ status: 'Database not connected' });
        }
        
        await db.execute('SELECT 1');
        res.json({ 
            status: 'OK', 
            database: 'connected',
            timestamp: new Date().toISOString()
        });
    } catch (error) {
        res.status(503).json({ 
            status: 'ERROR', 
            database: 'disconnected',
            error: error.message 
        });
    }
});

// 🔧 CORREÇÃO: Rota para verificar conexão
app.get('/api/debug/db', async (req, res) => {
    try {
        if (!db) {
            return res.json({ connected: false, error: 'No database connection' });
        }
        
        const [result] = await db.execute('SELECT 1 as test');
        res.json({ 
            connected: true, 
            test: result,
            config: {
                host: dbConfig.host,
                database: dbConfig.database,
                user: dbConfig.user
            }
        });
    } catch (error) {
        res.json({ 
            connected: false, 
            error: error.message 
        });
    }
});

// Outras rotas (manter como estão, mas com tratamento de erro)
app.post('/api/movies', async (req, res) => {
    try {
        if (!db) {
            return res.status(503).json({ error: 'Banco de dados não conectado' });
        }
        
        const { title, genre, release_year, director, country } = req.body;
        const [result] = await db.execute(
            'INSERT INTO movies (title, genre, release_year, director, country) VALUES (?, ?, ?, ?, ?)',
            [title, genre, release_year, director, country]
        );
        res.json({ id: result.insertId, message: 'Filme cadastrado com sucesso!' });
    } catch (error) {
        handleDBError(res, error);
    }
});

app.post('/api/ratings', async (req, res) => {
    try {
        if (!db) {
            return res.status(503).json({ error: 'Banco de dados não conectado' });
        }
        
        const { movie_id, user_id, rating, comment } = req.body;
        const [result] = await db.execute(
            'INSERT INTO ratings (movie_id, user_id, rating, comment, created_at) VALUES (?, ?, ?, ?, NOW())',
            [movie_id, user_id, rating, comment]
        );
        res.json({ id: result.insertId, message: 'Avaliação registrada com sucesso!' });
    } catch (error) {
        handleDBError(res, error);
    }
});

app.get('/api/users', async (req, res) => {
    try {
        if (!db) {
            return res.status(503).json({ error: 'Banco de dados não conectado' });
        }
        
        const [rows] = await db.execute('SELECT * FROM users');
        res.json(rows);
    } catch (error) {
        handleDBError(res, error);
    }
});

// 🔧 CORREÇÃO: Iniciar servidor com melhor tratamento de erro
app.listen(port, () => {
    console.log(`🚀 Servidor rodando na porta ${port}`);
    console.log(`📊 Tentando conectar no MySQL: ${dbConfig.host}:${dbConfig.port}`);
    connectDB();
});


// Rotas para consultas analíticas
app.get('/api/analytics/top-movies', async (req, res) => {
    try {
        const [rows] = await db.execute(`
            SELECT m.title, COUNT(r.rating) as rating_count, AVG(r.rating) as avg_rating
            FROM movies m
            JOIN ratings r ON m.id = r.movie_id
            GROUP BY m.id, m.title
            HAVING COUNT(r.rating) >= 5
            ORDER BY rating_count DESC, avg_rating DESC
            LIMIT 5
        `);
        res.json(rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/analytics/top-genres', async (req, res) => {
    try {
        const [rows] = await db.execute(`
            SELECT 
                SUBSTRING_INDEX(SUBSTRING_INDEX(m.genre, ',', 1), ',', -1) as genre,
                COUNT(r.rating) as rating_count,
                AVG(r.rating) as avg_rating
            FROM movies m
            JOIN ratings r ON m.id = r.movie_id
            GROUP BY genre
            HAVING COUNT(r.rating) >= 10
            ORDER BY avg_rating DESC
            LIMIT 5
        `);
        res.json(rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/analytics/top-countries', async (req, res) => {
    try {
        const [rows] = await db.execute(`
            SELECT u.country, COUNT(r.rating) as rating_count, AVG(r.rating) as avg_rating
            FROM ratings r
            JOIN users u ON r.user_id = u.id
            GROUP BY u.country
            HAVING COUNT(r.rating) >= 5
            ORDER BY rating_count DESC
            LIMIT 5
        `);
        res.json(rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/analytics/age-ratings', async (req, res) => {
    try {
        const [rows] = await db.execute(`
            SELECT 
                CASE 
                    WHEN u.age < 25 THEN '18-24'
                    WHEN u.age BETWEEN 25 AND 34 THEN '25-34'
                    WHEN u.age BETWEEN 35 AND 50 THEN '35-50'
                    ELSE '50+'
                END as age_group,
                AVG(r.rating) as avg_rating,
                COUNT(r.rating) as rating_count
            FROM ratings r
            JOIN users u ON r.user_id = u.id
            WHERE u.age IS NOT NULL
            GROUP BY age_group
            ORDER BY age_group
        `);
        res.json(rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// 4. Evolução das avaliações ao longo do tempo
app.get('/api/analytics/monthly-trends', async (req, res) => {
    try {
        const [rows] = await db.execute(`
            SELECT 
                DATE_FORMAT(r.created_at, '%Y-%m') as month,
                COUNT(r.rating) as rating_count,
                AVG(r.rating) as avg_rating
            FROM ratings r
            GROUP BY DATE_FORMAT(r.created_at, '%Y-%m')
            ORDER BY month DESC
            LIMIT 6
        `);
        res.json(rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// 6. Diretores mais populares
app.get('/api/analytics/top-directors', async (req, res) => {
    try {
        const [rows] = await db.execute(`
            SELECT 
                m.director,
                COUNT(r.rating) as rating_count,
                AVG(r.rating) as avg_rating,
                COUNT(DISTINCT m.id) as movie_count
            FROM movies m
            JOIN ratings r ON m.id = r.movie_id
            WHERE m.director IS NOT NULL AND m.director != ''
            GROUP BY m.director
            HAVING COUNT(r.rating) >= 3
            ORDER BY avg_rating DESC
            LIMIT 5
        `);
        res.json(rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// 7. Performance de filmes por país de origem
app.get('/api/analytics/movies-by-country', async (req, res) => {
    try {
        const [rows] = await db.execute(`
            SELECT 
                m.country as movie_country,
                COUNT(r.rating) as rating_count,
                AVG(r.rating) as avg_rating,
                COUNT(DISTINCT m.id) as movie_count
            FROM movies m
            LEFT JOIN ratings r ON m.id = r.movie_id
            WHERE m.country IS NOT NULL AND m.country != ''
            GROUP BY m.country
            HAVING COUNT(r.rating) >= 1
            ORDER BY rating_count DESC
            LIMIT 5
        `);
        res.json(rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// 8. Usuários mais ativos
app.get('/api/analytics/most-active-users', async (req, res) => {
    try {
        const [rows] = await db.execute(`
            SELECT 
                u.name,
                u.country,
                COUNT(r.rating) as ratings_given,
                AVG(r.rating) as avg_rating_given
            FROM users u
            JOIN ratings r ON u.id = r.user_id
            GROUP BY u.id, u.name, u.country
            ORDER BY ratings_given DESC
            LIMIT 5
        `);
        res.json(rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// 9. Análise sazonal
app.get('/api/analytics/seasonal-analysis', async (req, res) => {
    try {
        const [rows] = await db.execute(`
            SELECT 
                QUARTER(r.created_at) as quarter,
                CASE 
                    WHEN QUARTER(r.created_at) = 1 THEN 'Jan-Mar'
                    WHEN QUARTER(r.created_at) = 2 THEN 'Abr-Jun'
                    WHEN QUARTER(r.created_at) = 3 THEN 'Jul-Set'
                    WHEN QUARTER(r.created_at) = 4 THEN 'Out-Dez'
                END as month,
                COUNT(r.rating) as rating_count,
                AVG(r.rating) as avg_rating
            FROM ratings r
            GROUP BY QUARTER(r.created_at), month
            ORDER BY quarter
        `);
        res.json(rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// 10. Filmes com maior variação de avaliações
app.get('/api/analytics/movies-variance', async (req, res) => {
    try {
        const [rows] = await db.execute(`
            SELECT 
                m.title,
                m.genre,
                AVG(r.rating) as avg_rating,
                STD(r.rating) as rating_std,
                COUNT(r.rating) as rating_count
            FROM movies m
            JOIN ratings r ON m.id = r.movie_id
            GROUP BY m.id, m.title, m.genre
            HAVING COUNT(r.rating) >= 5
            ORDER BY rating_std DESC
            LIMIT 5
        `);
        res.json(rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});