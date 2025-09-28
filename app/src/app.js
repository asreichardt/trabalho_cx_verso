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