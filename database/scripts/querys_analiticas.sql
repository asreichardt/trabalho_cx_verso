-- Consultas Analíticas para o Data Warehouse

--  Top 5 filmes mais populares (com mais avaliações)
SELECT m.title, m.genre, COUNT(r.rating) as rating_count, AVG(r.rating) as avg_rating
FROM movies m
INNER JOIN ratings r ON m.id = r.movie_id
GROUP BY m.id, m.title, m.genre
ORDER BY rating_count DESC
LIMIT 5;

--  Gênero com melhor avaliação média
SELECT genre, AVG(r.rating) as avg_rating, COUNT(r.rating) as rating_count
FROM movies m
INNER JOIN ratings r ON m.id = r.movie_id
GROUP BY genre
ORDER BY avg_rating DESC
LIMIT 1;

--  País com mais avaliações (mais assiste filmes)
SELECT u.country, COUNT(r.rating) as rating_count, AVG(r.rating) as avg_rating
FROM ratings r
INNER JOIN users u ON r.user_id = u.id
GROUP BY u.country
ORDER BY rating_count DESC
LIMIT 5;

--  Evolução das avaliações ao longo do tempo
SELECT 
    DATE_FORMAT(r.created_at, '%Y-%m') as month,
    COUNT(r.rating) as rating_count,
    AVG(r.rating) as avg_rating
FROM ratings r
GROUP BY DATE_FORMAT(r.created_at, '%Y-%m')
ORDER BY month;

-- Relação entre idade e preferência de gênero
SELECT 
    CASE 
        WHEN u.age < 25 THEN '18-24'
        WHEN u.age BETWEEN 25 AND 34 THEN '25-34'
        WHEN u.age BETWEEN 35 AND 50 THEN '35-50'
        ELSE '50+'
    END as age_group,
    m.genre,
    COUNT(r.rating) as rating_count,
    AVG(r.rating) as avg_rating
FROM ratings r
INNER JOIN users u ON r.user_id = u.id
INNER JOIN movies m ON r.movie_id = m.id
GROUP BY age_group, m.genre
ORDER BY age_group, rating_count DESC;

--  diretores mais populares
SELECT 
    m.director,
    COUNT(r.rating) as rating_count,
    AVG(r.rating) as avg_rating,
    COUNT(DISTINCT m.id) as movie_count
FROM movies m
JOIN ratings r ON m.id = r.movie_id
GROUP BY m.director
ORDER BY avg_rating DESC
LIMIT 10;

--  Performance de filmes por país de origem
SELECT 
    m.country as movie_country,
    COUNT(r.rating) as rating_count,
    AVG(r.rating) as avg_rating,
    COUNT(DISTINCT m.id) as movie_count
FROM movies m
LEFT JOIN ratings r ON m.id = r.movie_id
GROUP BY m.country
ORDER BY rating_count DESC;

--  usuários mais ativos
SELECT 
    u.name,
    u.country,
    COUNT(r.rating) as ratings_given,
    AVG(r.rating) as avg_rating_given
FROM users u
JOIN ratings r ON u.id = r.user_id
GROUP BY u.id, u.name, u.country
ORDER BY ratings_given DESC
LIMIT 10;

--  análise sazonal
SELECT 
    QUARTER(r.created_at) as quarter,
    MONTHNAME(r.created_at) as month,
    COUNT(r.rating) as rating_count,
    AVG(r.rating) as avg_rating
FROM ratings r
GROUP BY QUARTER(r.created_at), MONTHNAME(r.created_at)
ORDER BY quarter, MONTH(r.created_at);

--filmes com maior variação de avaliações
SELECT 
    m.title,
    m.genre,
    AVG(r.rating) as avg_rating,
    STD(r.rating) as rating_std,
    COUNT(r.rating) as rating_count
FROM movies m
JOIN ratings r ON m.id = r.movie_id
GROUP BY m.id, m.title, m.genre
ORDER BY rating_std DESC
LIMIT 10;