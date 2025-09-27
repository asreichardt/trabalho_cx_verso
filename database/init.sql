-- Criar banco de dados
CREATE DATABASE IF NOT EXISTS movieflix;
USE movieflix;

-- Tabela de usuários
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    age INT,
    country VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de filmes
CREATE TABLE movies (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    genre VARCHAR(50),
    release_year INT,
    director VARCHAR(100),
    country VARCHAR(50),
    duration INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de avaliações
CREATE TABLE ratings (
    id INT AUTO_INCREMENT PRIMARY KEY,
    movie_id INT,
    user_id INT,
    rating INT CHECK (rating >= 1 AND rating <= 5),
    comment TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Inserir dados iniciais
INSERT INTO users (name, email, age, country) VALUES
('João Silva', 'joao@email.com', 25, 'Brasil'),
('Maria Santos', 'maria@email.com', 30, 'Brasil'),
('Carlos Oliveira', 'carlos@email.com', 22, 'Portugal'),
('Ana Costa', 'ana@email.com', 28, 'Brasil'),
('Pedro Martins', 'pedro@email.com', 35, 'Portugal'),
('Laura Ferreira', 'laura@email.com', 19, 'Brasil'),
('Miguel Souza', 'miguel@email.com', 40, 'Espanha'),
('Sofia Rodrigues', 'sofia@email.com', 27, 'Portugal'),
('Lucas Alves', 'lucas@email.com', 32, 'Brasil'),
('Isabela Lima', 'isabela@email.com', 29, 'Brasil');

INSERT INTO movies (title, genre, release_year, director, country, duration) VALUES
('O Poderoso Chefão', 'Drama', 1972, 'Francis Ford Coppola', 'EUA', 175),
('Pulp Fiction', 'Crime', 1994, 'Quentin Tarantino', 'EUA', 154),
('O Senhor dos Anéis', 'Fantasia', 2001, 'Peter Jackson', 'Nova Zelândia', 178),
('Matrix', 'Ficção Científica', 1999, 'Lana Wachowski', 'EUA', 136),
('Cidade de Deus', 'Drama', 2002, 'Fernando Meirelles', 'Brasil', 130),
('Tropa de Elite', 'Ação', 2007, 'José Padilha', 'Brasil', 115),
('O Auto da Compadecida', 'Comédia', 2000, 'Guel Arraes', 'Brasil', 104),
('Central do Brasil', 'Drama', 1998, 'Walter Salles', 'Brasil', 110),
('Lisbela e o Prisioneiro', 'Comédia', 2003, 'Guel Arraes', 'Brasil', 106),
('O Alto da Compadecida', 'Comédia', 1999, 'Guel Arraes', 'Brasil', 90);

-- Data Mart: Views analíticas
CREATE VIEW top_movies_by_genre AS
SELECT m.genre, m.title, AVG(r.rating) as avg_rating, COUNT(r.rating) as rating_count
FROM movies m
JOIN ratings r ON m.id = r.movie_id
GROUP BY m.genre, m.title
ORDER BY m.genre, avg_rating DESC;

CREATE VIEW ratings_by_age_group AS
SELECT 
    CASE 
        WHEN u.age < 18 THEN 'Under 18'
        WHEN u.age BETWEEN 18 AND 25 THEN '18-25'
        WHEN u.age BETWEEN 26 AND 35 THEN '26-35'
        WHEN u.age BETWEEN 36 AND 50 THEN '36-50'
        ELSE 'Over 50'
    END as age_group,
    AVG(r.rating) as avg_rating,
    COUNT(r.rating) as rating_count
FROM ratings r
JOIN users u ON r.user_id = u.id
GROUP BY age_group;

CREATE VIEW ratings_by_country AS
SELECT u.country, COUNT(r.rating) as rating_count, AVG(r.rating) as avg_rating
FROM ratings r
JOIN users u ON r.user_id = u.id
GROUP BY u.country
ORDER BY rating_count DESC;