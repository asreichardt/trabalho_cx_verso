
CREATE DATABASE IF NOT EXISTS movieflix;
USE movieflix;

CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    age INT,
    country VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


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

