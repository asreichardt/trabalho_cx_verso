import pandas as pd
import numpy as np
import requests
import random
from datetime import datetime, timedelta
import time
import sqlite3
from collections import Counter

class RealMovieDataGenerator:
    def __init__(self):
        self.movies_data = []
        self.users_data = []
        self.ratings_data = []
        
    def get_real_movies_from_api(self, num_movies=200):
        """Obtém dados reais de filmes usando API pública (OMDb/JSON)"""
        print("Coletando dados reais de filmes...")
        
        # Lista de filmes brasileiros e internacionais conhecidos
        brazilian_movies = [
            {'title': 'Cidade de Deus', 'year': '2002', 'genre': 'Crime, Drama'},
            {'title': 'Tropa de Elite', 'year': '2007', 'genre': 'Action, Crime, Drama'},
            {'title': 'Central do Brasil', 'year': '1998', 'genre': 'Drama'},
            {'title': 'O Auto da Compadecida', 'year': '2000', 'genre': 'Adventure, Comedy'},
            {'title': 'Lisbela e o Prisioneiro', 'year': '2003', 'genre': 'Comedy, Romance'},
            {'title': 'O Que É Isso, Companheiro?', 'year': '1997', 'genre': 'Drama, History'},
            {'title': 'Carandiru', 'year': '2003', 'genre': 'Crime, Drama'},
            {'title': 'O Pagador de Promessas', 'year': '1962', 'genre': 'Drama'},
            {'title': 'Bye Bye Brasil', 'year': '1980', 'genre': 'Adventure, Comedy, Drama'},
            {'title': 'Dona Flor e Seus Dois Maridos', 'year': '1976', 'genre': 'Comedy, Fantasy'}
        ]
        
        international_movies = [
            {'title': 'The Godfather', 'year': '1972', 'genre': 'Crime, Drama'},
            {'title': 'Pulp Fiction', 'year': '1994', 'genre': 'Crime, Drama'},
            {'title': 'The Shawshank Redemption', 'year': '1994', 'genre': 'Drama'},
            {'title': 'Forrest Gump', 'year': '1994', 'genre': 'Drama, Romance'},
            {'title': 'The Matrix', 'year': '1999', 'genre': 'Action, Sci-Fi'},
            {'title': 'Inception', 'year': '2010', 'genre': 'Action, Adventure, Sci-Fi'},
            {'title': 'The Dark Knight', 'year': '2008', 'genre': 'Action, Crime, Drama'},
            {'title': 'Fight Club', 'year': '1999', 'genre': 'Drama'},
            {'title': 'Goodfellas', 'year': '1990', 'genre': 'Biography, Crime, Drama'},
            {'title': 'The Silence of the Lambs', 'year': '1991', 'genre': 'Crime, Drama, Thriller'}
        ]
        
        # Combinar e expandir a lista
        all_movies = brazilian_movies + international_movies
        
        # Adicionar mais filmes variados
        additional_movies = [
            {'title': 'Avatar', 'year': '2009', 'genre': 'Action, Adventure, Fantasy'},
            {'title': 'Titanic', 'year': '1997', 'genre': 'Drama, Romance'},
            {'title': 'Star Wars: A New Hope', 'year': '1977', 'genre': 'Action, Adventure, Fantasy'},
            {'title': 'The Lord of the Rings: The Fellowship of the Ring', 'year': '2001', 'genre': 'Action, Adventure, Drama'},
            {'title': 'Jurassic Park', 'year': '1993', 'genre': 'Action, Adventure, Sci-Fi'},
            {'title': 'The Avengers', 'year': '2012', 'genre': 'Action, Adventure, Sci-Fi'},
            {'title': 'Black Panther', 'year': '2018', 'genre': 'Action, Adventure, Sci-Fi'},
            {'title': 'Parasite', 'year': '2019', 'genre': 'Comedy, Drama, Thriller'},
            {'title': 'La La Land', 'year': '2016', 'genre': 'Drama, Music, Romance'},
            {'title': 'Get Out', 'year': '2017', 'genre': 'Horror, Mystery, Thriller'}
        ]
        
        all_movies.extend(additional_movies)
        
        # Gerar dados detalhados para cada filme
        movie_id = 1
        for movie in all_movies:
            # CORREÇÃO: Garantir que primary_genre sempre tenha um valor
            genre_list = [g.strip() for g in movie['genre'].split(',')]
            primary_genre = genre_list[0] if genre_list else 'Drama'
            
            # Diretores famosos por gênero
            directors = {
                'Drama': ['Fernando Meirelles', 'Walter Salles', 'Martin Scorsese', 'Steven Spielberg'],
                'Comedy': ['Guel Arraes', 'Judd Apatow', 'Edgar Wright', 'Woody Allen'],
                'Action': ['James Cameron', 'Christopher Nolan', 'Michael Bay', 'John Woo'],
                'Crime': ['Quentin Tarantino', 'Francis Ford Coppola', 'Brian De Palma'],
                'Sci-Fi': ['Lana Wachowski', 'Ridley Scott', 'Denis Villeneuve'],
                'Horror': ['Jordan Peele', 'John Carpenter', 'Wes Craven'],
                'Adventure': ['Steven Spielberg', 'Peter Jackson', 'James Cameron'],
                'Romance': ['Richard Curtis', 'Nancy Meyers', 'Nick Cassavetes'],
                'Fantasy': ['Peter Jackson', 'Guillermo del Toro', 'Tim Burton'],
                'Thriller': ['David Fincher', 'Alfred Hitchcock', 'Jonathan Demme']
            }
            
            # Determinar país baseado no filme
            if any(br_movie['title'] in movie['title'] for br_movie in brazilian_movies):
                country = 'Brasil'
                # Escolher diretor brasileiro apropriado
                if 'Cidade' in movie['title']:
                    director = 'Fernando Meirelles'
                elif 'Tropa' in movie['title']:
                    director = 'José Padilha'
                elif 'Auto' in movie['title'] or 'Lisbela' in movie['title']:
                    director = 'Guel Arraes'
                elif 'Central' in movie['title']:
                    director = 'Walter Salles'
                else:
                    director = random.choice(directors.get(primary_genre, ['Director']))
            else:
                country = random.choice(['EUA', 'Reino Unido', 'Canadá', 'Coreia do Sul', 'Japão'])
                # Escolher diretor baseado no gênero
                director = random.choice(directors.get(primary_genre, ['Unknown Director']))
            
            # Duração realística baseada no gênero - CORREÇÃO: usar primary_genre definida acima
            duration_ranges = {
                'Action': (90, 150), 'Drama': (100, 180), 'Comedy': (85, 120),
                'Crime': (110, 160), 'Sci-Fi': (120, 180), 'Horror': (85, 120),
                'Adventure': (100, 180), 'Romance': (90, 140), 'Fantasy': (100, 180),
                'Thriller': (95, 150), 'Biography': (110, 180), 'History': (120, 200)
            }
            min_dur, max_dur = duration_ranges.get(primary_genre, (90, 120))
            duration = random.randint(min_dur, max_dur)
            
            self.movies_data.append({
                'id': movie_id,
                'title': movie['title'],
                'genre': movie['genre'],
                'release_year': int(movie['year']),
                'director': director,
                'country': country,
                'duration': duration,
                'created_at': datetime.now() - timedelta(days=random.randint(1, 365))
            })
            movie_id += 1
        
        # Adicionar mais filmes para completar o número desejado
        while len(self.movies_data) < num_movies:
            self.movies_data.append(self._generate_synthetic_movie(movie_id))
            movie_id += 1
        
        return self.movies_data
    
    def _generate_synthetic_movie(self, movie_id):
        """Gera filmes sintéticos baseados em padrões reais"""
        genres_weights = [
            ('Drama', 25), ('Action', 20), ('Comedy', 18), 
            ('Sci-Fi', 15), ('Crime', 12), ('Horror', 10)
        ]
        
        countries_weights = [
            ('EUA', 40), ('Brasil', 20), ('Reino Unido', 15),
            ('Coreia do Sul', 10), ('Japão', 8), ('França', 7)
        ]
        
        # Escolher gênero e país baseado em pesos
        genres = [g for g, w in genres_weights for _ in range(w)]
        countries = [c for c, w in countries_weights for _ in range(w)]
        
        genre = random.choice(genres)
        country = random.choice(countries)
        
        # Títulos realistas por gênero
        title_templates = {
            'Drama': ['The Last {}', 'Shadows of {}', 'Echoes of {}', 'The {} Promise'],
            'Action': ['{} Rising', 'The {} Protocol', '{} Run', 'The {} Directive'],
            'Comedy': ['The {} Adventure', '{} Nights', 'The {} Connection', '{} Party'],
            'Sci-Fi': ['{} Legacy', 'The {} Paradox', 'Beyond {}', 'The {} Effect'],
            'Crime': ['The {} Syndicate', '{} City', 'The {} Connection', 'Street {}'],
            'Horror': ['The {} Curse', 'Whispers of {}', 'The {} House', 'Dark {}']
        }
        
        words = ['Midnight', 'Sun', 'Shadow', 'Dream', 'Echo', 'Silent', 'Final', 'Lost']
        template = random.choice(title_templates.get(genre, ['The {}']))
        title = template.format(random.choice(words))
        
        # Diretores por país e gênero
        directors_by_country = {
            'Brasil': ['Fernando Meirelles', 'José Padilha', 'Walter Salles', 'Guel Arraes'],
            'EUA': ['Steven Spielberg', 'Martin Scorsese', 'Christopher Nolan', 'Quentin Tarantino'],
            'Reino Unido': ['Alfred Hitchcock', 'Ridley Scott', 'Danny Boyle', 'Christopher Nolan'],
            'Coreia do Sul': ['Bong Joon-ho', 'Park Chan-wook', 'Kim Jee-woon'],
            'Japão': ['Akira Kurosawa', 'Hayao Miyazaki', 'Takashi Miike'],
            'França': ['Jean-Luc Godard', 'François Truffaut', 'Luc Besson']
        }
        
        director = random.choice(directors_by_country.get(country, ['Unknown Director']))
        release_year = random.randint(1960, 2023)
        duration = random.randint(85, 180)
        
        return {
            'id': movie_id,
            'title': title,
            'genre': genre,
            'release_year': release_year,
            'director': director,
            'country': country,
            'duration': duration,
            'created_at': datetime.now() - timedelta(days=random.randint(1, 365))
        }
    
    def generate_realistic_users(self, num_users=1000):
        """Gera usuários realistas com distribuição por idade e país"""
        print("Gerando dados de usuários realistas...")
        
        # Distribuição realista por país (focada em Brasil e Portugal)
        countries_dist = [
            ('Brasil', 65), ('Portugal', 15), ('EUA', 8), 
            ('Espanha', 5), ('Argentina', 4), ('Chile', 3)
        ]
        
        # Distribuição por idade (baseada em dados reais de streaming)
        age_dist = [
            (16, 24, 25), (25, 34, 35), (35, 44, 20),
            (45, 54, 12), (55, 70, 8)
        ]
        
        countries = [c for c, w in countries_dist for _ in range(w)]
        age_ranges = [(min_a, max_a) for min_a, max_a, w in age_dist for _ in range(w)]
        
        for user_id in range(1, num_users + 1):
            country = random.choice(countries)
            min_age, max_age = random.choice(age_ranges)
            age = random.randint(min_age, max_age)
            
            # Nomes realistas por país
            if country == 'Brasil':
                first_names = ['João', 'Maria', 'Pedro', 'Ana', 'Carlos', 'Juliana', 'Lucas', 'Fernanda']
                last_names = ['Silva', 'Santos', 'Oliveira', 'Souza', 'Rodrigues', 'Ferreira', 'Alves']
            elif country == 'Portugal':
                first_names = ['António', 'Maria', 'João', 'Ana', 'Francisco', 'Isabel', 'Miguel', 'Sofia']
                last_names = ['Silva', 'Santos', 'Ferreira', 'Costa', 'Oliveira', 'Rodrigues', 'Martins']
            else:
                first_names = ['John', 'Mary', 'Robert', 'Jennifer', 'Michael', 'Linda', 'David', 'Susan']
                last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller']
            
            name = f"{random.choice(first_names)} {random.choice(last_names)}"
            email = f"{name.lower().replace(' ', '.')}@email.com"
            
            self.users_data.append({
                'id': user_id,
                'name': name,
                'email': email,
                'age': age,
                'country': country,
                'created_at': datetime.now() - timedelta(days=random.randint(1, 730))
            })
        
        # Introduzir alguns erros para realismo do Data Lake
        self._introduce_data_errors()
        
        return self.users_data
    
    def _introduce_data_errors(self):
        """Introduz erros realistas encontrados em dados brutos"""
        # Erros em usuários
        if len(self.users_data) > 10:
            self.users_data[5]['age'] = 150  # Idade impossível
            self.users_data[15]['email'] = 'email_invalido'  # Email sem @
            self.users_data[25]['country'] = ''  # País vazio
            self.users_data[35]['name'] = ''  # Nome vazio
        
        # Erros em filmes
        if len(self.movies_data) > 10:
            self.movies_data[8]['release_year'] = 2050  # Ano futuro
            self.movies_data[18]['duration'] = -120  # Duração negativa
            self.movies_data[28]['genre'] = ''  # Gênero vazio
            self.movies_data[38]['title'] = ''  # Título vazio
    
    def generate_realistic_ratings(self, num_ratings=10000):
        """Gera avaliações realistas baseadas em padrões de comportamento"""
        print("Gerando avaliações realistas...")
        
        rating_id = 1
        user_rating_patterns = {}
        
        for user in self.users_data:
            # Padrão de avaliação por idade
            if user['age'] < 25:
                rating_bias = random.uniform(0.8, 1.2)  # Jovens tendem a dar notas mais altas
            elif user['age'] > 50:
                rating_bias = random.uniform(0.7, 1.0)  # Mais críticos
            else:
                rating_bias = random.uniform(0.9, 1.1)
            
            user_rating_patterns[user['id']] = {
                'bias': rating_bias,
                'rating_count': random.randint(5, 50)  # Número de avaliações por usuário
            }
        
        # Gerar avaliações
        for user_id, pattern in user_rating_patterns.items():
            ratings_to_generate = min(pattern['rating_count'], 
                                    num_ratings // len(user_rating_patterns))
            
            for _ in range(ratings_to_generate):
                movie = random.choice(self.movies_data)
                
                # Nota base baseada no gênero e ano do filme
                base_rating = self._calculate_base_rating(movie)
                
                # Aplicar viés do usuário
                biased_rating = base_rating * pattern['bias']
                
                # Adicionar variação aleatória
                final_rating = max(1, min(5, round(biased_rating + random.uniform(-0.5, 0.5))))
                
                # Comentários (apenas para algumas avaliações)
                comment = ''
                if random.random() < 0.3:  # 30% das avaliações têm comentários
                    comment = self._generate_comment(final_rating, movie['title'])
                
                self.ratings_data.append({
                    'id': rating_id,
                    'movie_id': movie['id'],
                    'user_id': user_id,
                    'rating': int(final_rating),
                    'comment': comment,
                    'created_at': datetime.now() - timedelta(days=random.randint(1, 365))
                })
                rating_id += 1
                
                if rating_id > num_ratings:
                    break
            if rating_id > num_ratings:
                break
        
        # Adicionar alguns outliers
        self._add_rating_outliers()
        
        return self.ratings_data
    
    def _calculate_base_rating(self, movie):
        """Calcula nota base baseada em características do filme"""
        base_rating = 3.0  # Nota média base
        
        # CORREÇÃO: Extrair primary_genre de forma segura
        genre_text = movie.get('genre', '')
        if isinstance(genre_text, str) and ',' in genre_text:
            primary_genre = genre_text.split(',')[0].strip()
        else:
            primary_genre = genre_text if genre_text else 'Drama'
        
        # Ajustes por gênero (baseado em dados reais de avaliação)
        genre_adjustments = {
            'Drama': 0.3, 'Comedy': 0.1, 'Action': -0.1, 
            'Sci-Fi': 0.2, 'Crime': 0.4, 'Horror': -0.2
        }
        
        base_rating += genre_adjustments.get(primary_genre, 0.0)
        
        # Filmes mais recentes tendem a ter notas mais altas
        current_year = datetime.now().year
        if movie['release_year'] > current_year - 10:
            base_rating += 0.2
        
        # Filmes clássicos (mais de 30 anos) também têm boas avaliações
        elif movie['release_year'] < current_year - 30:
            base_rating += 0.3
        
        return base_rating
    
    def _generate_comment(self, rating, movie_title):
        """Gera comentários realistas baseados na nota"""
        positive_comments = [
            f"Excelente filme! {movie_title} superou minhas expectativas.",
            f"Adorei {movie_title}. Atuações incríveis e roteiro envolvente.",
            f"{movie_title} é uma obra-prima. Recomendo muito!",
            f"Que filme incrível! {movie_title} merece todos os prêmios."
        ]
        
        neutral_comments = [
            f"{movie_title} é um filme decente. Vale a pena assistir.",
            f"Bom filme, mas esperava mais de {movie_title}.",
            f"{movie_title} tem momentos bons, mas poderia ser melhor.",
            f"Entretenimento razoável. {movie_title} cumpre seu propósito."
        ]
        
        negative_comments = [
            f"{movie_title} foi uma decepção. Não recomendo.",
            f"Que desperdício de tempo! {movie_title} é muito fraco.",
            f"{movie_title} tem uma premissa boa, mas a execução é péssima.",
            f"Evitem {movie_title}. Péssimo roteiro e atuações."
        ]
        
        if rating >= 4:
            return random.choice(positive_comments)
        elif rating == 3:
            return random.choice(neutral_comments)
        else:
            return random.choice(negative_comments)
    
    def _add_rating_outliers(self):
        """Adiciona outliers realistas encontrados em dados reais"""
        if len(self.ratings_data) > 100:
            # Notas inválidas
            self.ratings_data[50]['rating'] = 0
            self.ratings_data[150]['rating'] = 6
            self.ratings_data[250]['rating'] = 10
            
            # Avaliações duplicadas
            dup_rating = self.ratings_data[350].copy()
            dup_rating['id'] = max(r['id'] for r in self.ratings_data) + 1
            self.ratings_data.append(dup_rating)
    
    def save_to_csv(self):
        """Salva os dados em arquivos CSV"""
        print("Salvando dados nos arquivos CSV...")
        
        # Criar Data Lake directory
        import os
        os.makedirs('data_lake', exist_ok=True)
        
        # Salvar CSVs
        pd.DataFrame(self.movies_data).to_csv('data_lake/movies.csv', index=False)
        pd.DataFrame(self.users_data).to_csv('data_lake/users.csv', index=False)
        pd.DataFrame(self.ratings_data).to_csv('data_lake/ratings.csv', index=False)
        
        print("✅ Dados salvos com sucesso!")
        print(f"🎬 Filmes: {len(self.movies_data)} registros")
        print(f"👥 Usuários: {len(self.users_data)} registros")
        print(f"⭐ Avaliações: {len(self.ratings_data)} registros")
    
    def generate_sample_analytics(self):
        """Gera um relatório analítico simples dos dados"""
        print("\n" + "="*50)
        print("📊 RELATÓRIO ANALÍTICO - DADOS GERADOS")
        print("="*50)
        
        df_movies = pd.DataFrame(self.movies_data)
        df_users = pd.DataFrame(self.users_data)
        df_ratings = pd.DataFrame(self.ratings_data)
        
        # Estatísticas básicas
        print(f"\n🎭 Distribuição por Gênero:")
        # CORREÇÃO: Lidar com múltiplos gêneros
        all_genres = []
        for genres in df_movies['genre']:
            if isinstance(genres, str):
                # Separar gêneros múltiplos
                for genre in genres.split(','):
                    all_genres.append(genre.strip())
        
        genre_counts = pd.Series(all_genres).value_counts()
        for genre, count in genre_counts.head(10).items():
            print(f"   {genre}: {count} filmes")
        
        print(f"\n🌍 Distribuição por País:")
        country_counts = df_movies['country'].value_counts()
        for country, count in country_counts.head().items():
            print(f"   {country}: {count} filmes")
        
        print(f"\n👥 Demografia dos Usuários:")
        age_groups = pd.cut(df_users['age'], bins=[0, 18, 25, 35, 50, 100], 
                           labels=['<18', '18-25', '26-35', '36-50', '50+'])
        print(age_groups.value_counts().sort_index())
        
        print(f"\n⭐ Estatísticas de Avaliação:")
        print(f"   Média geral: {df_ratings['rating'].mean():.2f}")
        print(f"   Mediana: {df_ratings['rating'].median():.2f}")
        print(f"   Desvio padrão: {df_ratings['rating'].std():.2f}")
        
        # Top filmes
        movie_ratings = df_ratings.groupby('movie_id').agg({
            'rating': ['mean', 'count']
        }).round(2)
        movie_ratings.columns = ['avg_rating', 'rating_count']
        movie_ratings = movie_ratings.reset_index()
        
        top_movies = movie_ratings[movie_ratings['rating_count'] >= 5].nlargest(5, 'avg_rating')
        top_movies_with_titles = top_movies.merge(df_movies[['id', 'title']], 
                                                left_on='movie_id', right_on='id')
        
        print(f"\n🏆 Top 5 Filmes Melhor Avaliados:")
        for _, movie in top_movies_with_titles.iterrows():
            print(f"   {movie['title']}: {movie['avg_rating']}⭐ ({movie['rating_count']} avaliações)")

def main():
    """Função principal"""
    print("🎬 MovieFlix Analytics - Gerador de Dados Realistas")
    print("="*60)
    
    generator = RealMovieDataGenerator()
    
    # Gerar dados
    generator.get_real_movies_from_api(200)  
    generator.generate_realistic_users(450)  
    generator.generate_realistic_ratings(3000)  # Reduzido para teste
    
    # Salvar e mostrar analytics
    generator.save_to_csv()
    generator.generate_sample_analytics()
    
    print("\n✅ Todos os dados foram gerados com sucesso!")
    print("📁 Arquivos salvos em: data_lake/")
    print("\n💡 Próximos passos:")
    print("   1. Execute o script de carga no Data Warehouse")
    print("   2. Rode as consultas analíticas")
    print("   3. Acesse a aplicação web em http://localhost")

if __name__ == "__main__":
    main()