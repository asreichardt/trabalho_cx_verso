import pandas as pd
import os
from datetime import datetime

class ETLGenerator:
    def __init__(self):
        self.data_lake_path = 'data_lake/'
        self.output_sql_file = 'etl_output.sql'
        
    def clean_and_transform_data(self):
        """limpa e transforma os dados do Data Lake""" 
        
        # carrega dados brutos
        movies_df = pd.read_csv(f'{self.data_lake_path}movies.csv')
        users_df = pd.read_csv(f'{self.data_lake_path}users.csv')
        ratings_df = pd.read_csv(f'{self.data_lake_path}ratings.csv')
        
        # limpa os dados
        movies_clean = self.clean_movies_data(movies_df)
        users_clean = self.clean_users_data(users_df)
        ratings_clean = self.clean_ratings_data(ratings_df)
        
        return movies_clean, users_clean, ratings_clean
    
    def clean_movies_data(self, df):
        """Limpa dados de filmes"""
        # Remover filmes sem título
        df = df[df['title'].notna() & (df['title'] != '')]
        
        # Corrigir anos inválidos
        current_year = pd.Timestamp.now().year
        df = df[(df['release_year'] >= 1900) & (df['release_year'] <= current_year)]
        
        # Corrigir durações inválidas
        df = df[df['duration'] > 0]
        
        # Preencher gêneros vazios
        df['genre'] = df['genre'].fillna('Unknown')
        
        # Preencher diretor vazio
        df['director'] = df['director'].fillna('Unknown')
        
        # Preencher país vazio
        df['country'] = df['country'].fillna('Unknown')
        
        return df
    
    def clean_users_data(self, df):
        """Limpa dados de usuários"""
        # Remover usuários sem nome
        df = df[df['name'].notna() & (df['name'] != '')]
        
        # Corrigir idades inválidas
        df = df[(df['age'] >= 13) & (df['age'] <= 120)]
        
        # Preencher países vazios
        df['country'] = df['country'].fillna('Unknown')
        
        # Validar emails
        df = df[df['email'].str.contains('@', na=False)]
        
        return df
    
    def clean_ratings_data(self, df):
        """Limpa dados de avaliações"""
        # Remover avaliações com notas inválidas
        df = df[(df['rating'] >= 1) & (df['rating'] <= 5)]
        
        # Remover duplicatas (mesmo usuário + mesmo filme)
        df = df.drop_duplicates(subset=['user_id', 'movie_id'], keep='last')
        
        # Preencher comentários vazios
        df['comment'] = df['comment'].fillna('')
        
        return df
    
    def escape_sql_string(self, value):
        """Escapa aspas simples para SQL"""
        if pd.isna(value):
            return ''
        return str(value).replace("'", "''")
    
    def generate_sql_file(self):
        """Gera arquivo SQL com INSERTs dos dados tratados"""
        print("📝 Gerando arquivo SQL...")
        
        movies_clean, users_clean, ratings_clean = self.clean_and_transform_data()
        
        with open(self.output_sql_file, 'w', encoding='utf-8') as f:
            # Cabeçalho
            f.write("--  MovieFlix - ETL SQL Generated\n")
            f.write(f"--  Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("--  Data cleaned and transformed from CSV files\n\n")
            
            # Limpar tabelas existentes
            f.write("-- 🗑️ Cleaning existing data\n")
            f.write("DELETE FROM ratings;\n")
            f.write("DELETE FROM movies;\n")
            f.write("DELETE FROM users;\n\n")
            
            # Inserir usuários
            f.write("--  Inserting users\n")
            for _, user in users_clean.iterrows():
                created_at = user.get('created_at', 'NOW()')
                if pd.isna(created_at):
                    created_at = 'NOW()'
                elif isinstance(created_at, str) and created_at.strip():
                    created_at = f"'{created_at}'"
                else:
                    created_at = 'NOW()'
                
                f.write(f"INSERT INTO users (id, name, email, age, country, created_at) VALUES (")
                f.write(f"{user['id']}, ")
                f.write(f"'{self.escape_sql_string(user['name'])}', ")
                f.write(f"'{self.escape_sql_string(user['email'])}', ")
                f.write(f"{user['age']}, ")
                f.write(f"'{self.escape_sql_string(user['country'])}', ")
                f.write(f"{created_at});\n")
            
            f.write("\n")
            
            # Inserir filmes
            f.write("-- 🎬 Inserting movies\n")
            for _, movie in movies_clean.iterrows():
                created_at = movie.get('created_at', 'NOW()')
                if pd.isna(created_at):
                    created_at = 'NOW()'
                elif isinstance(created_at, str) and created_at.strip():
                    created_at = f"'{created_at}'"
                else:
                    created_at = 'NOW()'
                
                f.write(f"INSERT INTO movies (id, title, genre, release_year, director, country, duration, created_at) VALUES (")
                f.write(f"{movie['id']}, ")
                f.write(f"'{self.escape_sql_string(movie['title'])}', ")
                f.write(f"'{self.escape_sql_string(movie['genre'])}', ")
                f.write(f"{movie['release_year']}, ")
                f.write(f"'{self.escape_sql_string(movie['director'])}', ")
                f.write(f"'{self.escape_sql_string(movie['country'])}', ")
                f.write(f"{movie['duration']}, ")
                f.write(f"{created_at});\n")
            
            f.write("\n")
            
            # Inserir avaliações
            f.write("-- ⭐ Inserting ratings\n")
            for _, rating in ratings_clean.iterrows():
                created_at = rating.get('created_at', 'NOW()')
                if pd.isna(created_at):
                    created_at = 'NOW()'
                elif isinstance(created_at, str) and created_at.strip():
                    created_at = f"'{created_at}'"
                else:
                    created_at = 'NOW()'
                
                comment = rating.get('comment', '')
                if pd.isna(comment):
                    comment = ''
                
                f.write(f"INSERT INTO ratings (id, movie_id, user_id, rating, comment, created_at) VALUES (")
                f.write(f"{rating['id']}, ")
                f.write(f"{rating['movie_id']}, ")
                f.write(f"{rating['user_id']}, ")
                f.write(f"{rating['rating']}, ")
                f.write(f"'{self.escape_sql_string(comment)}', ")
                f.write(f"{created_at});\n")
            
            
            f.write(f"\n--  ETL Statistics\n")
            f.write(f"-- Users: {len(users_clean)} inserted\n")
            f.write(f"-- Movies: {len(movies_clean)} inserted\n")
            f.write(f"-- Ratings: {len(ratings_clean)} inserted\n")
            
        print(f" Arquivo SQL gerado: {self.output_sql_file}")
        print(f" Estatísticas:")
        print(f"    Usuários: {len(users_clean)}")
        print(f"    Filmes: {len(movies_clean)}")
        print(f"    Avaliações: {len(ratings_clean)}")

def main():
    """Função principal"""
    print(" MovieFlix - ETL SQL Generator")
    
    generator = ETLGenerator()
    generator.generate_sql_file()
    
    print(f"\n💡 Próximos passos:")
    print(f"   1. Execute o arquivo: mysql -u usuario -p database < {generator.output_sql_file}")
    print(f"   2. Ou copie e cole no MySQL Workbench")
    print(f"   3. Verifique os dados no banco")

if __name__ == "__main__":
    main()