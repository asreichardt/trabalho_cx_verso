import pandas as pd
import mysql.connector
from mysql.connector import Error
import os

class DataWarehouseLoader:
    def __init__(self):
        self.connection = None
        self.data_lake_path = 'data_lake/'
        
    def connect_to_mysql(self):
        """Conecta ao MySQL"""
        try:
            self.connection = mysql.connector.connect(
                host='localhost',
                user='movieflix_user',
                password='movieflix123',
                database='movieflix',
                port=3306
            )
            print(" Conectado ao MySQL com sucesso!")
            return True
        except Error as e:
            print(f" erro ao conectar ao MySQL: {e}")
            return False
    
    def clean_and_transform_data(self):
        """Limpa e transforma os dados do Data Lake"""
        print("🔄 Limpando e transformando dados...")
        
        # carrega dados brutos
        movies_df = pd.read_csv(f'{self.data_lake_path}movies.csv')
        users_df = pd.read_csv(f'{self.data_lake_path}users.csv')
        ratings_df = pd.read_csv(f'{self.data_lake_path}ratings.csv')
        
        # Limpeza de dados
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
        
        return df
    
    def load_to_data_warehouse(self):
        """Carrega dados limpos no Data Warehouse"""
        if not self.connect_to_mysql():
            return False
        
        try:
            cursor = self.connection.cursor()
            
            # Carregar dados transformados
            movies_clean, users_clean, ratings_clean = self.clean_and_transform_data()
            
            # Limpar tabelas existentes
            cursor.execute("DELETE FROM ratings")
            cursor.execute("DELETE FROM movies")
            cursor.execute("DELETE FROM users")
            
            # Inserir usuários
            print("📥 Inserindo usuários...")
            for _, user in users_clean.iterrows():
                cursor.execute("""
                    INSERT INTO users (id, name, email, age, country, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (user['id'], user['name'], user['email'], user['age'], 
                      user['country'], user.get('created_at', None)))
            
            # Inserir filmes
            print("📥 Inserindo filmes...")
            for _, movie in movies_clean.iterrows():
                cursor.execute("""
                    INSERT INTO movies (id, title, genre, release_year, director, country, duration, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (movie['id'], movie['title'], movie['genre'], movie['release_year'],
                      movie['director'], movie['country'], movie['duration'], 
                      movie.get('created_at', None)))
            
            # Inserir avaliações
            print("📥 Inserindo avaliações...")
            for _, rating in ratings_clean.iterrows():
                cursor.execute("""
                    INSERT INTO ratings (id, movie_id, user_id, rating, comment, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (rating['id'], rating['movie_id'], rating['user_id'], 
                      rating['rating'], rating.get('comment', ''), 
                      rating.get('created_at', None)))
            
            self.connection.commit()
            print("✅ Dados carregados no Data Warehouse com sucesso!")
            
            # Gerar relatório de qualidade
            self.generate_data_quality_report(cursor, 
                                            len(movies_clean), 
                                            len(users_clean), 
                                            len(ratings_clean))
            
            return True
            
        except Error as e:
            print(f"❌ Erro ao carregar dados: {e}")
            return False
        finally:
            if self.connection.is_connected():
                cursor.close()
                self.connection.close()
    
    def generate_data_quality_report(self, cursor, movies_count, users_count, ratings_count):
        """Gera relatório de qualidade dos dados"""
        print("\n" + "="*60)
        print("📊 RELATÓRIO DE QUALIDADE - DATA WAREHOUSE")
        print("="*60)
        
        # Contagens finais
        cursor.execute("SELECT COUNT(*) FROM movies")
        final_movies = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM users")
        final_users = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM ratings")
        final_ratings = cursor.fetchone()[0]
        
        print(f"\n📈 Estatísticas de Carga:")
        print(f"   Filmes: {movies_count} → {final_movies} ({final_movies/movies_count*100:.1f}% retidos)")
        print(f"   Usuários: {users_count} → {final_users} ({final_users/users_count*100:.1f}% retidos)")
        print(f"   Avaliações: {ratings_count} → {final_ratings} ({final_ratings/ratings_count*100:.1f}% retidos)")
        
        # Qualidade dos dados
        cursor.execute("SELECT AVG(rating) FROM ratings")
        avg_rating = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT user_id) FROM ratings")
        active_users = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT movie_id) FROM ratings")
        rated_movies = cursor.fetchone()[0]
        
        print(f"\n✅ Métricas de Qualidade:")
        print(f"   Nota média: {avg_rating:.2f}⭐")
        print(f"   Usuários ativos: {active_users}")
        print(f"   Filmes avaliados: {rated_movies}")
        print(f"   Taxa de atividade: {active_users/final_users*100:.1f}%")

def main():
    """Função principal"""
    print("🏗️  MovieFlix Analytics - Carga no Data Warehouse")
    print("="*60)
    
    loader = DataWarehouseLoader()
    
    if loader.load_to_data_warehouse():
        print("\n🎉 Processo de ETL concluído com sucesso!")
        print("\n💡 Próximos passos:")
        print("   1. Execute as consultas analíticas")
        print("   2. Verifique as views do Data Mart")
        print("   3. Acesse a aplicação web")
    else:
        print("\n❌ Falha no processo de ETL")

if __name__ == "__main__":
    main()