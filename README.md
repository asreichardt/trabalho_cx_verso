#  MovieFlix Analytics

Sistema de análise de dados para plataforma de streaming de filmes.

##  Tecnologias

- **Backend:** Node.js + Express
- **Banco de Dados:** MySQL
- **Proxy:** Nginx
- **Containerização:** Docker
- **Frontend:** HTML, CSS, JavaScript
- **CI/CD:** GitHub Actions

  Estrutura do Projeto


├── app/                 # Aplicação Node.js
├── nginx/              # Configuração do proxy reverso
├── database/           # Scripts do banco de dados
│   ├── init.sql       # Schema inicial
│   ├── scripts/       # Scripts de ETL
│   └── data_lake/     # Dados brutos (CSV)
├── .github/workflows/  # CI/CD
└── docker-compose.yml  # Orquestração
├── fotos/              # Fotos da aplicação
├── test_app.ps1   # testa para ver se a aplicação está no ar

##  Funcionalidades

-  Cadastro de filmes e usuários
-  Sistema de avaliações (1-5 estrelas)
-  Dashboard analítico com rankings
-  Data Lake → Data Warehouse → Data Mart
-  Consultas analíticas em tempo real
-  Interface web responsiva

## URLs

- **Aplicação:** http://localhost
- **MySQL:** localhost:3306

##  Análises Disponíveis

- Top filmes mais populares
- Gêneros melhor avaliados
- Países mais ativos
- Avaliação por faixa etária
- Evolução temporal das avaliações
- Diretores mais populares
 - Performance por País de Origem
 - Usuários Mais Ativos
 -  Análise Sazonal

##  Variáveis de Ambiente

Copie `.env.example` para `.env` e configure as credenciais do banco.

##  Licença

MIT License
