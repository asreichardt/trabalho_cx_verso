Write-Host "Testando aplicação MovieFlix..." -ForegroundColor Green

# Verificar se os containers estão rodando
Write-Host "`n1. Verificando containers Docker..." -ForegroundColor Yellow
docker-compose ps

# Testar health check da aplicação
Write-Host "`n2. Testando endpoint de health check..." -ForegroundColor Yellow
try {
    $response = Invoke-WebRequest -Uri "http://localhost/health" -UseBasicParsing -TimeoutSec 10
    if ($response.StatusCode -eq 200) {
        Write-Host "SUCESSO - Health Check: Resposta 200 OK" -ForegroundColor Green
        Write-Host "   Conteudo: $($response.Content)" -ForegroundColor White
    } else {
        Write-Host "FALHA - Health Check: Status $($response.StatusCode)" -ForegroundColor Red
        exit 1
    }
} catch {
    Write-Host "ERRO - Health Check: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

# Testar endpoint principal
Write-Host "`n3. Testando pagina principal..." -ForegroundColor Yellow
try {
    $response = Invoke-WebRequest -Uri "http://localhost" -UseBasicParsing -TimeoutSec 10
    if ($response.StatusCode -eq 200) {
        Write-Host "SUCESSO - Pagina principal: Resposta 200 OK" -ForegroundColor Green
    } else {
        Write-Host "FALHA - Pagina principal: Status $($response.StatusCode)" -ForegroundColor Red
        exit 1
    }
} catch {
    Write-Host "ERRO - Pagina principal: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

# Testar API de filmes
Write-Host "`n4. Testando API de filmes..." -ForegroundColor Yellow
try {
    $response = Invoke-WebRequest -Uri "http://localhost/api/movies" -UseBasicParsing -TimeoutSec 10
    if ($response.StatusCode -eq 200) {
        Write-Host "SUCESSO - API de filmes: Resposta 200 OK" -ForegroundColor Green
        $movies = $response.Content | ConvertFrom-Json
        Write-Host "   Quantidade de filmes: $($movies.Count)" -ForegroundColor White
    } else {
        Write-Host "FALHA - API de filmes: Status $($response.StatusCode)" -ForegroundColor Red
        exit 1
    }
} catch {
    Write-Host "ERRO - API de filmes: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

# Testar conexao com banco de dados
Write-Host "`n5. Testando conexao com banco de dados..." -ForegroundColor Yellow
try {
    $response = Invoke-WebRequest -Uri "http://localhost/api/debug/db" -UseBasicParsing -TimeoutSec 10
    if ($response.StatusCode -eq 200) {
        $dbInfo = $response.Content | ConvertFrom-Json
        if ($dbInfo.connected -eq $true) {
            Write-Host "SUCESSO - Banco de dados: Conectado" -ForegroundColor Green
        } else {
            Write-Host "FALHA - Banco de dados: Desconectado" -ForegroundColor Red
            exit 1
        }
    } else {
        Write-Host "FALHA - Teste do banco: Status $($response.StatusCode)" -ForegroundColor Red
        exit 1
    }
} catch {
    Write-Host "ERRO - Teste do banco: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

Write-Host "`nRESUMO DOS TESTES:" -ForegroundColor Cyan
Write-Host "   Health Check: SUCESSO" -ForegroundColor Green
Write-Host "   Pagina Web: SUCESSO" -ForegroundColor Green  
Write-Host "   API REST: SUCESSO" -ForegroundColor Green
Write-Host "   Banco de Dados: SUCESSO" -ForegroundColor Green
Write-Host "`nAPLICACAO FUNCIONANDO CORRETAMENTE!" -ForegroundColor Green