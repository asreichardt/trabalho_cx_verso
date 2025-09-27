Write-Host "🎬 MovieFlix Analytics - Teste Completo da Aplicação" -ForegroundColor Green
Write-Host "=" * 60

# 1. Testar Health Check
Write-Host "`n1. Testando Health Check..." -ForegroundColor Yellow
$health = Invoke-WebRequest -Uri "http://localhost/health" -UseBasicParsing
Write-Host "✅ Health Check: $($health.Content)" -ForegroundColor Green

# 2. Testar Conexão com Banco
Write-Host "`n2. Testando Conexão com Banco..." -ForegroundColor Yellow
$dbDebug = Invoke-WebRequest -Uri "http://localhost/api/debug/db" -UseBasicParsing
Write-Host "✅ Debug Banco: $($dbDebug.Content)" -ForegroundColor Green

# 3. Testar Lista de Filmes
Write-Host "`n3. Testando API de Filmes..." -ForegroundColor Yellow
try {
    $movies = Invoke-WebRequest -Uri "http://localhost/api/movies" -UseBasicParsing
    $moviesData = $movies.Content | ConvertFrom-Json
    Write-Host "✅ Filmes encontrados: $($moviesData.Count) registros" -ForegroundColor Green
} catch {
    Write-Host "❌ Erro ao buscar filmes: $($_.Exception.Message)" -ForegroundColor Red
}

# 4. Testar Lista de Usuários
Write-Host "`n4. Testando API de Usuários..." -ForegroundColor Yellow
try {
    $users = Invoke-WebRequest -Uri "http://localhost/api/users" -UseBasicParsing
    $usersData = $users.Content | ConvertFrom-Json
    Write-Host "✅ Usuários encontrados: $($usersData.Count) registros" -ForegroundColor Green
} catch {
    Write-Host "❌ Erro ao buscar usuários: $($_.Exception.Message)" -ForegroundColor Red
}

# 5. Verificar Containers
Write-Host "`n5. Verificando Estado dos Containers..." -ForegroundColor Yellow
docker-compose ps

Write-Host "`n🎉 Teste Completo Concluído!" -ForegroundColor Green