# 企业知识中台 — 一键启动所有服务
Write-Host "=== Starting Knowledge Platform ===" -ForegroundColor Cyan

$backend = "d:/knowledge-platform/backend"
$python = "d:/Anaconda/envs/knowledge-platform/python"

# 终端 1: FastAPI
Start-Process powershell -ArgumentList "-NoExit", "-Command", `
    "cd $backend; $python -m uvicorn api.main:app --port 8000 --host 0.0.0.0; Write-Host 'FastAPI started on :8000'"

# 终端 2: MinerU API
Start-Process powershell -ArgumentList "-NoExit", "-Command", `
    "`$env:MINERU_MODEL_SOURCE='modelscope'; d:/Anaconda/envs/knowledge-platform/Scripts/mineru-api.exe --port 8001; Write-Host 'MinerU API started on :8001'"

# 终端 3: Celery Worker
Start-Process powershell -ArgumentList "-NoExit", "-Command", `
    "cd $backend; $python -m celery -A core.celery worker -Q gpu_queue,cpu_queue --concurrency=4 -n worker; Write-Host 'Celery Worker started'"

# 终端 4: Celery Beat
Start-Process powershell -ArgumentList "-NoExit", "-Command", `
    "cd $backend; $python -m celery -A core.celery beat; Write-Host 'Celery Beat started'"

# 终端 5: 前端
Start-Process powershell -ArgumentList "-NoExit", "-Command", `
    "cd d:/knowledge-platform/frontend; npm run dev; Write-Host 'Frontend started on :5173'"

Write-Host "=== All services starting... ===" -ForegroundColor Green
Write-Host "Frontend: http://localhost:5173" -ForegroundColor Cyan
Write-Host "API Docs: http://localhost:8000/docs" -ForegroundColor Cyan
Write-Host "RabbitMQ:  http://localhost:15672 (guest/guest)" -ForegroundColor Cyan
Write-Host "Wait ~30s for all services to initialize" -ForegroundColor Yellow
