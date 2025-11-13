@echo off
echo === Data Platform Health Check ===
echo.

echo Checking PostgreSQL...
powershell -Command "try { $tcp = New-Object System.Net.Sockets.TcpClient; $tcp.Connect('localhost', 5432); $tcp.Close(); Write-Host '✅ PostgreSQL is accessible on port 5432' } catch { Write-Host '❌ PostgreSQL is not accessible' }"

echo Checking Kafka UI...
powershell -Command "try { Invoke-WebRequest -Uri http://localhost:8888 -TimeoutSec 5 -UseBasicParsing | Out-Null; Write-Host '✅ Kafka UI is accessible on port 8888' } catch { Write-Host '❌ Kafka UI is not accessible' }"

echo Checking Gravitino...
powershell -Command "try { Invoke-WebRequest -Uri http://localhost:8090 -TimeoutSec 5 -UseBasicParsing | Out-Null; Write-Host '✅ Gravitino is accessible on port 8090' } catch { Write-Host '❌ Gravitino is not accessible' }"

echo Checking Gravitino Iceberg REST...
powershell -Command "try { Invoke-WebRequest -Uri http://localhost:9001 -TimeoutSec 5 -UseBasicParsing | Out-Null; Write-Host '✅ Gravitino Iceberg REST is accessible on port 9001' } catch { Write-Host '❌ Gravitino Iceberg REST is not accessible' }"

echo Checking MinIO...
powershell -Command "try { Invoke-WebRequest -Uri http://localhost:9000/minio/health/live -TimeoutSec 5 -UseBasicParsing | Out-Null; Write-Host '✅ MinIO is accessible on port 9000' } catch { Write-Host '❌ MinIO is not accessible' }"

echo Checking Kafka Broker 1...
powershell -Command "try { $tcp = New-Object System.Net.Sockets.TcpClient; $tcp.Connect('localhost', 9092); $tcp.Close(); Write-Host '✅ Kafka Broker 1 is accessible on port 9092' } catch { Write-Host '❌ Kafka Broker 1 is not accessible' }"

echo Checking Kafka Broker 2...
powershell -Command "try { $tcp = New-Object System.Net.Sockets.TcpClient; $tcp.Connect('localhost', 9094); $tcp.Close(); Write-Host '✅ Kafka Broker 2 is accessible on port 9094' } catch { Write-Host '❌ Kafka Broker 2 is not accessible' }"

echo.
echo === Service URLs ===
echo Kafka UI: http://localhost:8888
echo Gravitino UI: http://localhost:8090
echo Gravitino Iceberg REST: http://localhost:9001
echo MinIO Console: http://localhost:9002
echo Spark Master UI: http://localhost:8080
echo PostgreSQL: localhost:5432 (user: gravitino, password: gravitino123, db: gravitino)
echo.
pause