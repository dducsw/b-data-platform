@echo off
REM Stop Data Platform on Windows

echo Stopping Data Platform...

docker-compose down -v

echo Data Platform stopped and volumes cleaned up.

pause