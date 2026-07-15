@echo off
REM Build and push the production image to the local registry.
REM Usage: scripts\build_and_push.bat [version]

set VERSION=%1
if "%VERSION%"=="" set VERSION=dev

set IMAGE=localhost:5000/askmydocs-spark:%VERSION%

echo Building %IMAGE% ...
docker build -t %IMAGE% -f docker/Dockerfile.spark.prod . || exit /b 1

echo Pushing %IMAGE% ...
docker push %IMAGE% || exit /b 1

echo Done. Image available at %IMAGE%