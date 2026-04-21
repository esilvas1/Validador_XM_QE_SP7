@echo off
cd /d "%~dp0"
set DATA_DIR=\\CENS-AD02.cens.corp.epm.com.co\Administrativa\UO01\CISO01\CISO01_CIMDESS\S0022\data
echo Iniciando servidor Django...
echo DATA_DIR=%DATA_DIR%
py manage.py runserver 8000
pause
