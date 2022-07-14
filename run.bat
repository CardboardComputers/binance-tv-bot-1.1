call .\venv\Scripts\activate
for /f %%i in ('python -m certifi') do set SSL_CERT_FILE=%%i
.\venv\Scripts\python.exe .\main.py