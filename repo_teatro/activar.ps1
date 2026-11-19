.venv\Scripts\activate
$env:HADOOP_HOME = "C:\hadoop"
$env:PATH = "C:\hadoop\bin;$env:PATH"
$env:PYSPARK_PYTHON = "$PWD\.venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "$PWD\.venv\Scripts\python.exe"
Write-Host "Entorno listo" -ForegroundColor Green