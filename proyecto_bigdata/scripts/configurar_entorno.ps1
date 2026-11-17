<#
.SYNOPSIS
  Configura en Windows 10/11 todo lo necesario para ejecutar el proyecto:
  Spark 3.5.9, winutils/hadoop.dll (Hadoop 3.3.5), variables de entorno y
  el entorno virtual de Python con requirements.txt.

.DESCRIPTION
  Requisito previo (manual): JDK 17 instalado (ver README, paso 1).
  Ejecutar desde la RAÍZ del proyecto, en PowerShell (NO hace falta administrador):

      Set-ExecutionPolicy -Scope CurrentUser RemoteSigned    # solo la primera vez
      .\scripts\configurar_entorno.ps1 -JavaHome "C:\Java\jdk-17"

  Las variables se guardan a nivel de USUARIO. Al terminar, CIERRE y vuelva a
  abrir la terminal (y VS Code / Jupyter) para que tomen los nuevos valores.

.PARAMETER JavaHome
  Carpeta del JDK 17 (la que contiene bin\java.exe). Si se omite, usa la
  variable JAVA_HOME existente o intenta detectar Eclipse Temurin.

.PARAMETER Base
  Carpeta donde se instalan Spark y Hadoop. Por defecto C:\bigdata (sin espacios).

.PARAMETER OmitirDescargas
  No descarga Spark ni winutils (útil si ya los descargó a mano).
#>
param(
    [string]$JavaHome = $env:JAVA_HOME,
    [string]$Base = "C:\bigdata",
    [string]$SparkVersion = "3.5.9",
    [string]$HadoopWinutils = "3.3.5",
    [string]$PythonVersion = "3.11",
    [switch]$OmitirDescargas
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"   # Invoke-WebRequest es mucho más rápido sin barra
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12  # PowerShell 5.1
$Proyecto = (Resolve-Path "$PSScriptRoot\..").Path

function Paso($t)  { Write-Host "`n==> $t" -ForegroundColor Cyan }
function Ok($t)    { Write-Host "    [OK] $t" -ForegroundColor Green }
function Aviso($t) { Write-Host "    [AVISO] $t" -ForegroundColor Yellow }

function Set-VarUsuario($nombre, $valor) {
    [Environment]::SetEnvironmentVariable($nombre, $valor, "User")
    Set-Item -Path "Env:$nombre" -Value $valor      # también en esta sesión
    Ok "$nombre = $valor"
}

function Agregar-PathUsuario($ruta) {
    $actual = [Environment]::GetEnvironmentVariable("Path", "User")
    $partes = @()
    if ($actual) { $partes = $actual.Split(";") | Where-Object { $_ -ne "" } }
    if ($partes -notcontains $ruta) {
        $nuevo = (@($ruta) + $partes) -join ";"      # al inicio, para que tenga prioridad
        [Environment]::SetEnvironmentVariable("Path", $nuevo, "User")
        Ok "Agregado al PATH de usuario: $ruta"
    } else {
        Ok "Ya estaba en el PATH: $ruta"
    }
    $env:Path = "$ruta;$env:Path"
}

function RutaCorta($ruta) {
    # Convierte "C:\Program Files\..." en "C:\PROGRA~1\..." (Spark falla con espacios)
    $fso = New-Object -ComObject Scripting.FileSystemObject
    return $fso.GetFolder($ruta).ShortPath
}

# ---------------------------------------------------------------------------
Paso "1. Java (JDK 17)"
if (-not $JavaHome) {
    $candidato = Get-ChildItem "C:\Program Files\Eclipse Adoptium" -Directory -Filter "jdk-17*" `
                 -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($candidato) { $JavaHome = $candidato.FullName }
}
if (-not $JavaHome -or -not (Test-Path "$JavaHome\bin\java.exe")) {
    throw "No se encontró el JDK. Instale JDK 17 (README, paso 1) y ejecute: .\scripts\configurar_entorno.ps1 -JavaHome 'C:\Java\jdk-17'"
}
if ($JavaHome -match " ") {
    $corta = RutaCorta $JavaHome
    Aviso "La ruta del JDK tiene espacios; se usará la ruta corta $corta"
    $JavaHome = $corta
}
# java -version escribe en stderr: en PowerShell 5.1 eso se volvería error con "Stop"
$ErrorActionPreference = "Continue"
$verJava = (& "$JavaHome\bin\java.exe" -version 2>&1 | Select-Object -First 1 | Out-String).Trim()
$ErrorActionPreference = "Stop"
Ok $verJava
if ($verJava -notmatch '"(17|11|1\.8)') { Aviso "Se recomienda JDK 17 (el enunciado admite 8, 11 o 17)." }
Set-VarUsuario "JAVA_HOME" $JavaHome

# ---------------------------------------------------------------------------
Paso "2. Apache Spark $SparkVersion"
$SparkHome = "$Base\spark-$SparkVersion-bin-hadoop3"
New-Item -ItemType Directory -Force -Path $Base | Out-Null
if (-not (Test-Path "$SparkHome\bin\spark-submit.cmd")) {
    if ($OmitirDescargas) { throw "No existe $SparkHome y se indicó -OmitirDescargas" }
    $tgz = "$Base\spark-$SparkVersion-bin-hadoop3.tgz"
    $urls = @(
        "https://dlcdn.apache.org/spark/spark-$SparkVersion/spark-$SparkVersion-bin-hadoop3.tgz",
        "https://archive.apache.org/dist/spark/spark-$SparkVersion/spark-$SparkVersion-bin-hadoop3.tgz"
    )
    $descargado = $false
    foreach ($u in $urls) {
        try {
            Write-Host "    Descargando $u (~400 MB)..."
            Invoke-WebRequest -Uri $u -OutFile $tgz -UseBasicParsing
            $descargado = $true; break
        } catch { Aviso "Falló $u" }
    }
    if (-not $descargado) { throw "No se pudo descargar Spark. Descárguelo a mano (README, paso 2)." }
    Write-Host "    Descomprimiendo..."
    tar -xzf $tgz -C $Base          # tar viene incluido en Windows 10 (1803+) y 11
    Remove-Item $tgz
}
Ok "Spark en $SparkHome"
Set-VarUsuario "SPARK_HOME" $SparkHome
Agregar-PathUsuario "$SparkHome\bin"

# ---------------------------------------------------------------------------
Paso "3. Hadoop winutils.exe + hadoop.dll ($HadoopWinutils)"
$HadoopHome = "$Base\hadoop"
New-Item -ItemType Directory -Force -Path "$HadoopHome\bin" | Out-Null
foreach ($f in @("winutils.exe", "hadoop.dll")) {
    $dest = "$HadoopHome\bin\$f"
    if (-not (Test-Path $dest)) {
        if ($OmitirDescargas) { throw "Falta $dest" }
        $u = "https://github.com/cdarlint/winutils/raw/master/hadoop-$HadoopWinutils/bin/$f"
        Write-Host "    Descargando $u"
        Invoke-WebRequest -Uri $u -OutFile $dest -UseBasicParsing
    }
    Ok $dest
}
Set-VarUsuario "HADOOP_HOME" $HadoopHome
Agregar-PathUsuario "$HadoopHome\bin"

# Carpeta temporal de Spark en un disco con espacio y sin espacios en la ruta
New-Item -ItemType Directory -Force -Path "$Base\tmp" | Out-Null
Set-VarUsuario "SPARK_LOCAL_DIRS" "$Base\tmp"

# ---------------------------------------------------------------------------
Paso "4. Entorno virtual de Python $PythonVersion"
$venv = "$Proyecto\.venv"
if (-not (Test-Path "$venv\Scripts\python.exe")) {
    if (-not (Get-Command py -ErrorAction SilentlyContinue)) {
        throw "No se encontró el lanzador 'py'. Instale Python $PythonVersion desde python.org (README, paso 3)."
    }
    $ErrorActionPreference = "Continue"
    & py -$PythonVersion --version 2>&1 | Out-Null
    $codigo = $LASTEXITCODE
    $ErrorActionPreference = "Stop"
    if ($codigo -ne 0) { throw "No se encontró Python $PythonVersion. Instálelo desde python.org (README, paso 3)." }
    & py -$PythonVersion -m venv $venv
}
& "$venv\Scripts\python.exe" -m pip install --upgrade pip --quiet
& "$venv\Scripts\python.exe" -m pip install -r "$Proyecto\requirements.txt"
Ok "Dependencias instaladas en $venv"

# PySpark debe lanzar los workers con ESTE python (evita 'Python worker failed to connect back')
Set-VarUsuario "PYSPARK_PYTHON" "$venv\Scripts\python.exe"
Set-VarUsuario "PYSPARK_DRIVER_PYTHON" "$venv\Scripts\python.exe"

# ---------------------------------------------------------------------------
Paso "5. Verificación"
& "$venv\Scripts\python.exe" "$Proyecto\scripts\verificar_entorno.py"
Write-Host "`nListo. CIERRE esta terminal y abra una nueva antes de seguir." -ForegroundColor Cyan
