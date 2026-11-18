@echo off
title PySpark 100%% - Proyecto Big Data Nota 3
cd /d C:\Users\samue\Downloads\proyecto-bigdata-nota3\proyecto_bigdata
set JAVA_HOME=C:\Java\jdk-17
set SPARK_HOME=C:\bigdata\spark-3.5.9-bin-hadoop3
set HADOOP_HOME=C:\bigdata\hadoop
set SPARK_LOCAL_DIRS=C:\bigdata\tmp
set SPARK_LOCAL_IP=127.0.0.1
set PYSPARK_PYTHON=C:\Users\samue\Downloads\proyecto-bigdata-nota3\proyecto_bigdata\.venv\Scripts\python.exe
set PYSPARK_DRIVER_PYTHON=C:\Users\samue\Downloads\proyecto-bigdata-nota3\proyecto_bigdata\.venv\Scripts\python.exe
set PATH=C:\Java\jdk-17\bin;C:\bigdata\spark-3.5.9-bin-hadoop3\bin;C:\bigdata\hadoop\bin;%PATH%
set PYTHONUTF8=1
call .venv\Scripts\activate.bat
python version_spark\main.py --muestra 100 --guardar-base --explain --esperar
pause
