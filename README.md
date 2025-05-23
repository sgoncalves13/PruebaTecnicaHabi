﻿# PruebaTecnicaHabi

# VM

```
sudo apt update
sudo apt upgrade -y
sudo apt install git -y
```

```
git clone https://github.com/sgoncalves13/PruebaTecnicaHabi.git
```

```
cd PruebaTecnicaHabi
```

```
sudo apt install python3.13-venv
```

```
python3 -m venv dagster-env
```

```
source dagster-env/bin/activate
```

```
pip install -r requirements.txt
```

Subir el archivo con las credenciales a la MV

![1746315452982](image/README/1746315452982.png)

```
export GOOGLE_APPLICATION_CREDENTIALS="/ruta/absoluta/tu-archivo.json"
```

```
dagster dev -f repository.py -h 0.0.0.0 -p 3000
```

NOTA IMPORTANTE: No funciona por un problema de versiones en el que dagster no está soportado para python <3.13


# Local

```
python3 -m venv dagster-env
```

```
.\dagster-env\Scripts\Activate.ps1
```

```
$env:GOOGLE_APPLICATION_CREDENTIALS = "papyrus-technical-test-c1c9d41dad00.json"
```

```
pip install -r requirements.txt
```

```
dagster dev -f repository.py
```

# Ejecución de los jobs

Para la ejecución de los jobs después de iniciar la IU de dagster, correr primero el job `ingest_job `para cargar los datos a BigQuery a partir de Google Sheets, en la carga ya se hace directamente la limpieza de los datos. Después se corre el `transform_job `que corre las consultas SQL que responden las preguntas. Se pueden correr las consultas SQL directamente en BigQuery después de correr el `ingest_job ` y no correr el `transform_job `.
