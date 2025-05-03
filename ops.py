from dagster import op
import gspread, pandas as pd
from google.cloud import bigquery

SHEET_URL = "https://docs.google.com/spreadsheets/d/1y7GQejKLerQgmkSFw0YBd7ih74kBGSIwaccMA5JgGqA/edit?gid=1022176486#gid=1022176486"
@op
def sheets_to_dfs(context):
    gc = gspread.service_account(
        filename=r"papyrus-technical-test-c1c9d41dad00.json"
    )
    sh = gc.open_by_url(SHEET_URL)
    dfs = {}
    for w in sh.worksheets():
        df = pd.DataFrame(w.get_all_records())
        dfs[w.title] = df
        context.log.info(f"Cargada hoja {w.title} -> {df.shape}")
    return dfs



DATASET = "prueba_tecnica_samuelgoncalves"
client = bigquery.Client()

@op
def dfs_to_bq(context, dfs):
    client.create_dataset(DATASET, exists_ok=True)
    for name, df in dfs.items():

        obj_cols = df.select_dtypes(include=["object"]).columns
        df[obj_cols] = df[obj_cols].astype("string[pyarrow]")

        fecha_cols = [col for col in df.columns if "fecha" in col.lower()]

        id_cols = [col for col in df.columns if "id" == col[0:2].lower() and col != "idCliente"] 

        # Limpieza de datos
        
        for col in fecha_cols:
             context.log.info(f"Corrigiendo columna '{col}' en hoja: {name}")
             df[col] = df[col].astype(str).str.replace("/", "-", regex=False)
             df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

        for col in id_cols:
              context.log.info(f"Corrigiendo columna '{col}' en hoja: {name}")
              df[col] = df[col].astype(str).str.replace(" ", "", regex=False)
              df[col] = pd.to_numeric(df[col], errors="coerce")

        ###

        table_id = f"{client.project}.{DATASET}.{name}_samuelgoncalves"
        job = client.load_table_from_dataframe(df, table_id, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
        job.result()
        context.log.info(f"{table_id} cargada ({df.shape[0]} filas)")

# Pregunta 1

Q1_SQL = """
SELECT 
  EXTRACT(YEAR FROM DATE(FechaPedido)) AS anio,
  EXTRACT(MONTH FROM DATE(FechaPedido)) AS mes,
  COUNT(*) AS total_pedidos
FROM `papyrus-technical-test.prueba_tecnica_samuelgoncalves.pedidos_samuelgoncalves`
WHERE DATE(FechaPedido) >= DATE_SUB(DATE({date}), INTERVAL 6 MONTH)
  AND DATE(FechaPedido) < DATE({date})
GROUP BY anio, mes
ORDER BY anio, mes;
"""

@op
def pedidos_ultimos6meses_sql(context):
    fecha_referencia = "'2015-01-01'"
    #fecha_actual = "CURRENT_DATE()"
    df = client.query(Q1_SQL.format(date=fecha_referencia)).to_dataframe()
    context.log.info(f"{df}")
    return df


# Pregunta 2

Q2_SQL = """
SELECT 
c.nombreCategoria,
SUM((pd.cantidad * pd.precioUnitario) - pd.descuento) AS total_ventas,
COUNT(*) AS numero_pedidos
FROM `papyrus-technical-test.prueba_tecnica_samuelgoncalves.pedidos_detalles_samuelgoncalves` AS pd
JOIN `papyrus-technical-test.prueba_tecnica_samuelgoncalves.productos_samuelgoncalves` AS p
ON pd.idProducto = p.idProducto
JOIN `papyrus-technical-test.prueba_tecnica_samuelgoncalves.categorias_samuelgoncalves` AS c
ON p.idCategoria = c.idCategoria
GROUP BY c.nombreCategoria
ORDER BY total_ventas DESC;
"""

@op
def totalventas_categoria_producto_sql(context, prev):
    df = client.query(Q2_SQL).to_dataframe()
    context.log.info(f"{df}")
    return df

# Pregunta 3

Q3_SQL = """
SELECT c.nombreContacto, c.nombreEmpresa, COUNT(*) AS num_pedidos_tofu FROM `papyrus-technical-test.prueba_tecnica_samuelgoncalves.pedidos_samuelgoncalves` AS p
JOIN `papyrus-technical-test.prueba_tecnica_samuelgoncalves.pedidos_detalles_samuelgoncalves` AS pd
ON p.idPedido = pd.idPedido
JOIN `papyrus-technical-test.prueba_tecnica_samuelgoncalves.productos_samuelgoncalves` AS pr
ON pd.idProducto = pr.idProducto
JOIN `papyrus-technical-test.prueba_tecnica_samuelgoncalves.clientes_samuelgoncalves` as c
ON p.idCliente = c.idCliente
WHERE LOWER(pr.nombreProducto) LIKE '%tofu%'
GROUP BY nombreContacto, nombreEmpresa
ORDER BY num_pedidos_tofu DESC
LIMIT 1000;
"""

@op
def clientes_compran_tofu_sql(context, prev):
    df = client.query(Q3_SQL).to_dataframe()
    context.log.info(f"{df}")
    return df

# Pregunta 4

Q4_SQL = """
SELECT t.nombreEmpresa, COUNT(*) AS num_entregas FROM `papyrus-technical-test.prueba_tecnica_samuelgoncalves.pedidos_samuelgoncalves` AS p
JOIN `papyrus-technical-test.prueba_tecnica_samuelgoncalves.pedidos_detalles_samuelgoncalves` AS pd
ON p.idPedido = pd.idPedido
JOIN `papyrus-technical-test.prueba_tecnica_samuelgoncalves.productos_samuelgoncalves` as pr
ON pd.idProducto = pr.idProducto
JOIN `papyrus-technical-test.prueba_tecnica_samuelgoncalves.categorias_samuelgoncalves` as c
ON pr.idCategoria = c.idCategoria
JOIN `papyrus-technical-test.prueba_tecnica_samuelgoncalves.transportistas_samuelgoncalves` as t
ON p.idTransportista = t.idTransportista
WHERE LOWER(c.nombreCategoria) = 'beverages'
GROUP BY t.nombreEmpresa
ORDER BY num_entregas DESC
LIMIT 1000;
"""

@op
def transportistas_mas_entregas_beverages_sql(context, prev):
    df = client.query(Q4_SQL).to_dataframe()
    context.log.info(f"{df}")
    return df