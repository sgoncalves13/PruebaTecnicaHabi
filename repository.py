from dagster import job
from ops import sheets_to_dfs
from ops import dfs_to_bq
from dagster import Definitions
from dagster import job
from ops import pedidos_ultimos6meses_sql, totalventas_categoria_producto_sql, clientes_compran_tofu_sql, transportistas_mas_entregas_beverages_sql


@job
def ingest_job():
    dfs_to_bq(sheets_to_dfs())

@job
def transform_job():
    paso1 = pedidos_ultimos6meses_sql()
    paso2 = totalventas_categoria_producto_sql(paso1)
    paso3 = clientes_compran_tofu_sql(paso2)
    transportistas_mas_entregas_beverages_sql(paso3)



defs = Definitions(jobs=[ingest_job, transform_job])
