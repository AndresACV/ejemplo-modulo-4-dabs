# Databricks notebook source
from decimal import Decimal

from pyspark.sql.types import DecimalType, IntegerType, StringType, StructField, StructType


catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema")
if not catalog or not schema_name:
    raise ValueError("Los parametros catalog y schema son obligatorios")


def quote_identifier(identifier):
    return f"`{identifier.replace('`', '``')}`"


target_table = ".".join(
    quote_identifier(part) for part in (catalog, schema_name, "clientes")
)
table = spark.table(target_table)
column_names = {field.name.lower() for field in table.schema.fields}

if "provincia" not in column_names:
    spark.sql(f"ALTER TABLE {target_table} ADD COLUMNS (provincia STRING)")

customer_schema = StructType(
    [
        StructField("cliente_id", IntegerType(), nullable=False),
        StructField("nombre", StringType(), nullable=False),
        StructField("email", StringType(), nullable=False),
        StructField("telefono", StringType(), nullable=False),
        StructField("region", StringType(), nullable=False),
        StructField("monto", DecimalType(6, 2), nullable=False),
        StructField("provincia", StringType(), nullable=False),
    ]
)

customers = [
    (-900001, "Ana Ejemplo", "ana.ejemplo@example.com", "000-000-0001", "Central", Decimal("15.25"), "San José"),
    (-900002, "Bruno Muestra", "bruno.muestra@example.com", "000-000-0002", "Central", Decimal("28.50"), "San José"),
    (-900003, "Carla Demo", "carla.demo@example.com", "000-000-0003", "Central", Decimal("42.75"), "San José"),
    (-900004, "Diego Prueba", "diego.prueba@example.com", "000-000-0004", "Central", Decimal("63.00"), "San José"),
    (-900005, "Elena Ficticia", "elena.ficticia@example.com", "000-000-0005", "Central", Decimal("91.10"), "San José"),
]

source_view = "clientes_provincia_dummy_source"
spark.createDataFrame(customers, customer_schema).createOrReplaceTempView(source_view)

spark.sql(
    f"""
    MERGE INTO {target_table} AS target
    USING {source_view} AS source
      ON target.cliente_id = source.cliente_id
    WHEN NOT MATCHED THEN INSERT (
      cliente_id, nombre, email, telefono, region, monto, provincia
    ) VALUES (
      source.cliente_id, source.nombre, source.email, source.telefono,
      source.region, source.monto, source.provincia
    )
    """
)