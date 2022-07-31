#!/usr/bin/env python
# coding: utf-8

# In[71]:


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from google.cloud import bigquery


# In[72]:


spark = SparkSession.builder.getOrCreate()


# In[73]:


spark


# In[74]:


schema_name = "inner-strategy-351517.dwh." # Bigquery # "maximal-beach-351616.dwh." 
storage_name = "gs://ultima-clase-teorica" # Bucket #"gs://etl-dmc-ultima-clase-erps"


# In[75]:


# 4.1 Estructura del dataframe.
df_schema = StructType([
StructField("BANCO", StringType(),True),
StructField("CARTERA", StringType(),True),
StructField("PARTICIPACION", StringType(),True),
StructField("RANKING", StringType(),True),
StructField("PERIODO", StringType(),True),
StructField("TIPO_ENTIDAD", StringType(),True),

])


# In[76]:


# 4.2 Definimos ruta del archivo
#Archivo en Cloud Storage - Google Cloud Platform
ruta_colocaciones_raw = storage_name+"/datalake/WORKLOAD/SAP/colocaciones.csv"


# In[77]:


#Leer el archivo de origen
df_colocaciones = spark.read.format("CSV").option("header","true").option("delimiter","|").schema(df_schema).load(ruta_colocaciones_raw)


# In[78]:


#4.4 Mostramos la estructura del dataframe.
df_colocaciones.printSchema()


# In[79]:


# 4.5 Mostraremos todos los datos del dataframe.
df_colocaciones.show(5)


# In[80]:


#Archivo en Cloud Storage - Google Cloud Platform
ruta_colocaciones_landing = storage_name+"/datalake/LANDING/SAP/colocaciones"

df_colocaciones.write.mode("overwrite").format("parquet").save(ruta_colocaciones_landing)


# In[81]:


print("=============================================")
print("Termino procesamiento de Colocaciones Landing")
print("=============================================")
print(" ")
print("============================================")
print("Inicio procesamiento de Depositos Landing")
print("============================================")


# In[82]:


# 4.2 Definimos ruta del archivo
#Archivo en Cloud Storage - Google Cloud Platform
ruta_depositos_raw = storage_name+"/datalake/WORKLOAD/SAP/depositos.csv"


# In[83]:


#Leer el archivo de origen
df_depositos = spark.read.format("CSV").option("header","true").option("delimiter","|").schema(df_schema).load(ruta_depositos_raw)


# In[84]:


#4.4 Mostramos la estructura del dataframe.
df_depositos.printSchema()


# In[85]:


df_depositos.show(5)


# In[86]:


#Archivo en Cloud Storage - Google Cloud Platform
ruta_depositos_landing = storage_name+"/datalake/LANDING/SAP/depositos"

df_depositos.write.mode("overwrite").format("parquet").save(ruta_depositos_landing)


# In[87]:


print("=============================================")
print("Termino procesamiento de Depositos Landing")
print("=============================================")


# In[88]:


print("=============================================")
print("Inicio Poblamiento Curated")
print("=============================================")


# In[89]:


df_colocaciones_landing = spark.read.format("parquet").option("header","true").load(ruta_colocaciones_landing)


# In[90]:


df_colocaciones_landing.show(10)


# In[91]:


df_colocaciones_landing.printSchema()


# In[92]:


df_colocaciones_procesado = df_colocaciones_landing.withColumn('CARTERA', regexp_replace('CARTERA', '- - -', '0'))
df_colocaciones_procesado = df_colocaciones_procesado.withColumn('PARTICIPACION', regexp_replace('PARTICIPACION', '- - -', '0'))
df_colocaciones_procesado = df_colocaciones_procesado.withColumn('RANKING', regexp_replace('RANKING', '- - -', '0'))
df_colocaciones_procesado.show(5)


# In[93]:


#Archivo en Cloud Storage - Google Cloud Platform
ruta_colocaciones_curated = storage_name+"/datalake/CURATED/SAP/colocaciones"


# In[94]:


df_colocaciones_procesado.write.mode("overwrite").format("parquet").save(ruta_colocaciones_curated)


# In[95]:


df_depositos_landing = spark.read.format("parquet").option("header","true").load(ruta_depositos_landing)


# In[96]:


df_depositos_landing.show(10)


# In[97]:


df_depositos_landing.printSchema()


# In[98]:


df_depositos_procesado = df_depositos_landing.withColumn('CARTERA', regexp_replace('CARTERA', '- - -', '0'))
df_depositos_procesado = df_depositos_procesado.withColumn('PARTICIPACION', regexp_replace('PARTICIPACION', '- - -', '0'))
df_depositos_procesado = df_depositos_procesado.withColumn('RANKING', regexp_replace('RANKING', '- - -', '0'))
df_depositos_procesado.show(5)


# In[99]:


#Archivo en Cloud Storage - Google Cloud Platform
ruta_depositos_curated = storage_name+"/datalake/CURATED/SAP/depositos"


# In[100]:


df_depositos_procesado.write.mode("overwrite").format("parquet").save(ruta_depositos_curated)


# In[101]:


print("=============================================")
print("Fin Poblamiento Curated")
print("=============================================")


# In[102]:


df_colocaciones_curated = spark.read.format("parquet").option("header","true").load(ruta_colocaciones_curated)

df_depositos_curated = spark.read.format("parquet").option("header","true").load(ruta_depositos_curated)


# In[103]:


df_colocaciones_curated.show(5)
df_depositos_curated.show(5)


# In[104]:


df_colocaciones_curated.createOrReplaceTempView("tb_colocaciones")
df_depositos_curated.createOrReplaceTempView("tb_depositos")

df_union = spark.sql("Select banco, cast(cartera as decimal(10,2)) monto,                     cast(participacion as decimal(10,4)) participacion,                      cast(ranking as decimal(10,2)) ranking, periodo, 'colocaciones' as tipoproducto                       from tb_colocaciones                     union                      Select banco, cast(cartera as decimal(10,2)) monto,                     cast(participacion as decimal(10,4)) participacion,                      cast(ranking as decimal(10,2)) ranking, periodo, 'depositos' as tipoproducto                       from tb_depositos")
df_union.show(5)


# In[105]:


df_bancos = spark.sql("Select distinct c.banco, c.tipo_entidad                        from (SELECT * FROM tb_colocaciones union SELECT * FROM tb_depositos) as c")
df_bancos.show(5)


# In[106]:


df_periodo = spark.sql("Select distinct c.periodo from                       (SELECT * FROM tb_colocaciones union SELECT * FROM tb_depositos) as c")
df_periodo.show(5)


# In[107]:


ruta_functional_captaciones_depositos =  storage_name+"/datalake/FUNCTIONAL/SAP/captaciones_depositos"

df_union.write.mode("overwrite").format("parquet").save(ruta_functional_captaciones_depositos)


# In[108]:


ruta_functional_bancos = storage_name+"/datalake/FUNCTIONAL/SAP/bancos/"

df_bancos.write.mode("overwrite").format("parquet").save(ruta_functional_bancos)


# In[109]:


ruta_functional_periodo = storage_name+"/datalake/FUNCTIONAL/SAP/periodo/"

df_periodo.write.mode("overwrite").format("parquet").save(ruta_functional_periodo)


# In[110]:


df_colocaciones_depositos = spark.read.format("parquet").option("header","true").load(ruta_functional_captaciones_depositos)
df_colocaciones_depositos.show(10)


# In[111]:


df_bancos = spark.read.format("parquet").option("header","true").load(ruta_functional_bancos)
df_bancos.show(10)


# In[112]:


df_periodo = spark.read.format("parquet").option("header","true").load(ruta_functional_periodo)
df_periodo.show(10)


# In[113]:


print("=============================================")
print("Fin Poblamiento Functional")
print("=============================================")


# In[114]:


print("=============================================")
print("Inicio Big Query")
print("=============================================")


# # BigData

# In[115]:


# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the table to create.
#table_id = "your-project.your_dataset.your_table_name"
table_id = schema_name+"tbl_bancos"

schema = [
    bigquery.SchemaField("banco", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("tipo_entidad", "STRING", mode="REQUIRED"),
]

table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
)


# In[116]:


# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the table to create.
#table_id = "your-project.your_dataset.your_table_name"
table_id = schema_name+"tbl_periodo"

schema = [
    bigquery.SchemaField("periodo", "STRING", mode="REQUIRED"),
]

table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
)


# In[117]:


# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the table to create.
#table_id = "your-project.your_dataset.your_table_name"
table_id = schema_name+"tbl_colocaciones_depositos"

schema = [
    bigquery.SchemaField("banco", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("monto", "NUMERIC", mode="REQUIRED"),
    bigquery.SchemaField("participacion", "NUMERIC", mode="REQUIRED"),
    bigquery.SchemaField("ranking", "NUMERIC", mode="REQUIRED"),
    bigquery.SchemaField("periodo", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("tipoproducto", "STRING", mode="REQUIRED"),
]

table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
)


# In[118]:


client = bigquery.Client()

# TODO: MODIFICAR EL NOMBRE DE TABLA DE BIGQUERY
table_id = schema_name+"tbl_bancos"

job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                                    source_format=bigquery.SourceFormat.PARQUET,
                                   )
uri = storage_name+"/datalake/FUNCTIONAL/SAP/bancos/*.parquet"

load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  

load_job.result()

destination_table = client.get_table(table_id)
print("Loaded {} rows.".format(destination_table.num_rows))


# In[119]:


table_id = schema_name+"tbl_periodo"

uri = storage_name+"/datalake/FUNCTIONAL/SAP/periodo/*.parquet"

load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  

load_job.result()

destination_table = client.get_table(table_id)
print("Loaded {} rows.".format(destination_table.num_rows))


# In[120]:


table_id = schema_name+"tbl_colocaciones_depositos"

uri = storage_name+"/datalake/FUNCTIONAL/SAP/captaciones_depositos/*.parquet"

load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  

load_job.result()

destination_table = client.get_table(table_id)
print("Loaded {} rows.".format(destination_table.num_rows))


# In[121]:


print("=============================================")
print("Termino Poblamiento Big Query")
print("=============================================")


# In[ ]:




