# Databricks notebook source
# MAGIC %run ../utils/DremioEnvironmentBuilder

# COMMAND ----------

# MAGIC %run ../utils/ConfigReader

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("dataset", "")
dbutils.widgets.text("dpr_path", "data-observability/data-product-registry/french_food/glb/salus_glb/datasets/")
dbutils.widgets.text("ingestion_dates", "") #or date in yyyyMMdd format
dbutils.widgets.text("azure_data_factory_id", "")

dataset = dbutils.widgets.get("dataset")
dpr_path = dbutils.widgets.get("dpr_path")
ingestion_dates = dbutils.widgets.get("ingestion_dates").lower()
azure_data_factory_id = dbutils.widgets.get("azure_data_factory_id")

if not dataset:
    raise Exception("Missing 'dataset' value in parameters")
if not dpr_path:
    raise Exception("Missing 'dpr_path' value in parameters")
if not ingestion_dates:
    raise Exception("Missing 'ingestion_date' value in parameters")
if not azure_data_factory_id:
    raise Exception("Missing 'azure_data_factory_id' value in parameters")

# COMMAND ----------

dremio_url = dbutils.secrets.get("data-platform-secrets", "dremio-url")
dremio_user = dbutils.secrets.get("data-platform-secrets", "dremio-user")
dremio_secret = dbutils.secrets.get("data-platform-secrets", "dremio-password")

data_observability_events_hub_url = dbutils.secrets.get("data-platform-secrets","data-observability-events-hub-url")
data_observability_events_hub_share_access_key_name = dbutils.secrets.get("data-platform-secrets","data-observability-events-hub-share-access-key-name")
data_observability_events_hub_share_access_key = dbutils.secrets.get("data-platform-secrets","data-observability-events-hub-share-access-key")
data_observability_event_hub_name = dbutils.secrets.get("data-platform-secrets","data-observability-event-hub")

# COMMAND ----------

staging_storage_account = dbutils.secrets.get("data-platform-secrets", "staging-storage-account")
staging_client_id = dbutils.secrets.get("data-platform-secrets", "adls-staging-client-id")
staging_tenant_id = dbutils.secrets.get("data-platform-secrets", "staging-tenant-id")
staging_client_secret = dbutils.secrets.get("data-platform-secrets", "adls-staging-client-secret")

storage_account_write = dbutils.secrets.get("data-platform-secrets","silver-storage-account")
silver_storage_account = dbutils.secrets.get("data-platform-secrets", "silver-storage-account")
silver_client_id = dbutils.secrets.get("data-platform-secrets","adls-silver-client-id")
silver_client_secret = dbutils.secrets.get("data-platform-secrets","adls-silver-client-secret")
silver_tenant_id = dbutils.secrets.get("data-platform-secrets","silver-tenant-id")

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{staging_storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{staging_storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{staging_storage_account}.dfs.core.windows.net", staging_client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{staging_storage_account}.dfs.core.windows.net", staging_client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{staging_storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{staging_tenant_id}/oauth2/token")

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{storage_account_write}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_write}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_write}.dfs.core.windows.net", silver_client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_write}.dfs.core.windows.net", silver_client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_write}.dfs.core.windows.net", f"https://login.microsoftonline.com/{silver_tenant_id}/oauth2/token")


# COMMAND ----------

log4j_logger = spark._jvm.org.apache.log4j
logger = log4j_logger.LogManager.getLogger("salus_silver_ingestion")

# COMMAND ----------

from observability import DataObs
from dataops.context import Zone, Kind, ExitCode
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column
from delta import *
from pyspark.sql.window import Window

auto_cast_types = [
        SparkTypes.NullType, SparkTypes.StringType, SparkTypes.BinaryType, SparkTypes.BooleanType,
        SparkTypes.DecimalType, SparkTypes.DoubleType, SparkTypes.FloatType,
        SparkTypes.ByteType, SparkTypes.IntegerType,
        SparkTypes.LongType, SparkTypes.ShortType, SparkTypes.StructType, SparkTypes.ArrayType
    ]

dpr_containter = "project"
staging_container = "root"
silver_container = "silver"
silver_root_folder = "standardized"
dremio_folder_name = "oss_glb_salus_glb"

#add technical columns
def addTechnicalColumns(df : DataFrame) -> DataFrame : 
    df = df.withColumn("processing_time", F.lit(datetime.utcnow()))
    return df

#recursive call form selectOriginFields
def selectOriginField(field : SparkTypes.StructField, listColumns : list) -> Column :
    if isinstance(field.dataType, SparkTypes.StructType) or (isinstance(field.dataType, SparkTypes.ArrayType) and isinstance(field.dataType.elementType, SparkTypes.StructType)) :
        return F.from_json(F.to_json(F.col(field.name)), field.dataType).alias(field.name) if field.name in listColumns else F.lit(None).alias(field.name).cast(field.dataType)
    else :
        selected_field = F.col(field.name) if field.name in listColumns else F.lit(None).alias(field.name).cast(field.dataType)
        return selected_field

#get only fields in product registry
def selectOriginFields(originSchema : SparkTypes.StructType, df : DataFrame) -> DataFrame :
    selectFields = [selectOriginField(field, df.columns)  for field in originSchema.fields] + [F.col("ingestion_date")]
    return df.select(selectFields)

#recursive call form castAndRenameFields
def castAndRenameField(target_field : dict) -> Column :
    struct_field = StructField.fromJson(target_field)
    if target_field.get("expression", None):
        return F.expr(target_field.get("expression", None)).cast(struct_field.dataType).alias(target_field["name"])
    elif any(isinstance(struct_field.dataType, auto_cast_type) for auto_cast_type in auto_cast_types) :
        return F.col(target_field["source_name"]).cast(struct_field.dataType).alias(target_field["name"])
    elif isinstance(struct_field.dataType, SparkTypes.TimestampType): 
        timestamp_definition = target_field["metadata"]["timestamp_definition"]
        source_format = timestamp_definition["source_format"]
        timezone = timestamp_definition["timezone"]
        cast_to_timestamp_field = None
        if source_format["type"] == "string" :
            cast_to_timestamp_field = F.to_timestamp(F.col(target_field["source_name"]))
        else :
            raise Exception(f"Unsupported source_format {source_format} for field {target_field['source_name']} ")
        if timezone["type"] == "field" :
            return F.to_utc_timestamp(cast_to_timestamp_field, F.col(timezone["value"])).alias(target_field["name"])
        elif timezone["type"] == "literal" :
            return F.to_utc_timestamp(cast_to_timestamp_field, F.lit(timezone["value"])).alias(target_field["name"])
        else :
            raise Exception(f"Unsupported timezone {timezone} for field {target_field['source_name']}")
    elif isinstance(struct_field.dataType, SparkTypes.DateType): 
        date_definition = target_field["metadata"]["date_definition"]
        source_format = date_definition["source_format"]
        if source_format["type"] == "string" :
            return F.to_date(F.col(target_field["source_name"]), source_format["date_format"]).alias(target_field["name"])
        else :
            raise Exception(f"Unsupported source_format {source_format} for field {target_field['source_name']} ")
    else :
        raise Exception(f"Unsupported cast to {struct_field.dataType} for field {target_field['source_name']}")

#rename and cast columns based on product registry
def castAndRenameFields(targetFields : dict, df : DataFrame) -> DataFrame :
    select_columns = [castAndRenameField(field) for field in targetFields] + [F.col("ingestion_date")]
    return df.select(select_columns)

#get connection to adls
def createADLSConnection(storage_account: str, client_id: str, ingestion_secret: str, tenant_id: str):
    spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", ingestion_secret)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")


#get last folder in path based on name
def getLatestFile(path: str) -> list:
    files = getFiles(path)
    if files:
        return [max(files)]
    else:
        return files

#get list of files recursively
def getFilesRecursively(path: str) -> set:
    for dir_path in dbutils.fs.ls(path):
        if dir_path.isFile():
            yield dir_path.path
        elif dir_path.isDir() and path != dir_path.path:
            yield from getFilesRecursively(dir_path.path)
            
#get files and handle empty folder            
def getFiles(path: str) -> list:
    try:
        return list(getFilesRecursively(path))
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            return []
        else:
            raise e

def convert_json_fields_to_string(dataframe : DataFrame) -> DataFrame : 
    fields = dataframe.schema.fields
    select_fields = [F.to_json(field.name).alias(field.name) if isinstance(field.dataType,SparkTypes.StructType) 
                     or (isinstance(field.dataType,SparkTypes.ArrayType) and (isinstance(field.dataType.elementType,SparkTypes.StructType))) else F.col(field.name)  for field in dataframe.schema.fields]
    return dataframe.select(select_fields)        

def flatten_json_datas(dataframe : DataFrame, field_to_flatten : str) -> DataFrame :
    explode_dataframe = dataframe.select(F.explode(F.col(field_to_flatten)).alias(field_to_flatten))
    return explode_dataframe.select(F.col(f"{field_to_flatten}.*"))       

#read data
def readData(ingestion_date: str, config: ConfigReader, storage_account: str, container: str) -> DataFrame:
    path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/history/corporatedata/ist/glb/salus_glb/{dataset}"
    return spark.read.option("format", "delta").load(path) 


#write data
def writeData(df: DataFrame, config: ConfigReader, storage_account: str, container: str, root_folder: str, ingestion_date: str):
    path = f"abfss://silver@{storage_account}.dfs.core.windows.net/standardized/corporatedata/ist/glb/glb/ist_glb_salus/{dataset}"
    if df.count() == 0:
        raise EmptyInputException(f"There are not any data for ingestion date : {ingestion_date}")

    #get the latest version of primary key only
    window = Window.partitionBy('id').orderBy(F.col('ingestion_date').desc())
    df = df.withColumn("row_number", F.row_number().over(window))
    df = df.filter(df.row_number == 1).drop("row_number")

    deltaTable = DeltaTable.forPath(spark, path)
    
    if ingestion_date == "all" and deltaTable.history().count() > 1:
        deltaTable.delete()
        

    deltaTable.alias("origin").merge(
    source = df.coalesce(1).alias("updates"),
    condition = F.expr("origin.id = updates.id")
  ).whenMatchedUpdateAll(condition = f"updates.ingestion_date >= origin.ingestion_date").whenNotMatchedInsertAll().execute()      


def refreshDremioView(dremio_url: str, dremio_user: str, dremio_secret: str, dataset: str, config: ConfigReader, storage_account: str, root_folder: str, container: str, dremio_folder_name: str):
    #change dremio location for master data in silver
    if root_folder == "master" and container == "silver":
        dremio_space = root_folder
        dremio_path = dremio_folder_name
    else:
        dremio_space = container
        dremio_path = f"{root_folder}/{dremio_folder_name}"
        
    dremio_builder = DremioEnvironmentBuilder(dremio_url, spark, dremio_space, storage_account, dremio_path, dataset, f"{container}/{root_folder}/corporatedata/ist/glb/glb/ist_glb_salus/{dataset}", dremio_user, dremio_secret)
    dremio_builder.createOrRefreshView()

class EmptyInputException(Exception):
    pass

# COMMAND ----------

configuration_params = {
    "storage_account" : silver_storage_account,
    "client_id" : silver_client_id,
    "client_secret" : silver_client_secret,
    "tenant_id" : silver_tenant_id,
    "freshness_column" : "date_int"
}


#get adls connection for source data
createADLSConnection(staging_storage_account, staging_client_id, staging_client_secret, staging_tenant_id)

#get adls connection for destination data
createADLSConnection(silver_storage_account, silver_client_id, silver_client_secret, silver_tenant_id)

#get DPR config
config = ConfigReader("project", f"data-observability/data-product-registry/french_food/glb/salus_glb/datasets/{dataset.lower()}.json",configuration_params)

eventhub_url = "Endpoint={0};SharedAccessKeyName={1};SharedAccessKey={2}".format(data_observability_events_hub_url,data_observability_events_hub_share_access_key_name,data_observability_events_hub_share_access_key)

# Initialize the DataObs object
dataobs = DataObs(
  publisher_name="data_core_services",
  project_name="oss.glb.glb.salus",
  dataset_name=dataset,
  landing_zone=Zone.SILVER,
  takeoff_zone=Zone.BRONZE,
  kind=Kind.TABLE,
  job_id=f"oss.glb.glb.salus:bronze_to_silver:{azure_data_factory_id}",
  event_hub_connection_string=eventhub_url,
  event_hub_name=data_observability_event_hub_name
)

# Send a processing started event
dataobs.send_processing_event()

# COMMAND ----------

try :
    print(dataset)
    df = readData(ingestion_dates, config, silver_storage_account, "bronze")
    df = selectOriginFields(config.origin_conf, df)
    df = castAndRenameFields(config.fields, df)
    df = addTechnicalColumns(df)
    writeData(df, config, silver_storage_account, silver_container, silver_root_folder, ingestion_dates)   
    refreshDremioView(dremio_url, dremio_user, dremio_secret, dataset, config, silver_storage_account, silver_root_folder, silver_container, dremio_folder_name)
    dataobs.send_finished_event(exit_code=ExitCode.SUCCESS)
except EmptyInputException as ex:
    dataobs.send_finished_event(exit_code=ExitCode.NO_NEW_DATA)
    dbutils.notebook.exit(ex)
except Exception as ex :
    dataobs.send_finished_event(exit_code=ExitCode.ERROR)
    raise ex

# COMMAND ----------

dataobs.push(df, volume=True, freshness_column=configuration_params["freshness_column"], distribution=True)

# COMMAND ----------


