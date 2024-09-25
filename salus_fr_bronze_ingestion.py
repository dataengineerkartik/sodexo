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

input_dataset = dbutils.widgets.get("dataset")
dpr_path = dbutils.widgets.get("dpr_path")
ingestion_dates = dbutils.widgets.get("ingestion_dates").lower()
azure_data_factory_id = dbutils.widgets.get("azure_data_factory_id")

if not input_dataset:
    raise Exception("Missing 'dataset' value in parameters")
if not dpr_path:
    raise Exception("Missing 'dpr_path' value in parameters")
if not ingestion_dates:
    raise Exception("Missing 'ingestion_date' value in parameters")
if not azure_data_factory_id:
    raise Exception("Missing 'azure_data_factory_id' value in parameters")

# COMMAND ----------

staging_storage_account = dbutils.secrets.get("data-platform-secrets", "staging-storage-account")
staging_client_id = dbutils.secrets.get("data-platform-secrets", "adls-staging-client-id")
staging_tenant_id = dbutils.secrets.get("data-platform-secrets", "staging-tenant-id")
staging_client_secret = dbutils.secrets.get("data-platform-secrets", "adls-staging-client-secret")

bronze_storage_account = dbutils.secrets.get("data-platform-secrets", "bronze-storage-account")
bronze_client_id = dbutils.secrets.get("data-platform-secrets", "adls-bronze-client-id")
bronze_tenant_id = dbutils.secrets.get("data-platform-secrets", "bronze-tenant-id")
bronze_client_secret = dbutils.secrets.get("data-platform-secrets", "adls-bronze-client-secret")

dremio_url = dbutils.secrets.get("data-platform-secrets", "dremio-url")
dremio_user = dbutils.secrets.get("data-platform-secrets", "dremio-user")
dremio_secret = dbutils.secrets.get("data-platform-secrets", "dremio-password")

event_hub_endpoint = dbutils.secrets.get("data-platform-secrets", "data-observability-events-hub-url")
event_hub_shared_access_key_name = dbutils.secrets.get("data-platform-secrets", "data-observability-events-hub-share-access-key-name")
event_hub_shared_access_key = dbutils.secrets.get("data-platform-secrets", "data-observability-events-hub-share-access-key")
event_hub_entity_path = dbutils.secrets.get("data-platform-secrets", "data-observability-event-hub")
event_hub_name = dbutils.secrets.get("data-platform-secrets", "data-observability-event-hub")


data_observability_events_hub_url = dbutils.secrets.get("data-platform-secrets","data-observability-events-hub-url")
data_observability_events_hub_share_access_key_name = dbutils.secrets.get("data-platform-secrets","data-observability-events-hub-share-access-key-name")
data_observability_events_hub_share_access_key = dbutils.secrets.get("data-platform-secrets","data-observability-events-hub-share-access-key")
data_observability_event_hub_name = dbutils.secrets.get("data-platform-secrets","data-observability-event-hub")

# COMMAND ----------

log4j_logger = spark._jvm.org.apache.log4j
logger = log4j_logger.LogManager.getLogger("salus_glb_bronze_ingestion")

# COMMAND ----------

storage_account_read = dbutils.secrets.get("data-platform-secrets","staging-storage-account")
storage_account_write = dbutils.secrets.get("data-platform-secrets","bronze-storage-account")
staging_client_id = dbutils.secrets.get("data-platform-secrets","adls-staging-client-id")
staging_client_secret = dbutils.secrets.get("data-platform-secrets","adls-staging-client-secret")
staging_tenant_id = dbutils.secrets.get("data-platform-secrets","staging-tenant-id")
bronze_client_id = dbutils.secrets.get("data-platform-secrets","adls-bronze-client-id")
bronze_client_secret = dbutils.secrets.get("data-platform-secrets","adls-bronze-client-secret")
bronze_tenant_id = dbutils.secrets.get("data-platform-secrets","bronze-tenant-id")

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{storage_account_read}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_read}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_read}.dfs.core.windows.net", staging_client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_read}.dfs.core.windows.net", staging_client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_read}.dfs.core.windows.net", f"https://login.microsoftonline.com/{staging_tenant_id}/oauth2/token")

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{storage_account_write}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_write}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_write}.dfs.core.windows.net", bronze_client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_write}.dfs.core.windows.net", bronze_client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_write}.dfs.core.windows.net", f"https://login.microsoftonline.com/{bronze_tenant_id}/oauth2/token")

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame, Column
import pyspark.sql.functions as F
from delta import DeltaTable
from functools import reduce
import pyspark.sql.types as SparkTypes
import re
from observability import DataObs
from dataops.context import Zone, Kind, ExitCode

dpr_containter = "project"
staging_container = "root"
bronze_container = "bronze"
bronze_root_folder = "history"
dremio_folder_name = "oss_glb_salus_glb"

def fileExists(path : str) -> bool : 
    try :
        files = dbutils.fs.ls(path)
        return len(files) > 0
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            logger.warn("No ingestion found for path : " + path)
            return False
        else:
            raise e


#get connection to adls
def createADLSConnection(storage_account: str, client_id: str, ingestion_secret: str, tenant_id: str):
    spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", ingestion_secret)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token") 

def getPreviousDates(ingestion_date : str, starting_from : str = None) -> set :
    ingestion_date_list = list(filter(lambda x : starting_from == None or x >= starting_from , ingestion_date.split(",")))
    ingestion_date_initial = [datetime.strptime(date, '%Y%m%d') for date in ingestion_date_list]
    previous_dates = [(date - timedelta(days=1)).strftime('%Y%m%d') for date in ingestion_date_initial]
    return set(ingestion_date_list + previous_dates)

def getReadingPath(root_path : str, dataset : str, ingestion_date : str, starting_from : str = None) -> list  : 
    if ingestion_date == "all" :
        logger.info("Read all data from dataset " + dataset)
        folder_list = list(filter(lambda x : starting_from == None or x.name >= starting_from ,  dbutils.fs.ls(f"{root_path}/{dataset}")))
        logger.info(f"After filtering -> folder_list {folder_list}")
        return list(filter(lambda x : fileExists(x), [file_info.path for file_info in folder_list]))
    elif ingestion_date == "" :
        raise Exception("Empty input for ingestion_date")
    else :
        logger.info(f"Read from dataset with ingestions_dates {ingestion_date}")
        ingestion_date_list_unique = getPreviousDates(ingestion_date, starting_from)
        file_paths = [f'{root_path}/{dataset}/{date}' for date in ingestion_date_list_unique]
        logger.info(f"After filtering -> ingestions_dates {ingestion_date}")
        return list(filter(lambda x : fileExists(x), file_paths))

def add_technical_colums(input_df : DataFrame, filepaths : list) -> DataFrame :
    filepath_column = F.input_file_name().alias("source_path")
    ingestion_column = F.to_date(F.element_at(F.reverse(F.split(F.input_file_name(),"/")),2), "yyyyMMdd").alias("ingestion_date")
    timestamp_column = F.current_timestamp().alias("processing_time")
    select_columns = [F.col(column) for column in input_df.columns] + [filepath_column, ingestion_column, timestamp_column]
    return input_df.select(select_columns)

def select_columns(dataframe : DataFrame, column_names : list) -> DataFrame :
    dataframe_columns = dataframe.columns
    column_selection = [F.col(column_name) if column_name in dataframe_columns else F.lit(None).alias(column_name) for column_name in column_names]
    return dataframe.select(column_selection)

def flatten_json_datas(dataframe : DataFrame, field_to_flatten : str) -> DataFrame :
    explode_dataframe = dataframe.select(F.explode(F.col(field_to_flatten)).alias(field_to_flatten))
    return explode_dataframe.select([F.col(f"{field_to_flatten}.*") if field_to_flatten == field else field for field in explode_dataframe.columns])

def convert_json_fields_to_string(dataframe : DataFrame) -> DataFrame : 
    fields = dataframe.schema.fields
    select_fields = [F.to_json(field.name).alias(field.name) if isinstance(field.dataType,SparkTypes.StructType) 
                     or (isinstance(field.dataType,SparkTypes.ArrayType)) else F.col(field.name)  for field in dataframe.schema.fields]
    return dataframe.select(select_fields)

def get_bronze_dataframes(filepaths : list, field_to_flatten : str) -> DataFrame :
    df = spark.read.option("multiline","true").option("columnNameOfCorruptRecord", "corrupt_record").option("primitivesAsString", True).option("inferSchema","false").json(filepaths)
    if not ("Data" in df.columns): # In case we receive empty files a dataframe with one empty row is created.
        raise EmptyInputException(f"There are not any data for ingestion date")
    return df.transform(lambda x : flatten_json_datas(x, field_to_flatten)).transform(convert_json_fields_to_string).transform(lambda x : add_technical_colums(x, filepaths))
    

def ingest_to_bronze_history(sparkSession : SparkSession, dataset : str, ingestion_dates : str, storage_account_staging : str, storage_account_bronze : str, field_to_flatten : str, starting_from : str = None) :
    root_path = f"abfss://root@{storage_account_staging}.dfs.core.windows.net/staging/corporatedata/IST/GLB/ist-glb-salus_GLB"
    bronze_dataset_path = f"abfss://bronze@{storage_account_bronze}.dfs.core.windows.net/history/corporatedata/ist/glb/salus_glb/{dataset}"
    filepaths = getReadingPath(root_path, dataset, ingestion_dates, starting_from)
    #print(filepaths)
    #if not filepaths:
    #    raise Exception(f"No file to read for ingestion dates : {ingestion_dates}")
    #logger.info(f"Filepaths : f{filepaths}")
    if not filepaths :
        raise EmptyInputException(f"There are not any data for ingestion date : {ingestion_dates}")
    dataobs.send_processing_event()
    logger.info(f"Filepaths : f{filepaths}")
    bronze_df = get_bronze_dataframes(filepaths, field_to_flatten)
    if dataset == 'impacts': # This should be removed when fixed
        bronze_df = bronze_df.select([F.col(c).cast("string") for c in bronze_df.columns])

    if ingestion_dates != "all" : 
        curr_prev_ingestion_dates = getPreviousDates(ingestion_dates, starting_from)
        curr_prev_ingestion_dates = str(curr_prev_ingestion_dates)[1:-1]
        bronze_df.coalesce(1).write \
                .mode("overwrite")\
                .option("mergeSchema", "true")\
                .option("replaceWhere", f"date_format(ingestion_date, 'yyyyMMdd') IN ({curr_prev_ingestion_dates})")\
                .partitionBy("ingestion_date")\
                .save(bronze_dataset_path)
                
    else :
        bronze_df.coalesce(1).write \
                .mode("overwrite")\
                .option("mergeSchema", "true")\
                .partitionBy("ingestion_date")\
                .save(bronze_dataset_path)
    
    return bronze_df

def refreshDremioView(dremio_url: str, dremio_user: str, dremio_secret: str, dataset: str, config: ConfigReader, storage_account: str, root_folder: str, container: str, dremio_folder_name: str):
    #change dremio location for master data in silver
    if root_folder == "master" and container == "silver":
        dremio_space = root_folder
        dremio_path = dremio_folder_name
    else:
        dremio_space = container
        dremio_path = f"{root_folder}/{dremio_folder_name}"

    dremio_builder = DremioEnvironmentBuilder(dremio_url, spark, dremio_space, storage_account, dremio_path, dataset, f"{container}/{root_folder}/corporatedata/ist/glb/salus_glb/{dataset}", dremio_user, dremio_secret)
    dremio_builder.createOrRefreshView()

class EmptyInputException(Exception):
    pass


# COMMAND ----------

#get adls connection for source data
createADLSConnection(staging_storage_account, staging_client_id, staging_client_secret, staging_tenant_id)

#get adls connection for destination data
createADLSConnection(bronze_storage_account, bronze_client_id, bronze_client_secret, bronze_tenant_id)

#Config params
config_params = {
    "storage_account" : bronze_storage_account,
    "client_id" : bronze_client_id,
    "client_secret" : bronze_client_secret,
    "tenant_id" : bronze_tenant_id,
    "freshness_column" : "date_int"
}

# get DPR config
config = ConfigReader("project", f"data-observability/data-product-registry/french_food/glb/salus_glb/datasets/{input_dataset.lower()}.json",config_params)

eventhub_url = "Endpoint={0};SharedAccessKeyName={1};SharedAccessKey={2}".format(data_observability_events_hub_url,data_observability_events_hub_share_access_key_name,data_observability_events_hub_share_access_key)

# Initialize the DataObs object
dataobs = DataObs(
  publisher_name="data_core_services",
  project_name="oss.glb.glb.salus",
  dataset_name=input_dataset,
  landing_zone=Zone.BRONZE,
  takeoff_zone=Zone.STAGING,
  kind=Kind.TABLE,
  job_id=f"oss.glb.glb.salus:staging_to_bronze:{azure_data_factory_id}",
  event_hub_connection_string=eventhub_url,
  event_hub_name=data_observability_event_hub_name
)

# Send a processing started event
dataobs.send_processing_event()

# COMMAND ----------


try :
    print(input_dataset)
    df = ingest_to_bronze_history(spark,input_dataset, ingestion_dates, storage_account_read, storage_account_write,"Data")
    refreshDremioView(dremio_url, dremio_user, dremio_secret, input_dataset, config, bronze_storage_account, bronze_root_folder, bronze_container, dremio_folder_name)
    dataobs.send_finished_event(exit_code=ExitCode.SUCCESS)
except EmptyInputException as ex:
    dataobs.send_finished_event(exit_code=ExitCode.NO_NEW_DATA)
    dbutils.notebook.exit(ex)
except Exception as ex:
    dataobs.send_finished_event(exit_code=ExitCode.ERROR)
    raise ex

# COMMAND ----------

dataobs.push(df)
