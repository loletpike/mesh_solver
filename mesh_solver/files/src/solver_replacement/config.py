
from datetime import date, timedelta, datetime, timezone
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, BooleanType, MapType
from pyspark.dbutils import DBUtils # type: ignore

import logging

# SPARK
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.databricks.execution.timeout", "18000")



# LOGGING
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# CREATE DATE VARIABLES
yesterday = date.today() - timedelta(days=1)

# read_day = yesterday.day
# read_month = yesterday.month
# read_year = yesterday.year

# get dt of 3 hours ago utc
now_utc = datetime.now(timezone.utc)
read_dt = now_utc - timedelta(hours=3)

read_year = read_dt.year
read_month = read_dt.month
read_day = read_dt.day
read_hour = read_dt.hour




data_retention_days = 7

targetschema='mesh_wifi'
sourcetab='hive_metastore.dct.mesh_location_optimize_solver_response'
cctab="mesh_channel_change_all"
targetcoretab='mesh_solver_replacement_core_summary'
targetselectiontab='mesh_solver_replacement_selections'
targetdshbrdfilttab='mesh_solver_replacement_kpi_filters'
backfilldays=1
###db_name='dap_dev'
sourcepath='s3a://mesh-prod-w2-location-optimize-response/000-prod-chi/shard=000-prod-chi'