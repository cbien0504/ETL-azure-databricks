# Databricks notebook source
storage_account_name = ""
container_name = ""
client_id = ""
client_secret = ""
tenant_id = ""


dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point  = "/mnt/test",
    extra_configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": client_id,
        "fs.azure.account.oauth2.client.secret": client_secret,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    }
)


# COMMAND ----------

# MAGIC %fs ls "/mnt/test/raw-data"

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

athletes = spark.read.format("csv").option("header","true").load("dbfs:/mnt/test/raw-data/athletes.csv")
athletes = athletes.withColumn("Discipline", regexp_replace(athletes["Discipline"], r"\n$", ""))
athletes = athletes.dropna()
athletes.show()
print(athletes.count())

# COMMAND ----------

athletes = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/test/raw-data/athletes.csv")
coaches = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/test/raw-data/coaches.csv")
entriesgender = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/test/raw-data/entriesgender.csv")
medals = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/test/raw-data/medals.csv")
teams = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/test/raw-data/teams.csv")

# COMMAND ----------

# Calculate the average number of entries by gender for each discipline
entriesgender = entriesgender.withColumn(
    'Avg_Female', entriesgender['Female'] / entriesgender['Total']
).withColumn(
    'Avg_Male', entriesgender['Male'] / entriesgender['Total']
)
entriesgender.show()

# COMMAND ----------

coaches.show()

# COMMAND ----------


from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType
entriesgender = entriesgender.withColumn("Female",col("Female").cast(IntegerType()))\
    .withColumn("Male",col("Male").cast(IntegerType()))\
    .withColumn("Total",col("Total").cast(IntegerType()))
     

# COMMAND ----------



athletes.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/test/transformed-data/athletes")
     

coaches.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/test/transformed-data/coaches")
entriesgender.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/test/transformed-data/entriesgender")
medals.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/test/transformed-data/medals")
teams.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/test/transformed-data/teams")
     

