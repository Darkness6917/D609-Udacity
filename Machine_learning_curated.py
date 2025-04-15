from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import col
from pyspark.sql.types import LongType

# Setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Load correct sources
step_trainer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi", table_name="step_trainer_trusted"
)

accelerometer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi", table_name="accelerometer_trusted"
)

# Convert to DataFrames
df_step = step_trainer_trusted.toDF()
df_accel = accelerometer_trusted.toDF()

# Cast timestamps
df_step = df_step.withColumn("sensorreadingtime", col("sensorreadingtime").cast(LongType()))
df_accel = df_accel.withColumn("timestamp", col("timestamp").cast(LongType()))

# Strict join: timestamp == sensorreadingtime
df_joined = df_step.join(
    df_accel,
    df_step["sensorreadingtime"] == df_accel["timestamp"],
    "inner"
)

# Convert back to DynamicFrame
ml_curated = DynamicFrame.fromDF(df_joined, glueContext, "ml_curated")

# Write to curated ML path
glueContext.write_dynamic_frame.from_options(
    frame=ml_curated,
    connection_type="s3",
    connection_options={"path": "s3://chasecolestedi/ml/curated/"},
    format="json"
)