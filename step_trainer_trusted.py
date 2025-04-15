from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import col

# Setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Load from catalog
step_trainer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi", table_name="step_trainer_landing"
)

customer_curated = glueContext.create_dynamic_frame.from_catalog(
    database="stedi", table_name="customer_curated"
)

# Convert to DataFrame
df_step = step_trainer_landing.toDF()
df_customer = customer_curated.toDF()

# Clean join on serialnumber
df_joined = df_step.join(df_customer, df_step["serialnumber"] == df_customer["serialnumber"], "inner")

# Convert back to DynamicFrame
step_trainer_curated = DynamicFrame.fromDF(df_joined, glueContext, "step_trainer_curated")

# Write curated step trainer to S3
glueContext.write_dynamic_frame.from_options(
    frame=step_trainer_curated,
    connection_type="s3",
    connection_options={"path": "s3://chasecolestedi/step_trainer/trusted/"},
    format="json"
)