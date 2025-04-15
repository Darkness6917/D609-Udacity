from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql.functions import col
from pyspark.sql import SparkSession

# Init
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Load trusted tables
customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted"
)

accelerometer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted"
)

# Join on email = user
joined = Join.apply(customer_trusted, accelerometer_trusted, 'email', 'user')

# Drop accelerometer fields
customer_only = joined.drop_fields(['user', 'timestamp', 'x', 'y', 'z'])

# Deduplicate by email
df_customers = customer_only.toDF().dropDuplicates(['email'])
customer_curated = DynamicFrame.fromDF(df_customers, glueContext, "customer_curated")

# Write to curated zone
glueContext.write_dynamic_frame.from_options(
    frame=customer_curated,
    connection_type="s3",
    connection_options={"path": "s3://chasecolestedi/customer/curated/"},
    format="json"
)