import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Glue boilerplate
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Load trusted customer data
customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted"
)

# Load trusted accelerometer data
accelerometer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted"
)

# Load curated step trainer data
step_trainer_curated = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_curated"
)

# Join accelerometer with customer on user = email
accel_customer_joined = Join.apply(
    frame1=accelerometer_trusted,
    frame2=customer_trusted,
    keys1=["user"],
    keys2=["email"]
)


# Join with step trainer on timestamp = sensorreadingtime
final_join = Join.apply(
    frame1=step_trainer_curated,
    frame2=accel_customer_joined,
    keys1=["sensorReadingTime"],
    keys2=["timestamp"]
)

print("Final join count:", final_join.count())
customer_trusted.printSchema()
accelerometer_trusted.printSchema()
step_trainer_curated.printSchema()

# Drop unnecessary fields
cleaned = final_join.drop_fields([
    "user", "email", "timestamp", "sensorReadingTime", 
    "serialnumber", "distancefromobject"
])

# Write output to S3 in JSON format
glueContext.write_dynamic_frame.from_options(
    frame=cleaned,
    connection_type="s3",
    connection_options={"path": "s3://chasecolestedi/ml/output/"},
    format="json"
)

job.commit()