import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load customer_trusted data
customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted"
)

# Load step_trainer_landing data
step_trainer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing"
)

# Join the datasets on serialNumber
joined_df = Join.apply(
    frame1=step_trainer_landing,
    frame2=customer_trusted,
    keys1=["serialNumber"],
    keys2=["serialNumber"]
)

# Drop unnecessary fields from the joined dataset
step_trainer_trusted = joined_df.drop_fields([
    "email", "phone", "birthDay", "customerName", "registrationDate"
])

# Write the trusted dataset to S3
glueContext.write_dynamic_frame.from_options(
    frame=step_trainer_trusted,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://chasecolestedi/step_trainer/trusted/",
        "partitionKeys": []
    }
)

job.commit()
