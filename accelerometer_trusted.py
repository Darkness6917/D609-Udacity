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

# Load customer_trusted
customer_df = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted"
)

# Load accelerometer_landing
accelerometer_df = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing"
)

# Join on email → user
joined_df = Join.apply(
    frame1=accelerometer_df,
    frame2=customer_df,
    keys1=["user"],
    keys2=["email"]
)

# Drop customer fields (keep only accelerometer data)
cleaned_df = joined_df.drop_fields(["serialnumber", "birthdate", "sharewithfriendsasofdate", "sharewithpublicasofdate", "sharewithresearchasofdate", "email", "phone", "registrationdate", "lastupdatedate"])

# Write to S3
glueContext.write_dynamic_frame.from_options(
    frame=cleaned_df,
    connection_type="s3",
    connection_options={"path": "s3://chasecolestedi/accelerometer/trusted/"},
    format="json"
)

job.commit()
