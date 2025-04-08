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

# Load customer landing
customer_df = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing")

print("🔍 RECORD COUNT BEFORE FILTER:", customer_df.count())
customer_df.show(5)
customer_df.printSchema()
# Filter for customers who opted in
trusted_df = Filter.apply(frame=customer_df, f=lambda x: x["shareWithResearchAsOfDate"] is not None)

# Write to trusted zone
glueContext.write_dynamic_frame.from_options(
    frame=trusted_df,
    connection_type="s3",
    connection_options={"path": "s3://chasecolestedi/customer/trusted/"},
    format="json"
)

job.commit()
