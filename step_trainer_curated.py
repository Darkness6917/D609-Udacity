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

# Load trusted data
trusted_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="stedi", 
    table_name="step_trainer_trusted"
)

# You can apply any transformation here if needed
# For now, we pass it straight through

# Write to curated zone
glueContext.write_dynamic_frame.from_options(
    frame=trusted_dyf,
    connection_type="s3",
    connection_options={"path": "s3://chasecolestedi/step_trainer/curated/"},
    format="json"
)

job.commit()
