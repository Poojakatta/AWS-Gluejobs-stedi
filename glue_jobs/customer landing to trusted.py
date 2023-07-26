import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer_trusted
Customer_trusted_node1690287729834 = glueContext.create_dynamic_frame.from_catalog(
    database="my-trial-db",
    table_name="customer_trusted",
    transformation_ctx="Customer_trusted_node1690287729834",
)

# Script generated for node Acelerometer
Acelerometer_node1690287735081 = glueContext.create_dynamic_frame.from_catalog(
    database="my-trial-db",
    table_name="accelerometer_landing",
    transformation_ctx="Acelerometer_node1690287735081",
)

# Script generated for node Join
Join_node1690287743762 = Join.apply(
    frame1=Acelerometer_node1690287735081,
    frame2=Customer_trusted_node1690287729834,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1690287743762",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1690287749349 = DynamicFrame.fromDF(
    Join_node1690287743762.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1690287749349",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1690287749349,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project-bucket/customers/customer_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
