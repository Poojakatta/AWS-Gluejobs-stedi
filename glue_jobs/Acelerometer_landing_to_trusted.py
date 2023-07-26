import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Acelerometer landing
Acelerometerlanding_node1690285680016 = glueContext.create_dynamic_frame.from_catalog(
    database="my-trial-db",
    table_name="accelerometer_landing",
    transformation_ctx="Acelerometerlanding_node1690285680016",
)

# Script generated for node Customer trusted
Customertrusted_node1690285668991 = glueContext.create_dynamic_frame.from_catalog(
    database="my-trial-db",
    table_name="customer_trusted",
    transformation_ctx="Customertrusted_node1690285668991",
)

# Script generated for node Join
Join_node1690285691905 = Join.apply(
    frame1=Acelerometerlanding_node1690285680016,
    frame2=Customertrusted_node1690285668991,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1690285691905",
)

# Script generated for node Drop Fields
DropFields_node1690285727248 = DropFields.apply(
    frame=Join_node1690285691905,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1690285727248",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1690285727248,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project-bucket/accelerometer/acelerometer_Results/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
