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
Acelerometerlanding_node1690535916655 = glueContext.create_dynamic_frame.from_catalog(
    database="my-trial-db",
    table_name="accelerometer_landing",
    transformation_ctx="Acelerometerlanding_node1690535916655",
)

# Script generated for node customer trusted
customertrusted_node1690535915987 = glueContext.create_dynamic_frame.from_catalog(
    database="my-trial-db",
    table_name="customer_trusted",
    transformation_ctx="customertrusted_node1690535915987",
)

# Script generated for node Step trainer landing
Steptrainerlanding_node1690535914919 = glueContext.create_dynamic_frame.from_catalog(
    database="my-trial-db",
    table_name="step_trainer_landing",
    transformation_ctx="Steptrainerlanding_node1690535914919",
)

# Script generated for node Join
Join_node1690535992502 = Join.apply(
    frame1=Acelerometerlanding_node1690535916655,
    frame2=customertrusted_node1690535915987,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1690535992502",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1690536036028 = ApplyMapping.apply(
    frame=Steptrainerlanding_node1690535914919,
    mappings=[
        ("sensorreadingtime", "long", "right_sensorreadingtime", "long"),
        ("serialnumber", "string", "right_serialnumber", "string"),
        ("distancefromobject", "int", "right_distancefromobject", "int"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1690536036028",
)

# Script generated for node Join
Join_node1690536025250 = Join.apply(
    frame1=Join_node1690535992502,
    frame2=RenamedkeysforJoin_node1690536036028,
    keys1=["timestamp"],
    keys2=["right_sensorreadingtime"],
    transformation_ctx="Join_node1690536025250",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1690536025250,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project-bucket/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
