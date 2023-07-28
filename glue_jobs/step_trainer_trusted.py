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
Acelerometerlanding_node1690528885248 = glueContext.create_dynamic_frame.from_catalog(
    database="my-trial-db",
    table_name="accelerometer_landing",
    transformation_ctx="Acelerometerlanding_node1690528885248",
)

# Script generated for node Customer trusted
Customertrusted_node1690528889841 = glueContext.create_dynamic_frame.from_catalog(
    database="my-trial-db",
    table_name="customer_trusted",
    transformation_ctx="Customertrusted_node1690528889841",
)

# Script generated for node Step trainer landing
Steptrainerlanding_node1690528880595 = glueContext.create_dynamic_frame.from_catalog(
    database="my-trial-db",
    table_name="step_trainer_landing",
    transformation_ctx="Steptrainerlanding_node1690528880595",
)

# Script generated for node Join-customer with acelerometer data and trusted
Joincustomerwithacelerometerdataandtrusted_node1690528904056 = Join.apply(
    frame1=Customertrusted_node1690528889841,
    frame2=Acelerometerlanding_node1690528885248,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Joincustomerwithacelerometerdataandtrusted_node1690528904056",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1690529117973 = ApplyMapping.apply(
    frame=Steptrainerlanding_node1690528880595,
    mappings=[
        ("sensorreadingtime", "long", "right_sensorreadingtime", "long"),
        ("serialnumber", "string", "right_serialnumber", "string"),
        ("distancefromobject", "int", "right_distancefromobject", "int"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1690529117973",
)

# Script generated for node Join
Join_node1690529098775 = Join.apply(
    frame1=Joincustomerwithacelerometerdataandtrusted_node1690528904056,
    frame2=RenamedkeysforJoin_node1690529117973,
    keys1=["serialnumber"],
    keys2=["right_serialnumber"],
    transformation_ctx="Join_node1690529098775",
)

# Script generated for node Amazon S3
AmazonS3_node1690529860281 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1690529098775,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project-bucket/step_trainer/step_trainer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1690529860281",
)

job.commit()
