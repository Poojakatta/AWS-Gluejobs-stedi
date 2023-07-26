import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1690283675772 = glueContext.create_dynamic_frame.from_catalog(
    database="my-trial-db",
    table_name="customer_landing",
    transformation_ctx="AWSGlueDataCatalog_node1690283675772",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1690283733915 = DynamicFrame.fromDF(
    AWSGlueDataCatalog_node1690283675772.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1690283733915",
)

# Script generated for node SQL Query
SqlQuery782 = """
select * from myDataSource where shareWithResearchAsOfDate is not null
"""
SQLQuery_node1690284925505 = sparkSqlQuery(
    glueContext,
    query=SqlQuery782,
    mapping={"myDataSource": DropDuplicates_node1690283733915},
    transformation_ctx="SQLQuery_node1690284925505",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1690284925505,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project-bucket/customers/customer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
