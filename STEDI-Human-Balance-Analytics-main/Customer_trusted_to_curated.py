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

# Script generated for node accelerometerlanding
accelerometerlanding_node1696864146790 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://accelerometerlanding/accelerometer/"],
        "recurse": True,
    },
    transformation_ctx="accelerometerlanding_node1696864146790",
)

# Script generated for node customertrusted
customertrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://customertrusted"], "recurse": True},
    transformation_ctx="customertrusted_node1",
)

# Script generated for node Join
Join_node1696864508226 = Join.apply(
    frame1=customertrusted_node1,
    frame2=accelerometerlanding_node1696864146790,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1696864508226",
)

# Script generated for node Drop Fields
DropFields_node1696864518137 = DropFields.apply(
    frame=Join_node1696864508226,
    paths=["user", "timeStamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1696864518137",
)

# Script generated for node S3 bucket
S3bucket_node2 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1696864518137,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://customercurated", "partitionKeys": []},
    transformation_ctx="S3bucket_node2",
)

job.commit()
