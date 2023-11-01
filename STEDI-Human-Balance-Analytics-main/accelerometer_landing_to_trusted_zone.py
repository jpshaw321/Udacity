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

# Script generated for node accelerometer
accelerometer_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://accelerometerlanding/accelerometer/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_node1",
)

# Script generated for node customertrusted
customertrusted_node1696802841178 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://customertrusted"], "recurse": True},
    transformation_ctx="customertrusted_node1696802841178",
)

# Script generated for node Join
Join_node1696802257958 = Join.apply(
    frame1=accelerometer_node1,
    frame2=customertrusted_node1696802841178,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1696802257958",
)

# Script generated for node Drop Fields
DropFields_node1696802876121 = DropFields.apply(
    frame=Join_node1696802257958,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1696802876121",
)

# Script generated for node accelerometertrusted
accelerometertrusted_node1696802625206 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1696802876121,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://accelerometertrusted", "partitionKeys": []},
    transformation_ctx="accelerometertrusted_node1696802625206",
)

job.commit()
