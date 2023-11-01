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

# Script generated for node Customer Curated
CustomerCurated_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://customercurated/"], "recurse": True},
    transformation_ctx="CustomerCurated_node1",
)

# Script generated for node step trainer landing
steptrainerlanding_node1696868905821 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://steptrainer/step_trainer/"], "recurse": True},
    transformation_ctx="steptrainerlanding_node1696868905821",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1696871122385 = ApplyMapping.apply(
    frame=CustomerCurated_node1,
    mappings=[
        ("serialNumber", "string", "right_serialNumber", "string"),
        ("birthDay", "string", "right_birthDay", "string"),
        ("shareWithPublicAsOfDate", "bigint", "shareWithPublicAsOfDate", "long"),
        ("shareWithResearchAsOfDate", "bigint", "shareWithResearchAsOfDate", "long"),
        ("registrationDate", "bigint", "registrationDate", "long"),
        ("customerName", "string", "right_customerName", "string"),
        ("email", "string", "right_email", "string"),
        ("lastUpdateDate", "bigint", "lastUpdateDate", "long"),
        ("phone", "string", "right_phone", "string"),
        ("shareWithFriendsAsOfDate", "bigint", "shareWithFriendsAsOfDate", "long"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1696871122385",
)

# Script generated for node Join
Join_node1696869908517 = Join.apply(
    frame1=steptrainerlanding_node1696868905821,
    frame2=RenamedkeysforJoin_node1696871122385,
    keys1=["serialNumber"],
    keys2=["right_serialNumber"],
    transformation_ctx="Join_node1696869908517",
)

# Script generated for node Drop Fields
DropFields_node1696870133689 = DropFields.apply(
    frame=Join_node1696869908517,
    paths=[
        "right_serialNumber",
        "right_birthDay",
        "right_customerName",
        "right_email",
        "right_phone",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "lastUpdateDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1696870133689",
)

# Script generated for node S3 bucket
S3bucket_node2 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1696870133689,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://steptrainertrusted", "partitionKeys": []},
    transformation_ctx="S3bucket_node2",
)

job.commit()
