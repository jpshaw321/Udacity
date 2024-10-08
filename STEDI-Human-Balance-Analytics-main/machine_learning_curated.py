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

# Script generated for node accelerometertrusted
accelerometertrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://accelerometertrusted"], "recurse": True},
    transformation_ctx="accelerometertrusted_node1",
)

# Script generated for node steptrainertrusted
steptrainertrusted_node1696874238552 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://steptrainertrusted"], "recurse": True},
    transformation_ctx="steptrainertrusted_node1696874238552",
)

# Script generated for node Join
Join_node1696874970806 = Join.apply(
    frame1=steptrainertrusted_node1696874238552,
    frame2=accelerometertrusted_node1,
    keys1=["sensorReadingTime"],
    keys2=["timeStamp"],
    transformation_ctx="Join_node1696874970806",
)

# Script generated for node S3 bucket
S3bucket_node2 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1696874970806,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://machinelearningcurated", "partitionKeys": []},
    transformation_ctx="S3bucket_node2",
)

job.commit()
