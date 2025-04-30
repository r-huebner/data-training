import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Accelerometer Trusted Node
AccelerometerTrustedNode_node1745581258701 = glueContext.create_dynamic_frame.from_catalog(database="udacitytest", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrustedNode_node1745581258701")

# Script generated for node Customer Trusted Node
CustomerTrustedNode_node1745581256644 = glueContext.create_dynamic_frame.from_catalog(database="udacitytest", table_name="customer_trusted", transformation_ctx="CustomerTrustedNode_node1745581256644")

# Script generated for node Join
Join_node1745581280820 = Join.apply(frame1=AccelerometerTrustedNode_node1745581258701, frame2=CustomerTrustedNode_node1745581256644, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1745581280820")

# Script generated for node Drop Fields
DropFields_node1745584257669 = DropFields.apply(frame=Join_node1745581280820, paths=["user", "timestamp", "x", "y", "z"], transformation_ctx="DropFields_node1745584257669")

# Script generated for node Drop Duplicates
DropDuplicates_node1745584299764 =  DynamicFrame.fromDF(DropFields_node1745584257669.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1745584299764")

# Script generated for node Customer Curated Node
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1745584299764, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745581204599", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCuratedNode_node1745581359595 = glueContext.getSink(path="s3://buckettestudacity/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCuratedNode_node1745581359595")
CustomerCuratedNode_node1745581359595.setCatalogInfo(catalogDatabase="udacitytest",catalogTableName="customer_curated")
CustomerCuratedNode_node1745581359595.setFormat("json")
CustomerCuratedNode_node1745581359595.writeFrame(DropDuplicates_node1745584299764)
job.commit()