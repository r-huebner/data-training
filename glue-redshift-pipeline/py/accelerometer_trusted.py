import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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

# Script generated for node Accelerometer Landing Node
AccelerometerLandingNode_node1745581258701 = glueContext.create_dynamic_frame.from_catalog(database="udacitytest", table_name="accelerometer_landing", transformation_ctx="AccelerometerLandingNode_node1745581258701")

# Script generated for node Customer Trusted Node
CustomerTrustedNode_node1745581256644 = glueContext.create_dynamic_frame.from_catalog(database="udacitytest", table_name="customer_trusted", transformation_ctx="CustomerTrustedNode_node1745581256644")

# Script generated for node Join
Join_node1745581280820 = Join.apply(frame1=AccelerometerLandingNode_node1745581258701, frame2=CustomerTrustedNode_node1745581256644, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1745581280820")

# Script generated for node Select Accelerometer Fields
SqlQuery880 = '''
select user,timestamp,x,y,z from myDataSource
'''
SelectAccelerometerFields_node1745582417728 = sparkSqlQuery(glueContext, query = SqlQuery880, mapping = {"myDataSource":Join_node1745581280820}, transformation_ctx = "SelectAccelerometerFields_node1745582417728")

# Script generated for node Accelerometer Trusted Node
EvaluateDataQuality().process_rows(frame=SelectAccelerometerFields_node1745582417728, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745581204599", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrustedNode_node1745581359595 = glueContext.getSink(path="s3://buckettestudacity/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrustedNode_node1745581359595")
AccelerometerTrustedNode_node1745581359595.setCatalogInfo(catalogDatabase="udacitytest",catalogTableName="accelerometer_trusted")
AccelerometerTrustedNode_node1745581359595.setFormat("json")
AccelerometerTrustedNode_node1745581359595.writeFrame(SelectAccelerometerFields_node1745582417728)
job.commit()