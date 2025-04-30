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

# Script generated for node Accelerometer Trusted Node
AccelerometerTrustedNode_node1745589252796 = glueContext.create_dynamic_frame.from_catalog(database="udacitytest", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrustedNode_node1745589252796")

# Script generated for node Step Trainer Trusted Node
StepTrainerTrustedNode_node1745581258701 = glueContext.create_dynamic_frame.from_catalog(database="udacitytest", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrustedNode_node1745581258701")

# Script generated for node Customer Curated Node
CustomerCuratedNode_node1745591633752 = glueContext.create_dynamic_frame.from_catalog(database="udacitytest", table_name="customer_curated", transformation_ctx="CustomerCuratedNode_node1745591633752")

# Script generated for node JOIN and SELECT DISTINCT
SqlQuery1611 = '''
select  stt.serialNumber, sensorReadingTime, distanceFromObject, x,y,z 
from step_trainer_trusted stt 
left join accelerometer_trusted act on act.timestamp=stt.sensorReadingTime
'''
JOINandSELECTDISTINCT_node1745591659215 = sparkSqlQuery(glueContext, query = SqlQuery1611, mapping = {"step_trainer_trusted":StepTrainerTrustedNode_node1745581258701, "accelerometer_trusted":AccelerometerTrustedNode_node1745589252796, "customer_curated":CustomerCuratedNode_node1745591633752}, transformation_ctx = "JOINandSELECTDISTINCT_node1745591659215")

# Script generated for node Machine Learning Curated Node
EvaluateDataQuality().process_rows(frame=JOINandSELECTDISTINCT_node1745591659215, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745581204599", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCuratedNode_node1745581359595 = glueContext.getSink(path="s3://buckettestudacity/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCuratedNode_node1745581359595")
MachineLearningCuratedNode_node1745581359595.setCatalogInfo(catalogDatabase="udacitytest",catalogTableName="machine_learning_curated")
MachineLearningCuratedNode_node1745581359595.setFormat("json")
MachineLearningCuratedNode_node1745581359595.writeFrame(JOINandSELECTDISTINCT_node1745591659215)
job.commit()