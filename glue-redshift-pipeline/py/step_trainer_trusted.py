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

# Script generated for node Step Trainer Landing Node
StepTrainerLandingNode_node1745581258701 = glueContext.create_dynamic_frame.from_catalog(database="udacitytest", table_name="step_trainer_landing", transformation_ctx="StepTrainerLandingNode_node1745581258701")

# Script generated for node Customer Curated Node
CustomerCuratedNode_node1745581256644 = glueContext.create_dynamic_frame.from_catalog(database="udacitytest", table_name="customer_curated", transformation_ctx="CustomerCuratedNode_node1745581256644")

# Script generated for node SELECT DISTINCT
SqlQuery1109 = '''
SELECT DISTINCT stl.serialNumber, sensorReadingTime, distanceFromObject 
FROM step_trainer_landing stl 
RIGHT JOIN customer_curated cc ON stl.serialnumber=cc.serialnumber;
'''
SELECTDISTINCT_node1745592090479 = sparkSqlQuery(glueContext, query = SqlQuery1109, mapping = {"step_trainer_landing":StepTrainerLandingNode_node1745581258701, "customer_curated":CustomerCuratedNode_node1745581256644}, transformation_ctx = "SELECTDISTINCT_node1745592090479")

# Script generated for node Step Trainer Trusted Node
EvaluateDataQuality().process_rows(frame=SELECTDISTINCT_node1745592090479, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745581204599", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrustedNode_node1745581359595 = glueContext.getSink(path="s3://buckettestudacity/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrustedNode_node1745581359595")
StepTrainerTrustedNode_node1745581359595.setCatalogInfo(catalogDatabase="udacitytest",catalogTableName="step_trainer_trusted")
StepTrainerTrustedNode_node1745581359595.setFormat("json")
StepTrainerTrustedNode_node1745581359595.writeFrame(SELECTDISTINCT_node1745592090479)
job.commit()