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

# Script generated for node Customer Landing Node
CustomerLandingNode_node1745506353779 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://buckettestudacity/customer/landing/"], "recurse": True}, transformation_ctx="CustomerLandingNode_node1745506353779")

# Script generated for node Research Consent Filter
SqlQuery850 = '''
select * from myDataSource
where shareWithResearchAsOfDate is not null
'''
ResearchConsentFilter_node1745565162674 = sparkSqlQuery(glueContext, query = SqlQuery850, mapping = {"myDataSource":CustomerLandingNode_node1745506353779}, transformation_ctx = "ResearchConsentFilter_node1745565162674")

# Script generated for node Customer Trusted Node
EvaluateDataQuality().process_rows(frame=ResearchConsentFilter_node1745565162674, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745505990635", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerTrustedNode_node1745506654260 = glueContext.getSink(path="s3://buckettestudacity/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrustedNode_node1745506654260")
CustomerTrustedNode_node1745506654260.setCatalogInfo(catalogDatabase="udacitytest",catalogTableName="customer_trusted")
CustomerTrustedNode_node1745506654260.setFormat("json")
CustomerTrustedNode_node1745506654260.writeFrame(ResearchConsentFilter_node1745565162674)
job.commit()