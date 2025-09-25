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

# Script generated for node customer-trusted
customertrusted_node1758775590670 = glueContext.create_dynamic_frame.from_catalog(database="ayush-project", table_name="customer-trusted", transformation_ctx="customertrusted_node1758775590670")

# Script generated for node accelerometer-landing
accelerometerlanding_node1758775590884 = glueContext.create_dynamic_frame.from_catalog(database="ayush-project", table_name="accelerometer-landing", transformation_ctx="accelerometerlanding_node1758775590884")

# Script generated for node SQL Query
SqlQuery0 = '''
select a.* from a inner join c on
c.email=a.user;

'''
SQLQuery_node1758775706469 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"a":accelerometerlanding_node1758775590884, "c":customertrusted_node1758775590670}, transformation_ctx = "SQLQuery_node1758775706469")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758775706469, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758774915256", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1758775859178 = glueContext.getSink(path="s3://ayush-project-udacity/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1758775859178")
AmazonS3_node1758775859178.setCatalogInfo(catalogDatabase="ayush-project",catalogTableName="accelerometer-trusted")
AmazonS3_node1758775859178.setFormat("json")
AmazonS3_node1758775859178.writeFrame(SQLQuery_node1758775706469)
job.commit()
