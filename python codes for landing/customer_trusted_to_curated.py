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

# Script generated for node accelerometer-trusted
accelerometertrusted_node1758776206004 = glueContext.create_dynamic_frame.from_catalog(database="ayush-project", table_name="accelerometer-trusted", transformation_ctx="accelerometertrusted_node1758776206004")

# Script generated for node customer-trusted
customertrusted_node1758776205738 = glueContext.create_dynamic_frame.from_catalog(database="ayush-project", table_name="customer-trusted", transformation_ctx="customertrusted_node1758776205738")

# Script generated for node SQL Query
SqlQuery0 = '''
select c.* from c inner join(
select distinct user from a)as a on
a.user=c.email;

'''
SQLQuery_node1758776350773 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"a":accelerometertrusted_node1758776206004, "c":customertrusted_node1758776205738}, transformation_ctx = "SQLQuery_node1758776350773")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758776350773, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758774915256", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1758776478641 = glueContext.getSink(path="s3://ayush-project-udacity/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1758776478641")
AmazonS3_node1758776478641.setCatalogInfo(catalogDatabase="ayush-project",catalogTableName="customer-curated")
AmazonS3_node1758776478641.setFormat("json")
AmazonS3_node1758776478641.writeFrame(SQLQuery_node1758776350773)
job.commit()
