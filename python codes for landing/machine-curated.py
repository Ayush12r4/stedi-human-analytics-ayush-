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

# Script generated for node step-trust
steptrust_node1758780889432 = glueContext.create_dynamic_frame.from_catalog(database="ayush-project", table_name="step-trust", transformation_ctx="steptrust_node1758780889432")

# Script generated for node accelerometer-trust
accelerometertrust_node1758780893553 = glueContext.create_dynamic_frame.from_catalog(database="ayush-project", table_name="accelerometer-trusted", transformation_ctx="accelerometertrust_node1758780893553")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from s inner join a on
s.sensorreadingtime=a.timestamp;

'''
SQLQuery_node1758781160073 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"s":steptrust_node1758780889432, "a":accelerometertrust_node1758780893553}, transformation_ctx = "SQLQuery_node1758781160073")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758781160073, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758780231861", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1758781283641 = glueContext.getSink(path="s3://ayush-project-udacity/machine-curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1758781283641")
AmazonS3_node1758781283641.setCatalogInfo(catalogDatabase="ayush-project",catalogTableName="machinelearningcurated")
AmazonS3_node1758781283641.setFormat("json")
AmazonS3_node1758781283641.writeFrame(SQLQuery_node1758781160073)
job.commit()
