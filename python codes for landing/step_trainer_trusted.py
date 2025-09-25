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

# Script generated for node step-landing
steplanding_node1758779807556 = glueContext.create_dynamic_frame.from_catalog(database="ayush-project", table_name="step_trainer-landing", transformation_ctx="steplanding_node1758779807556")

# Script generated for node curated cust
curatedcust_node1758779814546 = glueContext.create_dynamic_frame.from_catalog(database="ayush-project", table_name="customer-curated", transformation_ctx="curatedcust_node1758779814546")

# Script generated for node SQL Query
SqlQuery0 = '''
select s.* from  s inner join c 
on c.serialnumber=s.serialnumber;

'''
SQLQuery_node1758780047930 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"s":steplanding_node1758779807556, "c":curatedcust_node1758779814546}, transformation_ctx = "SQLQuery_node1758780047930")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758780047930, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758780231861", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1758780403905 = glueContext.getSink(path="s3://ayush-project-udacity/step-trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1758780403905")
AmazonS3_node1758780403905.setCatalogInfo(catalogDatabase="ayush-project",catalogTableName="step-trust")
AmazonS3_node1758780403905.setFormat("json")
AmazonS3_node1758780403905.writeFrame(SQLQuery_node1758780047930)
job.commit()
