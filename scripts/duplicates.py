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

# Script generated for node Duplicates Data Catalog
DuplicatesDataCatalog_node1760496827075 = glueContext.create_dynamic_frame.from_catalog(database="tmc72m", table_name="duplicates", transformation_ctx="DuplicatesDataCatalog_node1760496827075")

# Script generated for node Drop Duplicates
DropDuplicates_node1760496863357 =  DynamicFrame.fromDF(DuplicatesDataCatalog_node1760496827075.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1760496863357")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1760496863357, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1760496819281", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (DropDuplicates_node1760496863357.count() >= 1):
   DropDuplicates_node1760496863357 = DropDuplicates_node1760496863357.coalesce(1)
AmazonS3_node1760497009350 = glueContext.write_dynamic_frame.from_options(frame=DropDuplicates_node1760496863357, connection_type="s3", format="csv", connection_options={"path": "s3://tmc27m/destinationfolder/duplicatefolder/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1760497009350")

job.commit()