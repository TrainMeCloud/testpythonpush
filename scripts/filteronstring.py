import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
import re

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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1760323551757 = glueContext.create_dynamic_frame.from_catalog(database="tmc72m", table_name="s3tos3", transformation_ctx="AWSGlueDataCatalog_node1760323551757")

# Script generated for node Filter
Filter_node1760323592545 = Filter.apply(frame=AWSGlueDataCatalog_node1760323551757, f=lambda row: (bool(re.match("John$", row["firstname"]))), transformation_ctx="Filter_node1760323592545")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Filter_node1760323592545, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1760323542401", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1760324375053 = glueContext.write_dynamic_frame.from_options(frame=Filter_node1760323592545, connection_type="s3", format="glueparquet", connection_options={"path": "s3://tmc27m/destinationfolder/filterfolder/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1760324375053")

job.commit()