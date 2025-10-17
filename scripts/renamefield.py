import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node Load from S3
LoadfromS3_node1760064590141 = glueContext.create_dynamic_frame.from_catalog(database="tmc72m", table_name="s3tos3", transformation_ctx="LoadfromS3_node1760064590141")

# Script generated for node Rename Column
RenameColumn_node1760064751675 = RenameField.apply(frame=LoadfromS3_node1760064590141, old_name="customerid", new_name="CustID", transformation_ctx="RenameColumn_node1760064751675")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=RenameColumn_node1760064751675, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1760064132616", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1760065195391 = glueContext.write_dynamic_frame.from_options(frame=RenameColumn_node1760064751675, connection_type="s3", format="glueparquet", connection_options={"path": "s3://tmc27m/destinationfolder/renamefieldfolder/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1760065195391")

job.commit()