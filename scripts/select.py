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

# Script generated for node Employee Details Data Catalog
EmployeeDetailsDataCatalog_node1760498007726 = glueContext.create_dynamic_frame.from_catalog(database="tmc72m", table_name="employee_details_csv", transformation_ctx="EmployeeDetailsDataCatalog_node1760498007726")

# Script generated for node Employee Job Details Data Catalog
EmployeeJobDetailsDataCatalog_node1760498070956 = glueContext.create_dynamic_frame.from_catalog(database="tmc72m", table_name="employee_job_details_csv", transformation_ctx="EmployeeJobDetailsDataCatalog_node1760498070956")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1760498252210 = ApplyMapping.apply(frame=EmployeeJobDetailsDataCatalog_node1760498070956, mappings=[("jobtitleid", "long", "right_jobtitleid", "long"), ("jobtitle", "string", "right_jobtitle", "string")], transformation_ctx="RenamedkeysforJoin_node1760498252210")

# Script generated for node Join
Join_node1760498121808 = Join.apply(frame1=EmployeeDetailsDataCatalog_node1760498007726, frame2=RenamedkeysforJoin_node1760498252210, keys1=["jobtitleid"], keys2=["right_jobtitleid"], transformation_ctx="Join_node1760498121808")

# Script generated for node Select Fields
SelectFields_node1760583504765 = SelectFields.apply(frame=Join_node1760498121808, paths=["firstname", "lastname", "right_jobtitle"], transformation_ctx="SelectFields_node1760583504765")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SelectFields_node1760583504765, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1760497996626", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (SelectFields_node1760583504765.count() >= 1):
   SelectFields_node1760583504765 = SelectFields_node1760583504765.coalesce(1)
AmazonS3_node1760498493864 = glueContext.write_dynamic_frame.from_options(frame=SelectFields_node1760583504765, connection_type="s3", format="csv", connection_options={"path": "s3://tmc27m/destinationfolder/joinfolder/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1760498493864")

job.commit()