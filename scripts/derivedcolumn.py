import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
import gs_derived
from pyspark.sql import functions as SqlFuncs

def sparkAggregate(glueContext, parentFrame, groups, aggs, transformation_ctx) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs) if len(groups) > 0 else parentFrame.toDF().agg(*aggsFuncs)
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

# Script generated for node Aggregate Data Catalog
AggregateDataCatalog_node1760409788781 = glueContext.create_dynamic_frame.from_catalog(database="tmc72m", table_name="aggregate", transformation_ctx="AggregateDataCatalog_node1760409788781")

# Script generated for node Aggregate
Aggregate_node1760409873250 = sparkAggregate(glueContext, parentFrame = AggregateDataCatalog_node1760409788781, groups = ["agencyname"], aggs = [["sales", "sum"]], transformation_ctx = "Aggregate_node1760409873250")

# Script generated for node Rename Field
RenameField_node1760584574987 = RenameField.apply(frame=Aggregate_node1760409873250, old_name="`sum(sales)`", new_name="turnover", transformation_ctx="RenameField_node1760584574987")

# Script generated for node Derived Column
DerivedColumn_node1760584093102 = RenameField_node1760584574987.gs_derived(colName="Description", expr="CASE WHEN turnover > 200 THEN 'Sale is greater than 200' ELSE 'Sale is less than or equal to 200' END")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DerivedColumn_node1760584093102, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1760409763147", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (DerivedColumn_node1760584093102.count() >= 1):
   DerivedColumn_node1760584093102 = DerivedColumn_node1760584093102.coalesce(1)
AmazonS3_node1760410086352 = glueContext.write_dynamic_frame.from_options(frame=DerivedColumn_node1760584093102, connection_type="s3", format="csv", connection_options={"path": "s3://tmc27m/destinationfolder/derivedcolumn/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1760410086352")

job.commit()