import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
import concurrent.futures
import gs_derived
from pyspark.sql import functions as SqlFuncs
import re

class GroupFilter:
      def __init__(self, name, filters):
        self.name = name
        self.filters = filters

def apply_group_filter(source_DyF, group):
    return(Filter.apply(frame = source_DyF, f = group.filters))

def threadedRoute(glue_ctx, source_DyF, group_filters) -> DynamicFrameCollection:
    dynamic_frames = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_filter = {executor.submit(apply_group_filter, source_DyF, gf): gf for gf in group_filters}
        for future in concurrent.futures.as_completed(future_to_filter):
            gf = future_to_filter[future]
            if future.exception() is not None:
                print('%r generated an exception: %s' % (gf, future.exception()))
            else:
                dynamic_frames[gf.name] = future.result()
    return DynamicFrameCollection(dynamic_frames, glue_ctx)

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

# Script generated for node Conditional Router
ConditionalRouter_node1760669697315 = threadedRoute(glueContext,
  source_DyF = DerivedColumn_node1760584093102,
  group_filters = [GroupFilter(name = "gt200", filters = lambda row: (row["turnover"] > 200)), GroupFilter(name = "default_group", filters = lambda row: (not(row["turnover"] > 200)))])

# Script generated for node gt200
gt200_node1760669698218 = SelectFromCollection.apply(dfc=ConditionalRouter_node1760669697315, key="gt200", transformation_ctx="gt200_node1760669698218")

# Script generated for node default_group
default_group_node1760669698065 = SelectFromCollection.apply(dfc=ConditionalRouter_node1760669697315, key="default_group", transformation_ctx="default_group_node1760669698065")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=gt200_node1760669698218, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1760409763147", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (gt200_node1760669698218.count() >= 1):
   gt200_node1760669698218 = gt200_node1760669698218.coalesce(1)
AmazonS3_node1760410086352 = glueContext.write_dynamic_frame.from_options(frame=gt200_node1760669698218, connection_type="s3", format="csv", connection_options={"path": "s3://tmc27m/destinationfolder/conditionalrouter/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1760410086352")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=default_group_node1760669698065, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1760669333433", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1760669847522 = glueContext.write_dynamic_frame.from_options(frame=default_group_node1760669698065, connection_type="s3", format="csv", connection_options={"path": "s3://tmc27m/destinationfolder/conditionalrouter/", "compression": "snappy", "partitionKeys": []}, transformation_ctx="AmazonS3_node1760669847522")

job.commit()