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

# Script generated for node Customer_curated
Customer_curated_node1766415108071 = glueContext.create_dynamic_frame.from_catalog(database="d609", table_name="customer_curated", transformation_ctx="Customer_curated_node1766415108071")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1766415146669 = glueContext.create_dynamic_frame.from_catalog(database="d609", table_name="step_trainer_landing", transformation_ctx="AWSGlueDataCatalog_node1766415146669")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1766415528623 = ApplyMapping.apply(frame=AWSGlueDataCatalog_node1766415146669, mappings=[("sensorreadingtime", "long", "sensorreadingtime", "long"), ("serialnumber", "string", "serialnumber", "string"), ("distancefromobject", "int", "distancefromobject", "int")], transformation_ctx="RenamedkeysforJoin_node1766415528623")

# Script generated for node Join
Join_node1766415163924 = Join.apply(frame1=Customer_curated_node1766415108071, frame2=RenamedkeysforJoin_node1766415528623, keys1=["serialnumber"], keys2=["serialnumber"], transformation_ctx="Join_node1766415163924")

# Script generated for node SQL Query
SqlQuery1343 = '''
select sensorreadingtime, serialnumber, distancefromobject from myDataSource

'''
SQLQuery_node1766415188612 = sparkSqlQuery(glueContext, query = SqlQuery1343, mapping = {"myDataSource":Join_node1766415163924}, transformation_ctx = "SQLQuery_node1766415188612")

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1766415188612, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1766413048801", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1766415700975 = glueContext.getSink(path="s3://d609/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1766415700975")
StepTrainerTrusted_node1766415700975.setCatalogInfo(catalogDatabase="d609",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1766415700975.setFormat("json")
StepTrainerTrusted_node1766415700975.writeFrame(SQLQuery_node1766415188612)
job.commit()