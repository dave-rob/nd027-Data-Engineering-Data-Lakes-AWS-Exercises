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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1766275952346 = glueContext.create_dynamic_frame.from_catalog(database="d609", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1766275952346")

# Script generated for node Customer Trusted
CustomerTrusted_node1766275980550 = glueContext.create_dynamic_frame.from_catalog(database="d609", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1766275980550")

# Script generated for node Join
Join_node1766276005937 = Join.apply(frame1=AccelerometerLanding_node1766275952346, frame2=CustomerTrusted_node1766275980550, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1766276005937")

# Script generated for node Drop Fields
SqlQuery1091 = '''
select user, timestamp, x, y, z 
from myDataSource;
'''
DropFields_node1766276282971 = sparkSqlQuery(glueContext, query = SqlQuery1091, mapping = {"myDataSource":Join_node1766276005937}, transformation_ctx = "DropFields_node1766276282971")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1766276282971, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1766274306028", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1766276106077 = glueContext.getSink(path="s3://d609/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1766276106077")
AmazonS3_node1766276106077.setCatalogInfo(catalogDatabase="d609",catalogTableName="accelerometer_trusted")
AmazonS3_node1766276106077.setFormat("json")
AmazonS3_node1766276106077.writeFrame(DropFields_node1766276282971)
job.commit()