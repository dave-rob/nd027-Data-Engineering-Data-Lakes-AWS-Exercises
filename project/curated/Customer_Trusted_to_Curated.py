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

# Script generated for node customer_trusted
customer_trusted_node1766414224740 = glueContext.create_dynamic_frame.from_catalog(database="d609", table_name="customer_trusted", transformation_ctx="customer_trusted_node1766414224740")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1766414225973 = glueContext.create_dynamic_frame.from_catalog(database="d609", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1766414225973")

# Script generated for node Join
Join_node1766414333474 = Join.apply(frame1=customer_trusted_node1766414224740, frame2=accelerometer_trusted_node1766414225973, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1766414333474")

# Script generated for node SQL Query
SqlQuery990 = '''
select distinct customername, email, phone, birthday, serialnumber, registrationdate, lastupdatedate, sharewithresearchasofdate, sharewithpublicasofdate, sharewithfriendsasofdate from myDataSource

'''
SQLQuery_node1766414371785 = sparkSqlQuery(glueContext, query = SqlQuery990, mapping = {"myDataSource":Join_node1766414333474}, transformation_ctx = "SQLQuery_node1766414371785")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1766414371785, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1766413048801", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1766414601099 = glueContext.getSink(path="s3://d609/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1766414601099")
AmazonS3_node1766414601099.setCatalogInfo(catalogDatabase="d609",catalogTableName="customer_curated")
AmazonS3_node1766414601099.setFormat("json")
AmazonS3_node1766414601099.writeFrame(SQLQuery_node1766414371785)
job.commit()