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

# Script generated for node Amazon S3
AmazonS3_node1766264471194 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://d609/customer/landing/"]}, transformation_ctx="AmazonS3_node1766264471194")

# Script generated for node Change Schema
ChangeSchema_node1766275197389 = ApplyMapping.apply(frame=AmazonS3_node1766264471194, mappings=[("customername", "string", "customername", "string"), ("email", "string", "email", "string"), ("phone", "string", "phone", "string"), ("birthday", "string", "birthday", "string"), ("serialnumber", "string", "serialnumber", "string"), ("registrationdate", "bigint", "registrationdate", "long"), ("lastupdatedate", "bigint", "lastupdatedate", "long"), ("sharewithresearchasofdate", "bigint", "sharewithresearchasofdate", "long"), ("sharewithpublicasofdate", "bigint", "sharewithpublicasofdate", "long"), ("sharewithfriendsasofdate", "bigint", "sharewithfriendsasofdate", "long")], transformation_ctx="ChangeSchema_node1766275197389")

# Script generated for node PrivacyFilter
PrivacyFilter_node1766264720319 = Filter.apply(frame=ChangeSchema_node1766275197389, f=lambda row: (not(row["sharewithresearchasofdate"] == 0)), transformation_ctx="PrivacyFilter_node1766264720319")

# Script generated for node Trusted Customer Zone
EvaluateDataQuality().process_rows(frame=PrivacyFilter_node1766264720319, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1766263692126", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
TrustedCustomerZone_node1766264511046 = glueContext.getSink(path="s3://d609/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="TrustedCustomerZone_node1766264511046")
TrustedCustomerZone_node1766264511046.setCatalogInfo(catalogDatabase="d609",catalogTableName="customer_trusted")
TrustedCustomerZone_node1766264511046.setFormat("json")
TrustedCustomerZone_node1766264511046.writeFrame(PrivacyFilter_node1766264720319)
job.commit()