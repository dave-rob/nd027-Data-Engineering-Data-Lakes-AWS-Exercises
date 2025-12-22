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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1766416141450 = glueContext.create_dynamic_frame.from_catalog(database="d609", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1766416141450")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1766416235289 = glueContext.create_dynamic_frame.from_catalog(database="d609", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1766416235289")

# Script generated for node Join
Join_node1766416269720 = Join.apply(frame1=AccelerometerTrusted_node1766416235289, frame2=StepTrainerTrusted_node1766416141450, keys1=["timestamp"], keys2=["sensorreadingtime"], transformation_ctx="Join_node1766416269720")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Join_node1766416269720, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1766413048801", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1766416427138 = glueContext.getSink(path="s3://d609/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1766416427138")
AmazonS3_node1766416427138.setCatalogInfo(catalogDatabase="d609",catalogTableName="machine_learning_curated")
AmazonS3_node1766416427138.setFormat("json")
AmazonS3_node1766416427138.writeFrame(Join_node1766416269720)
job.commit()