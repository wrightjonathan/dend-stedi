import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1728563539057 = glueContext.create_dynamic_frame.from_catalog(database="sw", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1728563539057")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1728561783572 = glueContext.create_dynamic_frame.from_catalog(database="sw", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1728561783572")

# Script generated for node SQL Query
SqlQuery7073 = '''
select * from step_trainer_trusted 
join accelerometer_trusted on step_trainer_trusted.sensorreadingtime = accelerometer_trusted.timestamp;
'''
SQLQuery_node1728563416018 = sparkSqlQuery(glueContext, query = SqlQuery7073, mapping = {"step_trainer_trusted":step_trainer_trusted_node1728561783572, "accelerometer_trusted":accelerometer_trusted_node1728563539057}, transformation_ctx = "SQLQuery_node1728563416018")

# Script generated for node Amazon S3
AmazonS3_node1728563792033 = glueContext.getSink(path="s3://sw-lake-house/machine-learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1728563792033")
AmazonS3_node1728563792033.setCatalogInfo(catalogDatabase="sw",catalogTableName="machine_learning_curated")
AmazonS3_node1728563792033.setFormat("json")
AmazonS3_node1728563792033.writeFrame(SQLQuery_node1728563416018)
job.commit()