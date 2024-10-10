import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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

# Script generated for node accelerometer_landing
accelerometer_landing_node1728551191339 = glueContext.create_dynamic_frame.from_catalog(database="sw", table_name="accelerometer_landing", transformation_ctx="accelerometer_landing_node1728551191339")

# Script generated for node customer_trusted
customer_trusted_node1728551196785 = glueContext.create_dynamic_frame.from_catalog(database="sw", table_name="customer_trusted", transformation_ctx="customer_trusted_node1728551196785")

# Script generated for node SQL Query
SqlQuery7476 = '''
select * from accelerometer_landing where
user in (select distinct email from customer_trusted)
'''
SQLQuery_node1728562945476 = sparkSqlQuery(glueContext, query = SqlQuery7476, mapping = {"accelerometer_landing":accelerometer_landing_node1728551191339, "customer_trusted":customer_trusted_node1728551196785}, transformation_ctx = "SQLQuery_node1728562945476")

# Script generated for node Drop Duplicates
DropDuplicates_node1728553581337 =  DynamicFrame.fromDF(SQLQuery_node1728562945476.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1728553581337")

# Script generated for node Amazon S3
AmazonS3_node1728551211807 = glueContext.getSink(path="s3://sw-lake-house/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1728551211807")
AmazonS3_node1728551211807.setCatalogInfo(catalogDatabase="sw",catalogTableName="accelerometer_trusted")
AmazonS3_node1728551211807.setFormat("json")
AmazonS3_node1728551211807.writeFrame(DropDuplicates_node1728553581337)
job.commit()