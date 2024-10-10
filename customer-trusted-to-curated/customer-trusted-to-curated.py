import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node customer_trusted
customer_trusted_node1728551196785 = glueContext.create_dynamic_frame.from_catalog(database="sw", table_name="customer_trusted", transformation_ctx="customer_trusted_node1728551196785")

# Script generated for node Drop Duplicates
DropDuplicates_node1728553581337 =  DynamicFrame.fromDF(customer_trusted_node1728551196785.toDF().dropDuplicates(["email"]), glueContext, "DropDuplicates_node1728553581337")

# Script generated for node Amazon S3
AmazonS3_node1728551211807 = glueContext.getSink(path="s3://sw-lake-house/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1728551211807")
AmazonS3_node1728551211807.setCatalogInfo(catalogDatabase="sw",catalogTableName="customer_curated")
AmazonS3_node1728551211807.setFormat("json")
AmazonS3_node1728551211807.writeFrame(DropDuplicates_node1728553581337)
job.commit()