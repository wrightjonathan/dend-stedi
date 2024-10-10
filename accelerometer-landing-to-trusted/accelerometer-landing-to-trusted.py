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

# Script generated for node accelerometer_landing
accelerometer_landing_node1728551191339 = glueContext.create_dynamic_frame.from_catalog(database="sw", table_name="accelerometer_landing", transformation_ctx="accelerometer_landing_node1728551191339")

# Script generated for node customer_trusted
customer_trusted_node1728551196785 = glueContext.create_dynamic_frame.from_catalog(database="sw", table_name="customer_trusted", transformation_ctx="customer_trusted_node1728551196785")

# Script generated for node Join
Join_node1728551202652 = Join.apply(frame1=accelerometer_landing_node1728551191339, frame2=customer_trusted_node1728551196785, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1728551202652")

# Script generated for node Drop Duplicates
DropDuplicates_node1728553581337 =  DynamicFrame.fromDF(Join_node1728551202652.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1728553581337")

# Script generated for node Drop Fields
DropFields_node1728552646542 = DropFields.apply(frame=DropDuplicates_node1728553581337, paths=["email", "phone", "customername", "birthday", "serialnumber", "registrationdate", "lastupdatedate", "sharewithresearchasofdate", "sharewithpublicasofdate", "sharewithfriendsasofdate"], transformation_ctx="DropFields_node1728552646542")

# Script generated for node Amazon S3
AmazonS3_node1728551211807 = glueContext.getSink(path="s3://sw-lake-house/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1728551211807")
AmazonS3_node1728551211807.setCatalogInfo(catalogDatabase="sw",catalogTableName="accelerometer_trusted")
AmazonS3_node1728551211807.setFormat("json")
AmazonS3_node1728551211807.writeFrame(DropFields_node1728552646542)
job.commit()