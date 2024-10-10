import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node customer_curated
customer_curated_node1728558881767 = glueContext.create_dynamic_frame.from_catalog(database="sw", table_name="customer_curated", transformation_ctx="customer_curated_node1728558881767")

# Script generated for node step_trainer_landing
step_trainer_landing_node1728559468410 = glueContext.create_dynamic_frame.from_catalog(database="sw", table_name="step_trainer_landing", transformation_ctx="step_trainer_landing_node1728559468410")

# Script generated for node Join
Join_node1728558856143 = Join.apply(frame1=step_trainer_landing_node1728559468410, frame2=customer_curated_node1728558881767, keys1=["serialnumber"], keys2=["serialnumber"], transformation_ctx="Join_node1728558856143")

# Script generated for node Drop Fields
DropFields_node1728558974140 = DropFields.apply(frame=Join_node1728558856143, paths=["customername", "email", "phone", "birthday", "`.serialnumber`", "registrationdate", "lastupdatedate", "sharewithresearchasofdate", "sharewithpublicasofdate", "sharewithfriendsasofdate"], transformation_ctx="DropFields_node1728558974140")

# Script generated for node Amazon S3
AmazonS3_node1728558995270 = glueContext.getSink(path="s3://sw-lake-house/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1728558995270")
AmazonS3_node1728558995270.setCatalogInfo(catalogDatabase="sw",catalogTableName="step_trainer_trusted")
AmazonS3_node1728558995270.setFormat("json")
AmazonS3_node1728558995270.writeFrame(DropFields_node1728558974140)
job.commit()