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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1728558881767 = glueContext.create_dynamic_frame.from_catalog(database="sw", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1728558881767")

# Script generated for node Amazon S3
AmazonS3_node1728558786852 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://sw-lake-house/step_trainer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1728558786852")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1728558956519 = ApplyMapping.apply(frame=accelerometer_trusted_node1728558881767, mappings=[("serialnumber", "string", "right_serialnumber", "string"), ("z", "double", "right_z", "double"), ("birthday", "string", "right_birthday", "string"), ("sharewithpublicasofdate", "long", "right_sharewithpublicasofdate", "long"), ("sharewithresearchasofdate", "long", "right_sharewithresearchasofdate", "long"), ("registrationdate", "long", "right_registrationdate", "long"), ("customername", "string", "right_customername", "string"), ("user", "string", "right_user", "string"), ("sharewithfriendsasofdate", "long", "right_sharewithfriendsasofdate", "long"), ("y", "double", "right_y", "double"), ("x", "double", "right_x", "double"), ("timestamp", "long", "right_timestamp", "long"), ("lastupdatedate", "long", "right_lastupdatedate", "long")], transformation_ctx="RenamedkeysforJoin_node1728558956519")

# Script generated for node Join
Join_node1728558856143 = Join.apply(frame1=AmazonS3_node1728558786852, frame2=RenamedkeysforJoin_node1728558956519, keys1=["serialnumber"], keys2=["right_serialnumber"], transformation_ctx="Join_node1728558856143")

# Script generated for node Drop Fields
DropFields_node1728558974140 = DropFields.apply(frame=Join_node1728558856143, paths=["right_serialnumber", "right_z", "right_birthday", "right_sharewithpublicasofdate", "right_sharewithresearchasofdate", "right_registrationdate", "right_customername", "right_user", "right_sharewithfriendsasofdate", "right_y", "right_x", "right_timestamp", "right_lastupdatedate"], transformation_ctx="DropFields_node1728558974140")

# Script generated for node Amazon S3
AmazonS3_node1728558995270 = glueContext.getSink(path="s3://sw-lake-house/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1728558995270")
AmazonS3_node1728558995270.setCatalogInfo(catalogDatabase="sw",catalogTableName="step_trainer_trusted")
AmazonS3_node1728558995270.setFormat("json")
AmazonS3_node1728558995270.writeFrame(DropFields_node1728558974140)
job.commit()