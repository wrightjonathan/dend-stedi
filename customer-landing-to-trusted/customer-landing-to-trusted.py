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

# Script generated for node Amazon S3
AmazonS3_node1728515582595 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://sw-lake-house/customer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1728515582595")

# Script generated for node SQL Query
SqlQuery7434 = '''
select * from myDataSource
where sharewithresearchasofdate is not null
'''
SQLQuery_node1728516327834 = sparkSqlQuery(glueContext, query = SqlQuery7434, mapping = {"myDataSource":AmazonS3_node1728515582595}, transformation_ctx = "SQLQuery_node1728516327834")

# Script generated for node Trusted Customer Zone
TrustedCustomerZone_node1728516524088 = glueContext.getSink(path="s3://sw-lake-house/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="TrustedCustomerZone_node1728516524088")
TrustedCustomerZone_node1728516524088.setCatalogInfo(catalogDatabase="sw",catalogTableName="customer_trusted")
TrustedCustomerZone_node1728516524088.setFormat("json")
TrustedCustomerZone_node1728516524088.writeFrame(SQLQuery_node1728516327834)
job.commit()