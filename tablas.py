import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Data Catalog table
DataCatalogtable_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="bd_s3", table_name="month_04", transformation_ctx="DataCatalogtable_node1"
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1682657072094 = glueContext.create_dynamic_frame.from_catalog(
    database="bd_s3",
    table_name="month_04_7075cd8ce414a4162884e285ae5f9e65",
    transformation_ctx="AWSGlueDataCatalog_node1682657072094",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=DataCatalogtable_node1,
    mappings=[
        ("col1", "string", "categoria", "string"),
        ("col0", "string", "titulo", "string"),
        ("col2", "string", "link", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Change Schema
ChangeSchema_node1682657077848 = ApplyMapping.apply(
    frame=AWSGlueDataCatalog_node1682657072094,
    mappings=[
        ("col1", "string", "categoria", "string"),
        ("col0", "string", "titulo", "string"),
        ("col2", "string", "link", "string"),
    ],
    transformation_ctx="ChangeSchema_node1682657077848",
)

# Script generated for node Data Catalog table
DataCatalogtable_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=ApplyMapping_node2,
    database="bd_glue",
    table_name="noticias_noticiasespectador",
    transformation_ctx="DataCatalogtable_node3",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1682657086843 = glueContext.write_dynamic_frame.from_catalog(
    frame=ChangeSchema_node1682657077848,
    database="bd_glue",
    table_name="noticias_noticiastiempo",
    transformation_ctx="AWSGlueDataCatalog_node1682657086843",
)

job.commit()
