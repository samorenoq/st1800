import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [format_options = {"quoteChar":"\"","escaper":"","withHeader":True,"separator":","}, connection_type = "s3", format = "csv", connection_options = {"paths": ["s3://trabajo1-dgarciag-samorenoq/trusted/"], "recurse":True}, transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_options(format_options = {"quoteChar":"\"","escaper":"","withHeader":True,"separator":","}, connection_type = "s3", format = "csv", connection_options = {"paths": ["s3://trabajo1-dgarciag-samorenoq/trusted/"], "recurse":True}, transformation_ctx = "DataSource0")
## @type: DropFields
## @args: [paths = ["Unidad_de_medida_de_edad", "Tipo_de_contagio", "Codigo_ISO_del_pais", "Nombre_del_pais", "Tipo_de_recuperacion"], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame = DataSource0]
Transform0 = DropFields.apply(frame = DataSource0, paths = ["Unidad_de_medida_de_edad", "Tipo_de_contagio", "Codigo_ISO_del_pais", "Nombre_del_pais", "Tipo_de_recuperacion"], transformation_ctx = "Transform0")
## @type: DataSink
## @args: [connection_type = "s3", catalog_database_name = "trabajo1refined", format = "csv", connection_options = {"path": "s3://trabajo1-dgarciag-samorenoq/refined/", "partitionKeys": [], "enableUpdateCatalog":true, "updateBehavior":"LOG"}, catalog_table_name = "colombia_refined", transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform0]
DataSink0 = glueContext.getSink(path = "s3://trabajo1-dgarciag-samorenoq/refined/", connection_type = "s3", updateBehavior = "LOG", partitionKeys = [], enableUpdateCatalog = True, transformation_ctx = "DataSink0")
DataSink0.setCatalogInfo(catalogDatabase = "trabajo1refined",catalogTableName = "colombia_refined")
DataSink0.setFormat("csv")
DataSink0.writeFrame(Transform0)

job.commit()