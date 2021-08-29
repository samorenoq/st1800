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
## @args: [format_options = {"quoteChar":"\"","escaper":"","withHeader":True,"separator":","}, connection_type = "s3", format = "csv", connection_options = {"paths": ["s3://trabajo1-dgarciag-samorenoq/raw/"], "recurse":True}, transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_options(format_options = {"quoteChar":"\"","escaper":"","withHeader":True,"separator":","}, connection_type = "s3", format = "csv", connection_options = {"paths": ["s3://trabajo1-dgarciag-samorenoq/raw/"], "recurse":True}, transformation_ctx = "DataSource0")
## @type: ApplyMapping
## @args: [mappings = [("fecha reporte web", "string", "fecha_reporte_web", "string"), ("ID de caso", "long", "ID_de_caso", "long"), ("Fecha de notificación", "string", "Fecha_de_notificacion", "string"), ("Código DIVIPOLA departamento", "long", "Codigo_DIVIPOLA_departamento", "long"), ("Nombre departamento", "string", "Nombre_departamento", "string"), ("Código DIVIPOLA municipio", "long", "Codigo_DIVIPOLA_municipio", "long"), ("Nombre municipio", "string", "Nombre_municipio", "string"), ("Edad", "long", "Edad", "long"), ("Unidad de medida de edad", "long", "Unidad_de_medida_de_edad", "long"), ("Sexo", "string", "Sexo", "string"), ("Tipo de contagio", "string", "Tipo_de_contagio", "string"), ("Ubicación del caso", "string", "Ubicacion_del_caso", "string"), ("Estado", "string", "Estado", "string"), ("Código ISO del país", "long", "Codigo_ISO_del_pais", "long"), ("Nombre del país", "string", "Nombre_del_pais", "string"), ("Recuperado", "string", "Recuperado", "string"), ("Fecha de inicio de síntomas", "string", "Fecha_de_inicio_de_sintomas", "string"), ("Fecha de muerte", "string", "Fecha_de_muerte", "string"), ("Fecha de diagnóstico", "string", "Fecha_de_diagnostico", "string"), ("Fecha de recuperación", "string", "Fecha_de_recuperacion", "string"), ("Tipo de recuperación", "string", "Tipo_de_recuperacion", "string"), ("Pertenencia étnica", "long", "Pertenencia_etnica", "long"), ("Nombre del grupo étnico", "string", "Nombre _del_grupo_etnico", "string")], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame = DataSource0]
Transform0 = ApplyMapping.apply(frame = DataSource0, mappings = [("fecha reporte web", "string", "fecha_reporte_web", "string"), ("ID de caso", "long", "ID_de_caso", "long"), ("Fecha de notificación", "string", "Fecha_de_notificacion", "string"), ("Código DIVIPOLA departamento", "long", "Codigo_DIVIPOLA_departamento", "long"), ("Nombre departamento", "string", "Nombre_departamento", "string"), ("Código DIVIPOLA municipio", "long", "Codigo_DIVIPOLA_municipio", "long"), ("Nombre municipio", "string", "Nombre_municipio", "string"), ("Edad", "long", "Edad", "long"), ("Unidad de medida de edad", "long", "Unidad_de_medida_de_edad", "long"), ("Sexo", "string", "Sexo", "string"), ("Tipo de contagio", "string", "Tipo_de_contagio", "string"), ("Ubicación del caso", "string", "Ubicacion_del_caso", "string"), ("Estado", "string", "Estado", "string"), ("Código ISO del país", "long", "Codigo_ISO_del_pais", "long"), ("Nombre del país", "string", "Nombre_del_pais", "string"), ("Recuperado", "string", "Recuperado", "string"), ("Fecha de inicio de síntomas", "string", "Fecha_de_inicio_de_sintomas", "string"), ("Fecha de muerte", "string", "Fecha_de_muerte", "string"), ("Fecha de diagnóstico", "string", "Fecha_de_diagnostico", "string"), ("Fecha de recuperación", "string", "Fecha_de_recuperacion", "string"), ("Tipo de recuperación", "string", "Tipo_de_recuperacion", "string"), ("Pertenencia étnica", "long", "Pertenencia_etnica", "long"), ("Nombre del grupo étnico", "string", "Nombre _del_grupo_etnico", "string")], transformation_ctx = "Transform0")
## @type: DataSink
## @args: [connection_type = "s3", format = "csv", connection_options = {"path": "s3://trabajo1-dgarciag-samorenoq/trusted/", "partitionKeys": []}, transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform0]
DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform0, connection_type = "s3", format = "csv", connection_options = {"path": "s3://trabajo1-dgarciag-samorenoq/trusted/", "partitionKeys": []}, transformation_ctx = "DataSink0")
job.commit()