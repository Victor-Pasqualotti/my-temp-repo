# Dependências do Glue Job
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

# Dependencias de terceiros
import boto3
from SimpleETL import simple_etl

# Inicializa Spark e Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Instancia sessao do boto3
b3_session = boto3.Session()

# Instancia classe para execucao do ETL
etl = simple_etl.SimpleETL(
    b3_session = b3_session,
    spark_session = spark,
    glue_context = glueContext,
    domain = 'victor',
    subdomain = 'pasqualotti',
    dataset = 'baptista',
    project_name = 'tbl_victor_pasqua_dataset_bapt_simples_etl'
)
# Passa bucket onde estao arquivos de trabalho
etl.get_workspace_bucket('victor-pb-bucket')

# Resgata conteudo dos arquivos de orquestracao e sql
Orquestracao = etl.get_orchestration_file_content()
Script = etl.get_orchestration_file_content()

# Resgata metodo de leitura para as origens
source_params = etl.get_sources_read_params(Orquestracao)

# Faz carga da massa de dados pro ambiente de execucao
etl.load_sources(source_params)

# Executa script
df = etl.run_etl_script(Script)

# Validacao dos metadados
if Orquestracao.get('CopySourceSchema'):
    # Resgata schema da origem
    src_schema = etl.get_parent_source_schema()

    # Verifica se schema do nosso dataframe bate
    #TODO
    etl.validate_dataframe_schema(df, src_schema)

    etl.cast_columns_to_match_schema(df, src_schema)

# Roda Quality
etl.get_quality_alert_level()
etl.get_quality_rules()
etl.evaluate_quality()
etl.write_data_quality_results()

# Escreve no s3 e cataloga tabela
#TODO - Nao pensamos em um metodo de fazer o repartition ainda
etl.write_dataframe_to_s3()
etl.create_or_update_table_in_catalog()