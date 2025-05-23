from . import quality_manager

class SimpleETL(quality_manager.QualityManager):

    def __init__(
            self, 
            b3_session,
            spark_session,
            glue_context,
            domain, 
            subdomain, 
            dataset, 
            project_name
        ):
        super().__init__(
            self, 
            b3_session, 
            domain, 
            subdomain, 
            dataset, 
            project_name
        )
    
        # Resources manipulation variables
        self.spark_session = spark_session
        self.glue_context = glue_context

    #=================================================================
    # Methods - Read Sources and Run SQL
    #=================================================================

    def read_from_parquet_table(
            self, 
            database:str, 
            table_name:str, 
            push_dow_predicate:str, 
            additional_options:dict
        ):
        """
         Reads parquet tables by laoding s3 data into
        DynamicFrame.
         We chose not to expose the catalog_id option

        Returns Spark Dataframe
        """
        dyf = self.glue_context.create_dynamic_frame.from_catalog(
        database = database,
        table_name = table_name,
        transformation_ctx = f"Read source {database}.{table_name}",
        push_down_predicate = push_dow_predicate,
        additional_options = additional_options
        )
        return dyf.toDF()

    def load_sources(self, source_params:list):
        """
        TODO Deveriamos tratar tabelas iceberg?
        """
        for src in source_params:
            if src.get('table_type') == 'parquet':
                df = self.read_from_parquet_table(
                    database = src.get('database'),
                    table_name = src.get('table_name'),
                    push_dow_predicate = src.get('push_dow_predicate'),
                    additional_options = src.get('additional_options')
                )
                df.createOrReplaceTempView(
                    f"{src.get('database')}_{src.get('table_name')}"
                )

    def run_etl_script(self, script:str):
        """
         Returns SparkDataFrame result of
        sql script execution
        """
        df = self.spark_session.sql(script)
        return df

    #=================================================================
    # Methods - For validate table schema
    #=================================================================

    def get_parent_source_schema(self, database:str, tablename:str):
        """
        TODO
         Gets principal source metadata.
        Used to copy parent schema.
        """
        pass

    def validate_dataframe_schema(self, df, schema:dict):
        """
        TODO
         Validate if schema is equals to
        parent source table.
        """
        pass

    def cast_columns_to_match_schema(self, df, shcema:dict):
        """
        TODO
         Given a parent source schema, makes
        cast on columns to ensure preservation of
        data types
        """
        pass

    #=================================================================
    # Methods - For write table
    #=================================================================
    def write_dataframe_to_s3(self):
        """
        TODO
        """
        pass


    def create_or_update_table_in_catalog(self, table_metadata:dict):
        """
        TODO
        """
        pass