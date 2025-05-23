from . import simple_config
import json

class Orchestration(simple_config.SimpleConfig):

    def __init__(
            self, 
            b3_session, 
            domain, 
            subdomain, 
            dataset, 
            project_name
        ):
        super().__init__(
            b3_session, 
            domain, 
            subdomain, 
            dataset
        )
        # Project or table name
        # Project/tablename must be the same as file
        self._project_name = project_name
        # Folder where .json file will be saved
        self._orch_folder = 'orchestration'

    #=================================================================
    # Properties and attrs
    # Define Getters and Setters
    #=================================================================

    @property
    def project_name(self):
        """
        Returns object project_name property
        """
        return self._project_name
    
    @project_name.setter
    def project_name(self, project_name):
        """
        Property setter.
        Defines object project_name value
        """
        self._project_name = project_name

    @property
    def orch_folder(self):
        """
        Returns object orch_folder property
        """
        return self._orch_folder
    
    @orch_folder.setter
    def orch_folder(self, orch_folder):
        """
        Property setter.
        Defines object orch_folder value
        """
        self._orch_folder = orch_folder

    #=================================================================
    # Methods
    # Simple Methods to retrive config file content and sources params
    #=================================================================

    def get_orchestration_file_path(self):
        """
        Builds path to project_name.json
        Returns path to orchestration file
        """
        bucket = self.get_workspace_bucket()
        key = f'{self.domain}/'\
            +f'{self.parent_subdomain}/'\
            +f'{self.subdomain}/'\
            +f'{self.work_folder}'\
            +f'{self.orch_folder}'\
            +f'{self.dataset}/'\
            +f'{self.project_name}.json'

        return bucket, key
    
    def get_orchestration_file_content(self):
        """
         Returns JSON content from orchestration 
        file converted to a Python object
        """
        bucket, key = self.get_orchestration_file_path()
        response = self.s3.get_object(Bucket = bucket, Key = key)

        return json.loads(response['Body'].read().decode('utf-8'))
    
    def get_source_predicate(self, by):
        """
        TODO
        """
        if by.lower().strip() == "byvalue":
            pass
        elif by.lower().strip() == "byexpression":
            pass
        elif by.lower().strip() == "byathenaquerylimit":
            pass

    def get_sources_read_params(self, orch_file:dict = {}):
        """
         Get reading parameters for all data sources
        defined in orchestration file
        """
        sources = orch_file.get("Sources",list())

        source_params = list()
        for src in sources:
            source_params.append({
                'database':src.get('database'),
                'table_name':src.get('table_name'),
                'table_type':src.get('table_type'),
                'push_down_predicate':self.get_source_predicate(
                    next(iter(src.get('push_down_predicate')))
                ),
                'additional_options':src.get('additional_options')
            })
        return source_params

    def get_table_propertires(self, orch_file:dict = {}):
        """
        TODO
         Get catalog metadata (properties) the final table must
        have.
        """
        pass

    def get_write_mode(self):
        """
        TODO
        Get final table write mode
        """
        pass

    def get_write_path_to_s3(self):
        """
        TODO
        """
        pass

    def get_partition_column_params(self):
        """
        TODO
         Get table partition column parameters
        defined by the user.
        """        
        pass
