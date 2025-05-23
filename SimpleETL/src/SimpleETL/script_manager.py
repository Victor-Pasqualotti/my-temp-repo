from . import orchestration

class ScriptManager(orchestration.Orchestration):

    def __init__(
            self, 
            b3_session, 
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
        # Folder where .sql files will be saved
        self._script_folder = 'script'

    #=================================================================
    # Properties and attrs
    # Define Getters and Setters
    #=================================================================

    @property
    def script_folder(self):
        """
        Returns object script_folder property
        """
        return self._script_folder
    
    @script_folder.setter
    def script_folder(self, script_folder):
        """
        Property setter.
        Defines object script_folder value
        """
        self._script_folder = script_folder

    #=================================================================
    # Methods
    # Simple Methods to retrive sql script content
    #=================================================================

    def get_sql_file_path(self):
        """
        Builds path to project_name.sql
        Returns path to script file
        """
        self.get_account_id()
        bucket = self.get_workspace_bucket()
        key = f'{self.domain}/'\
            +f'{self.parent_subdomain}/'\
            +f'{self.subdomain}/'\
            +f'{self.work_folder}'\
            +f'{self.script_folder}'\
            +f'{self.dataset}/'\
            +f'{self.project_name}.sql'

        return bucket, key
    
    def get_sql_file_content(self):
        """
         Returns SQL content from script 
        file converted to a Python object
        """
        bucket, key = self.get_sql_file_path()
        response = self.s3.get_object(Bucket = bucket, Key = key)

        return response['Body'].read().decode('utf-8')