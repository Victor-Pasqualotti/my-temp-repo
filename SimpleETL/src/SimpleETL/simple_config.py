"""
TODO
    Esta classe deve ler um arquivo "domain-config.json".
    Esse arquivo deve ficar numa pasta config.
    A pasta config deve ficar na raiz da pasta Domain.

    Conteúdo do arquivo domain-config.json:
        > ARNs para execução dos serviços:
            - Step Functions
            - Lambda
            - ...

TODO Realmente precisamos de um metodo para resgatar ID Conta?            
"""
from .log_manager import LogManager
import boto3
import json

class SimpleConfig:

    def __init__(
            self, 
            b3_session:boto3.Session, 
            domain:str, 
            subdomain:str, 
            dataset:str
        ):
        self.logger = LogManager()

        # File tree
        self._domain = domain
        self._domain_config = 'config'
        self._parent_subdomain = 'teams'
        self._subdomain = subdomain
        self._work_folder = 'artifactory'
        self._dataset = dataset

        if isinstance(b3_session, boto3.Session):
            self.session = b3_session
        else:
            error_msg = 'b3_session expected to receive boto3.Session'
            raise Exception(error_msg)

        self._account_id = None
        self._workspace_bucket = None
        self.s3 = self.session.client('s3')
        self.sts = self.session.client('sts')
    
    #=================================================================
    # Properties and attrs
    # Define Getters and Setters
    #=================================================================

    @property
    def domain(self):
        """
        Returns object account_id property
        """
        return self._domain

    @domain.setter
    def domain(self, domain):
        """
        Property setter.
        Defines object domain value
        """
        self._domain = domain

    @property
    def domain_config(self):
        """
        Returns object domain_config property
        """
        return self._domain_config

    @domain_config.setter
    def domain_config(self, domain_config):
        """
        Property setter.
        Defines object domain_config value
        """
        self._domain_config = domain_config

    @property
    def parent_subdomain(self):
        """
        Returns object parent_subdomain property
        """
        return self._parent_subdomain

    @parent_subdomain.setter
    def parent_subdomain(self, parent_subdomain):
        """
        Property setter.
        Defines object parent_subdomain value
        """
        self._parent_subdomain = parent_subdomain

    @property
    def subdomain(self):
        """
        Returns object subdomain property
        """
        return self._subdomain

    @subdomain.setter
    def subdomain(self, subdomain):
        """
        Property setter.
        Defines object subdomain value
        """
        self._subdomain = subdomain

    @property
    def work_folder(self):
        """
        Returns object work_folder property
        """
        return self._work_folder

    @work_folder.setter
    def work_folder(self, work_folder):
        """
        Property setter.
        Defines object work_folder value
        """
        self._work_folder = work_folder

    @property
    def dataset(self):
        """
        Returns object dataset property
        """
        return self._dataset

    @dataset.setter
    def dataset(self, dataset):
        """
        Property setter.
        Defines object dataset value
        """
        self._dataset = dataset

    @property
    def account_id(self):
        """
        Returns object account_id property
        """
        return self._account_id

    @account_id.setter
    def account_id(self, account_id):
        """
        Property setter.
        Defines object account_id value
        """
        self._account_id = account_id

    @property
    def workspace_bucket(self):
        """
        Returns object workspace_bucket property
        """
        return self._workspace_bucket
    
    @workspace_bucket.setter
    def workspace_bucket(self, workspace_bucket):
        """
        Property setter.
        Defines object workspace_bucket value
        """
        self._workspace_bucket = workspace_bucket

    #=================================================================
    # Methods
    # Simple Methods to retrive account info and global domain config
    #=================================================================

    @LogManager.log_call_instance(stage = 'AccountInfo')
    def get_account_id(self) -> str:
        """
        Function returns AWS Account ID
        Can be used as setter
        """
        self.account_id = self.sts.get_caller_identity().get('Account')
        return self.account_id
    
    @LogManager.log_call_instance(stage = 'AccountInfo')
    def get_workspace_bucket(self, bucket_name:str = None) -> str:
        """
        Function returns AWS S3 workspace bucket
        Can be used as setter
        """
        if bucket_name:
            self.workspace_bucket = bucket_name
        
        return self.workspace_bucket
    
    @LogManager.log_call_instance(stage = 'DomainConfig')
    def get_domain_config_file_path(self):
        """
        Builds path to any file, given folders.
        Returns path to orchestration file
        """
        
        bucket = self.get_workspace_bucket()
        key = f'{self.domain}/'\
            +f'{self.domain_config}/'\
            +'domain-config.json'

        return bucket, key
    
    @LogManager.log_call_instance(stage = 'DomainConfig')
    def get_domain_config_file_content(self):
        """
         Returns JSON content from domain-config 
        file converted to a Python object
        """
        bucket, key = self.get_domain_config_file_path()
        response = self.s3.get_object(Bucket = bucket, Key = key)

        # Let user actually see something
        self.logger.log_info(
            'domain-config.json retrieved with success.',
            stage = 'DomainConfig'
        )

        return json.loads(response['Body'].read().decode('utf-8'))
    
    def get_domain_table_for_logs(self):
        """
        TODO - Resgata dados da tabela de logs.
         Para sabermos onde vamos escrever o resultado
        do pipeline.
        """
        pass

    def get_domain_table_for_quality_results(self):
        """
        TODO - Resgata dados da tabela de quality.
         Para sabermos onde vamos escrever o resultado
        da validacao de qualidade de dados.
        """
        pass