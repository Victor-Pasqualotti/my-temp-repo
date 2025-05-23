from . import script_manager

class QualityManager(script_manager.ScriptManager):

    def __init__(
            self, 
            b3_session, 
            domain, 
            subdomain, 
            dataset, 
            project_name,
            sor_name
        ):
        super().__init__(
            self, 
            b3_session, 
            domain, 
            subdomain, 
            dataset, 
            project_name
        )
        # Table where results will be saven
        self._sor_name = sor_name

    def get_quality_alert_level(self):
        """
        TODO
        """
        pass

    def get_quality_rules(self):
        """
        TODO
        """
        pass

    def evaluate_quality(self):
        """
        TODO
        """
        pass

    def write_data_quality_results(self):
        """
        TODO
        """
        pass