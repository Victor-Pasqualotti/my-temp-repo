import functools
import logging

class LogManager:
    def __init__(self):
        """
          TODO
          Argumentos log_to_file e file_name, assim como todo bloco IF
        da __init__ foram adicionados apenas para testes locais. Retirar.
        """
        
        self.logger = logging.getLogger(name = 'LogManager')
        self.logger.setLevel(logging.INFO)

        # Remover este bloco IF quando executar no Glue
        if not self.logger.handlers:
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def _format_message(self, message, stage=None):
        return f"[{stage}] {message}" if stage else message

    def log_info(self, message, stage=None):
        self.logger.info(self._format_message(message, stage))

    def log_warn(self, message, stage=None):
        self.logger.warning(self._format_message(message, stage))

    def log_error(self, message, stage=None):
        self.logger.error(self._format_message(message, stage))

    @staticmethod
    def log_call_instance(stage=None, logger_attr="logger"):
        """ Decorador que acessa o logger da instância """
        def decorator(func):
            @functools.wraps(func)
            def wrapper(self, *args, **kwargs):
                logger = getattr(self, logger_attr, None)
                if logger and hasattr(logger, "log_info"):
                    class_name = self.__class__.__name__
                    func_name = func.__name__
                    signature = ", ".join(
                        [repr(a) for a in args] +
                        [f"{k}={v!r}" for k, v in kwargs.items()]
                    )
                    logger.log_info(f"Chamando {class_name}.{func_name}({signature})", stage=stage or func_name)
                result = func(self, *args, **kwargs)
                if logger and hasattr(logger, "log_info"):
                    logger.log_info(f"{class_name}.{func_name} retornou {result!r}", stage=stage or func_name)
                return result
            return wrapper
        return decorator

    @staticmethod
    def write_pipeline_log():
        """
        TODO - Escreve em tabela athena
        o resultado do pipeline
        """
        pass