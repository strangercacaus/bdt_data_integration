import json
import logging
import discord
import requests

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class WebhookNotifier:
    """
    Classe para notificação de eventos de pipeline via webhook.

    Esta classe permite enviar notificações sobre o início, fim e erros de execução de um pipeline 
    para uma URL especificada. As mensagens são enviadas no formato JSON.

    Atributos:
        url (str): A URL do webhook para onde as notificações serão enviadas.
        pipeline (str): O nome do pipeline que está sendo monitorado.
    """
    def __init__(self, url, pipeline, silent=False):
        """
        Inicializa o WebhookNotifier com a URL do webhook e o nome do pipeline.

        Args:
            url (str): A URL do webhook para onde as notificações serão enviadas.
            pipeline (str): O nome do pipeline que está sendo monitorado.
        """
        self.url = url
        self.pipeline = pipeline
        self.silent = silent

    def pipeline_start(self, **kwargs):
        """
        Envia uma notificação de início de execução do pipeline.

        Este método envia uma mensagem para a URL do webhook informando que o pipeline 
        especificado foi iniciado.
        
        Returns:
            None
        """
        text = kwargs.get('text',None)
        message = text or f'Iniciando pipeline: {self.pipeline}'
        url = self.url
        if not self.silent:
            payload = json.dumps({"content": f'Iniciando pipeline: {message}'})
            headers = {
                'Content-Type': 'application/json'
            }
            response = requests.request("POST", url, headers=headers, data=payload)
            logger.info(f"Webhook Response: {response.text}")
        else:
            logger.info(f"Silent mode: {message}")

    def pipeline_end(self, **kwargs):
        """
        Envia uma notificação de fim de execução do pipeline.

        Este método envia uma mensagem para a URL do webhook informando que a execução 
        do pipeline especificado foi encerrada.

        Returns:
            None
        """
        text = kwargs.get('text',None)
        message = text or f'Execução de pipeline encerrada: {self.pipeline}'
        url = self.url
        if not self.silent:
            payload = json.dumps({"content": message})
            headers = {
                'Content-Type': 'application/json'
            }
            response = requests.request("POST", url, headers=headers, data=payload)
            logger.info(f"Webhook Response: {response.text}")
        else:
            logger.info(f"Silent mode: {message}")

    def pipeline_error(self, e=None):
        """
        Envia uma notificação de erro na execução do pipeline.
        
        Args:
            e (Exception, optional): A exceção que ocorreu durante a execução do pipeline.
            
        Returns:
            None
        """
        url = self.url
        # Convert the exception to a string and split it into lines if needed
        error_message = str(e).splitlines()[:2]  # Take only the first 2 lines of the error message
        if not self.silent:
            payload = json.dumps({"content": f'Erro na execução do pipeline: {self.pipeline}.\n{" ".join(error_message)}'})
            headers = {
                'Content-Type': 'application/json'
            }
            response = requests.request("POST", url, headers=headers, data=payload)
            logger.info(f"Webhook Response: {response.text}")
        else:
            logger.info(f"Silent mode: {error_message}")

    def error_handler(self, func):
        """
        Decorador para tratar erros durante a execução de funções.

        Este método envolve uma função com um manipulador de erros que, em caso de exceção, 
        envia uma notificação de erro para o webhook e re-lança a exceção. Isso permite que 
        erros sejam registrados e tratados de forma centralizada.

        Args:
            func (callable): A função a ser decorada, que será monitorada para erros.

        Returns:
            callable: A função decorada com tratamento de erros, que envia notificações em caso de falha.

        Raises:
            Exception: Re-lança a exceção original após o registro do erro.
        """
        def wrapper(*args, **kwargs):
            """
            Função interna que envolve a execução de uma função decorada.

            Esta função tenta executar a função original com os argumentos fornecidos. 
            Se ocorrer uma exceção, ela registra o erro usando o método de notificação de erro 
            e re-lança a exceção para que possa ser tratada em outro lugar.

            Args:
                *args: Argumentos posicionais a serem passados para a função decorada.
                **kwargs: Argumentos nomeados a serem passados para a função decorada.

            Returns:
                qualquer: O resultado da função decorada, se executada com sucesso.

            Raises:
                Exception: Re-lança a exceção original após o registro do erro.
            """
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if not self.silent:
                    self.pipeline_error(e)
                else:
                    logger.info(f"Silent mode: {e}")
                # raise e # Re-raise the exception after logging it
        return wrapper

class DiscordNotifier(discord.Client):
    """
    Notificador para eventos de pipeline via Discord.
    
    Ainda não foi feito: Implementar o notificador via discord para diminuir o consumo da API do Notion 
    e permitir a personalização do Bot.
    """
    def __init__(self, token, channel_id, pipeline):
        self.token = token
        self.channel_id = channel_id
        self.pipeline = pipeline
        self.client = discord.client 