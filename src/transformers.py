import os
import json
import yaml
import logging
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class NotionTransformer():
    """
    Classe responsável por transformar dados do Notion em formatos utilizáveis.
    
    Esta classe contém métodos para processar colunas de data, colunas de listas e extrair propriedades de registros do Notion. 
    Os métodos são projetados para facilitar a manipulação e a extração de dados de forma eficiente.

    Methods:
        process_date_columns(df, columns):
            Corrige o formato de data/hora de todas as colunas solicitadas.
        
        process_list_columns(df):
            Processa colunas com listas para que o resultado seja uma coluna de texto com valores separados por vírgula.
        
        extract_all_properties(record):
            Extrai todas as propriedades de um registro do Notion e retorna um dicionário de um único nível.
        
        extract_all_records(records):
            Extrai todos os registros de uma lista de registros do Notion e os retorna como um DataFrame do pandas.
    """
    @staticmethod
    def process_date_columns(df, columns):
        """
        Corrige o formato de data/hora de todas as colunas especificadas em um DataFrame.

        Este método converte as colunas de data/hora do DataFrame para o formato padrão ISO 8601. 
        Se uma coluna não estiver presente no DataFrame, ela será ignorada. As entradas que não puderem 
        ser convertidas serão definidas como null.

        Args:
            df (pd.DataFrame): O DataFrame contendo as colunas a serem processadas.
            columns (list): Uma lista de nomes de colunas que devem ser convertidas para o formato de data/hora.

        Returns:
            pd.DataFrame: O DataFrame com as colunas de data/hora corrigidas.
        """
        logger.info('Executando transformer.process_data_columns')
        date_format = '%Y-%m-%dT%H:%M:%S.%fZ'

        for col in columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format=date_format, errors='coerce')
                df[col] = df[col].where(df[col].notna(), None)
        logger.info('Colunas de data/hora transformadas com sucesso.')
        return df

    @staticmethod
    def process_list_columns(df):
        """
        Processa colunas que contêm listas, convertendo os valores em uma única string com elementos separados por vírgula.

        Este método verifica cada coluna do DataFrame e, se encontrar colunas que contêm listas, 
        transforma cada lista em uma string onde os elementos são concatenados e separados por vírgula. 
        As colunas que não contêm listas permanecem inalteradas.

        Args:
            df (pd.DataFrame): O DataFrame a ser processado, contendo colunas que podem ter listas como valores.

        Returns:
            pd.DataFrame: O DataFrame com as colunas de listas convertidas em strings.
        """
        logger.info('Executando transformer.process_list_columns')
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, list)).any():
                df[col] = df[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        return df   
        logger.info('Colunas de lista transformadas com sucesso.')
    def _extract_users_list(self,records: list) -> pd.DataFrame:
        """
        Extrai informações de usuários de uma lista de registros do Notion.

        Este método percorre os registros fornecidos e coleta informações sobre usuários, 
        incluindo ID, nome e e-mail. Ele verifica as propriedades de cada registro para identificar 
        campos do tipo 'person' ou 'people' e compila uma lista de usuários únicos. 
        O resultado é retornado como um DataFrame do pandas, onde cada linha representa um usuário.

        Args:
            records (list): Uma lista de dicionários representando os registros do Notion.

        Returns:
            pd.DataFrame: Um DataFrame contendo informações sobre os usuários extraídos, 
                        com colunas para ID, nome e e-mail, sem duplicatas.
        """
        logger.info('Executando transformer._extract_users_list')
        user_list = []
        for record in records:
            properties = record.get("properties", {})
            for key, property in properties.items():
                if property.get("type") in ['person', 'people'] or property.get("object") == 'user':
                    for user in property.get('people', []):
                        user_id = user.get('id', None)
                        name = user.get('name', None)
                        person_info = user.get('person', {})
                        email = person_info.get('email', None)
                        user_list.append({"id":user_id,"name":name,"email":email})
        logger.info('Lista de usuários obtida com sucesso')
        return pd.DataFrame(user_list).drop_duplicates()

    def extract_properties_from_page(self, page: dict):
        """
        Extrai todas as propriedades de uma página do Notion em um dicionário.

        Este método analisa um registro fornecido e extrai suas propriedades, 
        retornando um dicionário que contém informações relevantes, como ID, 
        horários de criação e edição, e outros campos específicos do Notion. 
        O método lida com diferentes tipos de propriedades, como seleções, 
        múltiplas seleções, datas, pessoas e textos ricos, garantindo que 
        os dados sejam formatados corretamente.

        Args:
            record (dict): Um dicionário representando um registro do Notion, 
                        contendo propriedades a serem extraídas.

        Returns:
            dict: Um dicionário contendo as propriedades extraídas do registro, 
                com chaves correspondentes aos tipos de dados do Notion.
        """
        properties = page.get('properties', {})
        extracted = {
            'id': page.get('id'),
            'created_time': page.get('created_time'),
            'last_edited_time': page.get('last_edited_time'),
            'archived': page.get('archived'),
            'in_trash': page.get('in_trash'),
        }
        for key, value in properties.items():
            if value['type'] == 'select':
                extracted[key] = value['select']['name'] if value['select'] else None
            elif value['type'] == 'multi_select':
                extracted[key] = [item['name'] for item in value['multi_select']]
            elif value['type'] == 'title':
                extracted[key] = [item['plain_text'] for item in value['title']]
            elif value['type'] == 'formula':
                extracted[key] = value['formula'].get('number') or value['formula'].get('string')
            elif value['type'] == 'people':
                extracted[key] = [person.get('name', '') for person in value['people']]
            elif value['type'] == 'relation':
                extracted[key] = [relation['id'] for relation in value['relation']]
            elif value['type'] == 'date':
                extracted[key] = value['date']['start'] if value['date'] else None
            elif value['type'] in ['created_by', 'last_edited_by']:
                extracted[key] = value[value['type']].get('name', '')
            elif value['type'] in ['created_time', 'last_edited_time']:
                extracted[key] = value[value['type']]
            elif value['type'] == 'number':
                extracted[key] = value['number']
            elif value['type'] == 'rich_text':
                extracted[key] = ''.join([text['plain_text'] for text in value['rich_text']])
            else:
                extracted[key] = None
        return extracted
    
    def extract_pages_from_records(self, records) -> pd.DataFrame:
        """
        Extrai informações de páginas a partir de uma lista de registros do Notion.

        Este método verifica se a entrada é uma lista de dicionários e, em caso afirmativo, 
        utiliza a função `extract_all_page_properties` para extrair as propriedades de cada 
        registro. Os dados extraídos são retornados como um DataFrame do pandas. 
        Se a estrutura dos registros não for válida, um erro é registrado.

        Args:
            records: Uma lista de dicionários representando os registros do Notion.

        Returns:
            pd.DataFrame: Um DataFrame contendo as propriedades extraídas das páginas, 
                        ou None se a estrutura dos registros for inválida.

        Raises:
            Exception: Se ocorrer um erro durante a extração das propriedades dos registros.
        """
        logger.info('Executando transformer.extract_pages_from_records.')
        if type(records) == list and type(records[0]) == dict:
            try:
                return pd.DataFrame([self.extract_properties_from_page(record)for record in records])
            except Exception as e:
                logger.error(f'Falha ao extrair propriedades do registro: {e}')
        else:
            logger.error(f'Estrutura de arquivos incorreta, esperava list[dict(),] e recebu {type(records)}')
        logger.info('Páginas extraídas com sucesso.')