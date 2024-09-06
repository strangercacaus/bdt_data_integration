import os
import json
import yaml
import logging
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class NotionTransformer():
    
    @staticmethod
    def process_date_columns(df, columns):
        """corrige o formato de data/hora de todas as colunas solicitadas"""
        date_format = '%Y-%m-%dT%H:%M:%S.%fZ'

        for col in columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format=date_format, errors='coerce')
        return df

    @staticmethod
    def process_list_columns(df):
        """
        Processa colunas com listas para que o resultado seja apenas uma coluna de texto com valores separados por vírgula
        """
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, list)).any():
                df[col] = df[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        return df   
    
    def _extract_users_list(self,records: list) -> pd.DataFrame:
        user_list = []
        for record in records[0:300]:
            properties = record.get("properties", {})
            for key, property in properties.items():
                if property.get("type") in ['person', 'people'] or property.get("object") == 'user':
                    for user in property.get('people', []):
                        id = user.get('id', None)
                        name = user.get('name', None)
                        person_info = user.get('person', {})
                        email = person_info.get('email', None)
                        user_list.append({"id":id,"name":name,"email":email})
        return pd.DataFrame(user_list).drop_duplicates()
 

    # @staticmethod
    # # Obtem a relação entre nomes de campos
    # def _get_field_mapping(processing_date):
    #     with open(f'{mapping_history_dir}/property_history.json', 'r') as file:
    #         # Load the JSON data into a Python dictionary using json.loads
    #         jsonfile = json.load(file)
    #         data_list = jsonfile['data']
    #         filtered_list = [obj for obj in data_list if datetime.strptime(obj["alter_date"], "%Y-%m-%d").date() <= processing_date]

    #         if len(filtered_list) > 1:
    #             return(filtered_list[-1]['mapping'])
    #         else:
    #             return(filtered_list[0]['mapping'])

    def extract_all_properties(self, record: dict):
        properties = record.get('properties', {})
        extracted = {
            'id': record.get('id'),
            'created_time': record.get('created_time'),
            'last_edited_time': record.get('last_edited_time'),
            'archived': record.get('archived'),
            'in_trash': record.get('in_trash'),
        }
        for key, value in properties.items():
            if value['type'] == 'select':
                extracted[key] = value['select']['name'] if value['select'] else None
            elif value['type'] == 'multi_select':
                extracted[key] = [item['name'] for item in value['multi_select']]
            elif value['type'] == 'formula':
                extracted[key] = value['formula'][value['formula']['type']]
            elif value['type'] == 'people':
                extracted[key] = [person.get('name', '') for person in value['people']]
            elif value['type'] == 'relation':
                extracted[key] = [relation['id'] for relation in value['relation']]
            elif value['type'] == 'date':
                extracted[key] = value['date']['start'] if value['date'] else None
            elif value['type'] == 'created_by' or value['type'] == 'last_edited_by':
                extracted[key] = value[value['type']].get('name', '')
            elif value['type'] == 'created_time' or value['type'] == 'last_edited_time':
                extracted[key] = value[value['type']]
            elif value['type'] == 'number':
                extracted[key] = value['number']
            elif value['type'] == 'rich_text':
                extracted[key] = ''.join([text['plain_text'] for text in value['rich_text']])
            else:
                extracted[key] = None
        return extracted
    
    def extract_all_records(self, records) -> pd.DataFrame:
        if type(records) == list and type(records[0]) == dict:
            try:
                return pd.DataFrame([self.extract_all_properties(record)for record in records])
            except Exception as e:
                logger.error(f'Falha ao extrair propriedades do registro: {e}')
        else:
            logger.error(f'Estrutura de arquivos incorreta, esperava list[dict(),] e recebu {type(records)}')