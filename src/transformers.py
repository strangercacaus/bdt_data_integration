import json
from datetime import datetime
import os
import pandas as pd
import yaml
    
with open('/work/config/notion/config.yaml', 'r') as file:
    config = yaml.safe_load(file)
mapping_history_dir = config["MAPPING_HISTORY_DIR"]

# Extrair um objeto datetime das strings de data e hora do Notion.
def parse_datetime(string):
    string = string.replace('T',' ')[0:19]
    if len(string) == 19:
        date_format = "%Y-%m-%d %H:%M:%S"
    elif len(string) == 10:
        date_format = "%Y-%m-%d"
    return datetime.strptime(string, date_format)

# Obtem a relação entre nomes de campos
def get_latest_valid_mapping(processing_date):
    with open(f'{mapping_history_dir}/property_history.json', 'r') as file:
        # Load the JSON data into a Python dictionary using json.loads
        jsonfile = json.load(file)
        data_list = jsonfile['data']
        filtered_list = [obj for obj in data_list if datetime.strptime(obj["alter_date"], "%Y-%m-%d").date() <= processing_date]

        if len(filtered_list) > 1:
            return(filtered_list[-1]['mapping'])
        else:
            return(filtered_list[0]['mapping'])


def get_raw_property_value(item):
    item_type = item["type"]
    
    if item_type is list:
        if item[item_type][0]["type"] == "text":
            return item[item_type][0]["plain_text"]

    if item_type == "select":
        try:
            return item["select"]["name"]
        except (KeyError, TypeError):
            return ""

    if item_type == "multi_select":
        return "; ".join(option["name"] for option in item["multi_select"])

    if item_type in ("created_time", "last_edited_time"):
        try:
            return parse_datetime(item[item_type])
        except (KeyError, TypeError):
            return None

    if item_type == "date":
        try:
            return parse_datetime(item["date"]["start"])

        except (KeyError, TypeError):
            return None

    if item_type == "title":
        try:
            return item["title"][0]["text"]["content"]
        except IndexError:
            return ""

    if item_type == "checkbox":
        return item["checkbox"]

    if item_type == "url":
        return item["url"]

    if item_type == "email":
        return item["email"]

    if item_type == "phone_number":
        return item["phone_number"]

    try:
        if item_type == "formula":
            return item["formula"]["number"]
    except KeyError:
        return ""

    if item_type in ("created_by", "last_edited_by"):
        try:
            return item[item_type]["name"]
        except (KeyError, TypeError):
            return ""

    if item_type == "people":
        return [person.get("name", "") for person in item.get("people", [])]

    if item_type == "relation":
        return item["relation"]

    if item_type == "rollup":
        return item["rollup"]["array"]

    if item_type == "unique_id":
        return item["unique_id"]["number"]

    if item_type == "rich_text":
        try:
            return item["rich_text"][0]["plain_text"]
        except IndexError:
            return ""

    return None

# Converte a lista de registros json em um dataframe pandas.
def format_records(records=None, mapping=None):
    all_values = []
    for record in records:
        properties = record['properties']
        all_values.append({
           ' ID': get_raw_property_value(properties[mapping['ID']]),
            'Título': get_raw_property_value(properties[mapping['Título']]),
            'Ambiente': get_raw_property_value(properties[mapping['Ambiente']]),
            'Branch': get_raw_property_value(properties[mapping['Branch']]),
            'Created At': get_raw_property_value(properties[mapping['Created At']]),
            'Created By': get_raw_property_value(properties[mapping['Created By']]),
            'Customer ID': get_raw_property_value(properties[mapping['Customer ID']]),
            'Entered Done At': get_raw_property_value(properties[mapping['Entered Done At']]),
            'Entered Approved At': get_raw_property_value(properties[mapping['Entered Approved At']]),
            'Entered Awaiting Version At': get_raw_property_value(properties[mapping['Entered Awaiting Version At']]),
            'Entered Testing At': get_raw_property_value(properties[mapping['Entered Testing At']]),
            'Entered To-Test At': get_raw_property_value(properties[mapping['Entered To-Test At']]),
            'Entered Doing At': get_raw_property_value(properties[mapping['Entered Doing At']]),
            'Entered To-Do At': get_raw_property_value(properties[mapping['Entered To-Do At']]),
            'Entered Backlog At': get_raw_property_value(properties[mapping['Entered Backlog At']]),
            'Stage Updated At': get_raw_property_value(properties[mapping['Stage Updated At']]),
            'Dias até a Conclusão': get_raw_property_value(properties[mapping['Dias até a Conclusão']]),
            'Dias até o Desenvolvimento': get_raw_property_value(properties[mapping['Dias até o Desenvolvimento']]),
            'Dias de Desenvolvimento': get_raw_property_value(properties[mapping['Dias de Desenvolvimento']]),
            'Dias na Etapa': get_raw_property_value(properties[mapping['Dias na Etapa']]),
            'Tempo Decorrido': get_raw_property_value(properties[mapping['Tempo Decorrido']]),
            'Etapa': get_raw_property_value(properties[mapping['Etapa']]).replace(' - ','')[1::],
            'Área': get_raw_property_value(properties[mapping['Área']]),
            # 'Frente de Trabalho': get_raw_property_value(properties[mapping['Frente de Trabalho']]), coluna removida
            'Task ID': get_raw_property_value(properties[mapping['Task ID']]),
            'Módulo do Sistema': get_raw_property_value(properties[mapping['Módulo do Sistema']]),
            'Nome do Cliente': get_raw_property_value(properties[mapping['Nome do Cliente']]),
            'Prioridade': get_raw_property_value(properties[mapping['Prioridade']]),
            'Release AppCenter': get_raw_property_value(properties[mapping['Release AppCenter']]),
            'Responsável': get_raw_property_value(properties[mapping['Responsável']]),
            'Solicitante': get_raw_property_value(properties[mapping['Solicitante']]),
            'Sprint': get_raw_property_value(properties[mapping['Sprint']]),
            'Status': get_raw_property_value(properties[mapping['Status']]),
            'Justificativa': get_raw_property_value(properties[mapping['Justificativa']]),
            'Tester': get_raw_property_value(properties[mapping['Tester']]),
            # 'Ticket do DropDesk': get_raw_property_value(properties[mapping['Ticket do DropDesk']]), coluna removida
            'Updated At': get_raw_property_value(properties[mapping['Updated At']]),
            'Updated By': get_raw_property_value(properties[mapping['Updated By']]),
            'Dias até o SLA': get_raw_property_value(properties[mapping['Dias até o SLA']]),
            'Integração': get_raw_property_value(properties[mapping['Integração']]),
        })
    df = pd.DataFrame(all_values)
    return df