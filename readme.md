# Integração de dados Bendito
## Descrição
Este projeto de ETL utiliza classes dedicadas para as ações de extração, transformação e carregamento de dados a partir de APIs rest para bancos de dados SQL.
O pipeline é organizado em camadas raw, processing e staging, equivalentes à convenção bronze/silver/gold de ETL.

Estrutura do Projeto
A estrutura do projeto contém uma pasta de código (src), um diretório de dados(data), um diretório de arquivos de configuração (config) e um ambiente de trabaho de notebooks python (Notebooks)

Rotinas de ETL são criadas em notebooks python, utilizando classes e funções armazenadas na pasta source.


### Chaves e segredos
O gerenciador de chaves do Deepnote é utilizado e pode ser acessado através do módulos

```python
import os
os.environ["nome do segredo"]
```

A configuração das chaves é realizada na seção integrations do Deepnote.

### Estrutura de Pastas

```
bdt_data_integration
│
├── src (Código do projeto)
│   │
│   ├── configuration/ - Gerenciador de configurações
│   ├── extractor/ - Extratores de dados
│   ├── loader/ - Carregadores de dados
│   ├── stream/ - Gerenciadores de fontes de dados
│   ├── transformer/ - Transformadores de dados
│   ├── util/ - Utilitários diversos
│   ├── writer/ - Gerenciadores de escrita de dados
│   └── __init__.py - Importações e configurações do módulo
│
├── requirements.txt - Dependências do projeto
├── setup.py - Configuração do pacote Python
├── MANIFEST.in - Configuração de arquivos adicionais para o pacote
├── __init__.py - Arquivo de inicialização do pacote
├── .gitignore - Arquivo para ignorar arquivos e diretórios que não devem ser versionados pelo Git
└── readme.md - Documentação do projeto
```
## Diagrama de Relacionamento de Entidades

``` mermaid
classDiagram
    %% Abstract Base Classes
    class GenericExtractor {
        <<abstract>>
        +String source
        +__init__(source)
    }
    
    class GenericAPIExtractor {
        <<abstract>>
        +String source
        +String token
        +__init__(source, token)
        +_get_endpoint() String
        +fetch_paginated() dict*
        +run()*
    }
    
    class GenericDatabaseExtractor {
        <<abstract>>
        +String source
        +__init__(source)
        +_get_endpoint() String
        +fetch_paginated() dict*
        +run()*
    }
    
    class BaseLoader {
        <<abstract>>
        +load_data(df, target_table, target_schema, mode)*
        +create_schema(target_schema)*
        +create_table(sql_command)*
        +check_if_schema_exists(target_schema) bool*
    }
    
    class DataWriter {
        <<abstract>>
        +String source
        +String stream
        +dict config
        +bool compression
        +__init__(config, source, stream, compression)
        +_get_raw_dir() String
        +_get_processing_dir() String
        +_get_staging_dir() String
        +_write_row(rows, filename, file_format)
        +get_output_file_path(filename, page_number, page_prefix, target_layer, output_name, date) String
        +dump_records(records, target_layer, file_format, date)
    }
    
    %% Concrete Implementations
    class NotionExtractor {
        +String source
        +String token
        +__init__(source, token)
        +_get_endpoint() String
        +fetch_paginated(database_id, n_pages) dict
        +run(database_id)
    }
    
    class BitrixExtractor {
        +String source
        +String token
        +__init__(source, token)
        +_get_endpoint() String
        +fetch_paginated() dict
        +run()
    }
    
    class BenditoExtractor {
        +String source
        +String token
        +__init__(source, token)
        +_get_endpoint() String
        +fetch_paginated() dict
        +run()
    }
    
    class PostgresLoader {
        +String connection_string
        +__init__(connection_string)
        +load_data(df, target_table, target_schema, mode)
        +create_schema(target_schema)
        +create_table(sql_command)
        +check_if_schema_exists(target_schema) bool
    }
    
    class NotionTransformer {
        +process_date_columns(df, columns) DataFrame
        +process_list_columns(df) DataFrame
        +_extract_users_list(records) DataFrame
        +extract_properties_from_page(page) dict
        +extract_pages_from_records(records) DataFrame
    }
    
    class DbtRunner {
        +String dbt_path
        +String profiles_dir
        +__init__(dbt_path, profiles_dir)
        +run_models(models)
        +run_project()
        +test_models(models)
        +run_snapshots()
        +run_seeds()
    }
    
    %% DBT Models (Representação conceptual)
    class NotionRawModel {
        <<dbt model>>
        +ntn_raw_universal_task_database
        +ntn_raw_action_items
    }
    
    class NotionProcessedModel {
        <<dbt model>>
        +ntn_processed_universal_task_database
    }
    
    class NotionCuratedModel {
        <<dbt model>>
        +ntn_curated_universal_task_database
        +ntn_curated_users
        +ntn_curated_enum_database_properties
    }
    
    class BitrixModels {
        <<dbt model>>
        +models
    }
    
    %% Relationships
    GenericExtractor <|-- GenericAPIExtractor
    GenericExtractor <|-- GenericDatabaseExtractor
    
    GenericAPIExtractor <|-- NotionExtractor
    GenericAPIExtractor <|-- BitrixExtractor
    GenericAPIExtractor <|-- BenditoExtractor
    
    BaseLoader <|-- PostgresLoader
    
    NotionExtractor -- NotionTransformer : uses >
    NotionExtractor -- DataWriter : uses >
    BitrixExtractor -- DataWriter : uses >
    
    PostgresLoader -- NotionRawModel : loads data to >
    PostgresLoader -- BitrixModels : loads data to >
    
    DbtRunner -- NotionRawModel : transforms >
    DbtRunner -- NotionProcessedModel : transforms >
    DbtRunner -- NotionCuratedModel : transforms >
    DbtRunner -- BitrixModels : transforms >
    
    NotionRawModel -- NotionProcessedModel : source for >
    NotionProcessedModel -- NotionCuratedModel : source for >
```

## Convenções de Nomenclatura

### Pascal Case (ExemploDeClasse):
- Nomes de Classes

### Snake Case (exemplo_de_metodo):
- Nomes de Métodos Python
- Nomes de Pastas
- Nomes de Arquivos

## Descrição dos Diretórios e Arquivos

- src/configuration/: Contém arquivos de configuração para as integrações.
- src/extractor/: Contém classes para extração de dados de diferentes fontes.
- src/loader/: Contém classes para carregamento de dados em bancos de dados.
- src/stream/: Contém classes para gerenciar fluxos de dados.
- src/transformer/: Contém classes para transformação de dados.
- src/util/: Contém funções utilitárias.
- src/writer/: Contém classes para escrita de dados.
- requirements.txt: Contém os módulos necessários para a execução do projeto.
- setup.py: Configuração para instalação do projeto como pacote Python.
- MANIFEST.in: Define arquivos adicionais a serem incluídos no pacote.
- .gitignore: Arquivo para ignorar arquivos e diretórios que não devem ser versionados pelo Git.

## Instalação como Pacote Python

Este projeto pode ser instalado como um pacote Python, permitindo sua importação em outros projetos.

### Instalação para Desenvolvimento

```bash
pip install -e .
```

### Construir o Pacote para Distribuição

```bash
pip install build
python -m build
```

### Importar o Pacote

```python
import bdt_data_integration
```

## Pré-requisitos
Antes de executar os testes, certifique-se de ter instalado:
- Interpretador Python 3.9 ou Superior