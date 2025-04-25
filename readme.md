# Integração de dados Bendito
## Descrição
Este projeto de ETL utiliza classes dedicadas para as ações de extração, transformação e carregamento de dados a partir de APIs rest para bancos de dados SQL.
O pipeline é organizado em camadas raw, processing e staging, equivalentes à convenção bronze/silver/gold de ETL.

## Estrutura do Projeto
Contém uma pasta com o código das classes (src), um diretório de scripts de ELT (script), uma pasta de configurações e modelos do DBT (dbt), e um diretório de testes (tests).

Rotinas de ETL são criados como scripts python, utilizando classes e funções armazenadas na src.


### Chaves e segredos
São configurados através do .env e acessados com os.environ.get('CHAVE_DO_VALOR')

### Estrutura de Pastas
```
bdt_data_integration
│
├── src (Código do projeto)
│   │
│   ├── extractors/ - Extratores de dados
│   │   ├── base_extractor.py - Classes base para extratores
│   │   ├── notion_extractor.py - Extrator para Notion
│   │   ├── bitrix_extractor.py - Extrator para Bitrix
│   │   └── bendito_extractor.py - Extrator para Bendito
│   │
│   ├── loaders/ - Carregadores de dados
│   │   ├── base_loader.py - Classe base para loaders
│   │   └── postgres_loader.py - Loader para PostgreSQL
│   │
│   ├── streams/ - Gerenciadores de fontes de dados
│   └── utils/ - Utilitários diversos
│
├── tests (Testes do projeto)
│   │
│   ├── extractors/ - Testes para extratores
│   │   ├── test_base_extractor.py - Testes para as classes base
│   │   └── test_notion_extractor.py - Testes para NotionDatabaseAPIExtractor
│   │
│   ├── loaders/ - Testes para carregadores
│   │   ├── test_base_loader.py - Testes para BaseLoader
│   │   ├── 
│   │   └── test_postgres_loader.py - Testes para PostgresLoader
│   │
│   ├── conftest.py - Fixtures compartilhadas para testes
│   ├── run_tests.py - Script para executar testes
│   └── README.md - Documentação específica de testes
│
├── dbt/ - Arquivos do dbt para transformação no banco de dados.
│   │
│   ├── macros/ - Templates SQL/Jinja reaproveitáveis em modelos
│   └── models/ - Modelos do DBT para a Materialização de Tabelas e Views
│   │   ├── bendito/ - Modelos para transformação de tabelas do Bendito
│   │   ├── notion/ - Modelos para transformação de bases de dados do Notion
│   │   ├── bitrix/ - Modelos para transformação de tabelas do Bitrix
│   │   └── public/ - Modelos de dados analíticos / derivados das fontes primárias
│   ├── dbt_project.yml - Configurações do Projeto DBT
│   ├── profiles.yml - Configurações do perfil de transformações.
│   └── user.yml
│
├── .gitignore - Arquivo para ignorar arquivos e diretórios que não devem ser versionados pelo Git
├── data/ - Armazenamento de dados
├── config/ - Arquivos de configuração
├── requirements.txt - Dependências do projeto
├── MANIFEST.in - Configuração de arquivos adicionais para o pacote
├── __init__.py - Arquivo de inicialização do pacote
├── setup.py - Configuração do pacote Python
└── readme.md - Documentação do projeto
```
## Diagrama de Relacionamento de Entidades

``` mermaid
classDiagram
    %% Abstract Base Classes

    class Script {
        <<file>>
        +main()
    }

    class GenericExtractor {
        <<abstract>>
        +String source
        +__init__(source)
    }

    class GenericDatabaseExtractor {
        <<abstract>>
        +String source
        +__init__(source)
        +_get_endpoint() String
        +fetch_paginated() dict*
        +run()*
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
    
    class BaseLoader {
        <<abstract>>
        +load_data(df, target_table, target_schema, mode)*
        +create_schema(target_schema)*
        +create_table(sql_command)*
        +check_if_schema_exists(target_schema) bool*
    }

    class Stream {
        <<abstract>>
        +set_extractor(df, target_table, target_schema, mode)*
        +extract_stream(target_schema)*
        +set_loader(sql_command)*
        +load_stream(target_schema) bool*
    }
    
    %% Concrete Implementations

    class Utils{
        +validate_sql(query)
        +load_config()

    }

    class SyncMetadataHandler {
        +_load_sync_meta()*
       +update_table_meta(self, table, active, last_successful_sync_at, last_sync_attempt_at):
    }
    
    class WebhookNotifier{
        +pipeline_start()*
        +pipeline_end()*
        +pipeline_error()*
        +error_handler()*
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

    %% Relationships
    GenericExtractor <|-- GenericAPIExtractor
    GenericExtractor <|-- GenericDatabaseExtractor

    Stream -- GenericExtractor: Uses
    Stream -- BaseLoader: Uses   

    PostgresLoader -- Utils: Uses 

    Script -- Stream: Uses
    Script -- DbtRunner: Uses
    Script -- WebhookNotifier: Uses
    Script -- SyncMetadataHandler: Uses

```

## Convenções de Nomenclatura

### Pascal Case (ExemploDeClasse):
- Nomes de Classes

### Snake Case (exemplo_de_metodo):
- Nomes de Métodos Python
- Nomes de Pastas
- Nomes de Arquivos

## Descrição dos Diretórios e Arquivos

- src/extractors/: Contém classes para extração de dados de diferentes fontes.
- src/loaders/: Contém classes para carregamento de dados em bancos de dados.
- src/streams/: Contém classes para gerenciar fluxos de dados.
- src/utils/: Contém funções utilitárias.
- tests/extractors/: Contém testes para as classes extratoras.
- tests/loaders/: Contém testes para as classes carregadoras.
- dbt/: Contém modelos dbt para transformação de dados.
- data/: Armazenamento de dados.
- config/: Arquivos de configuração.
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

## Testes

O projeto inclui uma suíte de testes completa para extratores e carregadores. Os testes foram implementados usando pytest e pytest-mock.

### Executando os Testes

```bash
# Executar testes para extratores
python -m pytest tests/extractors/ -v

# Executar testes para carregadores
python -m pytest tests/loaders/ -v

# Executar todos os testes
python -m pytest tests/ -v
```

Para mais detalhes sobre os testes, consulte a [documentação de testes](tests/README.md).

## Pré-requisitos
Antes de executar o projeto ou os testes, certifique-se de ter instalado:
- Interpretador Python 3.9 ou Superior
- Dependências listadas em requirements.txt