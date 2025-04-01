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

## Convenções de Nomenclatura

### Pascal Case (ExemploDeClasse):
- Nomes de Classes Python

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
- pipelines/: Contém notebooks com pipelines de dados.
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