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
BDT_SELENIUM_TESTES
│
├── src (Código do projeto)
│   │
│   ├── ingestors.py - Gerenciar a ingestão de dados.
│   ├── loaders.py - Gerencia o carregamento de dados para uma DW.
│   ├── streams.py - Gerenciar fontes de dados.
│   └── writers.py - Gerenciar escrita local de dados.
│
├── data (Dados em processamento e processados)
│   │
│   ├── raw - Dados no formato da fonte de origem.
│   ├── processing - Dados em processamento interno.
│   └── staging - Dados prontos para o carregamento no Data-Warehouse.
│
├── config (arquivos de configuração das integrações)
│   │
│   ├── notion 
│   │   │
│   │   ├──property_history.json - Histórico de formato de colunas da fonte
│   │   └──config.yaml - Configurações da fonte de dados do notion
│   │
│   └── integration_metadata.yaml - Metadados gerais das integraçòes
│
├── requirements.txt
├── init.ipynb
└── notion_pipeline.ipynb
```

### Pascal Case (ExemploDeClasse):
- Nomes de Classes Python

### Snake Case (exemplo_de_metodo):
- Nomes de Métodos Python
- Nomes de Pastas
- Nomes de Arquivos

## Descrição dos Diretórios e Arquivos

- src/main/: Contém classes e funções que dão apoio às rotinas de testes.

- src/rotinas/: Contém as rotinas de testes automatizado

- requirements.txt: Contém os módulos necessários para a execução do projeto.

- .gitignore: Arquivo para ignorar arquivos e diretórios que não devem ser versionados pelo Git.

## Pré-requisitos
Antes de executar os testes, certifique-se de ter instalado:
- Interpretador Python 3.9 ou Superior
- Navegador Google Chrome

## Executando os Testes
1. Clone o repositório:
``` bash
git clone https://github.com/strangercacaus/bdt_selenium_testes.git
cd bdt_selenium_testes
```
---
2. Crie o seu ambiente virtual Python
``` bash
python -m venv .env
```
---
2. Ative o ambiente virtual Python
---

```Bash:```

``` bash
source .env/bin/activate
```
---
```Powershell:```
``` bash
.env/Scripts/Activate
```
---
3. Instale os pacotes e módulos do projeto.
``` bash
pip install -r requirements.txt
```
---
4. Execute o arquivo main.py
```bash
python src/main/run_tests.py
```
