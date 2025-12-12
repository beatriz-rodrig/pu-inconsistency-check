# 🔎 pu-inconsistency-check

**Descrição:** Script em Python projetado para detectar inconsistências no Preço Unitário (PU) de ativos financeiros, comparando extratos de dados do Banco e do sistema interno Britech. A conciliação é realizada usando chaves sucessivas com uma tolerância de 1e-6.

## 🎯 Objetivos do Desafio

Este projeto foi desenvolvido para atender aos requisitos de um desafio prático de conciliação de dados, focando nos seguintes pontos de observação:

1.  **Domínio da Linguagem e Bibliotecas:** Utilização eficiente do Python e Pandas para manipulação e processamento de dados.
2.  **Arquitetura/Design:** Implementação de classes (`DataCleaner`, `ConsistencyChecker`) para separação clara de responsabilidades (Princípio da Responsabilidade Única - SRP).
3.  **Ferramentas:** Uso adequado do Git, CLI e ambientes virtuais.
4.  **Comunicação:** Documentação e clareza do código, incluindo a configuração de `logging`.

## 🏗️ Arquitetura e Design

A solução utiliza um fluxo de processamento de dados (simulando um ETL básico) com componentes modularizados:

### 1. `DataCleaner` (`src/data_processor.py`)
Responsável por carregar, limpar e preparar os dados:
* **Localização Dinâmica do Cabeçalho:** Encontra o cabeçalho correto, ignorando metadados superiores.
* **Preparação:** Limpa e tipifica colunas (datas, numéricos) para garantir a integridade dos dados.
* **Geração de Chaves:** Padroniza a criação de chaves de conciliação (`ASSET_ID_VENC` e `ASSET_ID_APL`).

### 2. `ConsistencyChecker` (`src/data_processor.py`)
Responsável pela união dos dados e validação da regra de negócio:
* **Conciliação Sucessiva (`_merge_data_successive`):** Realiza o `pd.merge` em duas etapas para maximizar a cobertura:
    1.  Prioriza o match por **Vencimento + Quantidade**.
    2.  Utiliza o match por **Aplicação + Quantidade** como *fallback*.
* **Validação:** Calcula a diferença de PU e aplica o critério de inconsistência de $|PU_{diff}| > 1 \times 10^{-6}$.

### 3. `main.py` (Orquestrador)
* Configura o sistema de `logging` para monitoramento.
* Orquestra o fluxo de processamento e gera os relatórios finais em Excel com formatação `DD/MM/YYYY` e precisão numérica.

## 🚀 Como Executar o Projeto

Siga os passos abaixo para configurar e rodar o script no seu ambiente.

### 1. Pré-requisitos

* Python 3.13+
* Git

### 2. Clonagem do Repositório

```bash
git clone https://github.com/seuusuario/pu-inconsistency-check.git
cd pu-inconsistency-check
```

### 3. Configuração do Ambiente VirtualÉ altamente recomendado usar um ambiente virtual para isolar as dependências:
No Linux/macOS:
```bash 
python3 -m venv .venv
source .venv/bin/activate
```
No Windows (PowerShell/CMD):
```bash 
python -m venv .venv
.\.venv\Scripts\activate
```

### 4. Instalação das DependênciasInstale todas as bibliotecas necessárias:
pip install -r requirements.txt

### 5. Estrutura de Pastas
Garanta que os arquivos Excel de entrada (Extrato_Banco.xlsx e Extrato_Britech.xlsx) estejam na pasta data/.

## 6. Execução
Execute o script principal a partir da raiz do projeto (com o ambiente virtual ativo)
```bash 
python main.py
```

### 📊 Resultados e Output
Após a execução, serão gerados os seguintes arquivos na raiz do projeto:relatorio_comparacao_completa.xlsx: Contém todos os ativos conciliados, ordenados pela maior diferença de valor absoluta.relatorio_inconsistencias.xlsx: Contém apenas os ativos onde a inconsistência de PU é maior que a tolerância de 1e-6.
log.log: Arquivo de log detalhado do sistema com status de INFO e ERROR da execução.

### 🧪 Testes Unitários
A lógica principal de conciliação e a verificação de inconsistência são validadas por testes unitários usando pytest.Para executar os testes (com o ambiente virtual ativo):
```bash 
pytest
```