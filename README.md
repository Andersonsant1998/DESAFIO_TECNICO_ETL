Pipeline ETL PySpark - Desafio Técnico

Este repositório contém a implementação completa, modularizada e parametrizável de uma pipeline de ETL (Extract, Transform, Load) desenvolvida em PySpark.

O projeto realiza a consolidação de transações financeiras e cadastro de clientes, tratando inconsistências, aplicando regras de negócio e persistindo os resultados particionados por data em formatos otimizados para Data Lakes.

📁 Estrutura do Repositório

DESAFIO_TECNICO_ETL/
│
├── datasets/                     # Origem de dados e geradores
│   ├── clientes.csv              # Cadastro de clientes em formato CSV
│   ├── vendas.txt                # Transações financeiras em formato TXT posicional
│   ├── gerando_csv.py            # Script Python para geração da massa de clientes
│   └── gerando_txt.py            # Script Python para geração da massa de vendas
│
├── ETL/                          # Orquestração da Pipeline
│   └── main.py                   # Script principal de execução do pipeline ETL
│
├── outputs/                      # Camada de entrega/persistência
│   ├── balanco_produtos/         # Relatório financeiro acumulado por produto
│   └── resumo_clientes/          # Relatório consolidado por cliente
│
├── teste_unitario_integração/    # Suíte de testes automatizados e evidências
│   ├── log_teste_integracao.txt  # Evidências de sucesso do teste de integração
│   ├── log_teste_unitario.txt    # Evidências de sucesso dos testes unitários
│   ├── teste_integracao.py       # Teste de integração End-to-End (E2E)
│   └── teste_unitario.py         # Testes unitários das funções e transformações
│
└── README.md                     # Documentação técnica do projeto


🛠️ Arquitetura e Explicação do Algoritmo
1. Geração da Massa de Testes (datasets/)
Os scripts gerando_csv.py e gerando_txt.py utilizam Python puro para gerar massas de dados sintéticas e validar a operação real do pipeline:

    clientes.csv: Gera cadastros fictícios (cliente_id, nome, data_nascimento).

    vendas.txt: Gera arquivos posicionais de largura fixa contendo transações financeiras.


2. Leitura e Extração (Extract)
    extract_clientes: Realiza a leitura do CSV de clientes aplicando schema explícito via StructType para otimizar o tempo de leitura e evitar inferência de tipos.

    extract_vendas: Realiza a leitura do arquivo TXT posicional utilizando spark.read.text() com fatiamento manual via F.substring:

        venda_id: Posições 1 a 5 (Integer)

        cliente_id: Posições 6 a 10 (Integer)

        produto_id: Posições 11 a 15 (Integer)

        valor: Posições 16 a 23 convertidas em DecimalType(10,2) e divididas por 100.0 para ajuste fino das casas decimais.

        data_venda: Posições 24 a 31 convertidas para o tipo data (yyyyMMdd).


3. Transformações de Negócio (Transform)
    transform_resumo_clientes:

        Broadcast Join: Como a tabela de clientes possui menor cardinalidade em relação às vendas, utilizamos F.broadcast(clientes) para eliminar a fase cara de Shuffle na rede.

        Tratamento de Nulos: Realiza um Left Join com F.coalesce para garantir que vendas efetuadas por clientes não cadastrados sejam atribuídas a "Não Identificado".

        Agregações Financeiras: Agrupa por cliente e data de venda calculando total_vendas (sum), quantidade_vendas (count) e ticket_medio (avg), todos arredondados para 2 casas decimais.

    transform_balanco_produtos:

        Agrupa as vendas por produto_id e data_venda, calculando o total_vendas_produto, quantidade_vendas_produto e ticket_medio_produto.



4. Carga e Particionamento (Load)
    load_data: Função reutilizável para persistência dos DataFrames.

    Particionamento Dinâmico: Aplica partitionBy("data_venda"), criando subdiretórios em disco no formato data_venda=YYYY-MM-DD. Essa estratégia otimiza consultas analíticas futuras (como via Amazon Athena) evitando Full Scans.

    Suporta gravação nos formatos CSV (com cabeçalho) e Parquet.


5. Resiliência e Ambientes (get_spark_session)
O código possui um gerenciador dinâmico de sessão PySpark: detecta automaticamente se está rodando localmente (Google Colab / Docker) ou em ambiente gerenciado AWS (AWS Glue / EMR), ajustando o driver e os parâmetros de shuffle conforme necessário.

🧪 Suíte de Testes Automatizados
O projeto conta com validação automatizada completa via framework unittest:

    Testes Unitários (teste_unitario.py):

        test_extract_vendas & test_extract_clientes: Garantem o parsing posicional e atribuição correta dos tipos do schema.

        test_transform_resumo_clientes_calculos: Valida a exatidão das somas e do cálculo do ticket médio.

        test_transform_resumo_clientes_trata_nulos: Valida o isolamento de clientes ausentes no cadastro (Não Identificado).

        test_transform_balanco_produtos: Valida as métricas acumuladas agrupadas por produto.

    Teste de Integração (teste_integracao.py):

        TestPipelineETLIntegration: Executa um fluxo End-to-End isolado usando diretórios temporários (tempfile).

        Simula gravação física em disco e valida a geração dos diretórios de particionamento Parquet (data_venda=).


6. Como Executar no Google Colab

    Abra o Google Colab (https://colab.research.google.com/).

    Compacte a pasta do seu projeto localmente em um arquivo .zip (ex: Desafio_Tecnico_ETL.zip).

    No painel lateral esquerdo do Colab (Arquivos), faça o upload do arquivo Desafio_Tecnico_ETL.zip.

    Crie uma célula de código para instalar o PySpark, descompactar os arquivos, garantir a criação do diretório de datasets e navegar para a pasta do projeto:

        !pip install pyspark -q

        !unzip -o Desafio_Tecnico_ETL.zip

        !mkdir -p /content/datasets

        %cd /content/Desafio_Tecnico_ETL

    Gerar a massa de dados sintética:

        !python datasets/gerando_csv.py

        !python datasets/gerando_txt.py

    Executar a Pipeline principal de ETL:

        !python ETL/main.py

    Executar a suíte de testes unitários e de integração:

        !python teste_unitario_integração/teste_unitario.py

        !python teste_unitario_integração/teste_integracao.py


## 📊 Exemplo dos Resultados de Saída

### Resumo por Cliente (`outputs/resumo_clientes`)

+------------+------------------+------------+--------------+-------------------+--------------+
| cliente_id | nome             | data_venda | total_vendas | quantidade_vendas | ticket_medio |
+------------+------------------+------------+--------------+-------------------+--------------+
| 1          | João Silva       | 2026-07-01 | 200.00       | 2                 | 100.00       |
| 999        | Não Identificado | 2026-07-02 | 100.00       | 1                 | 100.00       |
+------------+------------------+------------+--------------+-------------------+--------------+

### Balanço por Produto (`outputs/balanco_produtos`)

+------------+------------+----------------------+--------------------------+----------------------+
| produto_id | data_venda | total_vendas_produto | quantidade_vendas_produto | ticket_medio_produto |
+------------+------------+----------------------+--------------------------+----------------------+
| 10001      | 2026-07-01 | 150.00               | 2                        | 75.00                |
+------------+------------+----------------------+--------------------------+----------------------+