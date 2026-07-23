import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, StructType, StructField, StringType


def get_spark_session(app_name: str = "ETL_Vendas_Clientes") -> SparkSession:
    """
    Cria a SparkSession de forma dinâmica.
    Compatível com desenvolvimento local (Colab/Docker) e AWS Glue/EMR em Produção.
    """
    builder = SparkSession.builder.appName(app_name)
    
    # Detecção automática de ambiente (Local vs AWS Glue)
    try:
        from awsglue.context import GlueContext  # noqa: F401
    except ImportError:
        # Aplica flags locais apenas fora do ambiente Glue
        builder = builder.master("local[*]") \
                         .config("spark.driver.bindAddress", "127.0.0.1")
        
    return builder.getOrCreate()

def extract_clientes(spark: SparkSession, path_clientes: str):
    """Lê o CSV de clientes aplicando schema explícito."""
    schema_clientes = StructType([
        StructField("cliente_id", IntegerType(), True),
        StructField("nome", StringType(), True),
        StructField("data_nascimento", StringType(), True)
    ])

    return spark.read \
        .option("header", "true") \
        .schema(schema_clientes) \
        .csv(path_clientes)


def extract_vendas(spark: SparkSession, path_vendas: str):
    """Lê o TXT posicional de vendas garantindo precisão Decimal."""
    raw_df = spark.read.text(path_vendas)

    return raw_df.select(
        F.substring(F.col("value"), 1, 5).cast(IntegerType()).alias("venda_id"),
        F.substring(F.col("value"), 6, 5).cast(IntegerType()).alias("cliente_id"),
        F.substring(F.col("value"), 11, 5).cast(IntegerType()).alias("produto_id"),
        (F.substring(F.col("value"), 16, 8).cast(DecimalType(10, 2)) / 100.0).cast(DecimalType(10, 2)).alias("valor"),
        F.to_date(F.substring(F.col("value"), 24, 8), "yyyyMMdd").alias("data_venda")
    )


def transform_resumo_clientes(vendas_df, clientes_df):
    """
    Gera o resumo por cliente com Broadcast Join para eliminar o Shuffle da fato.
    Trata clientes sem cadastro como 'Não Identificado' via Left Join.
    """
    # Padroniza tratamento de cadastro nulo/ausente
    clientes_tratados = clientes_df.select(
        "cliente_id", 
        F.coalesce(F.col("nome"), F.lit("Não Identificado")).alias("nome")
    )

    df_joined = vendas_df.join(F.broadcast(clientes_tratados), on="cliente_id", how="left")

    return df_joined.groupBy("cliente_id", "nome", "data_venda").agg(
        F.round(F.sum("valor"), 2).alias("total_vendas"),
        F.count("venda_id").alias("quantidade_vendas"),
        F.round(F.avg("valor"), 2).alias("ticket_medio")
    )


def transform_balanco_produtos(vendas_df):
    """Gera o balanço acumulado por produto por data de venda."""
    return vendas_df.groupBy("produto_id", "data_venda").agg(
        F.round(F.sum("valor"), 2).alias("total_vendas_produto"),
        F.count("venda_id").alias("quantidade_vendas_produto"),
        F.round(F.avg("valor"), 2).alias("ticket_medio_produto")
    )


def load_data(df, path_output: str, partition_col: str = "data_venda", file_format: str = "csv"):
    """
    Salva o DataFrame mantendo controle sobre particionamento e volume de arquivos.
    """
    writer = df.write.mode("overwrite")

    if partition_col and partition_col in df.columns:
        writer = writer.partitionBy(partition_col)

    if file_format == "csv":
        writer.option("header", "true").csv(path_output)
    else:
        # Recomendado para Data Lakes / Produção
        writer.parquet(path_output)


def run_pipeline(path_clientes: str, path_vendas: str, path_out_clientes: str, path_out_produtos: str):
    """Orquestrador do Pipeline."""
    spark = get_spark_session()

    try:
        print(f"1. Leitura das fontes em: '{path_clientes}' e '{path_vendas}'...")
        df_clientes = extract_clientes(spark, path_clientes)
        df_vendas = extract_vendas(spark, path_vendas)

        print("2. Processando transformações (Broadcast Join & Agregações)...")
        df_resumo_clientes = transform_resumo_clientes(df_vendas, df_clientes)
        df_balanco_produtos = transform_balanco_produtos(df_vendas)

        print(f"3. Gravando resumo clientes (Particionamento por data)...")
        load_data(df_resumo_clientes, path_out_clientes, partition_col="data_venda", file_format="csv")

        print(f"4. Gravando balanço produtos (Particionamento por data)...")
        load_data(df_balanco_produtos, path_out_produtos, partition_col="data_venda", file_format="csv")

        print("\n✅ Pipeline executado com sucesso e pronto para escala!")

    except Exception as e:
        print(f"❌ Falha crítica no pipeline: {str(e)}", file=sys.stderr)
        sys.exit(1)

# Execução com caminhos do Google Colab
PATH_CLIENTES = "/content/datasets/clientes.csv"
PATH_VENDAS = "/content/datasets/vendas.txt"
PATH_OUT_CLIENTES = "/content/outputs/resumo_clientes"
PATH_OUT_PRODUTOS = "/content/outputs/balanco_produtos"

run_pipeline(
    path_clientes=PATH_CLIENTES,
    path_vendas=PATH_VENDAS,
    path_out_clientes=PATH_OUT_CLIENTES,
    path_out_produtos=PATH_OUT_PRODUTOS
)