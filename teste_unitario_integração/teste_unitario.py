import unittest
from decimal import Decimal
from pyspark.sql.types import IntegerType, DecimalType, StructType, StructField, StringType

# Inicializa a sessão Spark para o ambiente de testes do Colab
spark = get_spark_session("UnitTest_Colab")

class TestPipelineETL(unittest.TestCase):

    # ---------------------------------------------------------------------
    # 1. TESTE UNITÁRIO: extract_vendas
    # ---------------------------------------------------------------------
    def test_extract_vendas(self):
        path_mock = "/tmp/vendas_mock.txt"
        with open(path_mock, "w") as f:
            f.write("0000100010001000001050020260722\n")

        df_vendas = extract_vendas(spark, path_mock)
        
        schema_dict = {field.name: field.dataType for field in df_vendas.schema.fields}
        self.assertIsInstance(schema_dict["venda_id"], IntegerType)
        self.assertIsInstance(schema_dict["valor"], DecimalType)

        resultado = df_vendas.first()
        self.assertEqual(resultado["venda_id"], 1)
        self.assertEqual(resultado["cliente_id"], 10)
        self.assertEqual(resultado["produto_id"], 100)
        self.assertEqual(resultado["valor"], Decimal("105.00"))
        self.assertEqual(str(resultado["data_venda"]), "2026-07-22")

    # ---------------------------------------------------------------------
    # 2. TESTE UNITÁRIO: extract_clientes
    # ---------------------------------------------------------------------
    def test_extract_clientes(self):
        path_mock = "/tmp/clientes_mock.csv"
        with open(path_mock, "w") as f:
            f.write("cliente_id,nome,data_nascimento\n1,Carlos Eduardo,1985-05-20\n")

        df_clientes = extract_clientes(spark, path_mock)

        resultado = df_clientes.first()
        self.assertEqual(resultado["cliente_id"], 1)
        self.assertEqual(resultado["nome"], "Carlos Eduardo")

    # ---------------------------------------------------------------------
    # 3. TESTE UNITÁRIO: transform_resumo_clientes (Cálculos)
    # ---------------------------------------------------------------------
    def test_transform_resumo_clientes_calculos(self):
        schema_vendas = StructType([
            StructField("venda_id", IntegerType(), True),
            StructField("cliente_id", IntegerType(), True),
            StructField("produto_id", IntegerType(), True),
            StructField("valor", DecimalType(10, 2), True),
            StructField("data_venda", StringType(), True)
        ])
        data_vendas = [
            (1, 100, 1, Decimal("150.00"), "2026-07-22"),
            (2, 100, 2, Decimal("50.00"), "2026-07-22")
        ]

        schema_clientes = StructType([
            StructField("cliente_id", IntegerType(), True),
            StructField("nome", StringType(), True)
        ])
        data_clientes = [(100, "Fulano da Silva")]

        df_vendas = spark.createDataFrame(data_vendas, schema_vendas)
        df_clientes = spark.createDataFrame(data_clientes, schema_clientes)

        df_resumo = transform_resumo_clientes(df_vendas, df_clientes)
        resultado = df_resumo.first()

        self.assertEqual(resultado["cliente_id"], 100)
        self.assertEqual(resultado["nome"], "Fulano da Silva")
        self.assertEqual(resultado["quantidade_vendas"], 2)
        self.assertEqual(resultado["total_vendas"], Decimal("200.00"))
        self.assertEqual(resultado["ticket_medio"], Decimal("100.00"))

    # ---------------------------------------------------------------------
    # 4. TESTE UNITÁRIO: transform_resumo_clientes (Tratamento de Nulos)
    # CORRIGIDO: Adicionada a coluna data_venda no schema mock
    # ---------------------------------------------------------------------
    def test_transform_resumo_clientes_trata_nulos(self):
        schema_vendas = StructType([
            StructField("venda_id", IntegerType(), True),
            StructField("cliente_id", IntegerType(), True),
            StructField("valor", DecimalType(10, 2), True),
            StructField("data_venda", StringType(), True)  # <-- Adicionado
        ])
        data_vendas = [(1, 999, Decimal("80.00"), "2026-07-22")]  # <-- Adicionado

        schema_clientes = StructType([
            StructField("cliente_id", IntegerType(), True),
            StructField("nome", StringType(), True)
        ])
        data_clientes = [(1, "Cliente Valido")]

        df_vendas = spark.createDataFrame(data_vendas, schema_vendas)
        df_clientes = spark.createDataFrame(data_clientes, schema_clientes)

        df_resumo = transform_resumo_clientes(df_vendas, df_clientes)
        resultado = df_resumo.first()

        self.assertEqual(resultado["nome"], "Não Identificado")
        self.assertEqual(resultado["total_vendas"], Decimal("80.00"))

    # ---------------------------------------------------------------------
    # 5. TESTE UNITÁRIO: transform_balanco_produtos
    # CORRIGIDO: Adicionada a coluna data_venda no schema mock
    # ---------------------------------------------------------------------
    def test_transform_balanco_produtos(self):
        schema = StructType([
            StructField("produto_id", IntegerType(), True),
            StructField("valor", DecimalType(10, 2), True),
            StructField("venda_id", IntegerType(), True),
            StructField("data_venda", StringType(), True)  # <-- Adicionado
        ])
        data = [
            (50, Decimal("100.00"), 1, "2026-07-22"),  # <-- Adicionado
            (50, Decimal("300.00"), 2, "2026-07-22")   # <-- Adicionado
        ]

        df_vendas = spark.createDataFrame(data, schema)

        df_balanco = transform_balanco_produtos(df_vendas)
        resultado = df_balanco.first()

        self.assertEqual(resultado["produto_id"], 50)
        self.assertEqual(resultado["quantidade_vendas_produto"], 2)
        self.assertEqual(resultado["total_vendas_produto"], Decimal("400.00"))
        self.assertEqual(resultado["ticket_medio_produto"], Decimal("200.00"))

# =========================================================================
# EXECUTOR INTERATIVO
# =========================================================================
suite = unittest.TestLoader().loadTestsFromTestCase(TestPipelineETL)
unittest.TextTestRunner(verbosity=2).run(suite)