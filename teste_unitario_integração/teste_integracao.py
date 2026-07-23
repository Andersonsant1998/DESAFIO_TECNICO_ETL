import os
import shutil
import tempfile
import unittest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

for name in list(globals().keys()):
    if name.startswith("TestPipeline") and name != "TestPipelineETLIntegration":
        del globals()[name]


class TestPipelineETLIntegration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Inicializa ou recupera a SparkSession ativa."""
        cls.spark = (
            SparkSession.builder.appName("Test_Integration_ETL")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )

    def setUp(self):
        """Cria um diretório temporário isolado."""
        self.test_dir = tempfile.mkdtemp()
        self.path_clientes = os.path.join(self.test_dir, "clientes.csv")
        self.path_vendas = os.path.join(self.test_dir, "vendas.txt")
        self.output_dir = os.path.join(self.test_dir, "output")

        self._gerar_arquivos_origem()

    def tearDown(self):
        """Remove o diretório temporário após a execução."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def _gerar_arquivos_origem(self):
        """Gera a massa de dados de teste de acordo com o parser do extract_vendas."""
        # 1. Tabela Dimensão
        with open(self.path_clientes, "w", encoding="utf-8") as f:
            f.write("cliente_id,nome,data_nascimento\n")
            f.write("1,Joao Silva,1990-01-15\n")
            f.write("2,Maria Souza,1985-05-20\n")

        # 2. Tabela Fato
        with open(self.path_vendas, "w", encoding="utf-8") as f:
            f.write("00001000010010000050.0020260701\n")
            f.write("00002000010020000150.0020260701\n")
            f.write("00003009990010000100.0020260702\n")

    def test_pipeline_ponta_a_ponta(self):
        """Valida o fluxo completo de integração ETL."""
        # --- 1. EXTRACT ---
        df_clientes = extract_clientes(self.spark, self.path_clientes)
        df_vendas = extract_vendas(self.spark, self.path_vendas)

        self.assertEqual(df_clientes.count(), 2)
        self.assertEqual(df_vendas.count(), 3)

        # --- 2. TRANSFORM ---
        df_resumo = transform_resumo_clientes(df_vendas, df_clientes)
        df_balanco = transform_balanco_produtos(df_vendas)

        # --- 3. LOAD ---
        path_resumo = os.path.join(self.output_dir, "resumo_clientes")
        path_balanco = os.path.join(self.output_dir, "balanco_produtos")

        df_resumo.write.mode("overwrite").partitionBy("data_venda").parquet(path_resumo)
        df_balanco.write.mode("overwrite").partitionBy("data_venda").parquet(path_balanco)

        # --- 4. ASSERTIONS DE INTEGRAÇÃO ---
        df_resumo_lido = self.spark.read.parquet(path_resumo)

        # Total de registros resumidos
        self.assertEqual(df_resumo_lido.count(), 2)

        # Trata nulos do Left Join
        cliente_desconhecido = df_resumo_lido.filter(F.col("cliente_id") == 999).collect()
        self.assertEqual(len(cliente_desconhecido), 1)
        self.assertEqual(cliente_desconhecido[0]["nome"], "Não Identificado")

        # Validação dos valores acumulados do Cliente 1
        cliente_1 = df_resumo_lido.filter(F.col("cliente_id") == 1).collect()
        self.assertEqual(len(cliente_1), 1)
        self.assertEqual(cliente_1[0]["quantidade_vendas"], 2)
        
        # Aceita a comparação convertendo para float para bater os tipos Decimal/Float
        self.assertAlmostEqual(float(cliente_1[0]["total_vendas"]), 2.00, places=2)

        # Validação do particionamento no Parquet
        particoes_resumo = os.listdir(path_resumo)
        self.assertTrue(any("data_venda=" in p for p in particoes_resumo))

if __name__ == "__main__":
    # Roda somente a suíte de integração
    unittest.main(argv=[""], exit=False)