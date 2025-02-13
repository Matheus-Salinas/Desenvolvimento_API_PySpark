import requests
import json
from pyspark.sql import SparkSession
from google.cloud import bigquery
from google.cloud import secretmanager
from google.cloud.exceptions import NotFound
from datetime import datetime
import os
import tempfile
import logging
import sys

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Função para acessar segredos no Google Cloud Secret Manager
def acessar_segredo(nome_segredo):
    client = secretmanager.SecretManagerServiceClient()
    resposta = client.access_secret_version(request={"name": nome_segredo})
    return resposta.payload.data.decode("UTF-8")

def acessar_chave_api():
    client = secretmanager.SecretManagerServiceClient()
    nome_segredo = "projects/325835689813/secrets/chave_api_coingecko/versions/latest"
    resposta = client.access_secret_version(request={"name": nome_segredo})
    chave_api = resposta.payload.data.decode("UTF-8")
    # Remove aspas extras, se houver
    chave_api = chave_api.strip('"')
    return chave_api

def carregar_credenciais():
    credenciais = acessar_segredo("projects/325835689813/secrets/credencial_bigquery/versions/latest")
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
        temp_file.write(credenciais)
        return temp_file.name

# Configurações do ambiente
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = carregar_credenciais()
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-11"
os.environ["PYSPARK_PYTHON"] = r"C:\Users\mathe\anaconda3\envs\cripto_env\python.exe"
os.environ["SPARK_HOME"] = r"C:\spark-3.5.4-bin-hadoop3"
os.environ["HADOOP_HOME"] = r"C:\Winutils"

# Verificar se o SPARK_HOME está configurado corretamente
if not os.path.exists(os.environ["SPARK_HOME"]):
    logger.error(f"SPARK_HOME não encontrado: {os.environ['SPARK_HOME']}")
    sys.exit(1)

# Adicionar o Spark ao PATH
os.environ["PATH"] = os.environ["PATH"] + ";" + os.path.join(os.environ["SPARK_HOME"], "bin")

CHAVE_API = acessar_chave_api()
logger.info(f"Chave da API obtida: {CHAVE_API}")
TIPO_MOEDA = "brl"
DATASET_BIGQUERY = 'cripto_dataset'
TABELA_HISTORICO = 'tabela_criptomoedas'
TABELA_COMPLET = 'tabela_complet_criptomoedas'
URL_API = f"https://api.coingecko.com/api/v3/coins/markets?vs_currency={TIPO_MOEDA}&x_cg_demo_api_key={CHAVE_API}"

# Log da URL da API
logger.info(f"URL da API: {URL_API}")

# Criar a SparkSession
try:
    spark = (
        SparkSession.builder
        .master('local')
        .appName('ProcessamentoDadosCriptomoedas')
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.0")  # Adicionar suporte ao BigQuery
        .getOrCreate()
    )
    logger.info("SparkSession criada com sucesso.")
except Exception as e:
    logger.error(f"Erro ao criar SparkSession: {e}")
    sys.exit(1)

# Função para consumir a API
def buscar_dados_api():
    try:
        resposta = requests.get(URL_API)
        resposta.raise_for_status()  # Levanta uma exceção para códigos de status 4xx/5xx
        logger.info("Dados da API consumidos com sucesso.")
        return resposta.json()
    except requests.exceptions.HTTPError as err:
        logger.error(f"Erro HTTP ao consumir a API: {err}")
        raise
    except Exception as e:
        logger.error(f"Erro ao consumir a API: {e}")
        raise

# Função para adicionar timestamp aos dados
def adicionar_timestamp(dados):
    timestamp = datetime.utcnow().isoformat()  # Timestamp no formato ISO
    for item in dados:
        item["data_hora_coleta"] = timestamp  # Novo campo para o timestamp
    return dados

# Função para tratar os dados
def tratar_dados(dados):
    for item in dados:
        campos_numericos = [
            "current_price", "market_cap", "fully_diluted_valuation", "total_volume",
            "high_24h", "low_24h", "price_change_24h", "price_change_percentage_24h",
            "market_cap_change_24h", "market_cap_change_percentage_24h", "circulating_supply",
            "total_supply", "max_supply", "ath", "ath_change_percentage", "atl", "atl_change_percentage"
        ]
        for campo in campos_numericos:
            if item[campo] is not None:
                item[campo] = float(item[campo])

        # Tratar campos de data
        campos_data = ["ath_date", "atl_date", "last_updated"]
        for campo in campos_data:
            if item[campo] is not None:
                item[campo] = datetime.strptime(item[campo], "%Y-%m-%dT%H:%M:%S.%fZ").isoformat()

        # Tratar campo ROI (pode ser nulo ou um objeto)
        if item["roi"] is not None:
            item["roi"] = json.dumps(item["roi"])  # Converter objeto ROI para string JSON

    return dados

# Função para criar a tabela no BigQuery se ela não existir
def criar_tabela_se_nao_existir(tabela, schema):
    cliente = bigquery.Client()
    referencia_tabela = cliente.dataset(DATASET_BIGQUERY).table(tabela)
    
    try:
        cliente.get_table(referencia_tabela)  # Tenta obter a tabela
        logger.info(f"Tabela {tabela} já existe.")
    except NotFound:
        logger.info(f"Tabela {tabela} não existe. Criando...")
        tabela = bigquery.Table(referencia_tabela, schema=schema)
        cliente.create_table(tabela)
        logger.info(f"Tabela {tabela} criada com sucesso.")

# Função para salvar dados no BigQuery
def salvar_dados_bigquery(dados, tabela):
    cliente = bigquery.Client()
    referencia_tabela = cliente.dataset(DATASET_BIGQUERY).table(tabela)
    tabela = cliente.get_table(referencia_tabela)

    # Inserir os dados no BigQuery
    erros = cliente.insert_rows_json(tabela, dados)

    if erros == []:
        logger.info(f"Dados inseridos com sucesso no BigQuery na tabela {tabela}.")
    else:
        logger.error(f"Erros ao inserir dados no BigQuery: {erros}")

# Função para registrar a execução na tabela_complet_criptomoedas
def registrar_execucao(status):
    data_execucao = datetime.utcnow().isoformat()
    registro = {
        "data_execucao": data_execucao,
        "status": status
    }
    salvar_dados_bigquery([registro], TABELA_COMPLET)

# Schema da tabela_criptomoedas
schema_criptomoedas = [
    bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("simbolo", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("nome", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("imagem", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("preco_atual", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("capitalizacao_mercado", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("rank_capitalizacao", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("valor_total_diluido", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("volume_total", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("maior_preco_24h", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("menor_preco_24h", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("variacao_preco_24h", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("variacao_percentual_24h", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("variacao_capitalizacao_24h", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("variacao_percentual_capitalizacao_24h", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("oferta_circulante", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("oferta_total", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("oferta_maxima", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("preco_maximo_historico", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("variacao_percentual_preco_maximo", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("data_preco_maximo", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("preco_minimo_historico", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("variacao_percentual_preco_minimo", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("data_preco_minimo", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("retorno_investimento", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ultima_atualizacao", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("data_hora_coleta", "TIMESTAMP", mode="NULLABLE"),
]

# Função principal
def main():
    try:
        # Consumir a API
        dados_criptomoedas = buscar_dados_api()

        # Adicionar timestamp aos dados
        dados_com_timestamp = adicionar_timestamp(dados_criptomoedas)

        # Tratar os dados
        dados_tratados = tratar_dados(dados_com_timestamp)

        # Converter o JSON para um DataFrame do PySpark
        df = spark.createDataFrame(dados_tratados)

        # Renomear as colunas para português
        df = df.withColumnRenamed("id", "id") \
               .withColumnRenamed("symbol", "simbolo") \
               .withColumnRenamed("name", "nome") \
               .withColumnRenamed("image", "imagem") \
               .withColumnRenamed("current_price", "preco_atual") \
               .withColumnRenamed("market_cap", "capitalizacao_mercado") \
               .withColumnRenamed("market_cap_rank", "rank_capitalizacao") \
               .withColumnRenamed("fully_diluted_valuation", "valor_total_diluido") \
               .withColumnRenamed("total_volume", "volume_total") \
               .withColumnRenamed("high_24h", "maior_preco_24h") \
               .withColumnRenamed("low_24h", "menor_preco_24h") \
               .withColumnRenamed("price_change_24h", "variacao_preco_24h") \
               .withColumnRenamed("price_change_percentage_24h", "variacao_percentual_24h") \
               .withColumnRenamed("market_cap_change_24h", "variacao_capitalizacao_24h") \
               .withColumnRenamed("market_cap_change_percentage_24h", "variacao_percentual_capitalizacao_24h") \
               .withColumnRenamed("circulating_supply", "oferta_circulante") \
               .withColumnRenamed("total_supply", "oferta_total") \
               .withColumnRenamed("max_supply", "oferta_maxima") \
               .withColumnRenamed("ath", "preco_maximo_historico") \
               .withColumnRenamed("ath_change_percentage", "variacao_percentual_preco_maximo") \
               .withColumnRenamed("ath_date", "data_preco_maximo") \
               .withColumnRenamed("atl", "preco_minimo_historico") \
               .withColumnRenamed("atl_change_percentage", "variacao_percentual_preco_minimo") \
               .withColumnRenamed("atl_date", "data_preco_minimo") \
               .withColumnRenamed("roi", "retorno_investimento") \
               .withColumnRenamed("last_updated", "ultima_atualizacao") \
               .withColumnRenamed("data_hora_coleta", "data_hora_coleta")

        # Mostrar o schema e os dados
        df.printSchema()
        df.show()

        # Criar a tabela se não existir
        criar_tabela_se_nao_existir(TABELA_HISTORICO, schema_criptomoedas)

        # Converter o DataFrame para uma lista de dicionários Python
        dados_para_salvar = df.toJSON().map(lambda x: json.loads(x)).collect()

        # Salvar os dados no BigQuery
        salvar_dados_bigquery(dados_para_salvar, TABELA_HISTORICO)

        # Registrar a execução como sucesso
        registrar_execucao(True)

    except Exception as e:
        logger.error(f"Erro durante a execução: {e}")
        # Registrar a execução como falha
        registrar_execucao(False)
    finally:
        # Encerrar a sessão do Spark
        spark.stop()

if __name__ == "__main__":
    main()