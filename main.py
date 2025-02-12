import requests
import json
from pyspark.sql import SparkSession
from google.cloud import bigquery
from google.cloud import secretmanager
from datetime import datetime
import os
import tempfile

# Função para acessar segredos no Google Cloud Secret Manager
def acessar_segredo(nome_segredo):
    client = secretmanager.SecretManagerServiceClient()
    resposta = client.access_secret_version(request={"name": nome_segredo})
    return resposta.payload.data.decode("UTF-8")

def acessar_chave_api():
    client = secretmanager.SecretManagerServiceClient()
    nome_segredo = "projects/projeto-treinamento-450619/secrets/chave_api_coingecko/versions/latest"
    resposta = client.access_secret_version(request={"name": nome_segredo})
    return resposta.payload.data.decode("UTF-8")

# Carregar as credenciais do BigQuery
def carregar_credenciais():
    credenciais = acessar_segredo("projects/projeto-treinamento-450619/secrets/credencial_bigquery/versions/latest")
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
        temp_file.write(credenciais)
        return temp_file.name

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = carregar_credenciais()

# Configurações
CHAVE_API = acessar_chave_api()
TIPO_MOEDA = "brl"
DATASET_BIGQUERY = 'cripto_dataset'
TABELA_HISTORICO = 'tabela_criptomoedas'
URL_API = f"https://api.coingecko.com/api/v3/coins/markets?vs_currency={TIPO_MOEDA}&x_cg_demo_api_key={CHAVE_API}"

# Função para consumir a API
def buscar_dados_api():
    resposta = requests.get(URL_API)
    if resposta.status_code == 200:
        print("Dados da API consumidos com sucesso.")
        return resposta.json()
    else:
        raise Exception(f"Erro ao consumir a API: {resposta.status_code}")

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

# Função para salvar dados no BigQuery
def salvar_dados_bigquery(dados):
    cliente = bigquery.Client()
    referencia_tabela = cliente.dataset(DATASET_BIGQUERY).table(TABELA_HISTORICO)
    tabela = cliente.get_table(referencia_tabela)

    # Inserir os dados no BigQuery
    erros = cliente.insert_rows_json(tabela, dados)

    if erros == []:
        print("Dados inseridos com sucesso no BigQuery.")
    else:
        print(f"Erros ao inserir dados no BigQuery: {erros}")

# Função principal
def main():
    try:
        # Consumir a API
        dados_criptomoedas = buscar_dados_api()

        # Adicionar timestamp aos dados
        dados_com_timestamp = adicionar_timestamp(dados_criptomoedas)

        # Tratar os dados
        dados_tratados = tratar_dados(dados_com_timestamp)

        # Processar os dados com PySpark
        spark = SparkSession.builder \
            .appName("ProcessamentoDadosCriptomoedas") \
            .getOrCreate()

        # Converter o JSON para um DataFrame do PySpark
        df = spark.read.json(spark.sparkContext.parallelize([json.dumps(item) for item in dados_tratados]))

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

        # Converter o DataFrame para uma lista de dicionários Python
        dados_para_salvar = df.toJSON().map(lambda x: json.loads(x)).collect()

        # Salvar os dados no BigQuery
        salvar_dados_bigquery(dados_para_salvar)

        # Encerrar a sessão do Spark
        spark.stop()

    except Exception as e:
        print(f"Erro durante a execução: {e}")

if __name__ == "__main__":
    main()