# Use uma imagem base com Python e Spark
FROM bitnami/spark:3.5.0

# Copie o código para o container
COPY . /app
WORKDIR /app

# Instale as dependências
RUN pip install -r requirements.txt

# Defina o comando para executar o script
CMD ["python", "main.py"]