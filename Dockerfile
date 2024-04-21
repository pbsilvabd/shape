# Use uma imagem base do Spark
FROM apache/spark:3.5.1

# Defina o diretório de trabalho dentro do contêiner
WORKDIR /app

USER root

# Instale o Python 3 e o PySpark
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    pip3 install pyspark

# Copie todo o conteúdo do diretório atual para o contêiner
COPY . .

# Comando padrão a ser executado quando o contêiner for iniciado
CMD ["python3", "main.py"]

