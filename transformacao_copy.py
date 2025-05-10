from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import year, month, col, when
from pathlib import Path
from sqlalchemy import create_engine, types
import os
import shutil
import pandas as pd
def transformacao():

    def criar_df(spark, caminho, assunto, anos):
        df = None
        for ano in anos:
            nome_arquivo = ano + assunto + '.txt'
            caminho_arquivo_ano = os.path.join(caminho, ano, nome_arquivo)

            if os.path.exists(caminho_arquivo_ano):
                df_ano = spark.read.csv(caminho_arquivo_ano, header=True, inferSchema=True, sep=";")

                if df is None:
                    df = df_ano
                else:
                    df = df.union(df_ano)
        return df

    # Inicializar a sessão do Spark
    spark = SparkSession.builder \
        .appName('antaq_etl') \
        .config('spark.jars', '/usr/lib/jvm/java-8-openjdk-amd64/jre/lib/ext/mssql-jdbc-12.2.0.jre8.jar') \
        .getOrCreate()

    # Caminho para os arquivos
    caminho_pasta = r'C:\Users\leona\Desktop\Léo\DataOps\trabalho'
    caminho_pasta_silver = r'C:\Users\leona\Desktop\Léo\DataOps'

    assuntos = [
        'Atracacao', 'Carga', 'Carga_Conteinerizada', 'Carga_Hidrovia',
        'Carga_Regiao', 'Carga_Rio', 'CargaAreas', 'TaxaOcupacao',
        'TaxaOcupacaoComCarga', 'TaxaOcupacaoTOAtracacao', 'TemposAtracacao',
        'TemposAtracacaoParalisacao'
    ]

    # Lista de anos para processar
    anos = [pasta for pasta in os.listdir(caminho_pasta)
            if os.path.isdir(os.path.join(caminho_pasta, pasta)) and pasta.isnumeric()]
    anos_filtrados = anos[:3]

    # Criar os DataFrames
    df_atracacao = criar_df(spark, caminho_pasta, assuntos[0], anos_filtrados)
    df_carga = criar_df(spark, caminho_pasta, assuntos[1], anos_filtrados)
    df_carga_conteinerizada = criar_df(spark, caminho_pasta, assuntos[2], anos_filtrados)
    df_tempos_atracacao = criar_df(spark, caminho_pasta, assuntos[10], anos_filtrados)

    # Adicionar coluna "Ano" e "Mês"
    df_atracacao = df_atracacao.withColumn("Ano", year(col("Data Atracação"))) \
                            .withColumn("Mês", month(col("Data Atracação")))

    # Join com tempos de atracação
    final_df = df_atracacao.join(df_tempos_atracacao, "IDAtracacao", "left")

    df_atracacao_final = final_df.select(
        "IDAtracacao", "CDTUP", "IDBerco", "Berço", "Porto Atracação",
        "Apelido Instalação Portuária", "Complexo Portuário", "Tipo da Autoridade Portuária",
        F.to_timestamp(col("Data Atracação"), "dd/MM/yyyy HH:mm:ss").alias("Data Atracação"),
        F.to_timestamp(col("Data Chegada"), "dd/MM/yyyy HH:mm:ss").alias("Data Chegada"),
        F.to_timestamp(col("Data Desatracação"), "dd/MM/yyyy HH:mm:ss").alias("Data Desatracação"),
        F.to_timestamp(col("Data Início Operação"), "dd/MM/yyyy HH:mm:ss").alias("Data Início Operação"),
        F.to_timestamp(col("Data Término Operação"), "dd/MM/yyyy HH:mm:ss").alias("Data Término Operação"),
        year(F.to_timestamp(col("Data Início Operação"), "dd/MM/yyyy HH:mm:ss")).alias("Ano da data de início da operação"),
        F.lpad(month(F.to_timestamp(col("Data Início Operação"), "dd/MM/yyyy HH:mm:ss")).cast("string"), 2, "0").alias("Mês da data de início da operação"),
        "Tipo de Operação", "Nacionalidade do Armador", "FlagMCOperacaoAtracacao", "Terminal",
        "Município", "UF", "SGUF", "Região Geográfica", "TEsperaAtracacao", "TesperaInicioOp",
        "TOperacao", "TEsperaDesatracacao", "TAtracado", "TEstadia"
    )

    # Processar df_carga_final
    df_carga_final = df_carga.withColumn("Carga_Conteinerizada",
                                        when(col("Carga Geral Acondicionamento") == "Conteinerizada", True).otherwise(False))

    df_carga_final = df_carga_final.join(
        df_carga_conteinerizada,
        df_carga_final["IDCarga"] == df_carga_conteinerizada["IDCarga"],
        "left"
    ).select(
        df_carga_final["*"],
        df_carga_conteinerizada["CDMercadoriaConteinerizada"],
        df_carga_conteinerizada["VLPesoCargaConteinerizada"]
    )

    df_carga_final = df_carga_final.withColumn("Peso_liquido",
                                            when(col("Carga_Conteinerizada"), col("VLPesoCargaConteinerizada"))
                                            .otherwise(col("VLPesoCargaBruta")))

    df_carga_final = df_carga_final.withColumn("CDMercadoria",
                                            when(col("Carga_Conteinerizada"), col("CDMercadoriaConteinerizada"))
                                            .otherwise(col("CDMercadoria")))

    df_carga_final = df_carga_final.drop("CDMercadoriaConteinerizada")

    # Diretório de saída
    output_dir = os.path.join(caminho_pasta_silver, 'dados_transformados')

    # Exportar DataFrames
    df_atracacao_final.write.mode('overwrite') \
        .option('header', True) \
        .option('delimiter', ';') \
        .option('encoding', 'utf-8') \
        .csv(os.path.join(output_dir, 'df_atracacao_final'))

    df_carga_final.write.mode('overwrite') \
        .option('header', True) \
        .option('delimiter', ';') \
        .option('encoding', 'utf-8') \
        .csv(os.path.join(output_dir, 'df_carga_final'))

    print(f'Arquivos exportados com sucesso na pasta {output_dir}')