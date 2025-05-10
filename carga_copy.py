import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, types
import urllib

def carga():
    def limpar_valores_discrepantes(df):
        colunas_objeto = df.select_dtypes(include=['object']).columns
        df[colunas_objeto] = df[colunas_objeto].replace("Valor Discrepante", np.nan)
        return df

    pasta_silver = r'C:\Users\leona\Desktop\Léo\DataOps\dados_transformados'
    lista_dfs = os.listdir(pasta_silver)

    if not lista_dfs:
        print("Nenhum diretório encontrado na pasta de dados transformados.")
        return

    caminho_atracacao = os.path.join(pasta_silver, lista_dfs[0])
    df_atracacao = []

    for arquivo in os.listdir(caminho_atracacao):
        if arquivo.endswith('.csv'):
            caminho_arquivo = os.path.join(caminho_atracacao, arquivo)
            try:
                df = pd.read_csv(caminho_arquivo, sep=';', encoding='utf-8')
                df = limpar_valores_discrepantes(df)
                df_atracacao.append(df)
            except Exception as e:
                print(f"Erro ao ler o arquivo {arquivo}: {e}")

    if not df_atracacao:
        print("Nenhum dado foi carregado dos arquivos CSV.")
        return

    df_atracacao = pd.concat(df_atracacao, ignore_index=True)

    mapeamento_atracacao = {
        'IDAtracacao': types.INTEGER(),
        'CDTUP': types.VARCHAR(length=50),
        'IDBerco': types.VARCHAR(length=50),
        'Berco': types.VARCHAR(length=100),
        'PortoAtracacao': types.VARCHAR(length=100),
        'DataAtracacao': types.DATETIME(),
        'DataChegada': types.DATETIME(),
        'DataDesatracacao': types.DATETIME(),
        'DataInicioOperacao': types.DATETIME(),
        'DataTerminoOperacao': types.DATETIME(),
        'TipoOperacao': types.VARCHAR(length=100),
        'TipoNavegacaoAtracacao': types.VARCHAR(length=100),
        'Municipio': types.VARCHAR(length=100),
        'UF': types.VARCHAR(length=50),
        'TEsperaAtracacao': types.VARCHAR(length=50),
        'TesperaInicioOp': types.VARCHAR(length=50),
        'TOperacao': types.VARCHAR(length=50),
        'TEsperaDesatracacao': types.VARCHAR(length=50),
        'TAtracado': types.VARCHAR(length=50),
    }

    tabela_atracacao = 'atracacao_fato'
    server = 'localhost'
    database = 'master'
    username = 'leo'
    password = '12345'

    params = urllib.parse.quote_plus(
        f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}'
    )
    connection_string = f'mssql+pyodbc:///?odbc_connect={params}'
    engine = create_engine(connection_string)

    try:
        with engine.begin() as connection:
            df_atracacao.head(0).to_sql(
                name=tabela_atracacao,
                con=connection,
                if_exists='replace',
                index=False,
                dtype=mapeamento_atracacao
            )
            print(f"Tabela '{tabela_atracacao}' criada com sucesso.")

            df_atracacao.to_sql(
                name=tabela_atracacao,
                con=connection,
                if_exists='append',
                index=False
            )
            print(f"Dados inseridos na tabela '{tabela_atracacao}' com sucesso.")

    except Exception as e:
        print(f"Erro ao inserir dados no SQL Server: {e}")
