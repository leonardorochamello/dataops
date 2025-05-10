import os
import pandas as pd
import numpy as np
import pyodbc
from sqlalchemy import create_engine, types

def carga():
    # Função para limpar valores discrepantes
    def limpar_valores_discrepantes(df):
        colunas_objeto = df.select_dtypes(include=['object']).columns
        df[colunas_objeto] = df[colunas_objeto].replace("Valor Discrepante", np.nan)
        return df
    # Caminho da pasta onde estão os arquivos
    pasta_silver = r'C:\Users\leona\Desktop\Léo\DataOps\dados_transformados'
    lista_dfs = os.listdir(pasta_silver)

    caminho_atracacao = os.path.join(pasta_silver, lista_dfs[0])
    df_atracacao = []

    for arquivos_atracacao in os.listdir(caminho_atracacao):
        if arquivos_atracacao.endswith('.csv'):
            caminho_arquivo_atracacao = os.path.join(caminho_atracacao,arquivos_atracacao)
            atracacao_csv = pd.read_csv(caminho_arquivo_atracacao, sep=';')
            atracacao_csv = limpar_valores_discrepantes(atracacao_csv)
            df_atracacao.append(atracacao_csv)
    # Define o mapeamento de tipos de colunas para o SQL Server
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

    # Nome da tabela no SQL Server
    tabela_atracacao = 'atracacao_fato'
    # Configurações de conexão
    server = 'localhost'  # Nome do servidor SQL Server
    database = 'master'   # Nome do banco de dados
    username = 'leo'      # Nome de usuário
    password = '12345'    # Senha

    # String de conexão
    connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"

    # Cria a engine de conexão com o SQL Server
    engine = create_engine(connection_string)

    # Exemplo de configuração do engine (ajuste conforme seu servidor e credenciais)
    # engine = create_engine('mssql+pyodbc://usuario:senha@servidor/nome_banco?driver=ODBC+Driver+17+for+SQL+Server')

    # Supondo que df_atracacao foi definido antes e pode ser uma lista de DataFrames
    if isinstance(df_atracacao, list):
        df_atracacao = pd.concat(df_atracacao, ignore_index=True)

    try:
        with engine.connect() as connection:
            # Cria a tabela com apenas o cabeçalho e os tipos de dados
            df_atracacao.head(0).to_sql(
                name=tabela_atracacao,
                con=connection,
                if_exists='replace',
                index=False,
                dtype=mapeamento_atracacao  # dicionário de mapeamento: {'coluna': sqlalchemy.Tipo()}
            )
            print(f"Tabela '{tabela_atracacao}' criada com sucesso.")

            # Insere os dados na tabela
            df_atracacao.to_sql(
                name=tabela_atracacao,
                con=connection,
                if_exists='append',
                index=False
            )
            print(f"Dados inseridos na tabela '{tabela_atracacao}' com sucesso.")

    except Exception as e:
        print(f"Erro ao inserir dados no SQL Server: {e}")
