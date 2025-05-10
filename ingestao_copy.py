from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from webdriver_manager.chrome import ChromeDriverManager
import zipfile
import tarfile
import requests
import os
import shutil
def ingestao():
    def esperar_selecao_ano(driver):
        WebDriverWait(
            driver,10
        ).until(
                EC.visibility_of_element_located((By.TAG_NAME, 'select'))
            )
    def coletar_lista_selecao_anos(driver):
        lista_anos_disponiveis = []
        esperar_selecao_ano(driver)    
        elements = driver.find_elements(By.TAG_NAME, 'select')

        for element in elements:
            anos =  element.text.replace('\n',',').split(',')   

        # Criação de lista com todos os anos disponíveis para filtro
        for ano in anos:
            ano_tratado = ano.strip()
            lista_anos_disponiveis.append(ano_tratado) if ano_tratado != '' else None
        return lista_anos_disponiveis
    def coletar_links_dados(driver, lista_anos_disponiveis):
        dicionario_links_dados = {}
        for ano_filtro in lista_anos_disponiveis:
            WebDriverWait(
                            driver,10
                        ).until(
                                EC.visibility_of_element_located((By.TAG_NAME, 'select'))
                            ).send_keys(
                                        str(ano_filtro)
                                    )
            time.sleep(7)

            links = driver.find_elements(By.TAG_NAME, 'a')

            # Nesse looping retorna os zips de cada ano disponível na página
            for link in links:
                href = link.get_attribute('href')
                if link.text.lower() == 'clique aqui.' and href.endswith('txt/' + str(ano_filtro) + '.zip'):
                    print(f'Capturando link de download: {href}')
                    dicionario_links_dados[ano_filtro] = href
                    break
        return dicionario_links_dados
    def extrair_arquivos(caminho_arquivo, pasta):
        #Descompacta arquivos .zip ou .tar.gz e exclui o arquivo compactado.
        try:
            if caminho_arquivo.endswith('.zip'):
                with zipfile.ZipFile(caminho_arquivo, 'r') as zip_ref:
                    zip_ref.extractall(pasta)  # Extrai os arquivos na pasta de download
            elif caminho_arquivo.endswith('.tar.gz') or caminho_arquivo.endswith('.tar'):
                with tarfile.open(caminho_arquivo, 'r:*') as tar_ref:
                    tar_ref.extractall(caminho_arquivo)  # Extrai os arquivos na pasta de download
            else:
                print(f'Formato não suportado: {caminho_arquivo}')
                return
            
            os.remove(caminho_arquivo)  # Remove o arquivo compactado após extração
            print(f'Arquivo {caminho_arquivo} descompactado e pasta zip removido com sucesso.')
        except Exception as e:
            print(f'Erro ao descompactar {caminho_arquivo}: {e}')
    def baixar_arquivos(url, pasta, ano):
        pasta_ano = os.path.join(pasta, ano) if ano else pasta #Cria o nome da pasta para poder ter os dados por ano

        # Remove a pasta existente (se houver) e recria
        if os.path.exists(pasta_ano):
            shutil.rmtree(pasta_ano)  # Remove a pasta e todo seu conteúdo

        os.makedirs(pasta_ano, exist_ok=True)  # Cria a pasta se não existir

        #Faz o download do arquivo a partir da URL informada.
        response = requests.get(url, timeout=30)  # Faz a requisição do arquivo
        if response.status_code == 200:       
            nome_arquivo = url[-8:]  # Obtém o nome do arquivo a partir da URL
            caminho_arquivo = os.path.join(pasta_ano, nome_arquivo)  # Define o caminho completo do arquivo
            
            with open(caminho_arquivo, 'wb') as arquivo:
                arquivo.write(response.content)  # Salva o conteúdo baixado no arquivo
            
            print(f"Arquivo {nome_arquivo} baixado com sucesso.")
            extrair_arquivos(caminho_arquivo, pasta_ano)  # Chama a função para descompactar o arquivo, se necessário
        else:
            print(f'Erro ao baixar {url} ({response.status_code})')
    url = r'https://web3.antaq.gov.br/ea/sense/download.html#pt'
    pasta_download = r'C:\Users\leona\Desktop\Léo\DataOps\trabalho'

    # Configurações do Selenium
    chrome_options = Options()
    chrome_options.add_argument( '--headless' )  # Executa em modo headless (sem interface gráfica)
    service = Service( ChromeDriverManager().install() )

    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        print('Iniciando busca dos dados...\n')
        driver.get(url)
        esperar_selecao_ano(driver)
        lista_anos = coletar_lista_selecao_anos(driver)
        lista_anos_filtrados = lista_anos[:2]
        dicionario_links = coletar_links_dados(driver, lista_anos_filtrados)

    finally:
        driver.quit()
        print('Web Scrapping Finalizado')
    for ano, link in dicionario_links.items():
        baixar_arquivos(link, pasta_download, str(ano))
        print('*' * 40)
