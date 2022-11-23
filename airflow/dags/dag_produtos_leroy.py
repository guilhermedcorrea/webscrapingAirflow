from __future__ import annotations
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
import json
import pendulum
from airflow.decorators import dag, task
from includes.mssql.conn_sql import get_engine
from itertools import chain
from sqlalchemy import text
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from datetime import datetime
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.actions.wheel_input import ScrollOrigin
from selenium import webdriver
import time
from selenium.webdriver.support import expected_conditions as EC
import re

options = webdriver.ChromeOptions()
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option('useAutomationExtension', False)
options.add_argument("--headless")
options.add_argument("--disable-gpu")
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--no-sandbox')
options.add_argument("--start-maximized")


driver = webdriver.Chrome(options=options
, executable_path=r"/home/debian/airflowapp/airflow/dags/includes/chromedriver/chromedriver")


def scroll_page() -> None:
    lenOfPage = driver.execute_script(
        "window.scrollTo(0, document.body.scrollHeight);var lenOfPage=document.body.scrollHeight;return lenOfPage;")
    match=False
    while(match==False):
        lastCount = lenOfPage
        time.sleep(3)
        lenOfPage = driver.execute_script(
            "window.scrollTo(0, document.body.scrollHeight);var lenOfPage=document.body.scrollHeight;return lenOfPage;")
        if lastCount==lenOfPage:
            match=True

@dag(
    schedule=None,
    schedule_interval = "0 05 * * *",
    start_date = datetime(2022,10,23, 10 ,0),
    catchup=False,
    tags=['get atributos e precos Leroy'],
)
def get_atributos_leroy() -> str:

    @task()
    def select_query_hausz_mapa_leroy() -> str:

        engine = get_engine()
        with engine.begin() as connection:

            urls_sellers = (text("""SELECT top(3) urls.[idproduto] 
                    ,urls.[urlanuncio] urls,[dataanuncio]
                    ,urls.[referencia] ,urls.[marca], pbasico.NomeProduto, pbasico.SKU
                    ,pbasico.IdProduto, pbasico.IdMarca
                    FROM [HauszMapaDev2].[Produtos].[UrlsBase] AS urls
                    JOIN [HauszMapa].[Produtos].[ProdutoBasico] as pbasico
                    ON pbasico.EAN = URLS.referencia
                    WHERE urls.[loja] like '%www.leroymerlin.com.br%'"""))
                
            call_urls = connection.execute(urls_sellers).all()
            dicts = [{key: value for (key, value) in row.items()} for row in call_urls]
            return json.dumps(dicts,sort_keys=True, default=str)

    @task()
    def produtos_leroy(seller_urls: dict) -> str:
        lista_dicts = []
        produtos = json.loads(seller_urls)
        for produto in produtos:
            
            if isinstance(produto, dict):
                driver.implicitly_wait(7)
                driver.get(produto.get('urls'))
                dict_produtos = {}
                
                dict_produtos['PAGINAPRODUTO'] = produto.get('urlanuncio')
                try:
                    preco = driver.find_elements(By.XPATH
                    ,'/html/body/div[9]/div/div[1]/div[3]/div[1]/div/div[1]/div[1]/div/span')
                    precos = [p.text for p in preco]
                    precoproduto = "".join(list(chain(precos)))
                    dict_produtos['Preco'] = precoproduto
                except:
                    pass

                try:
                    nome_produto = driver.find_elements(By.XPATH
                    ,'/html/body/div[9]/div/div[1]/div[2]/div[1]/div[1]/h1')[0].text
                    dict_produtos['nomeproduto'] = nome_produto
                except:
                    print('erro')

                lista_imagens = []
                imagens = driver.find_elements(By.XPATH
                , '/html/body/div[9]/div/div[1]/div[1]/div[1]/div/div/div[2]/div[1]/div[2]/div/div/img')
                cont = 0
                for imagem in imagens:
                    dict_img = {}
                    dict_img['imagem'+str(cont)] = imagem.get_attribute('src')
                    lista_imagens.append(dict_img)
                    cont +=1
                dict_produtos['IMAGENS'] =  lista_imagens

                try:
                    categorias = driver.find_elements(By.XPATH
                    , '/html/body/div[6]/div/ul[1]/li/a')
                    cont = 0
                    for categoria in categorias:
                        dict_produtos['categoria'+str(cont)] = categoria.get_attribute('title')
                        cont+=1

                except:
                    print("erro")

                try:
                    url_categoria = driver.find_elements(By.XPATH
                    , '/html/body/div[6]/div/ul[1]/li/a')
                    cont=0
                    for urlsc in url_categoria:
                        dict_produtos['urlcategoria'+str(cont)] = urlsc.get_attribute('href')
                        cont+=1
                except:
                    print("erro")

            try:
                descricao = driver.find_elements(By.XPATH
                ,'/html/body/div[9]/div/div[1]/div[2]/div[2]/div/div[2]/div/div')
                for descri in descricao:
                    dict_produtos['descricao'] = descri.text
            except:
                print("erro")

            ref_key = driver.find_elements(By.XPATH
            ,'/html/body/div[9]/div/div[4]/div[2]/table/tbody/tr/th')
            valor_esp = driver.find_elements(By.XPATH
            ,'/html/body/div[9]/div/div[4]/div[2]/table/tbody/tr/td')
            cont = 0
            for val in ref_key:
                dict_produtos[val.text] = valor_esp[cont].text
                cont+=1

            referencias = driver.find_elements(By.XPATH
            ,'/html/body/div[9]/div/div[1]/div[2]/div[1]/span')
            for ref in referencias:
                if ref.get_attribute('itemprop') == 'mpn':
                    dict_produtos['MPN'] = ref.get_attribute('content')
                   
                else:
                    pass

            
            try:
                url_categoria = driver.find_elements(By.XPATH, '/html/body/div[6]/div/ul[1]/li/a')
                cont=0
                for urlsc in url_categoria:
                    dict_produtos['urlcategoria'+str(cont)] = urlsc.get_attribute('href')
                    cont+=1
            except:
                print("erro")

            try:
                descricao = driver.find_elements(By.XPATH
                ,'/html/body/div[9]/div/div[1]/div[2]/div[2]/div/div[2]/div/div')
                for descri in descricao:
                    dict_produtos['descricao'] = descri.text
            except:
                print("erro")

            ref_key = driver.find_elements(By.XPATH
            ,'/html/body/div[9]/div/div[4]/div[2]/table/tbody/tr/th')
            valor_esp = driver.find_elements(By.XPATH
            ,'/html/body/div[9]/div/div[4]/div[2]/table/tbody/tr/td')
            cont = 0
            for val in ref_key:
                dict_produtos[val.text] = valor_esp[cont].text
                cont+=1

            referencias = driver.find_elements(By.XPATH
            ,'/html/body/div[9]/div/div[1]/div[2]/div[1]/span')
            for ref in referencias:
                if ref.get_attribute('itemprop') == 'mpn':
                    dict_produtos['MPN'] = ref.get_attribute('content')
                   
                else:
                    pass
            dict_produtos['imagens'] = lista_imagens
            lista_dicts.append(dict_produtos)
     
        
        produtos_leroy_jsons = json.dumps(lista_dicts,sort_keys=True, default=str)
        return produtos_leroy_jsons
        

    seller_urls = select_query_hausz_mapa_leroy()
    produtos_leroy(seller_urls)

get_atributos_leroy()