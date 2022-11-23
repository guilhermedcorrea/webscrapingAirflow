from __future__ import annotations
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
import json
import pendulum
from airflow.decorators import dag, task
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
import random
from includes.mssql.conn_sql import get_engine


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

def scroll() -> None:
    lenOfPage = driver.execute_script(
            "window.scrollTo(0, document.body.scrollHeight);var lenOfPage=document.body.scrollHeight;return lenOfPage;")
    match=False
    while(match==False):
        lastCount = lenOfPage
        lenOfPage = driver.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);var lenOfPage=document.body.scrollHeight;return lenOfPage;")
        if lastCount==lenOfPage:
            match=True


def randoms() -> int:
    times = random.randint(3, 6)
    return int(times)
   

    
@dag(
    schedule=None,
    schedule_interval = "0 09 * * *",
    start_date = datetime(2022,10,23, 10 ,0),
    catchup=False,
    tags=['get sellers e precos google'],
)

def google_shopping_get_precos() -> None:

    @task()
    def seelct_urls_google() ->  list[dict]:
        engine = get_engine()
        with engine.begin() as connection:
            urls_sellers = (text("""SELECT DISTINCT TOP(5) urls.[idproduto],pmarca.Marca
            ,urls.[urlanuncio] urls,[dataanuncio]
            ,urls.[referencia] ,urls.[marca], pbasico.NomeProduto, pbasico.SKU
            ,pbasico.IdProduto, pbasico.IdMarca, urls.[loja]
            FROM [HauszMapaDev2].[Produtos].[UrlsBase] AS urls
            JOIN [HauszMapa].[Produtos].[ProdutoBasico] as pbasico
            ON pbasico.EAN = URLS.referencia
            JOIN [HauszMapaDev2].[Produtos].[Marca] as pmarca
            ON pmarca.IdMarca = pbasico.IdMarca
            WHERE urls.[loja] like '%www.google.com.br%'"""))
            call_urls = connection.execute(urls_sellers).all()
            dicts_urls = [{key: value for (key, value) in row.items()} for row in call_urls]

            return json.dumps(dicts_urls,sort_keys=True, default=str)

    @task()
    def get_sellers_prices(url_sellers) -> list:
        lista_dicts: list = []
        produtos = json.loads(url_sellers)
        for row in produtos:
    
            lista_nome_produto:list = []
            lista_eans:list = []
            lista_urls_google:list = []
            lista_sellers:list = []
            lista_precos:list = []
            lista_pagina_anuncio:list =[]

            times = randoms()
            time.sleep(times)

            driver.get(row['urls'])
            driver.implicitly_wait(7)
            scroll()
            time.sleep(1)
            try:
                names = driver.find_elements(By.XPATH
                    ,'//*[@id="sh-osd__online-sellers-cont"]/tr/td[1]/div[1]/a')
                for name in names:
                    lista_sellers.append(name.text)
                    lista_urls_google.append(row['urls'])
                    lista_nome_produto.append(row['NomeProduto'])
                    lista_eans.append(row['referencia'])
            except Exception as e:
                pass


            try:
                prices = driver.find_elements(By.XPATH
                    ,'//*[@id="sh-osd__online-sellers-cont"]/tr/td[4]/div/div[1]')
                for price in prices:
                    preco = price.text.replace("R$","").replace(".","").replace(",",".").strip()
                    lista_precos.append(preco)

            except Exception as e:
                pass

            try:
                perfil_seller = driver.find_elements(By.XPATH
                    ,'//*[@id="sh-osd__online-sellers-cont"]/tr/td[5]/div/a')

                for sellers in perfil_seller:
                    seller = sellers.get_attribute('href')

                    lista_pagina_anuncio.append(seller)
            except Exception as e:
                pass

            name_se = [name for name in lista_sellers if name !='']
        
            for i, num in enumerate(name_se):
                dict_items = {}
                try:
                    dict_items['Seller'] = name_se[i]
                except:
                    dict_items['Seller'] = 'NaoEncontrado'
                    
                try:
                    dict_items['NomeProduto'] = lista_nome_produto[i]
                except:
                    dict_items['NomeProduto'] = 'NaoEncontrado'
                try:
                    dict_items['EanReferencia'] = lista_eans[i]
                except:
                    pass

                dict_items['UrlGoogleShopping'] = lista_urls_google[i]
                try:
                    dict_items['Precos'] = lista_precos[i]
                except:
                    dict_items['Precos'] = 'NaoEncontrado'
                
                try:
                    dict_items['PaginaAnuncioSeller'] = lista_pagina_anuncio[i]
                except:
                    dict_items['PaginaAnuncioSeller'] = 'NaoEncontrado'
                    pass

                lista_dicts.append(dict_items)
                print(dict_items)
        
        precos_google = json.dumps(lista_dicts,sort_keys=True, default=str)
        return precos_google

    url_sellers= seelct_urls_google()
    get_sellers_prices(url_sellers)

google_shopping_get_precos()