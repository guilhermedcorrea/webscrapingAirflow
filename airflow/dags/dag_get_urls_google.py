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
, executable_path=r"/home/debian/pipelines/airflow/dags/includes/chromedriver/chromedriver")


@dag(
    schedule=None,
    schedule_interval = "0 12 * * *",
    start_date = datetime(2022,10,23, 10 ,0),
    catchup=False,
    tags=['get sellers e precos google'],
)

def get_urls_seller_google():

    @task()
    def select_urls_sellers() -> str:
        engine = get_engine()
        with engine.begin() as connection:
            urls_sellers = (text("""SELECT TOP(5) urls.[idproduto] 
                ,urls.[urlanuncio] urls,[dataanuncio]
                ,urls.[referencia] ,urls.[marca], pbasico.NomeProduto, pbasico.SKU
                ,pbasico.IdProduto, pbasico.IdMarca
                FROM [HauszMapaDev2].[Produtos].[UrlsBase] AS urls
                JOIN [HauszMapa].[Produtos].[ProdutoBasico] as pbasico
                ON pbasico.EAN = URLS.referencia
                WHERE urls.[loja] = 'www.google.com.br'"""))
            call_urls = connection.execute(urls_sellers).all()
            dicts = [{key: value for (key, value) in row.items()} for row in call_urls]
            return json.dumps(dicts,sort_keys=True, default=str)

    @task()
    def search_products(sellers_urls) -> (str | None):
        lista_dicts: list = []
        produtos = json.loads(sellers_urls)
        driver.implicitly_wait(3)
     
        for dic in produtos:
  
            time.sleep(3)
            driver.get(r'https://shopping.google.com.br')

            time.sleep(3)
            try:
                search = driver.find_element(By.XPATH,'//*[@id="REsRA"]')
                search.clear()
                search.send_keys(str(dic['referencia']))
            except:
                print("erro busca ean")
            try:
                busca = driver.find_element(By.XPATH
                    ,'//*[@id="kO001e"]/div/div/c-wiz/form/div[2]/div[1]/button/div/span').click()
            except:
                print("erro busca")
            time.sleep(1)
            try:
                google_url = driver.find_elements(By.XPATH
                    ,'//*[@id="rso"]/div/div[2]/div/div[1]/div[1]/div[2]/div[3]/div/a')
                urlgoogle = [url.get_dom_attribute('href') for url in google_url]
                if len(urlgoogle) <= 10:
                    google_url = driver.find_elements(By.XPATH
                        , '//*[@id="rso"]/div/div[2]/div/div/div[1]/div[2]/span/a')
                    urlgoogle = [url.get_dom_attribute('href') for url in google_url]
            except:
                print("valor invalido")

            if next(map(lambda k: k if len(k) >=200 else(k if type(k) == str else 'valorinvalido'),urlgoogle),None):

                dict_item: dict = {}
                if urlgoogle !='valorinvalido' or urlgoogle != None:
                    dict_item['URLGOOGLE'] = str('https://www.google.com/') + str(next(chain(urlgoogle)))
                    dict_item['EAN'] = str(dic['referencia'])
                    dict_item['NOMEPRODUTO'] = str(dic['NomeProduto'])
                    dict_item['Marca'] = dic['marca']
                    dict_item['SKU'] = str(dic['SKU'])
                    lista_dicts.append(dict_item)
                    print(dict_item)

            sellers = json.dumps(lista_dicts,sort_keys=True, default=str)
            return sellers

    sellers_urls = select_urls_sellers()
    search_products(sellers_urls)
    

get_urls_seller_google()