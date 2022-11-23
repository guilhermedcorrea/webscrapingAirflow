import pyodbc
from sqlalchemy import create_engine
import os
from urllib.parse import quote_plus
from sqlalchemy.engine import URL
from sqlalchemy import text
from sqlalchemy import (Column, String, Integer, Boolean, Float, DateTime
    , ForeignKey, join, select, insert, update)
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv
from os import path

load_dotenv()
username = os.getenv('username')
password = os.getenv('password')
host = os.getenv('host')
database = os.getenv('database')


connection_url = URL.create(
    "mssql+pyodbc",
    username=f"{username}",
    password=f"{password}",
    host=f"{host}",
    database=f"{database}",
    query={
        "driver": "ODBC Driver 17 for SQL Server",
        "autocommit": "True",
    },
)


def get_engine():
    engine = create_engine(connection_url).execution_options(
        isolation_level="AUTOCOMMIT", future=True
    )
    return engine



Base = declarative_base()
Session = sessionmaker(bind=get_engine())
session = Session()

class cadastroprodutos(Base):
    __tablename__="cadastroprodutos"
    __table_args__ = {"schema": "Produtos"}
    idcadastro = Column(Integer, primary_key=True)
    idpreco = Column(Integer)
    urljet = Column(String, unique=False, nullable=False)
    categoriashausz = Column(String, unique=False, nullable=False)
    categorias = Column(String, unique=False, nullable=False)
    sku = Column(String, unique=False, nullable=False)
    ean =Column(String, unique=False, nullable=False)
    marca = Column(String, unique=False, nullable=False)
    nomeproduto = Column(String, unique=False, nullable=False)
    descricao = Column(String, unique=False, nullable=False)
    imagens = Column(String, unique=False, nullable=False)
    atributos = Column(String, unique=False, nullable=False)
    skuhausz = Column(String, unique=False, nullable=False)
    nomeprodutohausz =Column(String, unique=False, nullable=False)
    urlproduto =Column(String, unique=False, nullable=False)
    origem =Column(String, unique=False, nullable=False)
    bitcompativel = Column(Boolean, unique=False, nullable=False)
    datacadastrado = Column(DateTime, unique=False, nullable=False)
    idurlbase = Column(Integer)


class ImagensColetadas(Base):
    __tablename__="ImagensColetadas"
    __table_args__ = {"schema": "Produtos"}
    idimaem = Column(Integer, primary_key=True)
    nomeproduto = Column(String, unique=False, nullable=False)
    SKU = Column(String, unique=False, nullable=False)
    marca = Column(String, unique=False, nullable=False)
    ean = Column(String, unique=False, nullable=False)
    imagem = Column(String, unique=False, nullable=False)
    datacadastro = Column(DateTime, unique=False, nullable=False)
    urlcoleta = Column(String, unique=False, nullable=False)