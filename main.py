import requests
import csv
import pandas as pd
import logging
import time
import os
from datetime import datetime
import sqlite3

# Obter dados
def get_swapi_data(category, page):
    base_url = f"https://swapi.dev/api/{category}/?page={page}"
    response = requests.get(base_url)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        logging.error(f"Erro ao obter dados da categoria {category}, página {page}. Código de status: {response.status_code}")
        return None

#Salva o arquivo em csv
def save_raw_data(data, category):
    with open(f'data/raw_{category}.csv', 'a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        if file.tell() == 0:  # Verifica se o arquivo está vazio para escrever o cabeçalho
            header = list(data['results'][0].keys())
            writer.writerow(header)

#Escrita de dados brutos
        for item in data['results']:
            writer.writerow(item.values())

#Processa e salva os dados em csv
def process_and_save_data(data, category):

#Valida API
    if data['results']:
#Criação de DataFrame
        df = pd.DataFrame(data['results'])

        if category != 'planets':

            if 'height' in df.columns:
#Tratamento
#Tratamento de tipos - converter coluna 'height' para numérica
                df['height'] = pd.to_numeric(df['height'], errors='coerce')

#Tratamento de nulos - preencher valores nulos com 'N/A'
                df.fillna('N/A', inplace=True)

#Salvando os dados tratados
        #Verifica se o arquivo ja existe
        isFile = os.path.isfile(f'data/clean_{category}.csv')
        if isFile == True:
          #Casos exista, faz a escrita sem o cabeçalho
          df.to_csv(f'data/clean_{category}.csv', mode='a', index=False, header=False, sep = ";" )
        else:
          #Casos não exista, faz a escrita com o cabeçalho
          df.to_csv(f'data/clean_{category}.csv', mode='a', index=False, sep = ";" )

    else:
        logging.error(f"Nenhum results encontrado para a categoria {category}.")

#Validar a qualidade dos dados
def validate_data(category):
    clean_data = pd.read_csv(f'./data/clean_{category}.csv', error_bad_lines=False, sep = ";")

#Validar os dados duplicados
    duplicates = clean_data[clean_data.duplicated()]
    if not duplicates.empty:
        logging.warning(f"Dados duplicados encontrados em {category}:\n{duplicates}")

#Validar os dados nulos
    null_values = clean_data.isnull().sum()
    if not null_values.empty:
        logging.warning(f"Valores nulos encontrados em {category}:\n{null_values}")

def armazenaDW(df):
  isFile = os.path.isfile(f'data/dw.csv')
  if isFile == True:
    #Casos exista, faz a escrita sem o cabeçalho
    df.to_csv(f'data/dw.csv', mode='a', index=False, header=False, sep = ";" )
  else:
    #Casos não exista, faz a escrita com o cabeçalho
    df.to_csv(f'data/dw.csv', mode='a', index=False, sep = ";" )

def agregacoesPeople():

  df = pd.read_csv("./data/clean_people.csv", sep = ";")

  ###AGREGAÇÃO DE PESSOAS POR SEXO
  df2 = pd.DataFrame(df.groupby(by=['gender'], sort=True, dropna=False).size().reset_index(name='value'))
  df2 = pd.DataFrame(df2[ (df2['gender'] == 'female') | (df2['gender'] == 'male') | (df2['gender'] == 'hermaphrodite') ])
  df2 = df2[['gender', 'value']].assign(category='people').assign(subcategory='sexo').assign(data=datetime.today()).rename(columns = {'gender':'tipo'})

  armazenaDW(df2)

def agregacoesPlanets():
  ###AGREGAÇÃO DE PLANETAS POR CLIMA
  df = pd.read_csv("./data/clean_planets.csv", sep = ";")

  df2 = pd.DataFrame(df.groupby(by=['climate'], sort=True, dropna=False).size().reset_index(name='value'))
  df2 = df2[['climate', 'value']].assign(category='planets').assign(subcategory='clima').assign(data=datetime.today()).rename(columns = {'climate':'tipo'})
  armazenaDW(df2)

#Atualizar os dados
def update_data():
    categories = ['people', 'planets', 'films']

    for category in categories:
        page = 1
        while True:
            data = get_swapi_data(category, page)
            if data:
                save_raw_data(data, category)
                process_and_save_data(data, category)
                if 'next' in data and data['next'] is not None:
                    page += 1
                else:
                    break
            else:
                logging.error(f"Falha ao obter dados para a categoria {category}.")

        validate_data(category)

#Criar pasta para armazenar
if not os.path.exists('data'):
    os.makedirs('data')

#Salvar log
logging.basicConfig(filename='data_collection.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Funções para operações no banco de dados
def create_tables():
    conn = sqlite3.connect('swapi_data.db')
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS people (
            id INTEGER PRIMARY KEY,
            name TEXT,
            height INTEGER,
            mass INTEGER,
            hair_color TEXT,
            skin_color TEXT,
            eye_color TEXT
            -- Adicione outras colunas necessárias
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS planets (
            id INTEGER PRIMARY KEY,
            name TEXT,
            climate TEXT,
            terrain TEXT
            -- Adicione outras colunas necessárias
        )
    ''')

    conn.commit()
    conn.close()

def insert_data_to_tables():
    conn = sqlite3.connect('swapi_data.db')
    cursor = conn.cursor()

    with open('data/clean_people.csv', 'r') as file:
        csv_reader = csv.DictReader(file)
        rows = []
        for row in csv_reader:
            clean_row = {k: v for k, v in row.items() if v}  # Excluir colunas vazias
            if all(clean_row.values()):  # Verificar se há valores nulos
                rows.append(clean_row)

        if rows:
            cursor.executemany('''
                INSERT INTO people (name, height, mass, hair_color, skin_color, eye_color)
                VALUES (:name, :height, :mass, :hair_color, :skin_color, :eye_color)
            ''', rows)
        else:
            logging.error("Nenhum dado válido para inserir na tabela people.")

    # Repita o mesmo processo para a tabela "planets" com um arquivo diferente

    conn.commit()
    conn.close()



if __name__ == '__main__':    
#Executa uma vez para o agendamento
    update_data()
    agregacoesPeople()
    agregacoesPlanets()
