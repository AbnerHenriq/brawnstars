import configparser
import requests
import pandas as pd
import duckdb
from concurrent.futures import ThreadPoolExecutor


# Ler arquivo de configuração
config = configparser.ConfigParser()
config.read('config.ini')

# Obter token 
token = config['API']['token']

# Função para ler as tags dos jogadores de um arquivo
def get_player_tags(filename):
    with open(filename, 'r') as file:
        tags = file.readlines()
    return [tag.strip() for tag in tags]

# Função para fazer a requisição da API e retornar os dados desejados
def fetch_player_data(tag, headers, url):
    formatted_tag = '%23' + tag.replace('#', '')  # Formatar a tag para o request
    response = requests.get(url.format(formatted_tag), headers=headers)
    if response.status_code == 200:
        data = response.json()
        return {
            'tag': data.get('tag'),
            'name': data.get('name'),
            'expLevel': data.get('expLevel'),
            'trophies': data.get('trophies')
        }
    else:
        print(f'Erro na requisição para tag {tag}: {response.status_code}')
        print(response.text)
        return None

# URL base para a API
url = 'https://api.brawlstars.com/v1/players/{}'
headers = { 
    'Authorization': f'Bearer {token}'
}

# Obter as tags dos jogadores
player_tags = get_player_tags('requests/utils/player_tags.txt')

# Conectar ao banco de dados
con = duckdb.connect('database/brawnstars.duckdb')

# Criar tabela se não existir
con.execute("""
CREATE TABLE IF NOT EXISTS players (
    tag VARCHAR,
    name VARCHAR,
    expLevel VARCHAR,
    trophies VARCHAR
)
""")

# Usar ThreadPoolExecutor para paralelizar as requisições
with ThreadPoolExecutor(max_workers=10) as executor:
    # Fazer requisições em paralelo
    results = list(executor.map(lambda tag: fetch_player_data(tag, headers, url), player_tags))

# Filtrar resultados válidos
filtered_results = [res for res in results if res is not None]

# Se houver resultados válidos, insira-os no banco de dados
if filtered_results:
    df = pd.DataFrame(filtered_results)
    con.from_df(df).insert_into('players')
    print(df)
    
else:
    print("Nenhum dado válido foi obtido.")

# Fechar conexão com o banco
con.close()
