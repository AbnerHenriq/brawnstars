import requests 
import pandas as pd
import duckdb
import configparser

# Ler arquivo de configuração
config = configparser.ConfigParser()
config.read('config.ini')

# Obter token 
token = config['API']['token']

url = 'https://api.brawlstars.com/v1/brawlers'
headers = { 
    'Authorization': f'Bearer {token}'
}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    data = response.json()
    df = pd.DataFrame(data["items"])
    print(df)

    # Open connection
    con = duckdb.connect('database/brawnstars.duckdb')

    # Insert dataframe
    con.execute("CREATE TABLE IF NOT EXISTS brawlers AS SELECT * FROM df")
    con.execute("INSERT INTO brawlers SELECT * FROM df")

    # Close connection
    con.close()

else:
    print(f'Erro na requisição: {response.status_code}')
    print(response.text)