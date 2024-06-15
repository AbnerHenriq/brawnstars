import requests 
import pandas as pd

url = 'https://api.brawlstars.com/v1/brawlers'
headers = { 
    'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiIsImtpZCI6IjI4YTMxOGY3LTAwMDAtYTFlYi03ZmExLTJjNzQzM2M2Y2NhNSJ9.eyJpc3MiOiJzdXBlcmNlbGwiLCJhdWQiOiJzdXBlcmNlbGw6Z2FtZWFwaSIsImp0aSI6ImNiNWQ5NGM0LWM3NzctNGUwMS05NDk2LWFjM2I1NWYwZWQ0MyIsImlhdCI6MTcxODQ2OTIzMiwic3ViIjoiZGV2ZWxvcGVyLzA5YTA4YmMzLTk4MzUtZDU5My04ODIyLWQ2NzcwNWFjNmFmZiIsInNjb3BlcyI6WyJicmF3bHN0YXJzIl0sImxpbWl0cyI6W3sidGllciI6ImRldmVsb3Blci9zaWx2ZXIiLCJ0eXBlIjoidGhyb3R0bGluZyJ9LHsiY2lkcnMiOlsiNDUuMTY2LjI1MS43OSJdLCJ0eXBlIjoiY2xpZW50In1dfQ.eFB-JpopdSGV9VytwRLrRVykHRRSBU3Sm4-UhYM-aFHL7ZKyWjUe3qlO4YOKWFS1Hf0cK4I-Tb8Pe5hcL_RFmQ'
}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    data = response.json()
    df = pd.DataFrame(data["items"])
    print(df)
else:
    print(f'Erro na requisição: {response.status_code}')
    print(response.text)