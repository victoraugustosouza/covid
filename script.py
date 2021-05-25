import requests
import json
from datetime import date
import pandas as pd

url = "https://imunizacao-es.saude.gov.br/_search"
data_hoje = str(date.today())
payload = json.dumps({
  "size": 10,
"query": {
    "bool" : {
        "must" : {
        "match": {"estabelecimento_uf": "ac"}
        },
        "filter": {
         "range" : {
            "vacina_dataAplicacao" : {
                 "gte": data_hoje,
                 "lte": data_hoje,
                 "format": "yyyy-MM-dd"
             }
         }
    }
  }
}
#,
#  "fields": [
#    "vacina_dataAplicacao",
#    "estabelecimento_uf"
#  ],
#   "_source": False
}

)
headers = {
  'Authorization': 'Basic aW11bml6YWNhb19wdWJsaWM6cWx0bzV0JjdyX0ArI1Rsc3RpZ2k=',
  'Content-Type': 'application/json',
  'Cookie': 'ELASTIC-PROD=1621895938.823.19618.765660'
}


response = requests.request("POST", url, headers=headers, data=payload)
res = response.json()
res = res['hits']['hits']
lista = list()
for i in res:
  lista.append(i['_source'])
df_final = pd.json_normalize(lista)
#print(df_nested_list)
#df_nested_list.to_csv("output_1.csv",sep=";")