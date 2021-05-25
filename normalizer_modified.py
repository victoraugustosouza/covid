import pandas as pd
import database as db
import math
import time
import traceback
from datetime import datetime
import requests
import json
from datetime import date
import pandas as pd


start = time.time()
db.execute('SET GLOBAL innodb_lock_wait_timeout = 5000;')
db.execute('SET innodb_lock_wait_timeout = 5000;')

# TODO: importar dados corretamente
# TODO: criar o MER
# TODO: criar um front?
# TODO: documentar processo e obstaculos

# Get incoming data
#variável utilizada quando usamos CSV
path=r'C:\Users\Victor\Documents\Periodo 2020.2\Bioinformática\part-00000-8feada9c-2005-4fe0-b2a5-9f51647d7637.c000 - Copia.csv'


url = "https://imunizacao-es.saude.gov.br/_search"
data_hoje = str(date.today())
payload = json.dumps({
  "size": 10000, # máximo de registros em uma query 
"query": {
    "bool" : {
        "must" : {
        "match": {"estabelecimento_uf": "ac"}
        },
        "filter": {
         "range" : {
            "vacina_dataAplicacao" : {
                 "gte": "2021-05-13",
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
#colocando todos os registros em uma lista para transformar em dataframe
for i in res:
  lista.append(i['_source'])
data = pd.json_normalize(lista)

#transformando nomes das colunas em minusculas 
column_name = list()
for col in data.columns:
    column_name.append(col.lower())

data.columns = column_name   

#data = pd.read_csv(path, delimiter=";", encoding='utf8')

# Renomeia colunas utilizadas
cols = {
    'document_id': 'id',
    'paciente_id': 'paciente_id', 
    'paciente_datanascimento': 'paciente_data_nascimento', 
    'paciente_enumsexobiologico': 'paciente_sexo', 
    'paciente_enumSexoBiologico': 'paciente_sexo', 
    'paciente_racacor_codigo': 'paciente_raca_codigo', 
    'paciente_racaCor_codigo'  : 'paciente_raca_codigo', 
    'paciente_racacor_valor': 'paciente_raca_nome', 
    'paciente_racaCor_valor' : 'paciente_raca_nome', 
    'paciente_endereco_coibgemunicipio': 'paciente_municipio_codigo', 
    'paciente_endereco_nmmunicipio': 'paciente_municipio_nome', 
    'paciente_endereco_copais': 'paciente_pais_codigo',
    'paciente_endereco_coPais': 'paciente_pais_codigo',
    'paciente_endereco_nmpais': 'paciente_pais_nome',
    'paciente_endereco_nmPais': 'paciente_pais_nome',
    'paciente_endereco_uf': 'paciente_estado',
    'paciente_endereco_cep': 'paciente_cep',
    'paciente_nacionalidade_enumnacionalidade': 'paciente_nacionalidade',
    'paciente_nacionalidade_enumNacionalidade': 'paciente_nacionalidade',
    'estabelecimento_valor': 'estabelecimento_codigo',
    'estabelecimento_razaosocial': 'estabelecimento_razao_social',
    'estalecimento_nofantasia': 'estabelecimento_nome',
    'estabelecimento_municipio_codigo': 'estabelecimento_municipio_codigo',
    'estabelecimento_municipio_nome': 'estabelecimento_municipio_nome',
    'estabelecimento_uf': 'estabelecimento_estado',
    'vacina_grupoatendimento_codigo': 'grupo_atendimento_codigo',
    'vacina_grupoatendimento_nome': 'grupo_atendimento_nome',
    'vacina_categoria_codigo': 'vacina_categoria_codigo',
    'vacina_categoria_nome': 'vacina_categoria_nome',
    'vacina_lote': 'vacina_lote',
    'vacina_fabricante_nome': 'fabricante_nome',
    'vacina_fabricante_referencia': 'fabricante_referencia',
    'vacina_dataaplicacao': 'data_aplicacao',
    'vacina_descricao_dose': 'descricao_dose',
    'vacina_codigo': 'vacina_codigo',
    'vacina_nome': 'vacina_nome',
    'sistema_origem': 'sistema_origem',
    'data_importacao_rnds': 'data_importacao_rnds'
}

# Remove colunas não utilizadas
non_used_columns = list(set(data.columns) - set(cols.keys()))
data = data.drop(columns=non_used_columns)
data.rename(columns=cols, inplace=True)
#data.to_csv("output_1.csv",sep=";")

def format_date(date_str):
    try:
        if(len(date_str) < 6):
            raise Exception()
        return f"'{date_str}'"
    except Exception as e:
        return 'NULL'


# Pega dados no banco de dados, serão usados para atribuir as chaves estrangeiras
print('Fetching database data.')
pacientes_db = db.query('SELECT id, sexo_id FROM pacientes').set_index('id')
racas_db = db.query('SELECT id, nome FROM racas').set_index('id')
sexos_db = db.query('SELECT id, codigo FROM sexos').set_index('codigo')
nacionalidades_db = db.query('SELECT id, codigo FROM nacionalidades').set_index('codigo')
paises_db = db.query('SELECT id, codigo FROM paises').set_index('codigo')
estados_db = db.query('SELECT id, codigo FROM estados').set_index('codigo')
municipios_db = db.query('SELECT id, codigo FROM municipios').set_index('codigo')
fabricantes_db = db.query('SELECT id, referencia FROM fabricantes').set_index('referencia')
grupos_de_atendimento_db = db.query('SELECT id, nome FROM grupos_de_atendimento').set_index('id')
categorias_db = db.query('SELECT id, nome FROM categorias').set_index('id')
vacinas_db = db.query('SELECT id, nome FROM vacinas').set_index('id')
sistemas_db = db.query('SELECT id, nome FROM sistemas').set_index('nome')
estabelecimentos_db = db.query('SELECT id, estado_id FROM estabelecimentos').set_index('id')
tipos_de_dose_db = db.query('SELECT id, descricao FROM tipos_de_dose').set_index('descricao')


# A funções abaixo servem para criar as colunas de chave estrangeiras, que irão ligar um dado a uma outra tabela
# 1. Verifica se o dado ja existe no BD
# 2. Se já existe pega o ID, se nao existe insere e pega o ID
# 3. Atualiza a tabela local pra garantir a validade das próximas verificações


def add_column_sexo_id(sexo):
    sexo = sexo.upper() if type(sexo) == str else 99
    if(sexo in sexos_db.index):
        return sexos_db.loc[sexo]['id']
    else:
        id = db.execute(f'''INSERT INTO sexos(codigo, nome) VALUES ('{sexo}', '{sexo}')''')
        sexos_db.loc[sexo] = [None]
        return id

def add_column_raca_id(row):
    raca_id = row['paciente_raca_codigo']
    raca_id = 99 if math.isnan(int(raca_id)) else int(raca_id)
    raca_nome = row['paciente_raca_nome']
    if(raca_id in racas_db.index):
        return raca_id
    else:
        id = db.execute(f'''INSERT INTO racas(id, nome) VALUES ('{raca_id}', '{raca_nome}')''')
        racas_db.loc[raca_id] = [None]
        return raca_id

def add_column_nacionalidade_id(nacionalidade):
    nacionalidade = nacionalidade.upper() if type(nacionalidade) == str else '99'
    if(nacionalidade in nacionalidades_db.index):
        return nacionalidades_db.loc[nacionalidade]['id']
    else:
        id = db.execute(f'''INSERT INTO nacionalidades(codigo, nome) VALUES ('{nacionalidade}', '{nacionalidade}')''')
        nacionalidades_db.loc[nacionalidade] = [None]
        return id

def add_column_municipio_id(row):
    municipio_id = row['paciente_municipio_codigo']
    if municipio_id=='':
        municipio_id=9999
    #print(municipio_id)
    municipio_id = 9999 if math.isnan(int(municipio_id)) else int(municipio_id)
    municipio_nome = row['paciente_municipio_nome'].replace("'", '') if type(row['paciente_municipio_nome']) == str else ''
    if(municipio_id in municipios_db.index):
        return municipios_db.loc[municipio_id]['id']
    else:
        id = db.execute(f'''INSERT INTO municipios(codigo, nome) VALUES ('{municipio_id}', '{municipio_nome}')''')
        municipios_db.loc[municipio_id] = [None]
        return id

def add_column_estado_id(estado):
    estado = estado.upper() if type(estado) == str else 99
    if(estado in estados_db.index):
        return estados_db.loc[estado]['id']
    else:
        id = db.execute(f'''INSERT INTO estados(codigo, nome) VALUES ('{estado}', '{estado}')''')
        estados_db.loc[estado] = [None]
        return id



def add_column_pais_id(row):
    pais_id = row['paciente_pais_codigo']
    if pais_id=='':
        pais_id=999
    pais_id = 999 if math.isnan(int(pais_id)) else int(pais_id)
    pais_nome = row['paciente_pais_nome']
    if(pais_id in paises_db.index):
        return paises_db.loc[pais_id]['id']
    else:
        id = db.execute(f'''INSERT INTO paises(codigo, nome) VALUES ('{pais_id}', '{pais_nome}')''')
        paises_db.loc[pais_id] = [None]
        return id

def add_column_estabelecimento_municipio_id(row):
    est_municipio_id = row['estabelecimento_municipio_codigo']
    est_municipio_id = 9999 if math.isnan(est_municipio_id) else int(est_municipio_id)
    est_municipio_nome = row['estabelecimento_municipio_nome'].replace("'", '') if type(row['estabelecimento_municipio_nome']) == str else ''
    if(est_municipio_id in municipios_db.index):
        return municipios_db.loc[est_municipio_id]['id']
    else:
        id = db.execute(f'''INSERT INTO municipios(codigo, nome) VALUES ('{est_municipio_id}', '{est_municipio_nome}')''')
        municipios_db.loc[est_municipio_id] = [None]
        return id

def add_column_estabelecimento_estado_id(estado):
    estado = estado.upper() if type(estado) == str else 99
    if(estado in estados_db.index):
        return estados_db.loc[estado]['id']
    else:
        id = db.execute(f'''INSERT INTO estados(codigo, nome) VALUES ('{estado}', '{estado}')''')
        estados_db.loc[estado] = [None]
        return id

def add_column_estabelecimento_id(row):
    est_id = row['estabelecimento_codigo']
    if est_id=='':
        est_id=9999
    est_id = 99999 if math.isnan(int(est_id)) else int(est_id)
    est_razao_social = row['estabelecimento_razao_social']
    est_nome = row['estabelecimento_nome']
    est_estado_id = row['estabelecimento_estado_id']
    est_municipio_id = row['estabelecimento_municipio_id']
    if(est_id in estabelecimentos_db.index):
        return est_id
    else:
        id = db.execute(f'''INSERT INTO estabelecimentos(id, razao_social, nome, municipio_id, estado_id) VALUES ({est_id}, '{est_razao_social}', '{est_nome}', '{est_municipio_id}', '{est_estado_id}')''')
        estabelecimentos_db.loc[est_id] = [None]
        return est_id

def add_column_vacina_id(row):
    vacina_id = row['vacina_codigo']
    if( vacina_id=='' or vacina_id==None):
        vacina_id=99
    vacina_id = 99 if math.isnan(int(vacina_id)) else int(vacina_id)
    vacina_nome = row['vacina_nome']
    if(vacina_id in vacinas_db.index):
        return vacina_id
    else:
        id = db.execute(f'''INSERT INTO vacinas(id, nome) VALUES ('{vacina_id}', '{vacina_nome}')''')
        vacinas_db.loc[vacina_id] = [None]
        return vacina_id

def add_column_categoria_id(row):
    categoria_id = row['vacina_categoria_codigo']
    if categoria_id=='' or categoria_id ==None:
        categoria_id = 99 
    #print(categoria_id)  
    categoria_id = 99 if math.isnan(int(categoria_id)) else int(categoria_id)
    categoria_nome = row['vacina_categoria_nome']
    if(categoria_id in categorias_db.index):
        return categoria_id
    else:
        id = db.execute(f'''INSERT INTO categorias(id, nome) VALUES ('{categoria_id}', '{categoria_nome}')''')
        categorias_db.loc[categoria_id] = [None]
        return categoria_id

def add_column_grupo_de_atendimento_id(row):
    grp_atd_id = row['grupo_atendimento_codigo']
    if grp_atd_id=='' or grp_atd_id ==None:
        grp_atd_id = 99 
    else:
        grp_atd_id=int(grp_atd_id)
    grp_atd_id = 99 if math.isnan(grp_atd_id) else int(grp_atd_id)
    grp_atd_nome = row['grupo_atendimento_nome']
    if(grp_atd_id in grupos_de_atendimento_db.index):
        return grp_atd_id
    else:
        id = db.execute(f'''INSERT INTO grupos_de_atendimento(id, nome) VALUES ('{grp_atd_id}', '{grp_atd_nome}')''')
        grupos_de_atendimento_db.loc[grp_atd_id] = [None]
        return grp_atd_id

def add_column_sistema_id(sistema):
    sistema = sistema.upper()
    if(sistema in sistemas_db.index):
        return sistemas_db.loc[sistema]['id']
    else:
        id = db.execute(f'''INSERT INTO sistemas(nome) VALUES ('{sistema}')''')
        sistemas_db.loc[sistema] = [None]
        return id

def add_column_fabricante_id(row):
    fabricante_id = row['fabricante_referencia'].upper() if type(row['fabricante_referencia']) == str else '99'
    fabricante_nome = 'Sem referencia' if fabricante_id == '99' else row['fabricante_nome']
    if(fabricante_id in fabricantes_db.index):
        return fabricantes_db.loc[fabricante_id]['id']
    else:
        id = db.execute(f'''INSERT INTO fabricantes(referencia, nome) VALUES ('{fabricante_id}', '{fabricante_nome}')''')
        fabricantes_db.loc[fabricante_id] = [None]
        return id

def add_column_tipo_de_dose_id(dose):
    tipo_de_dose_id = dose.upper() if type(dose) == str else '99'
    if(tipo_de_dose_id in tipos_de_dose_db.index):
        return tipos_de_dose_db.loc[tipo_de_dose_id]['id']
    else:
        id = db.execute(f'''INSERT INTO tipos_de_dose(descricao) VALUES ('{tipo_de_dose_id}')''')
        print('tipo de dose:', tipo_de_dose_id, id)
        tipos_de_dose_db.loc[tipo_de_dose_id] = [None]
        return id

def register_pacientes(row):
    paciente_id = row['paciente_id']
    data_nascimento = format_date(row['paciente_data_nascimento'])
    sexo_id = row['paciente_sexo_id']
    raca_id = row['paciente_raca_id']
    nacionalidade_id = row['paciente_nacionalidade_id']
    pais_id = row['paciente_pais_id']
    estado_id = row['paciente_estado_id']
    municipio_id = row['paciente_municipio_id']
    cep = row['paciente_cep']
    if(paciente_id in pacientes_db.index):
        return paciente_id
    else:
        id = db.execute(f'''INSERT INTO pacientes(
            id, data_nascimento, sexo_id, raca_id, nacionalidade_id, pais_id, estado_id, municipio_id, cep)
            VALUES ('{paciente_id}', {data_nascimento}, '{sexo_id}', '{raca_id}', '{nacionalidade_id}', '{pais_id}', '{estado_id}', '{municipio_id}', '{cep}')''')
        pacientes_db.loc[paciente_id] = [None]
        return id

def register_data(row):
    id = row['id']
    paciente_id = row['paciente_id']
    estabelecimento_id = row['estabelecimento_id']
    grupo_atendimento_id = row['grupo_atendimento_id']
    categoria_id = row['categoria_id']
    vacina_lote = row['vacina_lote']
    fabricante_id = row['fabricante_id']
    data_aplicacao = format_date(row['data_aplicacao'])
    vacina_id = row['vacina_id']
    sistema_origem_id = row['sistema_origem_id']
    data_importacao_rnds = format_date(row['data_importacao_rnds'])
    tipo_de_dose_id = row['tipo_de_dose_id']

    db.execute(f'''INSERT IGNORE INTO registros(id, paciente_id, estabelecimento_id, grupo_atendimento_id, categoria_id,
        vacina_id, fabricante_id, vacina_lote, data_aplicacao, tipo_de_dose_id, sistema_origem_id, data_importacao_rnds) 
        VALUES ('{id}', '{paciente_id}', '{estabelecimento_id}', '{grupo_atendimento_id}', '{categoria_id}', 
        '{vacina_id}', '{fabricante_id}', '{vacina_lote}', {data_aplicacao}, '{tipo_de_dose_id}', '{sistema_origem_id}', {data_importacao_rnds})''')
    
try:
    # Cria as colunas de chaves estrangeiras e registra os valores em suas respectivas tabelas
    print('Processando coluna sexo id')
    data['paciente_sexo_id'] = data['paciente_sexo'].apply(add_column_sexo_id)
    db.commit()

    print('Processando coluna raca id')
    data['paciente_raca_id'] = data.apply(add_column_raca_id, axis=1)
    db.commit()

    print('Processando coluna nacionalidade id')
    data['paciente_nacionalidade_id'] = data['paciente_nacionalidade'].apply(add_column_nacionalidade_id)
    db.commit()

    data['paciente_pais_id'] = data.apply(add_column_pais_id, axis=1)
    data['paciente_estado_id'] = data['paciente_estado'].apply(add_column_estado_id)
    db.commit()

    print('Processando coluna municipio id')
    data['paciente_municipio_id'] = data.apply(add_column_municipio_id, axis=1)
    db.commit()

    print('Processando coluna estabelecimento estado id')
    data['estabelecimento_estado_id'] = data['estabelecimento_estado'].apply(add_column_estado_id)
    db.commit()

    print('Processando coluna estabelecimento municipio id')
    data['estabelecimento_municipio_id'] = data.apply(add_column_municipio_id, axis=1)
    db.commit()

    print('Processando coluna estabelecimento id')
    data['estabelecimento_id'] = data.apply(add_column_estabelecimento_id, axis=1)
    db.commit()

    print('Processando coluna categoria id')
    data['categoria_id'] = data.apply(add_column_categoria_id, axis=1)
    db.commit()

    print('Processando coluna vacina id')
    data['vacina_id'] = data.apply(add_column_vacina_id, axis=1)
    db.commit()

    print('Processando coluna atendimento id')
    data['grupo_atendimento_id'] = data.apply(add_column_grupo_de_atendimento_id, axis=1)
    db.commit()

    print('Processando sistema origem id')
    data['sistema_origem_id'] = data['sistema_origem'].apply(add_column_sistema_id)
    db.commit()

    print('Processando coluna fabricante id')
    data['fabricante_id'] = data.apply(add_column_fabricante_id, axis=1)
    db.commit()

    print('Processando coluna tipo de dose id')
    data['tipo_de_dose_id'] = data['descricao_dose'].apply(add_column_tipo_de_dose_id)
    db.commit()

    print('Fazendo registro dos dados')
    # Registra os dados principais
    chunk_size = 5000
    chunk_index = 0
    chunk_total = math.ceil(data.shape[0] / chunk_size)
    while(True):
        chunk = data[chunk_index*chunk_size:chunk_index*chunk_size + chunk_size]
        print(f'Processando pacientes, chunk {chunk_index}/{chunk_total}')
        if(chunk.size == 0):
            break

        chunk.apply(register_pacientes, axis=1)
        db.commit()
        chunk_index += 1

    chunk_size = 5000
    chunk_index = 0
    chunk_total = math.ceil(data.shape[0] / chunk_size)
    while(True):
        chunk = data[chunk_index*chunk_size:chunk_index*chunk_size + chunk_size]
        print(f'Processando dados principais, chunk {chunk_index}/{chunk_total}')
        if(chunk.size == 0):
            break

        chunk.apply(register_data, axis=1)
        db.commit()
        chunk_index += 1
        
except Exception as e:
    print('Error message:', e)
    traceback.print_exc()
    db.rollback()


end = time.time()
print('Duration in secs:', end - start)