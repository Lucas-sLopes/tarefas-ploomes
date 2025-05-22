import requests
import pandas as pd
from datetime import date, datetime
from sqlalchemy import create_engine
import schedule
import time as tm
import datetime as dat
from dotenv import load_dotenv
import os


def atualizarBD():

    load_dotenv()


    chave_api = os.getenv("CHAVE_API")
    host = os.getenv("DB_HOST")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    database = os.getenv("DB_NAME")

    skip_ploomes = 0

    colunas = [
        'IdTarefa',
        'TituloTarefa',
        'Cliente',
        'DataCriacao',
        'UltAtualizacao',
        'DataFinalizacao',
        'Finalizado',
        'Responsavel',
        'TarefaPendente',
        'DataTarefa',
        'Marcador',
        'TipoTarefa',
        'Comentarios',
        'Tier',
        'CodProtheus',
        'StatusVenda',
        'Tentativas'
    ]

    df_ploomes = pd.DataFrame(columns=colunas)


    # Acessar Ploomes
    skip = skip_ploomes


    # Marcadores
    url = 'https://api2.ploomes.com/Tags'
    headers = {'User-Key': chave_api, 'Content-Type': 'application/json'}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    marcadores = response.json()
    if marcadores:
        marcadores_dic = {marcador['Id']: marcador['Name'] for marcador in marcadores['value']}
        
    # Usuarios
    url = 'https://api2.ploomes.com/Users'
    params = {'$select': "Id,Name"}
    headers = {'User-Key': chave_api, 'Content-Type': 'application/json'}
    resposta = requests.get(url, headers=headers)
    resposta.raise_for_status()
    usuarios = resposta.json()
    if usuarios:
        responsavel_dic = {user['Id']: user['Name'] for user in usuarios['value']}



    while True:
        base_url = 'https://api2.ploomes.com'
        endpoint = '/Tasks'
        headers = {
            'User-Key': chave_api,
            'Content-Type': 'application/json'
        }
        params = {
            '$skip': skip,
            '$filter':"contains(Title,'Carteira de Clientes')",
            '$select':"Id,Title,ContactName,CreateDate,LastUpdateDate,FinishDate,Finished,OwnerId,LastUpdateDate,Pending,Description,DateTime",
            '$expand':"Tags($select=TagId),Comments($select=Content),Type($select=Name),Contact($select=OtherProperties;$expand=OtherProperties($filter=FieldId+eq+50016195 or FieldId+eq+10278898 or FieldId+eq+50015855; $select=FieldId,ObjectValueName,StringValue))"      
        }


        response = requests.get(base_url + endpoint, headers=headers, params=params)
        response.raise_for_status()
        tarefas = response.json()

        for tarefa in tarefas['value']:


            dicionario = {
                'IdTarefa' : tarefa['Id'],
                'TituloTarefa' : tarefa['Title'],
                'Cliente' : tarefa['ContactName'],
                'DataCriacao' : datetime.strptime(str(tarefa['CreateDate'][:10]),'%Y-%m-%d').date(),
                'UltAtualizacao' : datetime.strptime(str(tarefa['LastUpdateDate'][:10]),'%Y-%m-%d').date() if len(tarefa['LastUpdateDate'][:10]) == 10 else None,
                'DataFinalizacao' : datetime.strptime(str(tarefa['FinishDate'][:10]),'%Y-%m-%d').date() if tarefa['FinishDate'] != None else '',
                'Finalizado' : 'Sim' if tarefa['Finished'] == True else 'Não',
                'Responsavel' : responsavel_dic.get(tarefa['OwnerId'], 'Usuario não existe'),
                'TarefaPendente' : 'Sim' if tarefa['Pending'] == True else 'Não',
                'DataTarefa' : datetime.strptime(str(tarefa['DateTime'][:10]),'%Y-%m-%d').date(),
                'Marcador' : marcadores_dic.get(tarefa['Tags'][0]['TagId'], 0) if tarefa['Tags'] else 0,
                'Comentarios' : '\n'.join(comment['Content'] for comment in tarefa['Comments']) if tarefa['Comments'] else 0,
                'TipoTarefa' : tarefa['Type']['Name'],
                'Tentativas' : len(tarefa['Comments'])
            }
            
            dados = tarefa['Contact']['OtherProperties']
            for item in dados:
                if item['FieldId'] == 50016195:
                    dicionario['Tier'] = item['ObjectValueName']
                elif item['FieldId'] == 10278898:
                    dicionario['CodProtheus'] = item['StringValue']
                elif item['FieldId'] == 50015855:
                    dicionario['StatusVenda'] = str(item['StringValue']).upper()
            
            df_ploomes.loc[len(df_ploomes)] = dicionario
            
        if '@odata.nextLink' in tarefas:
            skip = int(tarefas['@odata.nextLink'].split('$skip=')[1].split('&')[0])
        else:
            break
        
    df_ploomes['Tier'] = df_ploomes['Tier'].fillna(0)

    # String de conexão usando o driver ODBC para SQL Server
    connection_string = f'mssql+pyodbc://{user}:{password}@{host}/{database}?driver=ODBC+Driver+17+for+SQL+Server'
    engine = create_engine(connection_string)

    # Salvar o DataFrame na tabela "Tarefas" do banco de dados
    df_ploomes.to_sql('Tarefas', engine, if_exists='replace', index=False)
    ultma_atualizacao = dat.datetime.now()
    print(f'Ultima atualização: {ultma_atualizacao}')

schedule.every(30).minutes.until("20:30").do(atualizarBD)

while True:
    schedule.run_pending()
    tm.sleep(1)


