import pandas as pd
from google.cloud import bigquery
import requests
import zipfile
import io
import os
import sys
from datetime import datetime, timedelta

def get_valid_url(task_index):
    # Tenta primeiro o mês atual, depois o anterior
    for i in range(2):
        data = (datetime.now() - timedelta(days=30*i)).strftime("%Y-%m")
        url = f"https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9/{data}/Estabelecimentos{task_index}.zip"
        
        print(f"Testando data {data}...", flush=True)
        try:
            r = requests.head(url, timeout=30)
            if r.status_code == 200:
                print(f"Sucesso! Usando pasta {data}", flush=True)
                return url, data
        except:
            continue
    return None, None

def processar():
    client = bigquery.Client()
    task_index = os.environ.get("CLOUD_RUN_TASK_INDEX", "0")
    
    url, data_encontrada = get_valid_url(task_index)
    
    if not url:
        print("Erro: Nenhuma pasta de dados encontrada no servidor.", flush=True)
        sys.exit(1)

    print(f"Tarefa {task_index}: Processando {data_encontrada}...", flush=True)
    # ... resto do código de extração (MERGE e Chunks) permanece igual ...
