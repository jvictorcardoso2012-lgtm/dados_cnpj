import pandas as pd
from google.cloud import bigquery
import requests
import zipfile
import io
import os
import sys
from datetime import datetime, timedelta

def processar():
    client = bigquery.Client()
    TABLE_ID = "prospeccaob2b-489003.dados_cnpj.prospeccaob2b"
    
    # LÓGICA DINÂMICA: Pega o mês anterior ao atual
    # Se hoje é Março/2026, ele busca 2026-02. Em Abril, buscará 2026-03.
    hoje = datetime.now()
    primeiro_dia_mes_atual = hoje.replace(day=1)
    mes_anterior = primeiro_dia_mes_atual - timedelta(days=1)
    data_pasta = mes_anterior.strftime("%Y-%m")
    
    task_index = os.environ.get("CLOUD_RUN_TASK_INDEX", "0")
    file_name = f"Estabelecimentos{task_index}.zip"
    
    # A URL agora usa a variável 'data_pasta' em vez de '2026-02'
    url = f"https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9/{data_pasta}/{file_name}"
    
    print(f"Tarefa {task_index}: Buscando dados de {data_pasta} em {file_name}...")

    try:
        with requests.get(url, stream=True, timeout=1200) as r:
            if r.status_code == 404:
                print(f"Dados de {data_pasta} ainda não disponíveis. Tentando mês anterior...")
                # Lógica extra: se o mês atual não existe, ele poderia tentar o anterior do anterior
                sys.exit(0) # Encerra sem erro para não gastar retentativas
                
            r.raise_for_status()
            # ... (resto do código de processamento ZIP e BigQuery permanece igual) ...
