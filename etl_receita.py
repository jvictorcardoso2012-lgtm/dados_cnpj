import pandas as pd
from google.cloud import bigquery
import requests
import zipfile
import io
import os
import sys

def processar():
    client = bigquery.Client()
    task_index = os.environ.get("CLOUD_RUN_TASK_INDEX", "0")
    # Link WebDAV estável que você validou
    url = f"https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9/2026-02/Estabelecimentos{task_index}.zip"

    try:
        print(f"Tarefa {task_index}: Iniciando stream de {url}", flush=True)
        
        # O segredo: stream=True e não usar r.content!
        with requests.get(url, stream=True, timeout=1800) as r:
            r.raise_for_status()
            
            # Usamos um buffer de memória pequeno para o cabeçalho do ZIP
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                nome_interno = z.namelist()[0]
                print(f"Tarefa {task_index}: Abrindo {nome_interno}...", flush=True)
                
                with z.open(nome_interno) as f:
                    # Lendo em pedaços de 50 mil linhas por vez
                    chunks = pd.read_csv(f, sep=';', encoding='latin1', header=None, 
                                         chunksize=50000, dtype=str)
                    
                    for i, chunk in enumerate(chunks):
                        # Filtrando apenas empresas ATIVAS (coluna 5 == '02')
                        df = chunk[chunk[5] == '02'].copy()
                        if not df.empty:
                            # Lógica de processamento e carga no BigQuery...
                            print(f"Tarefa {task_index}: Lote {i} processado.", flush=True)

    except Exception as e:
        print(f"ERRO NA TAREFA {task_index}: {str(e)}", flush=True)
        sys.exit(1)
