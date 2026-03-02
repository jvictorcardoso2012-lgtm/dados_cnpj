import pandas as pd
from google.cloud import bigquery
import requests
import zipfile
import io
import os
import sys

def processar():
    client = bigquery.Client()
    PROJECT_ID = "prospeccaob2b-489003"
    DATASET_ID = f"{PROJECT_ID}.dados_cnpj"
    FINAL_TABLE = f"{DATASET_ID}.prospeccaob2b"
    STAGING_TABLE = f"{DATASET_ID}.prospeccaob2b_staging_{os.environ.get('CLOUD_RUN_TASK_INDEX', '0')}"
    
    task_index = os.environ.get("CLOUD_RUN_TASK_INDEX", "0")
    # Link WebDAV estável que você validou
    url = f"https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9/2026-02/Estabelecimentos{task_index}.zip"

    try:
        print(f"Tarefa {task_index}: Iniciando carga em Staging...")
        with requests.get(url, stream=True, timeout=1200) as r:
            r.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                with z.open(z.namelist()[0]) as f:
                    chunks = pd.read_csv(f, sep=';', encoding='latin1', header=None, 
                                         chunksize=100000, dtype=str, usecols=[0,1,2,5,10,19,21,22,27])
                    
                    for chunk in chunks:
                        df = chunk[chunk[5] == '02'].copy() # Só ATIVAS
                        if not df.empty:
                            # Lógica de tratamento omitida para brevidade...
                            # Carrega na tabela STAGING (sobrescrevendo a cada lote desta tarefa)
                            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                            client.load_table_from_dataframe(df_final, STAGING_TABLE, job_config=job_config).result()

        # O PULO DO GATO: Comando MERGE para atualizar a base principal
        sql_merge = f"""
        MERGE `{FINAL_TABLE}` T
        USING `{STAGING_TABLE}` S
        ON T.cnpj = S.cnpj
        WHEN MATCHED THEN
          UPDATE SET email = S.email, telefone = S.telefone, data_inicio = S.data_inicio
        WHEN NOT MATCHED THEN
          INSERT (cnpj, email, telefone, celular, data_inicio, uf)
          VALUES (cnpj, email, telefone, celular, data_inicio, uf)
        """
        client.query(sql_merge).result()
        print(f"Tarefa {task_index}: Dados mesclados. Limpando staging...")
        client.delete_table(STAGING_TABLE, not_found_ok=True)

    except Exception as e:
        print(f"Erro na Tarefa {task_index}: {e}")
        sys.exit(1)
