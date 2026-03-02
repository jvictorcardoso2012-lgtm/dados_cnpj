import pandas as pd
from google.cloud import bigquery
import requests
import zipfile
import io
import os
import sys

def processar():
    client = bigquery.Client()
    TABLE_ID = "prospeccaob2b-489003.dados_cnpj.prospeccaob2b"
    
    # O segredo da produtividade: o Google diz qual arquivo este robô deve pegar
    task_index = os.environ.get("CLOUD_RUN_TASK_INDEX", "0")
    file_name = f"Estabelecimentos{task_index}.zip"
    
    # URL dinâmica baseada na pasta de Fevereiro/2026
    url = f"https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9/download?path=%2F2026-02&files={file_name}"
    
    headers = {'User-Agent': 'Mozilla/5.0'}

    try:
        print(f"Trabalhador {task_index} iniciando download de {file_name}...")
        with requests.get(url, headers=headers, stream=True, timeout=600) as r:
            r.raise_for_status()
            
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                with z.open(z.namelist()[0]) as f:
                    # Colunas essenciais para prospecção
                    c_list = [0, 1, 2, 5, 10, 19, 21, 22, 27]
                    c_names = ['base', 'ordem', 'dv', 'sit', 'data', 'uf', 'ddd', 'tel', 'email']
                    
                    chunks = pd.read_csv(f, sep=';', encoding='latin1', header=None, 
                                         chunksize=100000, dtype=str, usecols=c_list, names=c_names)
                    
                    for i, chunk in enumerate(chunks):
                        df = chunk[chunk['sit'] == '02'].copy() # Só empresas ATIVAS
                        if not df.empty:
                            df['cnpj'] = df['base'] + df['ordem'] + df['dv']
                            df['telefone'] = df['ddd'].fillna('') + df['tel'].fillna('')
                            df['celular'] = ""
                            df['data_inicio'] = pd.to_datetime(df['data'], errors='coerce').dt.date
                            
                            df_final = df[['cnpj', 'email', 'telefone', 'celular', 'data_inicio', 'uf']]
                            
                            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                            client.load_table_from_dataframe(df_final, TABLE_ID, job_config=job_config).result()
                
        print(f"Trabalhador {task_index}: {file_name} processado com sucesso!")

    except Exception as e:
        print(f"Erro no trabalhador {task_index} ({file_name}): {e}")
        sys.exit(1)

if __name__ == "__main__":
    processar()
