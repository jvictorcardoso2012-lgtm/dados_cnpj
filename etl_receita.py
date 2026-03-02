import pandas as pd
from google.cloud import bigquery
import requests
import zipfile
import io

def processar():
    client = bigquery.Client()
    TABLE_ID = "prospeccaob2b-489003.dados_cnpj.prospeccaob2b"
    base_url = "https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9/download?path=%2F2026-02&files="
    
    # Loop para processar os 10 arquivos do Brasil
    for n in range(10):
        file_name = f"Estabelecimentos{n}.zip"
        url = base_url + file_name
        
        try:
            print(f"Baixando {file_name}...")
            r = requests.get(url, stream=True, timeout=600)
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                with z.open(z.namelist()[0]) as f:
                    # Filtro de colunas e processamento em chunks
                    chunks = pd.read_csv(f, sep=';', encoding='latin1', header=None, 
                                         chunksize=100000, dtype=str, 
                                         usecols=[0,1,2,5,10,19,21,22,27])
                    
                    for i, chunk in enumerate(chunks):
                        df = chunk[chunk[5] == '02'].copy() # Só ATIVAS
                        if not df.empty:
                            # ... lógica de tratamento de colunas (cnpj, tel, etc) ...
                            client.load_table_from_dataframe(df_final, TABLE_ID).result()
            print(f"{file_name} concluído!")
        except Exception as e:
            print(f"Erro no arquivo {n}: {e}")

if __name__ == "__main__":
    processar()
