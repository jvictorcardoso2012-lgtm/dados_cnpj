import pandas as pd
from google.cloud import bigquery
import requests
import zipfile
import io

def processar_brasil_todo():
    client = bigquery.Client()
    TABLE_ID = "prospeccaob2b-489003.dados_cnpj.prospeccaob2b"
    # Link base da pasta de Fevereiro que você enviou
    base_url = "https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9/download?path=%2F2026-02&files="
    
    for i in range(10):
        arquivo = f"Estabelecimentos{i}.zip"
        url = base_url + arquivo
        
        print(f"Iniciando captura do {arquivo}...")
        try:
            r = requests.get(url, stream=True, timeout=600)
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                with z.open(z.namelist()[0]) as f:
                    # Somente colunas de contato e UF
                    chunks = pd.read_csv(f, sep=';', encoding='latin1', header=None, 
                                         chunksize=100000, dtype=str, 
                                         usecols=[0,1,2,5,10,19,21,22,27])
                    
                    for chunk in chunks:
                        # Filtro Sênior: Apenas empresas ATIVAS (02)
                        df = chunk[chunk[5] == '02'].copy()
                        if not df.empty:
                            # ... lógica de tratamento de colunas ...
                            client.load_table_from_dataframe(df_tratado, TABLE_ID).result()
            print(f"Concluído: {arquivo}")
        except Exception as e:
            print(f"Erro no {arquivo}: {e}")

if __name__ == "__main__":
    processar_brasil_todo()
