import pandas as pd
from google.cloud import bigquery
import requests
import zipfile
import io
import sys

def processar():
    client = bigquery.Client()
    # Seu ID de projeto oficial
    TABLE_ID = "prospeccaob2b-489003.dados_cnpj.prospeccaob2b"
    
    # URL do Mirror Serpro (fevereiro/2026)
    url = "https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9/download?path=%2F2026-02&files=Estabelecimentos0.zip"
    headers = {'User-Agent': 'Mozilla/5.0'}

    try:
        print("Conectando ao repositório Serpro...")
        with requests.get(url, headers=headers, stream=True, timeout=600) as r:
            r.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                nome_arq = z.namelist()[0]
                with z.open(nome_arq) as f:
                    # Seleção de colunas para "Tabela Magra"
                    c_list = [0, 1, 2, 5, 10, 19, 21, 22, 27]
                    c_names = ['base', 'ordem', 'dv', 'sit', 'data', 'uf', 'ddd', 'tel', 'email']
                    
                    chunks = pd.read_csv(f, sep=';', encoding='latin1', header=None, 
                                         chunksize=50000, dtype=str, usecols=c_list, names=c_names)
                    
                    for i, chunk in enumerate(chunks):
                        # Filtrando apenas empresas ATIVAS
                        df = chunk[chunk['sit'] == '02'].copy()
                        if not df.empty:
                            df['cnpj'] = df['base'] + df['ordem'] + df['dv']
                            df['telefone'] = df['ddd'].fillna('') + df['tel'].fillna('')
                            df['celular'] = ""
                            df['data_inicio'] = pd.to_datetime(df['data'], errors='coerce').dt.date
                            df_final = df[['cnpj', 'email', 'telefone', 'celular', 'data_inicio', 'uf']]
                            
                            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                            client.load_table_from_dataframe(df_final, TABLE_ID, job_config=job_config).result()
                            print(f"Lote {i+1} integrado com sucesso.")
        print("Carga finalizada!")
    except Exception as e:
        print(f"Erro na execução: {e}")
        sys.exit(1)

if __name__ == "__main__":
    processar()
