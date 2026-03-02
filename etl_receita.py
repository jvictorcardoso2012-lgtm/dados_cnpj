import pandas as pd
from google.cloud import bigquery
import requests
import zipfile
import io
import sys

def processar():
    client = bigquery.Client()
    TABLE_ID = "prospeccaob2b-489003.dados_cnpj.prospeccaob2b"
    
    # URL de Download Direto para a pasta 2026-02 que indicou
    url = "https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9/download?path=%2F2026-02&files=Estabelecimentos0.zip"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Accept': 'application/zip'
    }

    try:
        print("Acedendo à pasta 2026-02 no Serpro...")
        with requests.get(url, headers=headers, stream=True, timeout=600) as r:
            r.raise_for_status()
            
            # Validação: os primeiros 2 bytes de um ZIP são sempre 'PK'
            content_start = r.raw.read(2)
            if content_start != b'PK':
                print("Erro: O link ainda aponta para a página e não para o ficheiro ZIP.")
                sys.exit(1)
            
            print("Download validado. A processar ficheiro de 6,9 GB...")
            
            # Reconstituir o stream e abrir o ZIP
            full_content = content_start + r.raw.read()
            with zipfile.ZipFile(io.BytesIO(full_content)) as z:
                nome_int = z.namelist()[0]
                with z.open(nome_int) as f:
                    # Filtro de colunas essenciais para leads
                    c_list = [0, 1, 2, 5, 10, 19, 21, 22, 27]
                    c_names = ['base', 'ordem', 'dv', 'sit', 'data', 'uf', 'ddd', 'tel', 'email']
                    
                    chunks = pd.read_csv(f, sep=';', encoding='latin1', header=None, 
                                         chunksize=50000, dtype=str, usecols=c_list, names=c_names)
                    
                    for i, chunk in enumerate(chunks):
                        # Filtrar apenas empresas ATIVAS (02)
                        df = chunk[chunk['sit'] == '02'].copy()
                        if not df.empty:
                            df['cnpj'] = df['base'] + df['ordem'] + df['dv']
                            df['telefone'] = df['ddd'].fillna('') + df['tel'].fillna('')
                            df['celular'] = ""
                            df['data_inicio'] = pd.to_datetime(df['data'], errors='coerce').dt.date
                            
                            df_final = df[['cnpj', 'email', 'telefone', 'celular', 'data_inicio', 'uf']]
                            
                            # Carregar no BigQuery
                            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                            client.load_table_from_dataframe(df_final, TABLE_ID, job_config=job_config).result()
                            print(f"Lote {i+1} integrado com sucesso.")
                            
        print("Carga de Fevereiro/2026 finalizada!")

    except Exception as e:
        print(f"Falha ao processar link direto: {e}")
        sys.exit(1)

if __name__ == "__main__":
    processar()
