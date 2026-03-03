import os
import io
import zipfile
import requests
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

client = bigquery.Client(project="prospeccaob2b-489003")
task_index = int(os.environ.get("CLOUD_RUN_TASK_INDEX", 0))

# --- LÓGICA DE DATA PRECISA ---
hoje = datetime.now()
# Busca sempre o mês anterior (Ex: Em Março/2026, busca 2026-02)
mes_ref = hoje.month - 1
ano_ref = hoje.year
if mes_ref <= 0:
    mes_ref = 12
    ano_ref -= 1

DATA_MES = f"{ano_ref}-{mes_ref:02d}"
URL_BASE = f"https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9/{DATA_MES}"

print(f"Iniciando Robô {task_index} para a base de {DATA_MES}...")

def carregar_dados(url, tabela, colunas, nomes_colunas):
    try:
        r = requests.get(url, stream=True, timeout=300)
        if r.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                with z.open(z.namelist()[0]) as f:
                    chunks = pd.read_csv(f, sep=';', encoding='latin1', header=None, chunksize=50000, dtype=str, usecols=colunas)
                    for chunk in chunks:
                        df = pd.DataFrame()
                        for i, nome in enumerate(nomes_colunas):
                            # Se for Estabelecimentos, fazemos a junção do CNPJ (0,1,2)
                            if nome == 'cnpj' and len(colunas) > 5:
                                df['cnpj'] = chunk[0].fillna('') + chunk[1].fillna('') + chunk[2].fillna('')
                            else:
                                df[nome] = chunk[colunas[i]].fillna('')
                        
                        # Tratamento específico para Capital Social (transformar em float)
                        if 'capital_social' in df.columns:
                            df['capital_social'] = df['capital_social'].str.replace(',', '.').astype(float)
                            
                        client.load_table_from_dataframe(df, f"prospeccaob2b-489003.dados_cnpj.{tabela}", 
                            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")).result()
    except Exception as e:
        print(f"Erro ao processar {url}: {e}")

# Execução das frentes
carregar_dados(f"{URL_BASE}/Empresas{task_index}.zip", "raw_empresas", [0, 1, 4], ['cnpj_base', 'razao_social', 'capital_social'])
carregar_dados(f"{URL_BASE}/Estabelecimentos{task_index}.zip", "raw_estabelecimentos", 
              [0, 1, 2, 4, 5, 10, 11, 13, 15, 19, 20, 21, 22, 23, 24, 27], 
              ['cnpj', 'fantasia', 'fantasia2', 'nome_fantasia', 'situacao', 'data_inicio', 'cnae', 'logradouro', 'bairro', 'uf', 'mun', 'tel1', 'tel2', 'tel3', 'tel4', 'email'])

if task_index == 0:
    carregar_dados(f"{URL_BASE}/Cnaes.zip", "raw_cnaes", [0, 1], ['id_cnae', 'descricao'])
    carregar_dados(f"{URL_BASE}/Municipios.zip", "raw_municipios", [0, 1], ['id_municipio', 'descricao'])
