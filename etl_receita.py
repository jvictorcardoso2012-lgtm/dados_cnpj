import os, io, zipfile, requests, shutil
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configuração de persistência extrema
session = requests.Session()
retry = Retry(total=10, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retry))

client = bigquery.Client(project="prospeccaob2b-489003")
task_index = int(os.environ.get("CLOUD_RUN_TASK_INDEX", 0))

# Lógica de data RFB
hoje = datetime.now()
mes_ref = hoje.month - 1
ano_ref = hoje.year
if mes_ref <= 0: mes_ref = 12; ano_ref -= 1
DATA_MES = f"{ano_ref}-{mes_ref:02d}"
URL_BASE = f"https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9/{DATA_MES}"

def carregar_dados(url, tabela, colunas, nomes_colunas):
    print(f"🚀 Task {task_index}: Iniciando {tabela}...")
    try:
        # Stream=True para evitar o IncompleteRead
        with session.get(url, stream=True, timeout=900) as r:
            r.raise_for_status()
            buffer = io.BytesIO()
            # Baixa em pedaços para não quebrar a conexão
            for chunk in r.iter_content(chunk_size=8192):
                if chunk: buffer.write(chunk)
            
            buffer.seek(0)
            with zipfile.ZipFile(buffer) as z:
                with z.open(z.namelist()[0]) as f:
                    # Lógica de chunks para o BigQuery
                    chunks = pd.read_csv(f, sep=';', encoding='latin1', header=None, chunksize=250000, dtype=str, usecols=colunas)
                    
                    for i, df_chunk in enumerate(chunks):
                        df_temp = pd.DataFrame()
                        for idx, nome in enumerate(nomes_colunas):
                            val = df_chunk[colunas[idx]].fillna('')
                            
                            # Limpeza de Símbolos (Custo Zero)
                            if nome in ['cnpj_base', 'cnpj_ordem', 'cnpj_dv', 'tel1', 'tel2', 'ddd1', 'ddd2']:
                                df_temp[nome] = val.str.replace(r'[^0-9]', '', regex=True)
                            elif nome == 'capital_social':
                                df_temp[nome] = val.str.replace(',', '.').astype(float)
                            else:
                                df_temp[nome] = val
                        
                        # Upload imediato do chunk para economizar RAM
                        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                        client.load_table_from_dataframe(df_temp, f"prospeccaob2b-489003.dados_cnpj.{tabela}", job_config=job_config).result()
                        print(f"✅ Chunk {i} de {tabela} enviado.")

    except Exception as e: 
        print(f"❌ ERRO na {tabela}: {e}")

# Execução Milimétrica
carregar_dados(f"{URL_BASE}/Empresas{task_index}.zip", "raw_empresas", [0, 1, 4, 5], ['cnpj_base', 'razao_social', 'capital_social', 'porte'])

carregar_dados(
    f"{URL_BASE}/Estabelecimentos{task_index}.zip", 
    "raw_estabelecimentos", 
    [0, 1, 2, 3, 4, 5, 10, 11, 12, 13, 14, 15, 17, 19, 21, 22, 23, 24, 25, 27], 
    ['cnpj_base', 'cnpj_ordem', 'cnpj_dv', 'matriz_filial', 'nome_fantasia', 'situacao', 'data_inicio', 'cnae_principal', 'cnae_secundario', 'tipo_logradouro', 'logradouro_nome', 'numero', 'bairro', 'uf', 'mun_id', 'ddd1', 'tel1', 'ddd2', 'tel2', 'email']
)

if task_index == 0:
    carregar_dados(f"{URL_BASE}/Simples.zip", "raw_simples", [0, 4], ['cnpj_base', 'opcao_mei'])
    carregar_dados(f"{URL_BASE}/Cnaes.zip", "raw_cnaes", [0, 1], ['id_cnae', 'descricao'])
    carregar_dados(f"{URL_BASE}/Municipios.zip", "raw_municipios", [0, 1], ['id_municipio', 'descricao'])
