import os, io, zipfile, requests
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

client = bigquery.Client(project="prospeccaob2b-489003")
task_index = int(os.environ.get("CLOUD_RUN_TASK_INDEX", 0))

# Lógica de Data Mantida
hoje = datetime.now()
mes_ref = hoje.month - 1
ano_ref = hoje.year
if mes_ref <= 0: mes_ref = 12; ano_ref -= 1
DATA_MES = f"{ano_ref}-{mes_ref:02d}"
URL_BASE = f"https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9/{DATA_MES}"

def carregar_dados_v2(url, tabela, colunas, nomes_colunas):
    print(f"DEBUG: Tentando baixar de: {url}")
    try:
        r = requests.get(url, stream=True, timeout=300)
        if r.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                with z.open(z.namelist()[0]) as f:
                    chunks = pd.read_csv(f, sep=';', encoding='latin1', header=None, chunksize=50000, dtype=str, usecols=colunas)
                    for chunk in chunks:
                        df = pd.DataFrame()
                        for i, nome in enumerate(nomes_colunas):
                            # Correção da montagem do CNPJ Total
                            if nome == 'cnpj':
                                df['cnpj'] = chunk[0].fillna('') + chunk[1].fillna('') + chunk[2].fillna('')
                            else:
                                df[nome] = chunk[colunas[i]].fillna('')
                        
                        if 'capital_social' in df.columns:
                            df['capital_social'] = df['capital_social'].str.replace(',', '.').astype(float)
                        
                        # Salvando como _v2 para staging seguro
                        client.load_table_from_dataframe(df, f"prospeccaob2b-489003.dados_cnpj.{tabela}", 
                            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")).result()
    except Exception as e: 
        import traceback
        print(f"ERRO CRÍTICO na tabela {tabela}: {e}")
        traceback.print_exc()

# EMPRESAS: Agora com Natureza (2) e Porte (5)
carregar_dados_v2(f"{URL_BASE}/Empresas{task_index}.zip", "raw_empresas", 
                 [0, 1, 2, 4, 5], 
                 ['cnpj_base', 'razao_social', 'natureza_juridica', 'capital_social', 'porte'])

# ESTABELECIMENTOS: Agora com Tipo (13), Nome Rua (14), Número (15) e Bairro (17)
carregar_dados_v2(f"{URL_BASE}/Estabelecimentos{task_index}.zip", "raw_estabelecimentos", 
                 [0, 1, 2, 4, 5, 11, 13, 14, 15, 17, 19, 20, 21, 22, 23, 24, 27], 
                 ['cnpj_base', 'cnpj_ordem', 'cnpj_dv', 'situacao', 'data_inicio', 'cnae', 
                  'tipo_logradouro', 'logradouro_nome', 'numero', 'bairro', 'uf', 'mun', 
                  'tel1', 'tel2', 'tel3', 'tel4', 'email'])

if task_index == 0:
    carregar_dados_v2(f"{URL_BASE}/Cnaes.zip", "raw_cnaes", [0, 1], ['id_cnae', 'descricao'])
    carregar_dados_v2(f"{URL_BASE}/Municipios.zip", "raw_municipios", [0, 1], ['id_municipio', 'descricao'])
