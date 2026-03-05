import os, io, zipfile, requests
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

client = bigquery.Client(project="prospeccaob2b-489003")
task_index = int(os.environ.get("CLOUD_RUN_TASK_INDEX", 0))

hoje = datetime.now()
mes_ref = hoje.month - 1
ano_ref = hoje.year
if mes_ref <= 0: mes_ref = 12; ano_ref -= 1
DATA_MES = f"{ano_ref}-{mes_ref:02d}"
URL_BASE = f"https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9/{DATA_MES}"

def carregar_dados(url, tabela, colunas, nomes_colunas):
    print(f"DEBUG: Tentando baixar de: {url}")
    try:
        r = requests.get(url, stream=True, timeout=300)
        if r.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                with z.open(z.namelist()[0]) as f:
                    # Lemos o arquivo inteiro para uma lista de DataFrames
                    chunks = pd.read_csv(f, sep=';', encoding='latin1', header=None, chunksize=200000, dtype=str, usecols=colunas)
                    
                    lista_dfs = []
                    for chunk in chunks:
                        df_temp = pd.DataFrame()
                        for i, nome in enumerate(nomes_colunas):
                            if nome == 'cnpj':
                                df_temp['cnpj'] = chunk[0].fillna('') + chunk[1].fillna('') + chunk[2].fillna('')
                            else:
                                df_temp[nome] = chunk[colunas[i]].fillna('')
                        
                        if 'capital_social' in df_temp.columns:
                            df_temp['capital_social'] = df_temp['capital_social'].str.replace(',', '.').astype(float)
                        
                        lista_dfs.append(df_temp)
                    
                    # CONSOLIDAÇÃO: Junta tudo e faz UM ÚNICO upload para o BigQuery
                    if lista_dfs:
                        df_final = pd.concat(lista_dfs, ignore_index=True)
                        print(f"Enviando {len(df_final)} linhas para {tabela}...")
                        client.load_table_from_dataframe(
                            df_final, 
                            f"prospeccaob2b-489003.dados_cnpj.{tabela}", 
                            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                        ).result()
                        print(f"Sucesso: {tabela} atualizada.")
        else:
            print(f"Erro no link: Status {r.status_code}")
    except Exception as e: 
        print(f"ERRO CRÍTICO em {tabela}: {e}")

# Execução das funções (mantendo a lógica original)
carregar_dados(f"{URL_BASE}/Empresas{task_index}.zip", "raw_empresas", [0, 1, 2, 4, 5], ['cnpj_base', 'razao_social', 'natureza_juridica', 'capital_social', 'porte'])
carregar_dados(f"{URL_BASE}/Estabelecimentos{task_index}.zip", "raw_estabelecimentos", [0, 1, 2, 4, 5, 11, 13, 14, 15, 17, 19, 20, 21, 22, 23, 24, 27], ['cnpj_base', 'cnpj_ordem', 'cnpj_dv', 'situacao', 'data_inicio', 'cnae', 'tipo_logradouro', 'logradouro_nome', 'numero', 'bairro', 'uf', 'mun', 'tel1', 'tel2', 'tel3', 'tel4', 'email'])

if task_index == 0:
    carregar_dados(f"{URL_BASE}/Cnaes.zip", "raw_cnaes", [0, 1], ['id_cnae', 'descricao'])
    carregar_dados(f"{URL_BASE}/Municipios.zip", "raw_municipios", [0, 1], ['id_municipio', 'descricao'])
