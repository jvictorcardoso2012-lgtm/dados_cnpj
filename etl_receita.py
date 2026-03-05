import os, io, zipfile, requests
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# CONFIGURAÇÃO DE RETENTATIVAS (Resolve o erro de IncompleteRead)
session = requests.Session()
retry = Retry(
    total=5, 
    backoff_factor=1, 
    status_forcelist=[502, 503, 504],
    raise_on_status=False
)
session.mount('https://', HTTPAdapter(max_retries=retry))

client = bigquery.Client(project="prospeccaob2b-489003")
task_index = int(os.environ.get("CLOUD_RUN_TASK_INDEX", 0))

hoje = datetime.now()
mes_ref = hoje.month - 1
ano_ref = hoje.year
if mes_ref <= 0: mes_ref = 12; ano_ref -= 1
DATA_MES = f"{ano_ref}-{mes_ref:02d}"
URL_BASE = f"https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9/{DATA_MES}"

def carregar_dados(url, tabela, colunas, nomes_colunas):
    print(f"DEBUG: Baixando {tabela} (Task {task_index})...")
    try:
        # Timeout aumentado e stream habilitado para estabilidade
        r = session.get(url, stream=True, timeout=600)
        if r.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                with z.open(z.namelist()[0]) as f:
                    chunks = pd.read_csv(f, sep=';', encoding='latin1', header=None, chunksize=200000, dtype=str, usecols=colunas)
                    
                    lista_dfs = []
                    for chunk in chunks:
                        df_temp = pd.DataFrame()
                        for i, nome in enumerate(nomes_colunas):
                            val = chunk[colunas[i]].fillna('')
                            
                            # LIMPEZA DE SÍMBOLOS (Custo Zero de Armazenamento)
                            # Remove pontos, traços e barras de campos numéricos
                            if nome in ['cnpj_base', 'cnpj_ordem', 'cnpj_dv', 'tel1', 'tel2', 'ddd1', 'ddd2']:
                                df_temp[nome] = val.str.replace(r'[^0-9]', '', regex=True)
                            elif nome == 'capital_social':
                                df_temp[nome] = val.str.replace(',', '.').astype(float)
                            else:
                                df_temp[nome] = val
                        
                        lista_dfs.append(df_temp)
                    
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
            print(f"Erro: Status {r.status_code} para {url}")
    except Exception as e: 
        print(f"ERRO CRÍTICO em {tabela}: {e}")

# EXECUÇÃO COM COLUNAS MILIMÉTRICAS
# Empresas: Razão Social, Capital Social e Porte
carregar_dados(f"{URL_BASE}/Empresas{task_index}.zip", "raw_empresas", [0, 1, 4, 5], ['cnpj_base', 'razao_social', 'capital_social', 'porte'])

# Estabelecimentos: Matriz/Filial, Fantasia, Datas, CNAEs, Endereço e Contatos (1 e 2)
carregar_dados(
    f"{URL_BASE}/Estabelecimentos{task_index}.zip", 
    "raw_estabelecimentos", 
    [0, 1, 2, 3, 4, 5, 10, 11, 12, 13, 14, 15, 17, 19, 21, 22, 23, 24, 25, 27], 
    ['cnpj_base', 'cnpj_ordem', 'cnpj_dv', 'matriz_filial', 'nome_fantasia', 'situacao', 'data_inicio', 'cnae_principal', 'cnae_secundario', 'tipo_logradouro', 'logradouro_nome', 'numero', 'bairro', 'uf', 'mun_id', 'ddd1', 'tel1', 'ddd2', 'tel2', 'email']
)

# Tabelas de Apoio e Simples (Apenas Task 0)
if task_index == 0:
    carregar_dados(f"{URL_BASE}/Simples.zip", "raw_simples", [0, 4], ['cnpj_base', 'opcao_mei'])
    carregar_dados(f"{URL_BASE}/Cnaes.zip", "raw_cnaes", [0, 1], ['id_cnae', 'descricao'])
    carregar_dados(f"{URL_BASE}/Municipios.zip", "raw_municipios", [0, 1], ['id_municipio', 'descricao'])
