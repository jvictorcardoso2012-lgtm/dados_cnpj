import os, io, zipfile, requests
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

client = bigquery.Client(project="prospeccaob2b-489003")
task_index = int(os.environ.get("CLOUD_RUN_TASK_INDEX", 0))

# Lógica de data para pegar o mês anterior (disponibilidade da RFB)
hoje = datetime.now()
mes_ref = hoje.month - 1
ano_ref = hoje.year
if mes_ref <= 0: 
    mes_ref = 12
    ano_ref -= 1

DATA_MES = f"{ano_ref}-{mes_ref:02d}"
URL_BASE = f"https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9/{DATA_MES}"

def carregar_dados(url, tabela, colunas, nomes_colunas):
    print(f"DEBUG: Tentando baixar de: {url}")
    try:
        r = requests.get(url, stream=True, timeout=300)
        if r.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                with z.open(z.namelist()[0]) as f:
                    # Lemos em chunks para economizar RAM (8GB configurados)
                    chunks = pd.read_csv(f, sep=';', encoding='latin1', header=None, chunksize=200000, dtype=str, usecols=colunas)
                    
                    lista_dfs = []
                    for chunk in chunks:
                        df_temp = pd.DataFrame()
                        # Mapeia as colunas conforme os índices informados
                        for i, nome in enumerate(nomes_colunas):
                            df_temp[nome] = chunk[colunas[i]].fillna('')
                        
                        # Conversão específica para Capital Social (Empresas)
                        if 'capital_social' in df_temp.columns:
                            df_temp['capital_social'] = df_temp['capital_social'].str.replace(',', '.').astype(float)
                        
                        lista_dfs.append(df_temp)
                    
                    # Consolidação para um único upload (Reduz custos de inserção no BQ)
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
            print(f"Aviso: Arquivo não encontrado ou link expirado (Status {r.status_code})")
    except Exception as e: 
        print(f"ERRO CRÍTICO em {tabela}: {e}")

# 1. EMPRESAS: Razão Social, Capital Social e Porte
carregar_dados(
    f"{URL_BASE}/Empresas{task_index}.zip", 
    "raw_empresas", 
    [0, 1, 4, 5], 
    ['cnpj_base', 'razao_social', 'capital_social', 'porte']
)

# 2. ESTABELECIMENTOS: Matriz/Filial, Nome Fantasia, CNAEs, Localização e Telefones (1 e 2)
carregar_dados(
    f"{URL_BASE}/Estabelecimentos{task_index}.zip", 
    "raw_estabelecimentos", 
    [0, 1, 2, 3, 4, 5, 10, 11, 12, 13, 14, 15, 17, 19, 21, 22, 23, 24, 25, 27], 
    ['cnpj_base', 'cnpj_ordem', 'cnpj_dv', 'matriz_filial', 'nome_fantasia', 'situacao', 'data_inicio', 'cnae_principal', 'cnae_secundario', 'tipo_logradouro', 'logradouro_nome', 'numero', 'bairro', 'uf', 'mun_id', 'ddd1', 'tel1', 'ddd2', 'tel2', 'email']
)

# 3. TABELAS DE DOMÍNIO E SIMPLES (Apenas na primeira Task para não duplicar)
if task_index == 0:
    # Para o filtro "Excluir MEIs"
    carregar_dados(f"{URL_BASE}/Simples.zip", "raw_simples", [0, 4], ['cnpj_base', 'opcao_mei'])
    
    # Para tradução de nomes no site
    carregar_dados(f"{URL_BASE}/Cnaes.zip", "raw_cnaes", [0, 1], ['id_cnae', 'descricao'])
    carregar_dados(f"{URL_BASE}/Municipios.zip", "raw_municipios", [0, 1], ['id_municipio', 'descricao'])
