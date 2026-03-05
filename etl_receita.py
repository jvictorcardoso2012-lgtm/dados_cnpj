import os, zipfile, requests, tempfile, gc
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 1. PROTEÇÃO DE CONEXÃO (Evita IncompleteRead)
session = requests.Session()
retry = Retry(total=5, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retry))

client = bigquery.Client(project="prospeccaob2b-489003")
task_index = int(os.environ.get("CLOUD_RUN_TASK_INDEX", 0))

# 2. SELETOR DE TASKS (Para você rodar só as que falharam sem mexer no código)
# Exemplo: Se falhar a 0 e a 3, você passa "0,3" no comando do gcloud
tasks_permitidas = os.environ.get("TASKS_ATIVAS", "0,1,2,3,4,5,6,7,8,9").split(',')

# Lógica de Data da RFB
hoje = datetime.now()
mes_ref = hoje.month - 1
ano_ref = hoje.year
if mes_ref <= 0: mes_ref = 12; ano_ref -= 1
DATA_MES = f"{ano_ref}-{mes_ref:02d}"
URL_BASE = f"https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9/{DATA_MES}"

def limpar_dados_antigos(tabela):
    """AUTO-LIMPEZA: Garante que nunca haverá dados duplicados (Idempotência)"""
    print(f"🧹 Iniciando auto-limpeza da Task {task_index} na tabela {tabela}...")
    try:
        if tabela in ['raw_empresas', 'raw_estabelecimentos', 'raw_simples']:
            # Deleta apenas os CNPJs do lote desta task (Baseados no último dígito da raiz)
            query = f"DELETE FROM `prospeccaob2b-489003.dados_cnpj.{tabela}` WHERE ENDS_WITH(cnpj_base, '{task_index}')"
        else:
            # Tabelas de apoio (Municipios e Cnaes) são recriadas do zero na task 0
            query = f"TRUNCATE TABLE `prospeccaob2b-489003.dados_cnpj.{tabela}`"
            
        client.query(query).result()
        print(f"✅ Limpeza concluída em {tabela}.")
    except Exception as e:
        print(f"⚠️ Aviso na limpeza (tabela pode estar vazia ou não existir): {e}")

def carregar_dados_permanente(url, tabela, colunas, nomes_colunas):
    print(f"🚀 Task {task_index}: Iniciando {tabela}...")
    
    # Executa a auto-limpeza antes do download
    limpar_dados_antigos(tabela)
    
    # Uso do Disco Temporário para não estourar a RAM no download
    with tempfile.NamedTemporaryFile(delete=True) as tmp_file:
        try:
            r = session.get(url, stream=True, timeout=1200)
            r.raise_for_status()
            for chunk in r.iter_content(chunk_size=1024*1024):
                tmp_file.write(chunk)
            tmp_file.flush()
            
            with zipfile.ZipFile(tmp_file.name) as z:
                with z.open(z.namelist()[0]) as f:
                    # Lendo em pedaços seguros (500k linhas)
                    chunks = pd.read_csv(f, sep=';', encoding='latin1', header=None, chunksize=500000, dtype=str, usecols=colunas)
                    
                    lista_chunks = []
                    linhas_acumuladas = 0
                    lote = 1
                    
                    for df_chunk in chunks:
                        # HIGIENE DE DADOS (Custo Zero de Armazenamento)
                        for idx, nome in enumerate(nomes_colunas):
                            if nome in ['cnpj_base', 'cnpj_ordem', 'cnpj_dv', 'tel1', 'tel2', 'ddd1', 'ddd2']:
                                df_chunk[colunas[idx]] = df_chunk[colunas[idx]].fillna('').str.replace(r'[^0-9]', '', regex=True)
                            elif nome == 'capital_social':
                                df_chunk[colunas[idx]] = df_chunk[colunas[idx]].fillna('').str.replace(',', '.').astype(float)
                        
                        df_chunk.columns = nomes_colunas
                        lista_chunks.append(df_chunk)
                        linhas_acumuladas += len(df_chunk)
                        
                        # GESTÃO ATIVA DE MEMÓRIA (Evita o Erro OOM / Signal 9)
                        if linhas_acumuladas >= 1500000:
                            df_final = pd.concat(lista_chunks, ignore_index=True)
                            print(f"📤 Enviando Lote {lote} ({len(df_final)} linhas) para {tabela}...")
                            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                            client.load_table_from_dataframe(df_final, f"prospeccaob2b-489003.dados_cnpj.{tabela}", job_config=job_config).result()
                            
                            # Força a liberação da RAM imediatamente
                            del df_final
                            del lista_chunks
                            gc.collect() 
                            
                            lista_chunks = []
                            linhas_acumuladas = 0
                            lote += 1
                    
                    # Upload do lote final (o que sobrou)
                    if lista_chunks:
                        df_final = pd.concat(lista_chunks, ignore_index=True)
                        print(f"📤 Enviando Lote Final {lote} ({len(df_final)} linhas) para {tabela}...")
                        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                        client.load_table_from_dataframe(df_final, f"prospeccaob2b-489003.dados_cnpj.{tabela}", job_config=job_config).result()
                        
                        del df_final
                        del lista_chunks
                        gc.collect()

            print(f"✅ {tabela} concluída com sucesso.")

        except Exception as e:
            print(f"❌ ERRO CRÍTICO em {tabela}: {e}")

# ==========================================
# GATILHO DE EXECUÇÃO
# ==========================================
if str(task_index) in tasks_permitidas:
    # carregar_dados_permanente(f"{URL_BASE}/Empresas{task_index}.zip", "raw_empresas", [0, 1, 4, 5], ['cnpj_base', 'razao_social', 'capital_social', 'porte'])
    carregar_dados_permanente(f"{URL_BASE}/Estabelecimentos{task_index}.zip", "raw_estabelecimentos", [0, 1, 2, 3, 4, 5, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 27], ['cnpj_base', 'cnpj_ordem', 'cnpj_dv', 'matriz_filial', 'nome_fantasia', 'situacao', 'data_inicio', 'cnae_principal', 'cnae_secundario', 'tipo_logradouro', 'logradouro_nome', 'numero', 'complemento', 'bairro', 'cep', 'uf', 'mun_id', 'ddd1', 'tel1', 'ddd2', 'tel2', 'email'])
   # if task_index == 0:
       # carregar_dados_permanente(f"{URL_BASE}/Simples.zip", "raw_simples", [0, 4], ['cnpj_base', 'opcao_mei'])
       # carregar_dados_permanente(f"{URL_BASE}/Cnaes.zip", "raw_cnaes", [0, 1], ['id_cnae', 'descricao'])
       # carregar_dados_permanente(f"{URL_BASE}/Municipios.zip", "raw_municipios", [0, 1], ['id_municipio', 'descricao'])
else:
    print(f"Task {task_index} pulada conforme configuração de TASKS_ATIVAS.")
