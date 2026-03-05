import os, zipfile, requests, tempfile
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

def carregar_dados_v3(url, tabela, colunas, nomes_colunas):
    print(f"🚀 Task {task_index}: Iniciando {tabela}...")
    
    # Criamos um arquivo temporário no disco (Custo Zero e economiza RAM)
    with tempfile.NamedTemporaryFile(delete=True) as tmp_file:
        try:
            r = requests.get(url, stream=True, timeout=600)
            r.raise_for_status()
            for chunk in r.iter_content(chunk_size=8192):
                tmp_file.write(chunk)
            tmp_file.flush()
            
            with zipfile.ZipFile(tmp_file.name) as z:
                with z.open(z.namelist()[0]) as f:
                    # Lemos em blocos para processar a limpeza, mas guardamos o resultado
                    chunks = pd.read_csv(f, sep=';', encoding='latin1', header=None, chunksize=500000, dtype=str, usecols=colunas)
                    
                    lista_chunks_limpos = []
                    for df_chunk in chunks:
                        # Limpeza de Símbolos (Otimização de armazenamento)
                        for idx, nome in enumerate(nomes_colunas):
                            if nome in ['cnpj_base', 'cnpj_ordem', 'cnpj_dv', 'tel1', 'tel2', 'ddd1', 'ddd2']:
                                df_chunk[colunas[idx]] = df_chunk[colunas[idx]].fillna('').str.replace(r'[^0-9]', '', regex=True)
                            elif nome == 'capital_social':
                                df_chunk[colunas[idx]] = df_chunk[colunas[idx]].fillna('').str.replace(',', '.').astype(float)
                        
                        df_chunk.columns = nomes_colunas
                        lista_chunks_limpos.append(df_chunk)
                    
                    # CONSOLIDAÇÃO: Transforma todos os pedaços em um único DataFrame
                    df_final = pd.concat(lista_chunks_limpos, ignore_index=True)
                    
                    # UPLOAD ÚNICO: Resolve o erro 429
                    print(f"📤 Enviando {len(df_final)} linhas (Upload único) para {tabela}...")
                    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                    client.load_table_from_dataframe(df_final, f"prospeccaob2b-489003.dados_cnpj.{tabela}", job_config=job_config).result()
                    print(f"✅ {tabela} concluída.")

        except Exception as e:
            print(f"❌ ERRO em {tabela}: {e}")

# Execução (Empresas e Estabelecimentos)
carregar_dados_v3(f"{URL_BASE}/Empresas{task_index}.zip", "raw_empresas", [0, 1, 4, 5], ['cnpj_base', 'razao_social', 'capital_social', 'porte'])
carregar_dados_v3(f"{URL_BASE}/Estabelecimentos{task_index}.zip", "raw_estabelecimentos", [0, 1, 2, 3, 4, 5, 10, 11, 12, 13, 14, 15, 17, 19, 21, 22, 23, 24, 25, 27], ['cnpj_base', 'cnpj_ordem', 'cnpj_dv', 'matriz_filial', 'nome_fantasia', 'situacao', 'data_inicio', 'cnae_principal', 'cnae_secundario', 'tipo_logradouro', 'logradouro_nome', 'numero', 'bairro', 'uf', 'mun_id', 'ddd1', 'tel1', 'ddd2', 'tel2', 'email'])

if task_index == 0:
    carregar_dados_v3(f"{URL_BASE}/Simples.zip", "raw_simples", [0, 4], ['cnpj_base', 'opcao_mei'])
    carregar_dados_v3(f"{URL_BASE}/Cnaes.zip", "raw_cnaes", [0, 1], ['id_cnae', 'descricao'])
    carregar_dados_v3(f"{URL_BASE}/Municipios.zip", "raw_municipios", [0, 1], ['id_municipio', 'descricao'])
