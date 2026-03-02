import pandas as pd
from google.cloud import bigquery
import requests
import zipfile
import io
import os
import sys
import datetime

def get_valid_url(task_index):
    hoje = datetime.datetime.now()
    datas_para_testar = []
    
    # Gera os últimos 3 meses para garantir que sempre ache o arquivo
    for i in range(3):
        mes = hoje.month - i
        ano = hoje.year
        if mes <= 0:
            mes += 12
            ano -= 1
        datas_para_testar.append(f"{ano}-{mes:02d}")
        
    for data in datas_para_testar:
        url = f"https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9/{data}/Estabelecimentos{task_index}.zip"
        print(f"Tarefa {task_index}: Simulando link -> {url}", flush=True)
        try:
            r = requests.head(url, timeout=30)
            if r.status_code == 200:
                print(f"Tarefa {task_index}: BINGO! Pasta {data} encontrada.", flush=True)
                return url, data
        except:
            continue
    return None, None

def processar():
    client = bigquery.Client()
    task_index = os.environ.get("CLOUD_RUN_TASK_INDEX", "0")
    
    url, data_encontrada = get_valid_url(task_index)
    
    if not url:
        print(f"Tarefa {task_index}: Erro crítico. Nenhuma pasta encontrada no servidor.", flush=True)
        sys.exit(1)

    print(f"Tarefa {task_index}: Baixando arquivo da Receita...", flush=True)
    
    try:
        # Download seguro para a memória
        r = requests.get(url, stream=True, timeout=1800)
        r.raise_for_status()
        
        zip_buffer = io.BytesIO()
        for chunk in r.iter_content(chunk_size=8192):
            zip_buffer.write(chunk)
        
        with zipfile.ZipFile(zip_buffer) as z:
            nome_interno = z.namelist()[0]
            print(f"Tarefa {task_index}: Lendo arquivo CSV {nome_interno}...", flush=True)
            
            with z.open(nome_interno) as f:
                # Processamento em lotes de 50 mil linhas para garantir velocidade e estabilidade
                chunks = pd.read_csv(f, sep=';', encoding='latin1', header=None, chunksize=50000, dtype=str)
                
                for i, chunk in enumerate(chunks):
                    # Filtro vital: Apenas empresas ATIVAS (02)
                    df = chunk[chunk[5] == '02'].copy()
                    
                    if not df.empty:
                        # O mapeamento exato para a sua tabela no BigQuery
                        df['cnpj'] = df[0].fillna('') + df[1].fillna('') + df[2].fillna('')
                        df['email'] = df[27].fillna('')
                        df['telefone'] = df[21].fillna('') + df[22].fillna('')
                        df['celular'] = df[23].fillna('') + df[24].fillna('')
                        df['uf'] = df[19].fillna('')
                        df['data_inicio'] = pd.to_datetime(df[10], format='%Y%m%d', errors='coerce').dt.date
                        
                        # Seleciona apenas as colunas que importam
                        df_final = df[['cnpj', 'email', 'telefone', 'celular', 'data_inicio', 'uf']]
                        
                        # Adiciona os dados no banco sem apagar os anteriores
                        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                        client.load_table_from_dataframe(
                            df_final, 
                            "prospeccaob2b-489003.dados_cnpj.prospeccaob2b", 
                            job_config=job_config
                        ).result()
                        
                        print(f"Tarefa {task_index}: Lote {i} ({len(df_final)} empresas) SALVO no banco de dados!", flush=True)
                        
    except Exception as e:
        print(f"Tarefa {task_index}: ERRO FATAL - {str(e)}", flush=True)
        sys.exit(1)

if __name__ == "__main__":
    processar()
