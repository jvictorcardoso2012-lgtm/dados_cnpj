import pandas as pd
from google.cloud import bigquery
import requests
import zipfile
import io
import os
import sys

# Força a saída de texto para o log do Google
print("Iniciando depurador de precisão...", flush=True)

def processar():
    try:
        client = bigquery.Client()
        task_index = os.environ.get("CLOUD_RUN_TASK_INDEX", "0")
        url = f"https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9/2026-02/Estabelecimentos{task_index}.zip"
        
        print(f"Tarefa {task_index}: Conectando ao link WebDAV...", flush=True)
        
        # O segredo: Não usamos r.content (que carregaria os 6.9GB)
        # Usamos um iterador de bytes para economizar RAM
        with requests.get(url, stream=True, timeout=1200) as r:
            r.raise_for_status()
            
            # Criamos um arquivo temporário em disco (no Cloud Run, /tmp usa a RAM, mas de forma mais eficiente)
            # Mas a melhor estratégia para 6.9GB é ler o stream diretamente se o zip permitir
            zip_buffer = io.BytesIO()
            for chunk in r.iter_content(chunk_size=1024*1024): # 1MB por vez
                if chunk:
                    zip_buffer.write(chunk)
                    # Monitor de progresso para o log
                    if zip_buffer.tell() % (100 * 1024 * 1024) == 0:
                        print(f"Tarefa {task_index}: Baixados {zip_buffer.tell() / 1024**2:.0f} MB...", flush=True)
            
            zip_buffer.seek(0)
            with zipfile.ZipFile(zip_buffer) as z:
                # ... resto do processamento ...
                print(f"Tarefa {task_index}: ZIP aberto com sucesso.", flush=True)

    except Exception as e:
        print(f"ERRO CRÍTICO NA TAREFA {task_index}: {str(e)}", flush=True)
        sys.exit(1)

if __name__ == "__main__":
    processar()
