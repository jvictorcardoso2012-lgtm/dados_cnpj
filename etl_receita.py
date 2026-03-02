import datetime
import requests

def get_valid_url(task_index):
    hoje = datetime.datetime.now()
    
    # Gera uma lista exata dos últimos 3 meses (ex: ['2026-03', '2026-02', '2026-01'])
    datas_para_testar = []
    for i in range(3):
        mes = hoje.month - i
        ano = hoje.year
        if mes <= 0:
            mes += 12
            ano -= 1
        datas_para_testar.append(f"{ano}-{mes:02d}")
        
    # Testa cada mês gerado
    for data in datas_para_testar:
        url = f"https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9/{data}/Estabelecimentos{task_index}.zip"
        
        print(f"Tarefa {task_index}: Simulando link -> {url}", flush=True)
        try:
            # Pede só o cabeçalho para ver se o arquivo existe (rápido e sem gastar RAM)
            r = requests.head(url, timeout=30)
            if r.status_code == 200:
                print(f"Tarefa {task_index}: BINGO! Pasta {data} encontrada e validada.", flush=True)
                return url, data
            else:
                print(f"Tarefa {task_index}: Status {r.status_code} na pasta {data}. Tentando o mês anterior...", flush=True)
        except Exception as e:
            print(f"Tarefa {task_index}: Erro de conexão ao testar {data}: {e}", flush=True)
            continue
            
    return None, None
