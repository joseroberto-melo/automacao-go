import os
import logging
from datetime import datetime

def gerar_nome_log(empresa_id, cpf, data_ini, data_fim, ie):
    return f"{empresa_id}_{cpf}_{data_ini}_{data_fim}_{ie}"

def setup_logger(nome_arquivo, caminho_log):
    logger = logging.getLogger(str(caminho_log))
    logger.setLevel(logging.INFO)
    if logger.hasHandlers():
        logger.handlers.clear()
    os.makedirs(os.path.dirname(str(caminho_log)), exist_ok=True)
    handler = logging.FileHandler(str(caminho_log), encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def log_funcionamento_execucao(id_automacao, empresa_id, cpf, data_ini, data_fim, ie, mensagem):
    from config.config import LOG_OK
    nome = f"LogFuncionamento_{id_automacao}_{empresa_id}_{cpf}_{data_ini}_{data_fim}_{ie}.log"
    caminho = os.path.join(LOG_OK, nome)
    logger = setup_logger(nome, caminho)
    logger.info(mensagem)

def log_erro_execucao(id_automacao, empresa_id, cpf, data_ini, data_fim, ie, mensagem):
    from config.config import LOG_ERRO
    nome = f"LogErro_{id_automacao}_{empresa_id}_{cpf}_{data_ini}_{data_fim}_{ie}.log"
    caminho = os.path.join(LOG_ERRO, nome)
    logger = setup_logger(nome, caminho)
    logger.error(mensagem)

def log_monitoramento(mensagem):
    from config.config import LOG_MONITORAMENTO
    data = datetime.now().strftime("%d%m%Y")
    caminho = os.path.join(LOG_MONITORAMENTO, f"logMonitoramento_{data}.log")
    logger = setup_logger("monitoramento", caminho)
    logger.info(mensagem)

def tirar_screenshot(driver, nome_arquivo, destino):
    nome_arquivo = nome_arquivo.replace("/", "-").replace("\\", "-")
    caminho = os.path.join(destino, f"{nome_arquivo}.png")
    os.makedirs(os.path.dirname(caminho), exist_ok=True)
    try:
        driver.save_screenshot(caminho)
    except Exception as e:
        print(f"Erro ao tirar screenshot Selenium: {e}")



def enviar_discord_mensagem(mensagem, webhook_url):
    import requests
    try:
        resp = requests.post(webhook_url, json={"content": mensagem}, timeout=10)
        print("Discord status:", resp.status_code, resp.text)  # Isso vai para o terminal/log, ajuda a debugar
    except Exception as e:
        print(f"Erro ao enviar para Discord: {e}")


def salvar_controle_ie(id_automacao, empresa_id, cpf, data_ini, data_fim, ie, pasta):
    nome = f"ControleIEs {id_automacao} {empresa_id} {cpf} {data_ini}_{data_fim}.log"
    caminho = os.path.join(pasta, nome)
    os.makedirs(os.path.dirname(caminho), exist_ok=True)
    with open(caminho, "a", encoding="utf-8") as f:
        f.write(f"{ie} - {datetime.now()}\n")
