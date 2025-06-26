import os
import json
import time
import shutil
import pika
import time
from pathlib import Path
import psutil
from datetime import datetime, timedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from config import secrets
from selenium.webdriver.support.ui import Select
from config.config import LOG_OK, LOG_ERRO, LOG_CONTROLE, LOG_SCREENSHOTS, DOWNLOAD_DIRECTORY, XMLS_DIRECTORY
from utils.logger import (
    setup_logger, gerar_nome_log, salvar_controle_ie,
    log_funcionamento_execucao, log_erro_execucao,
    tirar_screenshot, enviar_discord_mensagem
)

MAX_RAM_PERCENT = 85

def ram_livre():
    return psutil.virtual_memory().percent < MAX_RAM_PERCENT

def mapear_erro_legivel(erro_raw):
    erro_str = str(erro_raw).strip().lower()
    if "no such element" in erro_str:
        return "Elemento esperado não foi encontrado no site. O layout pode ter mudado."
    if "timeout" in erro_str:
        return "O site demorou demais para responder. Tente novamente mais tarde ou reduza o período de busca."
    if "captcha" in erro_str:
        return "O site solicitou validação captcha e não é possível automatizar esse passo."
    if "connection refused" in erro_str or "connectionreseterror" in erro_str:
        return "Falha na conexão com o navegador ou o site está fora do ar."
    if "stacktrace" in erro_str or "gethandleverifier" in erro_str:
        return "Ocorreu um erro inesperado ao acessar o portal SEFAZ. Tente novamente mais tarde."
    if "sem resultados" in erro_str:
        return "Nenhum resultado encontrado para o período/IE informado."
    if "perm" in erro_str and "nega" in erro_str:
        return "Permissão negada (verifique a data final)."
    msg = str(erro_raw).strip()
    if msg.startswith("Message:") and (len(msg) == 8 or msg == "Message:"):
        return "Erro desconhecido ao executar o robô."
    if msg == "INVALID_LOGIN":
        return "Usuário ou senha inválidos."
    return msg if msg else "Erro desconhecido ao executar o robô."

def enviar_relatorio_discord(relatorio, empresa_id, data_ini, data_fim, tem_erro):
    emoji = "✅" if not tem_erro else "❌"
    header = (
        "----- RELATÓRIO -----\n"
        f"{emoji} Executado {'com sucesso' if not tem_erro else 'com erro'}\n\n"
        f"Empresa: {empresa_id}\n"
        f"Período: {data_ini} até {data_fim}\n\n"
    )
    mensagem = "```\n" + header + relatorio + "```"
    enviar_discord_mensagem(mensagem, secrets.DISCORD_WEBHOOK)

def enviar_retorno(id_automacao, token, status="OK", obs="OK", caminho_xmls=""):
    status_rabbit = {
        "OK": "FINISHED",
        "INVALID": "INVALID",
        "ERROR": "ERROR",
        "PROCESSING": "PROCESSING"
    }.get(status, status)
    retorno = {
        "id": id_automacao,
        "status": status_rabbit,
        "obs": obs,
        "caminhoXmls": caminho_xmls,
        "dhConsulta": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    print(f"RabbitMQ retorno: {retorno}")
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=secrets.RABBITMQ_HOST,
                port=secrets.RABBITMQ_PORT,
                blocked_connection_timeout=300,
                credentials=pika.PlainCredentials(secrets.RABBITMQ_USER, secrets.RABBITMQ_PASSWORD)
            )
        )

        channel = connection.channel()
        channel.queue_declare(queue='retorno-consulta-xml', durable=True)
        props = pika.BasicProperties(headers={"token": token})
        channel.basic_publish(exchange='', routing_key='retorno-consulta-xml', body=json.dumps(retorno), properties=props)
        connection.close()
    except Exception as ex:
        print("Falha ao enviar RabbitMQ:", ex)

def mover_arquivos_para_xml(destino, total_notas, id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie):
    os.makedirs(destino, exist_ok=True)
    for nome_arquivo in os.listdir(DOWNLOAD_DIRECTORY):
        origem = os.path.join(DOWNLOAD_DIRECTORY, nome_arquivo)
        destino_arquivo = os.path.join(destino, nome_arquivo)
        shutil.move(origem, destino_arquivo)
    log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie,
        f"IE {empresa_ie}: Baixou {total_notas} XMLs. Arquivos salvos em {destino}")

def iniciar_driver():
    from automation.browser_driver import get_driver
    driver = get_driver(DOWNLOAD_DIRECTORY)
    driver.maximize_window()
    return driver

def checar_alerta(driver, xpath):
    try:
        el = driver.find_element(By.XPATH, xpath)
        return el.is_displayed()
    except Exception:
        return False

def dividir_periodo(data_inicial, data_final, dias=30):
    inicio = datetime.strptime(data_inicial, "%Y-%m-%d")
    fim = datetime.strptime(data_final, "%Y-%m-%d")
    periodos = []
    while inicio <= fim:
        sub_fim = min(inicio + timedelta(days=dias - 1), fim)
        periodos.append((inicio.strftime("%d/%m/%Y"), sub_fim.strftime("%d/%m/%Y")))
        inicio = sub_fim + timedelta(days=1)
    return periodos

def esperar_download_concluir(pasta, timeout=120):
    fim = time.time() + timeout
    while time.time() < fim:
        arquivos = os.listdir(pasta)
        if any(a.endswith(".zip") for a in arquivos) and not any(a.endswith(".crdownload") or a.endswith(".tmp") for a in arquivos):
            return True
        time.sleep(1)
    return False

def fazer_login(driver, cpf, senha):
    from selenium.common.exceptions import TimeoutException

    driver.set_page_load_timeout(90)
    driver.get("https://www.sefaz.go.gov.br/netaccess/000System/acessoRestrito/login/")

    driver.find_element(By.ID, "NetAccess.Login").send_keys(cpf)
    driver.find_element(By.ID, "NetAccess.Password").send_keys(senha)
    driver.find_element(By.ID, "btnAuthenticate").click()

    # Espera até 8 segundos para: login com sucesso OU alerta de erro aparecer
    try:
        WebDriverWait(driver, 8).until(
            lambda d: (
                "acessoRestrito/login" not in d.current_url or
                checar_alerta(d, "//*[@id='richValidationBox7']")  # elemento do alerta
            )
        )
    except TimeoutException:
        pass  # nada visível ainda, vamos verificar abaixo

    # Checagem final: se ainda está na página de login e alerta apareceu, login falhou
    if "acessoRestrito/login" in driver.current_url:
        try:
            erro_login = driver.find_element(By.XPATH, "//*[@id='richValidationBox7']")
            if "usuário ou senha inválidos" in erro_login.text.lower():
                raise Exception("INVALID_LOGIN")
        except Exception:
            raise Exception("INVALID_LOGIN")

def carregar_ies_processadas(arquivo_controle):
    try:
        with open(arquivo_controle, "r") as f:
            return set([linha.strip() for linha in f if linha.strip()])
    except FileNotFoundError:
        return set()

def preencher_periodo_robusto(driver, ini, fim, empresa_ie, max_tentativas=3):
    """
    Preenche datas de início e fim, clica em pesquisar e, se detectar erro de data (obrigatória/inválida),
    repreenche ambos os campos até 3 vezes antes de lançar erro real.
    """
    for tentativa in range(1, max_tentativas + 1):
        campo_ini = driver.find_element(By.ID, "cmpDataInicial")
        campo_ini.clear()
        time.sleep(0.3)
        campo_ini.send_keys(ini)
        campo_fim = driver.find_element(By.ID, "cmpDataFinal")
        campo_fim.clear()
        time.sleep(0.3)
        campo_fim.send_keys(fim)
        btn_pesquisar = driver.find_element(By.ID, "btnPesquisar")
        btn_pesquisar.click()
        time.sleep(2)

        # Checa TODOS os alertas conhecidos
        for alerta_msg in [
            "A data inicial é obrigatória",
            "A data final é obrigatória",
            "A data inicial é inválida",
            "A data final é inválida"
        ]:
            if checar_alerta(driver, f"//div[contains(@class,'alert-danger') and contains(.,'{alerta_msg}')]"):
                log_funcionamento_execucao(
                    None, None, None, ini, fim, empresa_ie,
                    f"Alerta detectado ao preencher datas na tentativa {tentativa}: {alerta_msg} — Repreenchendo campos."
                )
                break  # Tenta novamente
        else:
            # Nenhum alerta: sucesso!
            return True

    # Se chegou aqui, não conseguiu preencher certo após max_tentativas
    raise Exception(f"Erro ao preencher datas {ini} a {fim}: não conseguiu validar após {max_tentativas} tentativas.")


def preencher_periodo(driver, ini, fim, empresa_ie):
    time.sleep(2)
    campo_ini = driver.find_element(By.ID, "cmpDataInicial")
    campo_ini.clear()
    time.sleep(2)
    campo_ini.send_keys(ini)
    campo_fim = driver.find_element(By.ID, "cmpDataFinal")
    campo_fim.clear()
    time.sleep(2)
    campo_fim.send_keys(fim)
    campo_ie = driver.find_element(By.ID, "cmpNumIeDest")
    campo_ie.clear()
    time.sleep(2)
    campo_ie.send_keys(empresa_ie)
    radio_entrada = driver.find_element(By.XPATH, "//input[@id='cmpTipoNota' and @value='0']")
    radio_entrada.click()
    select_modelo = driver.find_element(By.ID, "cmpModelo")
    from selenium.webdriver.support.ui import Select
    Select(select_modelo).select_by_value("-")

def atualizar_status_parcial(id_automacao, token, empresa_ie, atual, total, empresa_id, cpf, dt_ini, dt_fim, caminho_xmls):
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=secrets.RABBITMQ_HOST,
                    port=secrets.RABBITMQ_PORT,
                    virtual_host='/',
                    credentials=pika.PlainCredentials(secrets.RABBITMQ_USER, secrets.RABBITMQ_PASSWORD)
                )
            )
            channel = connection.channel()
            channel.queue_declare(queue='retorno-consulta-xml', durable=True)
            props = pika.BasicProperties(headers={"token": token})
            payload = {
                "id": id_automacao,
                "status": "PROCESSING",
                "obs": f"Processando {atual}/{total} - IE {empresa_ie}",
                "caminhoXmls": caminho_xmls
            }
            channel.basic_publish(
                exchange='',
                routing_key='retorno-consulta-xml',
                body=json.dumps(payload),
                properties=props
            )
            connection.close()
            log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, f"📤 Atualização enviada: {payload}")
        except Exception as e:
            log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, f"❌ Erro ao enviar status parcial RabbitMQ: {e}")

def process_message(message, properties=None):
    caminho_xmls = []
    driver = None
    total_geral_notas = 0
    empresas_original = message["empresas"]
    empresas = []
    for e in empresas_original:
        oper = str(e.get("oper", "Todos")).strip()
        if oper.lower() == "todos":
            empresas.append({**e, "oper": "1"})  # Entrada
            empresas.append({**e, "oper": "0"})  # Saída
        else:
            empresas.append(e)

    data_inicial = message["dataInicial"][:10]
    data_final = message["dataFinal"][:10]
    cpf = message["contador"]["cpf"]
    senha = message["contador"]["senha"]
    id_automacao = message["id"]

    headers = getattr(properties, 'headers', None) or message.get('_headers', {})
    empresa_id = headers.get("identificador", "")
    token = headers.get("token", "")

    dt_ini = datetime.strptime(data_inicial, "%Y-%m-%d").strftime("%d%m%Y")
    dt_fim = datetime.strptime(data_final, "%Y-%m-%d").strftime("%d%m%Y")
    periodos = dividir_periodo(data_inicial, data_final, dias=30)
    total_ies = len(empresas)
    resultado_final = []
    login_verificado = False

    enviar_retorno(id_automacao, token, status="PROCESSING", obs=f"Iniciando processamento de {total_ies} IEs.")
    arquivo_controle = os.path.join(LOG_CONTROLE, f"{empresa_id}_{cpf}_{periodo_str}.txt")
    ies_ja_processadas = set()
    try:
        with open(arquivo_controle, "r") as f:
            ies_ja_processadas = set([linha.strip() for linha in f if linha.strip()])
    except FileNotFoundError:
        ies_ja_processadas = set()

    for idx, empresa in enumerate(empresas, 1):
        empresa_ie = empresa["ie"]
        if empresa_ie in ies_ja_processadas:
            log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie,
            f"IE {empresa_ie} já processada anteriormente — pulando.")
            continue
        periodo_str = f"{dt_ini}_{dt_fim}"
        destino_base = os.path.join(XMLS_DIRECTORY, str(empresa_id), str(cpf), periodo_str)
        destino = os.path.join(destino_base, empresa_ie)
        if not caminho_xmls:
            caminho_xmls.append(destino_base)
        atualizar_status_parcial(id_automacao, token, empresa_ie, idx, len(empresas), empresa_id, cpf, dt_ini, dt_fim, caminho_xmls=";".join(caminho_xmls))
        tentativas = 0
        max_tentativas = 5
        status_ie = "OK"
        erro_ie = ""
        total_notas = 0
        log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie,
            f"------ Iniciando execução para IE {empresa_ie} (tentativas máximas: {max_tentativas}) ------"
        )

        while tentativas < max_tentativas:
            tentativas += 1
            log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie,
                f"Tentativa {tentativas} de {max_tentativas} para IE {empresa_ie}"
            )
            if not ram_livre():
                log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, "RAM acima do limite, aguardando para nova tentativa...")
                time.sleep(30)
                continue
            try:
                driver = iniciar_driver()
                driver.execute_script(f"document.title = 'AUTOMACAO_{id_automacao}_{empresa_ie}'")
                log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, "Chrome aberto e maximizado.")

                try:
                    fazer_login(driver, cpf, senha)
                    log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, "Login realizado com sucesso.")
                    login_verificado = True
                except Exception as e:
                    erro_ie = mapear_erro_legivel(e)
                    url_atual = driver.current_url.lower()
                    if  not login_verificado and "acessorestrito/login" in url_atual and "usuário ou senha inválidos" in erro_ie.lower():
                        status_ie = "INVALID_LOGIN"
                        erro_ie = "Usuário ou senha inválidos."
                        tirar_screenshot(driver, f"{id_automacao}_{empresa_ie}_{dt_ini}_{dt_fim}".replace("/", "-"), LOG_SCREENSHOTS)
                        log_erro_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, erro_ie)

                        resultado_final.append({
                            "ie": empresa_ie,
                            "status": status_ie,
                            "erro": erro_ie
                        })

                        # Parar execução imediatamente
                        mensagem = (
                            "----- RELATÓRIO FINAL -----\n\n"
                            f"Empresa: {empresa_id}\n"
                            f"Período: {data_inicial} até {data_final}\n\n"
                            f"🔒 ERRO CRÍTICO: Usuário ou senha inválidos. Execução abortada.\n"
                        )
                        enviar_discord_mensagem(f"```\n{mensagem}\n```", secrets.DISCORD_WEBHOOK)
                        enviar_retorno(id_automacao, token, status="INVALID", obs=erro_ie)
                        return  # Finaliza a função process_message imediatamente

                    raise e  

                for ini, fim in periodos:
                    try:
                        executou_download_paginado = False
                        WebDriverWait(driver, 30).until(EC.frame_to_be_available_and_switch_to_it((By.ID, "iNetaccess")))
                        log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, "Entrou no iframe 'iNetaccess'.")

                        time.sleep(2)
                        driver.find_element(By.XPATH, "//a[contains(text(),'Baixar XML NFE')]").click()
                        log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, "Clicou em 'Baixar XML NFE'.")
                        time.sleep(3)

                        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, "btnPesquisar")))
                        log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, "Campo 'Pesquisar' disponível.")

                        # Preenche campos
                        preencher_periodo_robusto(driver, ini, fim, empresa_ie)
                        # Marcar tipo da nota: entrada (0) ou saída (1)
                        tipo_oper = str(empresa.get("oper", "0")).strip()
                        if tipo_oper == "1":
                            driver.find_element(By.XPATH, "//input[@id='cmpTipoNota' and @value='0']").click()
                        else:
                            driver.find_element(By.XPATH, "//input[@id='cmpTipoNota' and @value='1']").click()

                        log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, f"Preencheu período: {ini} a {fim}.")

                        btn_pesquisar = driver.find_element(By.ID, "btnPesquisar")
                        btn_pesquisar.click()
                        log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, "Clicou em 'Pesquisar'.")
                        time.sleep(2)

                        # Tentar corrigir datas se erro
                        for tent_corrige_data in range(3):
                            if checar_alerta(driver, "//div[contains(@class,'alert-danger') and contains(.,'A data final é obrigatória')]"):
                                campo_fim = driver.find_element(By.ID, "cmpDataFinal")
                                campo_fim.clear()
                                campo_fim.send_keys(fim)
                                btn_pesquisar.click()
                                log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, "Corrigiu data final obrigatória e clicou em 'Pesquisar'.")
                                time.sleep(2)
                                continue
                            if checar_alerta(driver, "//div[contains(@class,'alert-danger') and contains(.,'A data inicial é obrigatória')]"):
                                campo_ini = driver.find_element(By.ID, "cmpDataInicial")
                                campo_ini.clear()
                                campo_ini.send_keys(ini)
                                btn_pesquisar.click()
                                log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, "Corrigiu data inicial obrigatória e clicou em 'Pesquisar'.")
                                time.sleep(2)
                                continue

                        # Checa erro de permissão/erro por causa de data errada
                        if checar_alerta(driver, "//label[contains(.,'Você não tem permissão para acessar esta página')]"):
                            erro_ie = "Permissão negada (verifique a data final)."
                            status_ie = "PERMISSAO_NEGADA"
                            tirar_screenshot(driver, f"{id_automacao}_{empresa_ie}_{ini}_{fim}".replace("/", "-"), LOG_SCREENSHOTS)
                            log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, erro_ie)
                            tentativas = max_tentativas
                            break

                        # Checa sem resultados
                        if checar_alerta(driver, "//div[contains(@class,'alert-danger') and contains(.,'Sem Resultados!')]"):
                            erro_ie = "Nenhum resultado encontrado para o período/IE informado."
                            status_ie = "OK"
                            log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, erro_ie)
                            tentativas = 4
                            break
                        # Download XMLs
                        WebDriverWait(driver, 20).until(
                            EC.presence_of_element_located((By.XPATH, "//button[contains(@class, 'btn-download-all')]"))
                        )
                        total_notas = int(driver.find_element(
                            By.XPATH, "//div[contains(@class, 'table-legend-right-container')]/div"
                        ).text)
                        total_geral_notas += total_notas
                        if total_notas > 10000:
                            log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, f"Mais de 10.000 notas ({total_notas}). Iniciando download em blocos.")
                            ultima_pagina = int(driver.find_element(By.XPATH, "//*[@id='pagination-container']/div/ul/li[last()]").get_attribute("data"))
                            pagina_ini = 1
                            pagina_fim = 500
                            while pagina_ini <= ultima_pagina:
                                driver.find_element(By.XPATH, "//button[contains(@class, 'btn-download-all')]").click()
                                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, "campoSelectTipodwnload")))
                                Select(driver.find_element(By.ID, "campoSelectTipodwnload")).select_by_value("4")

                                campo_ini = driver.find_element(By.ID, "cmpPagIni")
                                campo_fim = driver.find_element(By.ID, "cmpPagFin")
                                campo_ini.clear()
                                campo_fim.clear()
                                campo_ini.send_keys(str(pagina_ini))
                                campo_fim.send_keys(str(min(pagina_fim, ultima_pagina)))

                                btn_modal_baixar = driver.find_element(By.ID, "dnwld-all-btn-ok")
                                btn_modal_baixar.click()

                                tentativas_download = 0
                                while tentativas_download < 5:
                                    tentativas_download += 1
                                    try:
                                        WebDriverWait(driver, 20).until(
                                            EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'modal-content')]//h4[contains(.,'Concluído')]"))
                                        )
                                        break  # sucesso
                                    except:
                                        if checar_alerta(driver, "//div[contains(@class,'alert-danger') and contains(.,'erro interno ao realizar o download')]"):
                                            log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie,
                                                f"Tentativa {tentativas_download}: erro interno detectado ao baixar. Recarregando a página.")
                                            driver.refresh()
                                            WebDriverWait(driver, 30).until(EC.frame_to_be_available_and_switch_to_it((By.ID, "iNetaccess")))
                                            driver.find_element(By.XPATH, "//a[contains(text(),'Baixar XML NFE')]").click()
                                            time.sleep(2)
                                            preencher_periodo_robusto(driver, ini, fim, empresa_ie)
                                            if tipo_oper == "1":
                                                driver.find_element(By.XPATH, "//input[@id='cmpTipoNota' and @value='0']").click()
                                            else:
                                                driver.find_element(By.XPATH, "//input[@id='cmpTipoNota' and @value='1']").click()
                                            driver.find_element(By.ID, "btnPesquisar").click()
                                            WebDriverWait(driver, 20).until(
                                                EC.presence_of_element_located((By.XPATH, "//button[contains(@class, 'btn-download-all')]"))
                                            )
                                            driver.find_element(By.XPATH, "//button[contains(@class, 'btn-download-all')]").click()
                                            WebDriverWait(driver, 15).until(
                                                EC.presence_of_element_located((By.ID, "campoSelectTipodwnload"))
                                            )
                                            Select(driver.find_element(By.ID, "campoSelectTipodwnload")).select_by_value("4")
                                            campo_ini = driver.find_element(By.ID, "cmpPagIni")
                                            campo_fim = driver.find_element(By.ID, "cmpPagFin")
                                            campo_ini.clear()
                                            campo_fim.clear()
                                            campo_ini.send_keys(str(pagina_ini))
                                            campo_fim.send_keys(str(min(pagina_fim, ultima_pagina)))
                                            btn_modal_baixar = driver.find_element(By.ID, "dnwld-all-btn-ok")
                                            btn_modal_baixar.click()
                                            time.sleep(2)
                                        else:
                                            raise Exception("Erro inesperado ao tentar baixar XML em blocos.")

                                log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie,
                                f"Bloco {pagina_ini}-{pagina_fim} baixado com sucesso.")
                                mover_arquivos_para_xml(destino, total_notas, id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie)
                                driver.find_element(By.XPATH, "/html/body/div[5]/div/div/div[3]/button[2]").click()
                                pagina_ini += 500
                                pagina_fim += 500
                                time.sleep(2)
                                executou_download_paginado = True
                                continue


                        if  not executou_download_paginado:
                            btn_baixar_tudo = driver.find_element(By.XPATH, "//button[contains(@class, 'btn-download-all')]")
                            btn_baixar_tudo.click()
                            log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, "Clicou em 'Baixar todos os arquivos'.")
                            WebDriverWait(driver, 20).until(
                                EC.visibility_of_element_located((By.XPATH, "//button[contains(@class, 'btn-info') and contains(.,'Baixar')]"))
                            )
                            btn_modal_baixar = driver.find_element(By.XPATH, "//button[contains(@class,'btn-info') and contains(.,'Baixar')]")
                            btn_modal_baixar.click()
                            log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, "Clicou em 'Baixar' no modal.")
                            WebDriverWait(driver, 600).until(
                                EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'modal-content')]//h4[contains(.,'Concluído')]"))
                            )
                            log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, "Download concluído (tela de concluído apareceu).")

                            if not esperar_download_concluir(DOWNLOAD_DIRECTORY, timeout=120):
                                erro_ie = "Nenhum arquivo ZIP identificado após a conclusão de download."
                                status_ie = "ERROR"
                                tirar_screenshot(driver, f"{id_automacao}_{empresa_ie}_{dt_ini}_{dt_fim}".replace("/", "-"), LOG_SCREENSHOTS)
                                log_erro_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, erro_ie)
                                continue

                            # Move arquivos para XMLS_DIRECTORY (com CPF!)
                            data_execucao = datetime.now().strftime("%Y-%m-%d")
                            # Data no formato ddmmyyyy_ddmmyyyy
                            periodo_str = f"{dt_ini}_{dt_fim}"  
                            destino_base = os.path.join(XMLS_DIRECTORY, str(empresa_id), str(cpf), periodo_str)
                            destino = os.path.join(destino_base, empresa_ie)
                            os.makedirs(destino, exist_ok=True)
                            for nome_arquivo in os.listdir(DOWNLOAD_DIRECTORY):
                                if not nome_arquivo.endswith(".zip"):
                                    continue
                                try:
                                    origem = os.path.join(DOWNLOAD_DIRECTORY, nome_arquivo)
                                    destino_arquivo = os.path.join(destino, nome_arquivo)
                                    shutil.move(origem, destino_arquivo)
                                except Exception as e:
                                    erro_ie = f"Erro ao mover arquivo: {e}"
                                    log_erro_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, erro_ie)
                            log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie,
                                f"IE {empresa_ie}: Baixou {total_notas} XMLs para o período {ini} a {fim}. Arquivos salvos em {destino}")
                        erro_ie = ""
                        status_ie = "OK"
                        break  # Sucesso
                    except Exception as e:
                        erro_ie = mapear_erro_legivel(e)
                        status_ie = "ERROR"
                        if tentativas == max_tentativas:
                            tirar_screenshot(driver, f"{id_automacao}_{empresa_ie}_{ini}_{fim}".replace("/", "-"), LOG_SCREENSHOTS)
                            log_erro_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, f"Erro no período {ini} a {fim}: {erro_ie}")
                        continue

                
                if status_ie == "OK":
                    salvar_controle_ie(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, LOG_CONTROLE)
                    break  # Não precisa tentar de novo se chegou aqui!
            except Exception as e:
                erro_ie = mapear_erro_legivel(e)
                status_ie = "ERROR"
                tirar_screenshot(driver, f"{id_automacao}_{empresa_ie}_{dt_ini}_{dt_fim}".replace("/", "-"), LOG_SCREENSHOTS)
                log_erro_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, f"Erro geral: {erro_ie}")
            finally:
                if driver:
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    driver = None
                time.sleep(2)

        resultado_final.append({
            "ie": empresa_ie,
            "status": status_ie,
            "erro": erro_ie
        })
        if status_ie in ("OK", "PERMISSAO_NEGADA", "SEM_RESULTADO"):
            salvar_controle_ie(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, LOG_CONTROLE)

    

    # Relatório único
    ies_sucesso = []
    ies_sem_resultado = []
    ies_erro_real = []
    ies_perm_negada = []

    for r in resultado_final:
        status = r.get('status', '').strip().upper()
        erro = r.get('erro', '').strip()

        if status == "OK" and erro:
            ies_sem_resultado.append(r['ie'])
        elif status == "PERMISSAO_NEGADA":
            ies_perm_negada.append(r['ie'])
        elif status == "ERROR":
            ies_erro_real.append(f"{r['ie']} ({erro})")
        else:
            ies_sucesso.append(r['ie'])

    mensagem = (
        "----- RELATÓRIO FINAL -----\n\n"
        f"Período: {data_inicial} até {data_final}\n"
        f"Empresa: {empresa_id}\n\n"
        f"✅ Sucesso: {len(ies_sucesso)} IEs\n"
        f"📦 Total de XMLs baixados: {total_geral_notas}\n"
        f"📄 Sem resultado: {len(ies_sem_resultado)} IEs\n"
        f"🚫 Permissão negada: {len(ies_perm_negada)} IEs\n"
        f"❌ Com erro: {len(ies_erro_real)} IEs\n\n"
    )

    enviar_discord_mensagem(f"```\n{mensagem}\n```", secrets.DISCORD_WEBHOOK)

    status_final = "OK"
    if any(r["status"] == "ERROR" for r in resultado_final):
        status_final = "ERROR"
    enviar_retorno(id_automacao, token, status=status_final, obs="", caminho_xmls=";".join(caminho_xmls))

    
