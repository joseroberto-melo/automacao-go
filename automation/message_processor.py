import os
import json
import time
import shutil
import pika
import psutil
from datetime import datetime, timedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from config import secrets
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

def enviar_retorno(id_automacao, token, status="OK", obs="OK"):
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
        "caminhoXmls": "",
        "dhConsulta": ""
    }
    print(f"RabbitMQ retorno: {retorno}")
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
        channel.basic_publish(exchange='', routing_key='retorno-consulta-xml', body=json.dumps(retorno), properties=props)
        connection.close()
    except Exception as ex:
        print("Falha ao enviar RabbitMQ:", ex)

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

def fazer_login(driver, cpf, senha):
    driver.set_page_load_timeout(90)
    driver.get("https://www.sefaz.go.gov.br/netaccess/000System/acessoRestrito/login/")
    driver.find_element(By.ID, "NetAccess.Login").send_keys(cpf)
    driver.find_element(By.ID, "NetAccess.Password").send_keys(senha)
    driver.find_element(By.ID, "btnAuthenticate").click()
    time.sleep(2)
    try:
        erro_login = driver.find_element(By.XPATH, "//div[contains(@class,'ui-state-error') and contains(., 'Usuário ou senha inválidos')]")
        if erro_login.is_displayed():
            raise Exception("INVALID_LOGIN")
    except NoSuchElementException:
        pass

def preencher_periodo(driver, ini, fim, empresa_ie):
    campo_ini = driver.find_element(By.ID, "cmpDataInicial")
    campo_ini.clear()
    campo_ini.send_keys(ini)
    campo_fim = driver.find_element(By.ID, "cmpDataFinal")
    campo_fim.clear()
    campo_fim.send_keys(fim)
    campo_ie = driver.find_element(By.ID, "cmpNumIeDest")
    campo_ie.clear()
    campo_ie.send_keys(empresa_ie)
    radio_entrada = driver.find_element(By.XPATH, "//input[@id='cmpTipoNota' and @value='0']")
    radio_entrada.click()
    select_modelo = driver.find_element(By.ID, "cmpModelo")
    from selenium.webdriver.support.ui import Select
    Select(select_modelo).select_by_value("-")

def process_message(message, properties=None):
    driver = None
    empresas = message["empresas"]
    data_inicial = message["dataInicial"][:10]
    data_final = message["dataFinal"][:10]
    cpf = message["contador"]["cpf"]
    senha = message["contador"]["senha"]
    id_automacao = message["id"]

    headers = getattr(properties, 'headers', None) or message.get('_headers', {})
    empresa_id = headers.get("identificador", "")
    token = headers.get("token", "")

    dt_ini = data_inicial.replace("-", "")
    dt_fim = data_final.replace("-", "")
    periodos = dividir_periodo(data_inicial, data_final, dias=30)
    total_ies = len(empresas)
    resultado_final = []

    enviar_retorno(id_automacao, token, status="PROCESSING", obs=f"Iniciando processamento de {total_ies} IEs.")

    for idx, empresa in enumerate(empresas, 1):
        empresa_ie = empresa["ie"]
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

                fazer_login(driver, cpf, senha)
                log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, "Login realizado com sucesso.")

                for ini, fim in periodos:
                    try:
                        WebDriverWait(driver, 30).until(EC.frame_to_be_available_and_switch_to_it((By.ID, "iNetaccess")))
                        log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, "Entrou no iframe 'iNetaccess'.")

                        time.sleep(2)
                        driver.find_element(By.XPATH, "//a[contains(text(),'Baixar XML NFE')]").click()
                        log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, "Clicou em 'Baixar XML NFE'.")
                        time.sleep(3)

                        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, "btnPesquisar")))
                        log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, "Campo 'Pesquisar' disponível.")

                        # Preenche campos
                        preencher_periodo(driver, ini, fim, empresa_ie)
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
                            status_ie = "ERROR"
                            tirar_screenshot(f"{id_automacao}_{empresa_ie}_{ini}_{fim}".replace("/", "-"), LOG_SCREENSHOTS)
                            log_erro_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, erro_ie)
                            break

                        # Checa sem resultados
                        if checar_alerta(driver, "//div[contains(@class,'alert-danger') and contains(.,'Sem Resultados!')]"):
                            erro_ie = "Nenhum resultado encontrado para o período/IE informado."
                            status_ie = "ERROR"
                            tirar_screenshot(f"{id_automacao}_{empresa_ie}_{ini}_{fim}".replace("/", "-"), LOG_SCREENSHOTS)
                            log_erro_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, erro_ie)
                            tentativas = max_tentativas
                            break
                        # Download XMLs
                        WebDriverWait(driver, 20).until(
                            EC.presence_of_element_located((By.XPATH, "//button[contains(@class, 'btn-download-all')]"))
                        )
                        total_notas = int(driver.find_element(
                            By.XPATH, "//div[contains(@class, 'table-legend-right-container')]/div"
                        ).text)
                        if total_notas > 10000:
                            erro_ie = "Consulta retornou mais de 10.000 notas. Reduza o período de consulta!"
                            status_ie = "ERROR"
                            tirar_screenshot(f"{id_automacao}_{empresa_ie}_{ini}_{fim}".replace("/", "-"), LOG_SCREENSHOTS)
                            log_erro_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, erro_ie)
                            continue

                        btn_baixar_tudo = driver.find_element(By.XPATH, "//button[contains(@class, 'btn-download-all')]")
                        btn_baixar_tudo.click()
                        log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, "Clicou em 'Baixar todos os arquivos'.")
                        WebDriverWait(driver, 20).until(
                            EC.visibility_of_element_located((By.XPATH, "//button[contains(@class, 'btn-info') and contains(.,'Baixar')]"))
                        )
                        btn_modal_baixar = driver.find_element(By.XPATH, "//button[contains(@class,'btn-info') and contains(.,'Baixar')]")
                        btn_modal_baixar.click()
                        log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, "Clicou em 'Baixar' no modal.")
                        WebDriverWait(driver, 120).until(
                            EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'modal-content')]//h4[contains(.,'Concluído')]"))
                        )
                        log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, "Download concluído (tela de concluído apareceu).")

                        # Move arquivos para XMLS_DIRECTORY (com CPF!)
                        data_execucao = datetime.now().strftime("%Y-%m-%d")
                        destino = os.path.join(XMLS_DIRECTORY, str(cpf), empresa_ie, data_execucao)
                        os.makedirs(destino, exist_ok=True)
                        for nome_arquivo in os.listdir(DOWNLOAD_DIRECTORY):
                            origem = os.path.join(DOWNLOAD_DIRECTORY, nome_arquivo)
                            destino_arquivo = os.path.join(destino, nome_arquivo)
                            shutil.move(origem, destino_arquivo)
                        log_funcionamento_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie,
                            f"IE {empresa_ie}: Baixou {total_notas} XMLs para o período {ini} a {fim}. Arquivos salvos em {destino}")
                        erro_ie = ""
                        status_ie = "OK"
                        break  # Sucesso
                    except Exception as e:
                        erro_ie = mapear_erro_legivel(e)
                        status_ie = "ERROR"
                        tirar_screenshot(f"{id_automacao}_{empresa_ie}_{ini}_{fim}".replace("/", "-"), LOG_SCREENSHOTS)
                        log_erro_execucao(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, f"Erro no período {ini} a {fim}: {erro_ie}")
                        continue

                salvar_controle_ie(id_automacao, empresa_id, cpf, dt_ini, dt_fim, empresa_ie, LOG_CONTROLE)
                if status_ie == "OK":
                    break  # Não precisa tentar de novo se chegou aqui!
            except Exception as e:
                erro_ie = mapear_erro_legivel(e)
                status_ie = "ERROR"
                tirar_screenshot(f"{id_automacao}_{empresa_ie}_{dt_ini}_{dt_fim}".replace("/", "-"), LOG_SCREENSHOTS)
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

    # Relatório único
    relatorio = ""
    tem_erro = False
    for r in resultado_final:
        relatorio += f"IE: {r['ie']} | Status: {r['status']}"
        if r['erro']:
            relatorio += f" | Erro: {r['erro']}"
            tem_erro = True
        relatorio += "\n"

    enviar_relatorio_discord(relatorio, empresa_id, data_inicial, data_final, tem_erro)
    status_final = "OK" if not tem_erro else "ERROR"
    enviar_retorno(id_automacao, token, status=status_final, obs=relatorio)
