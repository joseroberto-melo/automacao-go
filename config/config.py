import os
import shutil
from pathlib import Path

RABBITMQ_QUEUE_IN = 'consulta-xml'
RABBITMQ_QUEUE_OUT = 'retorno-consulta-xml'
MAX_CPU_USAGE = 80
MAX_RAM_USAGE = 80
MAX_CHROME_INSTANCES = 10
DOWNLOAD_DIRECTORY = r"C:\SAAM-AUTOMACAO-GO\downloads_temp"  # Onde o Chrome salva temporário
XMLS_DIRECTORY = r"C:\SAAM-AUTOMACAO-GO\XMLS"               # Onde ficam os XMLs organizados


SAFE_MODE = True

BASE_DIR = Path(r"C:\SAAM-AUTOMACAO-GOIAS")

LOG_ERRO = BASE_DIR / "logErro"
LOG_OK = BASE_DIR / "logFuncionamento"
LOG_MONITORAMENTO = BASE_DIR / "logsMonitoramento"
LOG_CONTROLE = BASE_DIR / "logControleIEs"
LOG_SCREENSHOTS = BASE_DIR / "logsScreenshot"

# Criar todas as pastas necessárias no início
for path in [LOG_ERRO, LOG_OK, LOG_MONITORAMENTO, LOG_CONTROLE, LOG_SCREENSHOTS]:
    path.mkdir(parents=True, exist_ok=True)
