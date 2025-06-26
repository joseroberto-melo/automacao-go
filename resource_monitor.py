import psutil
import time
import threading
from utils.logger import log_monitoramento
from config import config

def contar_chrome_selenium():
    count = 0
    for p in psutil.process_iter(['name', 'cmdline']):
        try:
            if (
                p.info['name']
                and 'chrome.exe' in p.info['name'].lower()
                and p.info['cmdline']
                and any('--remote-debugging-port' in arg for arg in p.info['cmdline'])
            ):
                count += 1
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return count

def monitor_resources():
    while True:
        cpu = psutil.cpu_percent(interval=1)
        ram = psutil.virtual_memory().percent
        chromes = contar_chrome_selenium()

        log_monitoramento(f"CPU: {cpu}% | RAM: {ram}% | Chrome Selenium: {chromes}")

        if cpu > config.MAX_CPU_USAGE:
            log_monitoramento(f"⚠️ Uso de CPU acima do limite ({cpu}% > {config.MAX_CPU_USAGE}%)")
        if ram > config.MAX_RAM_USAGE:
            log_monitoramento(f"⚠️ Uso de RAM acima do limite ({ram}% > {config.MAX_RAM_USAGE}%)")
        if chromes > config.MAX_CHROME_INSTANCES:
            log_monitoramento(f"⚠️ Instâncias do Chrome Selenium excedendo o limite ({chromes} > {config.MAX_CHROME_INSTANCES})")

        time.sleep(60)
