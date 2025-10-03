#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 19 11:05:12 2025

@author: marcosperrude
"""

import psutil
import threading
import time
import csv


def start_monitor(interval, output="outputs/monitoramento.csv"):
    def monitor():
        proc = psutil.Process()  # próprio script
        with open(output, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["tempo", "Uso_CPU (Porcentagem)", "Uso_RAM (Porcentagem)",
                             "Uso_Ram (MB)", "Memoria_Alocada (MB)"  ])
            while proc.is_running():
                cpu = proc.cpu_percent(interval=None)
                mem = proc.memory_percent()                            # % RAM usada
                rss = proc.memory_info().rss / (1024*1024)            # Memória residente MB
                vms = proc.memory_info().vms / (1024*1024)    
                # swap = psutil.swap_memory().percent # Memória virtual MB     # Memoria alocada para a swap MB
                writer.writerow([time.strftime("%Y-%m-%d %H:%M:%S"), cpu, mem, rss, vms ])
                f.flush()                                             # força gravação imediata
                time.sleep(interval)                                  # espera intervalo antes da próxima medição

    # Cria uma thread daemon para rodar em paralelo, sem bloquear o script principal
    threading.Thread(target=monitor, daemon=True).start()
