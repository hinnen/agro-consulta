@echo off
REM Copie este arquivo para config.bat e ajuste os valores abaixo.
REM Nome exato da impressora: Painel de controle ^> Impressoras ^> clique com botao direito ^> Propriedades.

set "AGRO_CHROME=C:\Program Files\Google\Chrome\Application\chrome.exe"
set "AGRO_URL_BASE=https://SEU-SITE.onrender.com"

REM Etiquetas 4x4 (termica ELGIN ou similar)
set "AGRO_IMPRESSORA_ETIQUETAS=ELGIN L42 PRO FULL"
set "AGRO_URL_ETIQUETAS=%AGRO_URL_BASE%/produtos/etiquetas/"

REM Cupom fiscal PDV (termica 80mm)
set "AGRO_IMPRESSORA_CUPOM=EPSON TM-T20"
set "AGRO_URL_PDV=%AGRO_URL_BASE%/consulta/"

REM 1 = define impressora padrao do Windows ao abrir o atalho (recomendado se usa dois atalhos no mesmo PC)
REM 0 = nao muda a padrao (voce define manualmente uma vez por estacao)
set "AGRO_DEFINIR_IMPRESSORA_PADRAO=1"
