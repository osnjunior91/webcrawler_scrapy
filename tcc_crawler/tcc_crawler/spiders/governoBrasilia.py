# -*- coding: utf-8 -*-
import scrapy
import urllib3
import json

from collections import namedtuple

class GovernobrasiliaSpider(scrapy.Spider):
    name = 'governoBrasilia'
    allowed_domains = ['www.transparencia.df.gov.br/#/servidores/remuneracao']
    start_urls = ['http://www.transparencia.df.gov.br/api/remuneracao?anoExercicio=2017&size=30&mesReferencia=7']

    def parse(self, response):
        http = urllib3.PoolManager()
        lista = []
        r = http.request('GET', response.url)
        jsonResposta = json.loads(r.data.decode('utf-8'), object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        contador = int(jsonResposta.totalPages) + 1
        for pageApi in range(1, contador):
            url = str(response.url) + "&page=" + str(pageApi) 
            resposta = http.request('GET', url)
            jsonFuncionario = json.loads(resposta.data.decode('utf-8'), object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
            for dadosFuncionario in jsonFuncionario.content:
                lista.append({
                    'id': str(dadosFuncionario.codigoMatricula).replace("'",""),
                    'nome': str(dadosFuncionario.nomeServidor).replace("'","").strip(),
                    'cargo': str(dadosFuncionario.cargo).replace("'","").strip(),
                    'orgaoLotacao': str(dadosFuncionario.nomeOrgao).replace("'","").strip(),
                    'orgaoExercicio': str(dadosFuncionario.nomeOrgao).replace("'","").strip(),
                    'situacao': str(dadosFuncionario.situacaoFuncional).replace("'","").strip(),
                    'mes': dadosFuncionario.mesReferencia,
                    'salarioLiquido': dadosFuncionario.valorLiquido,
                    'salarioBruto': dadosFuncionario.valorRemuneracaoBasica
                })
        yield{
            'resultado': lista
        }
