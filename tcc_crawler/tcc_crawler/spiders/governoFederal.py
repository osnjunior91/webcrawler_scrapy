# -*- coding: utf-8 -*-
import scrapy

class GovernofederalSpider(scrapy.Spider):
    name = 'governoFederal'
    #start_urls = ['http://portaldatransparencia.gov.br/servidores/Servidor-ListaServidores.asp']
    start_urls = ['http://portaldatransparencia.gov.br/servidores/Servidor-ListaServidores.asp?bogus=1&Pagina=%s' % page for page in range(1,7001)]
    
    def parse(self, response):
        items = response.xpath('//*[@id="listagem"]/table//tr')
        for item in items[1:]:
            link = item.xpath(".//td[2]/a")
            idFuncionario = str(link.xpath('./@href').extract_first()).split('=')[-1]
            yield scrapy.Request("http://portaldatransparencia.gov.br/servidores/" + link.xpath('./@href').extract_first(),
                                     callback=self.parse_page2, meta= {
                                         'idFuncionario': idFuncionario.strip(),
                                         'orgaoLotacao' : item.xpath(".//td[3]/text()").extract_first(),
                                         'orgaoExercicio': item.xpath(".//td[4]/text()").extract_first()
                                     })


    def parse_page2(self, response):
        trs = response.xpath('//*[@id="listagemConvenios"]/table//tbody//tr')
        for tr in trs:
            titulo =  str(tr.xpath('.//td[1]//text()').extract_first())
            if (("Cargo Emprego" in titulo) or ("Posto/Graduação:" in titulo)):
                cargo = str(tr.xpath('.//td[2]//text()').extract_first()).strip()
            if "Órgão Superior" in titulo:
                orgaoSuperior = str(tr.xpath('.//td[2]//text()').extract_first()).strip()
            if "Situação Vínculo" in titulo:
                situacao = str(tr.xpath('.//td[2]//text()').extract_first()).strip()
            if "Jornada de Trabalho" in titulo:
                jornada = str(tr.xpath('.//td[2]//text()').extract_first()).strip() 
        
        yield scrapy.Request("http://portaldatransparencia.gov.br" + 
                                 response.xpath('//*[@id="resumo"]/a/@href').extract_first(),
                                 callback=self.parse_page3, meta= {
                                    'idFuncionario': response.meta['idFuncionario'], 
                                    'cargo': cargo,
                                    'orgaoLotacao': str(response.meta['orgaoLotacao']).strip(),
                                    'orgaoExercicio': str(response.meta['orgaoExercicio']).strip(),
                                    'orgaoSuperior': orgaoSuperior,
                                    'situacao': situacao,
                                    'jornada': jornada
                                 })


    def parse_page3(self, response):
        meses = response.xpath('//*[@id="navegacaomeses"]//a')
        nome = response.xpath('//*[@id="resumo"]/table/tbody/tr[1]/td[2]/text()').extract_first()
        for mes in meses:
            yield scrapy.Request("http://portaldatransparencia.gov.br" + 
                                  mes.xpath('./@href').extract_first(), 
                                  callback=self.parse_page4, meta= {
                                    'idFuncionario': response.meta['idFuncionario'], 
                                    'nome': nome.strip(),
                                    'cargo': response.meta['cargo'],
                                    'orgaoLotacao': response.meta['orgaoLotacao'],
                                    'orgaoExercicio': response.meta['orgaoExercicio'],
                                    'orgaoSuperior': response.meta['orgaoSuperior'],
                                    'situacao': response.meta['situacao'],
                                    'jornada': response.meta['jornada']
                                  })
       

    def parse_page4(self, response):
        trs = response.xpath('//*[@id="listagemConvenios"]/table//tbody//tr')
        mes = int(response.url.split('=')[-1])
        for tr in trs:
            if "Remuneração básica bruta" in str(tr.xpath('.//td[2]//text()').extract_first()):
                salarioBruto = str(tr.xpath('.//td[3]//text()')
                                              .extract_first()).replace('.','').replace(',','.')

            if "Total da Remuneração Após Deduções" in str(tr.xpath('.//td[1]//text()').extract_first()):
                salarioLiquido = str(tr.xpath('.//td[2]//text()')
                                               .extract_first()).replace('.','').replace(',','.')
       
        yield {
            'id': str(response.meta['idFuncionario']),
            'nome': str(response.meta['nome']),
            'cargo': str(response.meta['cargo']),
            'orgaoLotacao': str(response.meta['orgaoLotacao']),
            'orgaoExercicio': str(response.meta['orgaoExercicio']),
            'orgaoSuperior' : str(response.meta['orgaoSuperior']),
            'situacao': str(response.meta['situacao']),
            'jornada': str(response.meta['jornada']),
            'mes': str(mes),
            'salarioLiquido': salarioLiquido,
            'salarioBruto': salarioBruto
        }
        