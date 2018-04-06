# -*- coding: utf-8 -*-
import scrapy


class GovernominasgeraisSpider(scrapy.Spider):
    name = 'governoMinasGerais'
    allowed_domains = ['www.transparencia.mg.gov.br']
    start_urls = ['http://www.transparencia.mg.gov.br/estado-pessoal/remuneracao-dos-servidores?view=estado_remuneracao']

    def parse(self, response):
        limite = len(response.xpath('//table[contains(@class, "table table-hover")]//tr')) - 1
        for tr in response.xpath('//table[contains(@class, "table table-hover table-bo")]//tr')[1:limite]:
            yield scrapy.Request('http://www.transparencia.mg.gov.br' + 
                            tr.xpath('.//td[1]/a/@href').extract_first(), callback=self.parse_page2)
    
    def parse_page2(self, response):
        limite = len(response.xpath('//table[contains(@class, "table table-hover")]//tr')) - 1
        for tr in response.xpath('//table[contains(@class, "table table-hover")]//tr')[1:limite]:
            yield scrapy.Request('http://www.transparencia.mg.gov.br' + 
                            tr.xpath('.//td[1]/a/@href').extract_first(), callback=self.parse_page3)

    def parse_page3(self, response):
        limite = len(response.xpath('//table[contains(@class, "table table-hover")]//tr')) - 1
        for tr in response.xpath('//table[contains(@class, "table table-hover")]//tr')[1:limite]:
            yield scrapy.Request('http://www.transparencia.mg.gov.br' + 
                            tr.xpath('.//td[1]/a/@href').extract_first(), callback=self.parse_page4)
    
    def parse_page4(self, response):
        limite = len(response.xpath('//table[contains(@class, "table table-hover")]//tr')) - 1
        for tr in response.xpath('//table[contains(@class, "table table-hover")]//tr')[1:limite]:
            yield scrapy.Request('http://www.transparencia.mg.gov.br' + 
                            tr.xpath('.//td[1]/a/@href').extract_first(), callback=self.parse_page5,
                            meta={'nome': tr.xpath('.//td[1]/a/text()').extract_first()})
    
    def parse_page5(self, response):
        for tr in response.xpath('//*[@id="t3-content"]/div[3]/div/table//tr'):
            titulo =  str(tr.xpath('.//td[1]//text()').extract_first())
            titulo2 =  str(tr.xpath('.//td[3]//text()').extract_first())
            if "Carga Horária" in titulo2:
                jornada = str(tr.xpath('.//td[4]//text()').extract_first()).strip()
            if "Identidade Funcional" in titulo2:
                idFuncionario = str(tr.xpath('.//td[4]//text()').extract_first()).strip()
            if "Descrição Situação do Servidor" in titulo2:
                situacao = str(tr.xpath('.//td[4]//text()').extract_first()).strip()
            if "Descrição Cargo Efetivo" in titulo2:
                cargo = str(tr.xpath('.//td[4]//text()').extract_first()).strip()
            if "Descrição Unid. Admin. de Exercício" in titulo:
                unidadeExercicio = str(tr.xpath('.//td[2]//text()').extract_first()).strip()
            if "Descrição Instituição Exercício" in titulo2:
                orgaoExercicio = str(tr.xpath('.//td[4]//text()').extract_first()).strip()
            if "Descrição Instituição Lotação" in titulo2:
                orgaoLotacao = str(tr.xpath('.//td[4]//text()').extract_first()).strip()
        dadosSalariais = []
        for tr in response.xpath('//*[@id="t3-content"]/div[4]/div/table//tr'):
            titulo =  str(tr.xpath('.//td[1]//text()').extract_first())
            if "Remuneração Básica Bruta" in titulo:
                brutoNovembro = str(tr.xpath('.//td[3]//text()').extract_first()).replace('.','').replace(',','.').strip()
                brutoOutubro = str(tr.xpath('.//td[4]//text()').extract_first()).replace('.','').replace(',','.').strip()
                brutoSetembro = str(tr.xpath('.//td[5]//text()').extract_first()).replace('.','').replace(',','.').strip()
                brutoAgosto = str(tr.xpath('.//td[6]//text()').extract_first()).replace('.','').replace(',','.').strip()
                brutoJulho = str(tr.xpath('.//td[7]//text()').extract_first()).replace('.','').replace(',','.').strip()
                brutoJunho = str(tr.xpath('.//td[8]//text()').extract_first()).replace('.','').replace(',','.').strip()
            
            if "Remuneração Após Deduções" in titulo:
                liquidoNovembro = str(tr.xpath('.//td[3]//text()').extract_first()).replace('.','').replace(',','.').strip()
                liquidoOutubro = str(tr.xpath('.//td[4]//text()').extract_first()).replace('.','').replace(',','.').strip()
                liquidoSetembro = str(tr.xpath('.//td[5]//text()').extract_first()).replace('.','').replace(',','.').strip()
                liquidoAgosto = str(tr.xpath('.//td[6]//text()').extract_first()).replace('.','').replace(',','.').strip()
                liquidoJulho = str(tr.xpath('.//td[7]//text()').extract_first()).replace('.','').replace(',','.').strip()
                liquidoJunho = str(tr.xpath('.//td[8]//text()').extract_first()).replace('.','').replace(',','.').strip()
        
        dadosSalariais.append({
            'mes': 6,
            'salarioLiquido': liquidoJunho,
            'salarioBruto': brutoJunho
        })
        dadosSalariais.append({
            'mes': 7,
            'salarioLiquido': liquidoJulho,
            'salarioBruto': brutoJulho
        })
        dadosSalariais.append({
            'mes': 8,
            'salarioLiquido': liquidoAgosto,
            'salarioBruto': brutoAgosto
        })
        dadosSalariais.append({
            'mes': 9,
            'salarioLiquido': liquidoSetembro,
            'salarioBruto': brutoSetembro
        })
        dadosSalariais.append({
            'mes': 10,
            'salarioLiquido': liquidoOutubro,
            'salarioBruto': brutoOutubro
        })
        dadosSalariais.append({
            'mes': 11,
            'salarioLiquido': liquidoNovembro,
            'salarioBruto': brutoNovembro
        })
        yield {
            'id': idFuncionario,
            'nome': str(response.meta['nome']).strip(),
            'cargo': cargo,
            'unidadeExercicio': unidadeExercicio,
            'orgaoLotacao': orgaoLotacao,
            'orgaoExercicio': orgaoExercicio,
            'situacao': situacao,
            'jornada': jornada,
            'dadosSalariais': dadosSalariais
        }