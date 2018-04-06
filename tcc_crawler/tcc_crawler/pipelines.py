
import pyodbc

# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


class TccCrawlerPipeline(object):

    def __init__(self, sql_connection):
        self.sql_connection = sql_connection

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            sql_connection = crawler.settings.get('SQL_CONNECTION')
        )
    
    def open_spider(self, spider):
        self.cnxn = pyodbc.connect(self.sql_connection)
        self.cursor = self.cnxn.cursor()
    
    def close_spider(self, spider):
        self.cursor.commit()
        self.cnxn.commit()
        self.cnxn.close()

    def governo_brasilia(self, item, spider):
        for result in item['resultado']:
            rowsFunc = self.cursor.execute("SELECT * FROM FUNCIONARIO_GOVERNO_BRASILIA WHERE ID = '{0}'".format(result['id'])).fetchall()
            if len(rowsFunc) == 0:
                sqlFuncionario = """INSERT INTO FUNCIONARIO_GOVERNO_BRASILIA (ID,NOME,CARGO,ORGAO_LOTACAO,ORGAO_EXERCICIO,SITUACAO) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}')""".format(
                result['id'],
                result['nome'],
                result['cargo'],
                result['orgaoLotacao'],
                result['orgaoExercicio'],
                result['situacao']
                )
                self.cursor.execute(sqlFuncionario)
            rowsSalario = self.cursor.execute("SELECT * FROM SALARIO_GOVERNO_BRASILIA WHERE ID_FUNCIONARIO = '{0}' AND MES = {1};".format(result['id'], result['mes'])).fetchall()
            if len(rowsSalario) == 0:
                sqlSalario = """INSERT INTO SALARIO_GOVERNO_BRASILIA (ID_FUNCIONARIO,MES,SALARIO_LIQUIDO,SALARIO_BRUTO) VALUES ('{0}', {1}, {2}, {3})""".format(
                result['id'],
                result['mes'],
                result['salarioLiquido'],
                result['salarioBruto']
                )
                self.cursor.execute(sqlSalario)
            self.cursor.commit()

    def governo_minas(self, item, spider):
        if ('None' == item['orgaoLotacao']):
            item['orgaoLotacao'] = item['orgaoExercicio']
        if ('None' == item['orgaoExercicio']):
            item['orgaoExercicio'] = item['orgaoLotacao']
        rowsFunc = self.cursor.execute("SELECT * FROM FUNCIONARIO_GOVERNO_MINAS WHERE ID = '{0}'".format(item['id'])).fetchall()
        if len(rowsFunc) == 0:
            sqlFuncionario = """INSERT INTO FUNCIONARIO_GOVERNO_MINAS (ID,NOME,CARGO,UNIDADE_EXERCICIO,ORGAO_LOTACAO,ORGAO_EXERCICIO,SITUACAO,JORNADA) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}', '{7}')""".format(
             item['id'],
             item['nome'],
             item['cargo'],
             item['unidadeExercicio'],
             item['orgaoLotacao'],
             item['orgaoExercicio'],
             item['situacao'],
             item['jornada']
            )
            self.cursor.execute(sqlFuncionario)
        for salario in item['dadosSalariais']:
            rowsSalario = self.cursor.execute("SELECT * FROM SALARIO_GOVERNO_MINAS WHERE ID_FUNCIONARIO = '{0}' AND MES = {1};".format(item['id'], salario['mes'])).fetchall()
            if len(rowsSalario) == 0:
                sqlSalario = """INSERT INTO SALARIO_GOVERNO_MINAS (ID_FUNCIONARIO,MES,SALARIO_LIQUIDO,SALARIO_BRUTO) VALUES ('{0}', {1}, {2}, {3})""".format(
                item['id'],
                salario['mes'],
                salario['salarioLiquido'],
                salario['salarioBruto']
                )
                self.cursor.execute(sqlSalario)
        self.cursor.commit()

    def governo_federal(self, item, spider):
        rowsFunc = self.cursor.execute("SELECT * FROM FUNCIONARIO_GOVERNO_FEDERAL WHERE ID = '{0}'".format(item['id'])).fetchall()
        if len(rowsFunc) == 0:
            sqlFuncionario = """INSERT INTO FUNCIONARIO_GOVERNO_FEDERAL (ID,NOME,CARGO,ORGAO_LOTACAO,ORGAO_EXERCICIO,ORGAO_SUPERIOR,SITUACAO,JORNADA) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}', '{7}')""".format(
             item['id'],
             item['nome'],
             item['cargo'],
             item['orgaoLotacao'],
             item['orgaoExercicio'],
             item['orgaoSuperior'],
             item['situacao'],
             item['jornada']
            )
            self.cursor.execute(sqlFuncionario)
        rowsSalario = self.cursor.execute("SELECT * FROM SALARIO_GOVERNO_FEDERAL WHERE ID_FUNCIONARIO = '{0}' AND MES = {1};".format(item['id'], item['mes'])).fetchall()
        if len(rowsSalario) == 0:
            sqlSalario = """INSERT INTO SALARIO_GOVERNO_FEDERAL (ID_FUNCIONARIO,MES,SALARIO_LIQUIDO,SALARIO_BRUTO) VALUES ('{0}', {1}, {2}, {3})""".format(
            item['id'],
            item['mes'],
            item['salarioLiquido'],
            item['salarioBruto']
            )
            self.cursor.execute(sqlSalario)
        self.cursor.commit()
    def process_item(self, item, spider):
        if "governoFederal" in str(spider.name):
            self.governo_federal(item, spider)
        if "governoMinasGerais" in str(spider.name):
            self.governo_minas(item, spider)
        if "governoBrasilia" in str(spider.name):
            self.governo_brasilia(item, spider)
        return item