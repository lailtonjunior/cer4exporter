import os
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, select, and_, func, text
from sqlalchemy.orm import sessionmaker
import datetime
import math
import configparser
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkcalendar import DateEntry
import csv

class BPAExporter:
   
    def obter_cbo_por_funcao(self, tp_funcao):
        """Obtém o código CBO baseado no tipo de função do profissional"""
        
        # Mapeamento de tp_funcao para códigos CBO
        mapeamento_cbo = {
            1: "422105",   # RECEPCIONISTA
            2: "351305",   # TÉCNICO ADMINISTRATIVO
            6: "2232",     # DENTISTA  
            10: "251605",  # ASSISTENTE SOCIAL
            12: "411005",  # ADMINISTRATIVA
            13: "223710",  # NUTRICIONISTA
            14: "521140",  # ATENDENTE
            7: "413115",   # FATURISTA
            15: "414105",  # ESTOQUISTA
            24: "223605",  # FISIOTERAPEUTA
            5: "410105",   # COORDENADOR ADMINISTRATIVO
            16: "514320",  # FAXINA
            17: "514320",  # AUXILIAR GERAL
            18: "142105",  # CONSULTORIA
            19: "225320",  # RADIOLOGISTA
            20: "317210",  # TÉCNICO DE INFORMÁTICA
            21: "317210",  # IMPLANTADOR
            22: "225170",  # MÉDICO
            23: "223805",  # FONOAUDIÓLOGO
            25: "223905",  # TERAPEUTA OCUPACIONAL
            26: "251510",  # PSICÓLOGO
            27: "225150",  # INFECTOLOGISTA
            28: "225160",  # PSIQUIATRA
            29: "223910",  # MUSICOTERAPEUTA
            30: "225145",  # OTORRINOLARINGOLOGISTA
            31: "223505",  # ENFERMEIRO
            32: "225135",  # NEUROLOGISTA
            33: "225140",  # OFTALMOLOGISTA
            34: "225130",  # ORTOPEDISTA
            35: "224110",  # PROFISSIONAL DE EDUCAÇÃO FÍSICA NA SAÚDE
            36: "239215",  # PEDAGOGA
            37: "225125",  # FISIATRA
            38: "322205",  # TÉCNICO DE ENFERMAGEM
        }
        
        # Converter tp_funcao para inteiro, se necessário
        try:
            if isinstance(tp_funcao, str) and tp_funcao.isdigit():
                tp_funcao = int(tp_funcao)
        except (ValueError, TypeError):
            pass
        
        # Retornar o CBO correspondente ou um valor padrão
        return mapeamento_cbo.get(tp_funcao, "000000")  # Retorna "000000" se não encontrar

    def inspecionar_banco_dados(self):
        """Inspeciona o banco de dados para verificar esquemas e tabelas disponíveis"""
        try:
            if not self.conn:
                print("Conexão não estabelecida!")
                return False
                
            print("\n=== INSPEÇÃO DO BANCO DE DADOS ===")
            
            # Listar todos os esquemas
            esquemas_query = "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name"
            esquemas = self.conn.execute(text(esquemas_query)).fetchall()
            
            print("\nEsquemas disponíveis:")
            for esquema_row in esquemas:
                esquema_nome = esquema_row[0]
                print(f"  - {esquema_nome}")
                
                # Verificar tabelas no esquema
                tabelas_query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{esquema_nome}' ORDER BY table_name"
                tabelas = self.conn.execute(text(tabelas_query)).fetchall()
                
                # Se alguma tabela foi encontrada, mostrar
                if tabelas:
                    print(f"    Tabelas em '{esquema_nome}' (primeiras 5):")
                    for i, tabela_row in enumerate(tabelas[:5]):  # Mostrar apenas as 5 primeiras
                        tabela_nome = tabela_row[0]
                        print(f"      - {tabela_nome}")
                        
                        # Se a tabela é uma das que precisamos, mostrar suas colunas
                        if tabela_nome in ['ficha_amb_int', 'lancamentos', 'pacientes', 'prestadores', 'municipios', 'bairros']:
                            colunas_query = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '{esquema_nome}' AND table_name = '{tabela_nome}' ORDER BY ordinal_position"
                            colunas = self.conn.execute(text(colunas_query)).fetchall()
                            
                            if colunas:
                                print(f"        Colunas da tabela '{tabela_nome}' (primeiras 5):")
                                for j, coluna_row in enumerate(colunas[:5]):
                                    coluna_nome, coluna_tipo = coluna_row
                                    print(f"          - {coluna_nome} ({coluna_tipo})")
                                
                                if len(colunas) > 5:
                                    print(f"          ... e mais {len(colunas) - 5} colunas")
                    
                    if len(tabelas) > 5:
                        print(f"      ... e mais {len(tabelas) - 5} tabelas")
            
            print("\n=== FIM DA INSPEÇÃO ===")
            
            # Verificar se algum esquema tem tabelas que podem servir ao nosso propósito
            print("\n=== SUGESTÕES DE ESQUEMA E TABELAS ===")
            for esquema_row in esquemas:
                esquema_nome = esquema_row[0]
                tabelas_query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{esquema_nome}' ORDER BY table_name"
                tabelas = self.conn.execute(text(tabelas_query)).fetchall()
                
                tabelas_nomes = [row[0] for row in tabelas]
                
                # Verificar se alguma tabela contém palavras-chave relevantes
                palavras_chave = ['ficha', 'atendimento', 'amb', 'lancamento', 'paciente', 'prestador']
                tabelas_relevantes = []
                
                for tabela in tabelas_nomes:
                    for palavra in palavras_chave:
                        if palavra.lower() in tabela.lower():
                            tabelas_relevantes.append(tabela)
                            break
                
                if tabelas_relevantes:
                    print(f"O esquema '{esquema_nome}' contém tabelas potencialmente relevantes:")
                    for tabela in tabelas_relevantes:
                        print(f"  - {tabela}")
            
            print("=== FIM DAS SUGESTÕES ===\n")
            return True
        except Exception as e:
            print(f"Erro ao inspecionar banco de dados: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
            
    def __init__(self):
        # Configurações iniciais
        self.engine = None
        self.metadata = None
        self.conn = None
        self.session = None
        
        # Tabelas refletidas
        self.ficha_amb_int = None
        self.lancamentos = None
        self.pacientes = None
        self.prestadores = None
        self.municipios = None
        self.bairros = None
        
        # Configurações do BPA
        self.config = {
            'orgao_responsavel': 'NOME DA CLINICA',
            'sigla_orgao': 'SIGLA',
            'cgc_cpf': '00000000000000',  # CNPJ/CPF com zeros à esquerda
            'orgao_destino': 'SECRETARIA MUNICIPAL DE SAUDE',
            'indicador_destino': 'M',  # M-Municipal ou E-Estadual
            'versao_sistema': 'v1.0.0',
            'cnes': '0000000'  # Código CNES com zeros à esquerda
        }
        
    def conectar_bd(self, db_name="bd0553", user="postgres", password="postgres", host="localhost", port="5432"):
        """Conecta ao banco de dados PostgreSQL"""
        try:
            connection_string = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
            self.engine = create_engine(connection_string)
            self.metadata = MetaData()
            self.metadata.reflect(bind=self.engine)
            self.conn = self.engine.connect()
            
            Session = sessionmaker(bind=self.engine)
            self.session = Session()
            
            # Usar o esquema correto "sigh"
            esquema = "sigh"
            print(f"\nConectando às tabelas no esquema: {esquema}")
            
            metadata = MetaData(schema=esquema)
            
            # Definir tabelas manualmente
            try:
                # Tabela ficha_amb_int
                self.ficha_amb_int = Table('ficha_amb_int', metadata, autoload_with=self.engine, schema=esquema)
                print("Tabela ficha_amb_int carregada com sucesso")
                
                # Tabela lancamentos
                self.lancamentos = Table('lancamentos', metadata, autoload_with=self.engine, schema=esquema)
                print("Tabela lancamentos carregada com sucesso")
                
                # Tabela procedimentos_fia
                self.procedimentos_fia = Table('procedimentos_fia', metadata, autoload_with=self.engine, schema=esquema)
                print("Tabela procedimentos_fia carregada com sucesso")
                
                # Tabela pacientes
                try:
                    self.pacientes = Table('pacientes', metadata, autoload_with=self.engine, schema=esquema)
                    print("Tabela pacientes carregada com sucesso")
                except Exception as e:
                    print(f"Aviso: Não foi possível carregar a tabela pacientes: {str(e)}")
                    self.pacientes = None
                
                # Tabela prestadores
                try:
                    self.prestadores = Table('prestadores', metadata, autoload_with=self.engine, schema=esquema)
                    print("Tabela prestadores carregada com sucesso")
                except Exception as e:
                    print(f"Aviso: Não foi possível carregar a tabela prestadores: {str(e)}")
                    self.prestadores = None
                
                # Adaptar para não usar tabelas não disponíveis
                self.municipios = None
                self.bairros = None
                
                print("\nConexão com o banco de dados estabelecida com sucesso!")
                return True
            
            except Exception as e:
                print(f"Erro ao carregar tabelas: {str(e)}")
                import traceback
                traceback.print_exc()
                return False
        
        except Exception as e:
            print(f"Erro ao conectar ao banco de dados: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def calcular_controle(self, registros):
        """Calcula o campo de controle conforme a fórmula: (∑códigos+quantidades)%1111 +1111"""
        total = 0
        
        for reg in registros:
            # Extrair apenas os dígitos do código do procedimento
            proc_code = ''.join(filter(str.isdigit, str(reg['prd_pa'])))
            
            # Converter para inteiro, considerando que pode estar vazio
            try:
                proc_code_int = int(proc_code) if proc_code else 0
            except ValueError:
                proc_code_int = 0
                
            # Somar com a quantidade - garantir que é um inteiro
            try:
                # Se for string, converter para inteiro
                if isinstance(reg['prd_qt'], str):
                    quantidade = int(reg['prd_qt'].strip()) if reg['prd_qt'].strip() else 0
                else:
                    quantidade = int(reg['prd_qt']) if reg['prd_qt'] else 0
            except (ValueError, TypeError):
                quantidade = 0
                
            total += proc_code_int + quantidade
            
        # Aplicar a fórmula (sum % 1111) + 1111
        resultado = (total % 1111) + 1111
        return resultado
    
    def gerar_header_bpa(self, competencia, registros):
        """Gera o cabeçalho do BPA conforme layout"""
        # Conta o número de linhas de detalhe
        num_linhas = len(registros)
        
        # Calcula o número de folhas (20 registros por folha, arredondando para cima)
        num_folhas = math.ceil(num_linhas / 20) if num_linhas > 0 else 1
        
        # Calcula o campo de controle
        campo_controle = self.calcular_controle(registros)
        
        header = {
            'cbc_hdr_1': '01',  # Indicador de linha do Header
            'cbc_hdr_2': '#BPA#',  # Indicador de início do cabeçalho
            'cbc_mvm': competencia,  # Ano e mês de Processamento (AAAAMM)
            'cbc_lin': str(num_linhas).zfill(6),  # Nº linhas de detalhe
            'cbc_flh': str(num_folhas).zfill(6),  # Nº de folhas
            'cbc_smt_vrf': str(campo_controle).zfill(4),  # Campo de controle
            'cbc_rsp': self.config['orgao_responsavel'].ljust(30),  # Nome do órgão de origem
            'cbc_sgl': self.config['sigla_orgao'].ljust(6),  # Sigla do órgão
            'cbc_cgccpf': self.config['cgc_cpf'].zfill(14),  # CGC/CPF do prestador
            'cbc_dst': self.config['orgao_destino'].ljust(40),  # Nome do órgão destino
            'cbc_dst_in': self.config['indicador_destino'],  # Indicador do órgão destino
            'cbc_versao': self.config['versao_sistema'].ljust(10),  # Versão do sistema
            'cbc_fim': '\r\n'  # CR+LF
        }
        
        return header
    
    def consultar_dados(self, data_inicio, data_fim):
        """Consulta os dados no banco para o período especificado"""
        try:
            print("\nIniciando consulta de dados para o período de", data_inicio, "a", data_fim)
            
            # Definir as colunas relevantes
            # Colunas da ficha_amb_int
            colunas_ficha = [
                self.ficha_amb_int.c.id_fia.label('id_fia'),
                self.ficha_amb_int.c.cod_paciente.label('cod_paciente'),
                self.ficha_amb_int.c.cod_medico.label('cod_medico'),
                self.ficha_amb_int.c.numero_guia.label('numero_guia'),
                self.ficha_amb_int.c.diagnostico.label('diagnostico'),
                self.ficha_amb_int.c.tipo_atend.label('tipo_atend'),
                self.ficha_amb_int.c.data_atendimento.label('data_atendimento'),
                self.ficha_amb_int.c.cod_municipio.label('cod_municipio'),
                self.ficha_amb_int.c.complemento.label('complemento'),
                self.ficha_amb_int.c.num_end_resp.label('num_end_resp'),
                self.ficha_amb_int.c.cod_logradouro.label('cod_logradouro'),
                self.ficha_amb_int.c.cod_bairro.label('cod_bairro'),
                self.ficha_amb_int.c.cod_cep.label('cod_cep'),
                self.ficha_amb_int.c.fone_resp.label('fone_resp'),
                self.ficha_amb_int.c.matricula.label('cnspac')  # CNS do paciente
            ]
            
            # Colunas dos lancamentos
            colunas_lancamentos = [
                self.lancamentos.c.id_lancamento.label('id_lancamento'),
                self.lancamentos.c.cod_proc.label('cod_proc'),
                self.lancamentos.c.quantidade.label('quantidade'),
                self.lancamentos.c.cod_cid.label('lanc_cod_cid'),
                self.lancamentos.c.cod_serv.label('cod_serv')
            ]
            
            # Colunas dos procedimentos_fia
            colunas_proc_fia = [
                self.procedimentos_fia.c.id_procedimento_fia.label('id_procedimento_fia'),
                self.procedimentos_fia.c.cod_fia.label('cod_fia'),
                self.procedimentos_fia.c.cod_procedimento_fia.label('cod_procedimento_fia'),
                self.procedimentos_fia.c.cod_cid.label('proc_cod_cid'),
                self.procedimentos_fia.c.qtd_solicitada.label('qtd_solicitada'),
                self.procedimentos_fia.c.qtd_autorizada.label('qtd_autorizada')
            ]
            
            # Colunas dos pacientes
            colunas_pacientes = []
            if self.pacientes is not None:
                colunas_pacientes = [
                    self.pacientes.c.nm_paciente.label('nm_paciente'),
                    self.pacientes.c.data_nasc.label('data_nasc'),
                    self.pacientes.c.cod_sexo.label('sexo'),
                    self.pacientes.c.email.label('email'),
                    self.pacientes.c.cod_raca_etnia.label('cod_raca')
                ]
            
            # Colunas dos prestadores
            colunas_prestadores = []
            if self.prestadores is not None:
                colunas_prestadores = [
                    self.prestadores.c.cns.label('cns_med'),
                    self.prestadores.c.cod_tp_funcao.label('tp_funcao'),  # Alterado para cod_tp_funcao
                ]
            
            # Juntar todas as colunas
            todas_colunas = colunas_ficha + colunas_lancamentos + colunas_proc_fia + colunas_pacientes + colunas_prestadores
            
            # Construir os joins
            from sqlalchemy import join, outerjoin
            
            # Join entre ficha_amb_int e lancamentos
            join_expr = self.ficha_amb_int.join(
                self.lancamentos,
                self.ficha_amb_int.c.id_fia == self.lancamentos.c.cod_conta
            )
            
            # Join com procedimentos_fia
            join_expr = join_expr.outerjoin(
                self.procedimentos_fia,
                self.ficha_amb_int.c.id_fia == self.procedimentos_fia.c.cod_fia
            )
            
            # Join com pacientes (se disponível)
            if self.pacientes is not None:
                join_expr = join_expr.outerjoin(
                    self.pacientes,
                    self.ficha_amb_int.c.cod_paciente == self.pacientes.c.id_paciente
                )
            
            # Join com prestadores (se disponível)
            if self.prestadores is not None:
                join_expr = join_expr.outerjoin(
                    self.prestadores,
                    self.ficha_amb_int.c.cod_medico == self.prestadores.c.id_prestador
                )
            
            # Construir a consulta final
            from sqlalchemy import select, and_
            query = (
                select(*todas_colunas)
                .select_from(join_expr)
                .where(
                    and_(
                        self.ficha_amb_int.c.data_atendimento >= data_inicio,
                        self.ficha_amb_int.c.data_atendimento <= data_fim
                    )
                )
                .limit(100)  # Limitar a 100 registros para teste
            )
            
            # Para ajudar na depuração, imprimir a consulta SQL
            from sqlalchemy import text
            sql_text = str(query.compile(dialect=self.engine.dialect, compile_kwargs={"literal_binds": True}))
            print("SQL gerada:", sql_text)

            # Executar a consulta
            print("Executando consulta SQL...")
            result = self.conn.execute(query)
            registros = [dict(row) for row in result.mappings()]  # Adicione .mappings() aqui
            print(f"Encontrados {len(registros)} registros.")
            
            # Processar os registros para o formato BPA-I
            return self.processar_registros_bpa_i(registros)
            
        except Exception as e:
            print(f"Erro ao consultar dados: {str(e)}")
            import traceback
            traceback.print_exc()
            return []

    def consultar_apenas_ficha(self, data_inicio, data_fim):
        """Consulta simplificada apenas na tabela ficha_amb_int"""
        try:
            print("Realizando consulta simplificada apenas na tabela ficha_amb_int...")
            
            # Definir colunas da ficha_amb_int
            colunas = [
                self.ficha_amb_int.c.id_fia,
                self.ficha_amb_int.c.cod_paciente,
                self.ficha_amb_int.c.cod_medico,
                self.ficha_amb_int.c.numero_guia,
                self.ficha_amb_int.c.diagnostico,
                self.ficha_amb_int.c.tipo_atend,
                self.ficha_amb_int.c.data_atendimento,
                self.ficha_amb_int.c.cod_municipio
            ]
            
            # Construir consulta
            from sqlalchemy import select, and_
            query = (
                select(colunas)
                .where(
                    and_(
                        self.ficha_amb_int.c.data_atendimento >= data_inicio,
                        self.ficha_amb_int.c.data_atendimento <= data_fim
                    )
                )
                .limit(100)  # Limitar a 100 registros para teste
            )
            
            # Executar consulta
            result = self.conn.execute(query)
            registros = [dict(row) for row in result]
            print(f"Consulta simplificada encontrou {len(registros)} registros.")
            
            # Se não encontrou registros, retornar lista vazia
            if not registros:
                print("Não foram encontrados registros na consulta simplificada.")
                return []
            
            # Processar registros para o formato BPA-I
            print("Processando registros para o formato BPA-I...")
            registros_bpa_i = []
            
            # Competência (AAAAMM)
            competencia = datetime.datetime.now().strftime("%Y%m")
            
            # Numeração sequencial para folhas e sequências
            for i, reg in enumerate(registros):
                # Calcular número da folha e sequência
                folha = math.floor(i / 20) + 1
                sequencia = (i % 20) + 1
                
                # Obter o tipo de função para este registro
                tp_funcao = reg.get('tp_funcao')

                # Criar registro BPA-I com os dados disponíveis
                registro_bpa_i = {
                    'prd_ident': '03',  # Identificação de linha de produção BPA-I
                    'prd_cnes': self.config['cnes'].zfill(7),  # Código CNES
                    'prd_cmp': competencia,  # Competência (AAAAMM)
                    'prd_cnsmed': ''.ljust(15),  # CNS do Profissional (não disponível)
                    'prd_cbo': self.obter_cbo_por_funcao(tp_funcao).ljust(6),  # CBO do Profissional (não disponível)
                    'prd_dtaten': reg['data_atendimento'].strftime('%Y%m%d') if reg['data_atendimento'] else '',  # Data de atendimento
                    'prd_flh': str(folha).zfill(3),  # Número da folha
                    'prd_seq': str(sequencia).zfill(2),  # Nº sequencial da linha
                    'prd_pa': '0301010013'.zfill(10),  # Código do procedimento padrão
                    'prd_cnspac': ''.ljust(15),  # CNS do paciente (não disponível)
                    'prd_sexo': 'M',  # Sexo do paciente (padrão)
                    'prd_ibge': str(reg['cod_municipio'] or '170550').zfill(6),  # Código IBGE do município
                    'prd_cid': 'Z000'.ljust(4),  # CID-10 padrão
                    'prd_ldade': '030'.zfill(3),  # Idade padrão
                    'prd_qt': '1'.zfill(6),  # Quantidade de procedimentos
                    'prd_caten': str(reg['tipo_atend'] or '01').zfill(2),  # Caract. atendimento
                    'prd_naut': str(reg['numero_guia'] or '').ljust(13),  # Nº Autorização
                    'prd_org': 'BPA',  # Origem das informações
                    'prd_nmpac': 'PACIENTE'.ljust(30),  # Nome do paciente
                    'prd_dtnasc': '19900101',  # Data de nasc. padrão
                    'prd_raca': '01'.zfill(2),  # Raça/Cor padrão
                    'prd_etnia': ''.zfill(4),  # Etnia
                    'prd_nac': '010'.zfill(3),  # Nacionalidade padrão
                    'prd_srv': ''.zfill(3),  # Código do Serviço
                    'prd_clf': ''.zfill(3),  # Código da Classificação
                    'prd_equipe_Seq': ''.zfill(8),  # Código Sequência Equipe
                    'prd_equipe_Area': ''.zfill(4),  # Código Área Equipe
                    'prd_cnpj': ''.zfill(14),  # CNPJ da empresa
                    'prd_cep_pcnte': ''.zfill(8),  # CEP do paciente
                    'prd_lograd_pcnte': ''.zfill(3),  # Código logradouro
                    'prd_end_pcnte': ''.ljust(30),  # Endereço do paciente
                    'prd_compl_pcnte': ''.ljust(10),  # Complemento endereço
                    'prd_num_pcnte': ''.ljust(5),  # Número do endereço
                    'prd_bairro_pcnte': ''.ljust(30),  # Bairro do paciente
                    'prd_ddtel_pcnte': ''.ljust(11),  # Telefone do paciente
                    'prd_email_pcnte': ''.ljust(40),  # E-mail do paciente
                    'prd_ine': ''.zfill(10),  # Identificação nacional de equipes
                    'prd_cpf_pcnte': ''.ljust(11),  # CPF do paciente
                    'prd_situacao_rua': 'N',  # Situação de rua
                    'prd_fim': '\r\n'  # CR+LF
                }
                
                registros_bpa_i.append(registro_bpa_i)
            
            return registros_bpa_i
            
        except Exception as e:
            print(f"Erro na consulta simplificada: {str(e)}")
            import traceback
            traceback.print_exc()
            return []

    def processar_registros_bpa_i(self, registros_bd):
        """Processa os registros do banco para o formato BPA-I"""
        registros_bpa_i = []
        competencia = datetime.datetime.now().strftime("%Y%m")
        
        # Carregar a tabela de procedimentos/CID
        tabela_proc_cid = self.carregar_tabela_procedimentos_cid()
        
        for i, reg in enumerate(registros_bd):
            # Calcular folha e sequência
            folha = math.floor(i / 20) + 1
            sequencia = (i % 20) + 1
            
            # Obter código do procedimento
            cod_proc_bd = reg.get('cod_proc')
            cod_proc_completo = '0301010013'  # Valor padrão
            cid_sugestao = None
            servico = '135'  # Tipo de serviço sempre 135
            classificacao = ''
            
            # Buscar o procedimento na tabela de procedimentos/CID
            if cod_proc_bd and str(cod_proc_bd).strip():
                proc_info = tabela_proc_cid.get(str(cod_proc_bd).strip())
                if proc_info:
                    cod_proc_completo = proc_info['codigo_sigtap']
                    classificacao = proc_info['classificacao']
                    if not (reg.get('lanc_cod_cid') or reg.get('proc_cod_cid') or reg.get('diagnostico')):
                        cid_sugestao = proc_info['cid_sugestao']
            
            # Obter CID (em ordem de prioridade)
            cid = None
            if reg.get('lanc_cod_cid'):  # Primeiro tentar CID do lancamento
                cid = reg.get('lanc_cod_cid')
            elif reg.get('proc_cod_cid'):  # Depois tentar CID do procedimento_fia
                cid = reg.get('proc_cod_cid')
            elif reg.get('diagnostico'):  # Depois tentar diagnóstico da ficha
                cid = reg.get('diagnostico')
            elif cid_sugestao:  # Por fim, usar CID sugestão da tabela
                cid = cid_sugestao
            else:
                cid = 'Z000'  # Valor padrão
                
            # Obter quantidade (em ordem de prioridade)
            quantidade = 1  # Valor padrão
            if reg.get('quantidade'):
                quantidade = reg.get('quantidade')
            elif reg.get('qtd_autorizada'):
                quantidade = reg.get('qtd_autorizada')
            elif reg.get('qtd_solicitada'):
                quantidade = reg.get('qtd_solicitada')
            
            # Mapear sexo
            sexo = 'M'  # Valor padrão
            if reg.get('sexo') == '1':
                sexo = 'M'
            elif reg.get('sexo') == '3':
                sexo = 'F'
            
            # Obter CNS do paciente
            cnspac = str(reg.get('cnspac') or '').ljust(15)
            
            # Calcular idade
            idade = 0
            if reg.get('data_nasc') and reg.get('data_atendimento'):
                data_nasc = reg['data_nasc']
                data_atend = reg['data_atendimento']
                idade = data_atend.year - data_nasc.year
                if (data_atend.month, data_atend.day) < (data_nasc.month, data_nasc.day):
                    idade -= 1
            
            # Obter o tipo de função para este registro específico
            tp_funcao = reg.get('tp_funcao')
            
            # Criar registro BPA-I
            registro_bpa_i = {
                'prd_ident': '03',  # Identificação de linha de produção BPA-I
                'prd_cnes': self.config['cnes'].zfill(7),  # Código CNES
                'prd_cmp': competencia,  # Competência (AAAAMM)
                'prd_cnsmed': str(reg.get('cns_med') or '').ljust(15),  # CNS do Profissional
                'prd_cbo': self.obter_cbo_por_funcao(tp_funcao).ljust(6),  # CBO do Profissional baseado no tipo de função
                'prd_dtaten': reg['data_atendimento'].strftime('%Y%m%d') if reg.get('data_atendimento') else '',  # Data de atendimento
                'prd_flh': str(folha).zfill(3),  # Número da folha
                'prd_seq': str(sequencia).zfill(2),  # Nº sequencial da linha
                'prd_pa': str(cod_proc_completo).zfill(10),  # Código do procedimento completo
                'prd_cnspac': cnspac,  # CNS do paciente 
                'prd_sexo': sexo,  # Sexo do paciente
                'prd_ibge': str(reg.get('cod_municipio') or '170550').zfill(6),  # Código IBGE do município
                'prd_cid': str(cid)[:4].ljust(4),  # CID-10 (4 primeiros caracteres)
                'prd_ldade': str(idade).zfill(3),  # Idade
                'prd_qt': str(quantidade).zfill(6),  # Quantidade de procedimentos
                'prd_caten': str(reg.get('urgente_eletivo') or '01').zfill(2),  # Caract. atendimento, geralmente eletivo
                'prd_naut': str(reg.get('numero_guia') or '').ljust(13),  # Nº Autorização
                'prd_org': 'BPA',  # Origem das informações
                'prd_nmpac': str(reg.get('nm_paciente') or '').ljust(30)[:30],  # Nome do paciente
                'prd_dtnasc': reg['data_nasc'].strftime('%Y%m%d') if reg.get('data_nasc') else '',  # Data de nasc.
                'prd_raca': str(reg.get('cod_raca') or '01').zfill(2),  # Raça/Cor
                'prd_etnia': ''.zfill(4),  # Etnia (não aplicável)
                'prd_nac': '010'.zfill(3),  # Nacionalidade (brasileiro)
                'prd_srv': servico.zfill(3),  # Código do Serviço (135)
                'prd_clf': classificacao.zfill(3),  # Código da Classificação
                'prd_equipe_Seq': ''.zfill(8),  # Código Sequência Equipe (zerado)
                'prd_equipe_Area': ''.zfill(4),  # Código Área Equipe (zerado)
                'prd_cnpj': ''.zfill(14),  # CNPJ da empresa (zerado)
                'prd_cep_pcnte': str(reg.get('cod_cep') or '').zfill(8),  # CEP do paciente
                'prd_lograd_pcnte': str(reg.get('cod_logradouro') or '').zfill(3),  # Código logradouro
                'prd_end_pcnte': ''.ljust(30),  # Endereço do paciente
                'prd_compl_pcnte': str(reg.get('complemento') or '').ljust(10),  # Complemento endereço
                'prd_num_pcnte': str(reg.get('num_end_resp') or '').ljust(5),  # Número do endereço
                'prd_bairro_pcnte': ''.ljust(30),  # Bairro do paciente
                'prd_ddtel_pcnte': str(reg.get('fone_resp') or '').ljust(11),  # Telefone do paciente
                'prd_email_pcnte': str(reg.get('email') or '').ljust(40),  # E-mail do paciente
                'prd_ine': ''.zfill(10),  # Identificação nacional de equipes (zerado)
                'prd_cpf_pcnte': ''.ljust(11),  # CPF do paciente (zerado)
                'prd_situacao_rua': 'N',  # Pessoa em situação de rua
                'prd_fim': '\r\n'  # CR+LF
            }
            
            registros_bpa_i.append(registro_bpa_i)
        
        return registros_bpa_i


    def carregar_tabela_procedimentos_cid(self):
        """Carrega a tabela de procedimentos e CIDs para consulta"""
        # Esta função poderia carregar de um arquivo CSV ou de uma tabela no banco
        tabela = {}
        
        # Reabilitação Física - Serviço 135, Classificação 03
        tabela['105'] = {'codigo_sigtap': '0301070105', 'classificacao': '03', 'cid_sugestao': 'M638'}
        tabela['121'] = {'codigo_sigtap': '0301070121', 'classificacao': '03', 'cid_sugestao': 'M638'}
        tabela['237'] = {'codigo_sigtap': '0301070237', 'classificacao': '03', 'cid_sugestao': ''}
        tabela['63'] = {'codigo_sigtap': '0301100063', 'classificacao': '03', 'cid_sugestao': ''}
        tabela['210'] = {'codigo_sigtap': '0301070210', 'classificacao': '03', 'cid_sugestao': ''}
        tabela['229'] = {'codigo_sigtap': '0301070229', 'classificacao': '03', 'cid_sugestao': ''}
        tabela['19'] = {'codigo_sigtap': '0302050019', 'classificacao': '03', 'cid_sugestao': 'M968'}
        tabela['27'] = {'codigo_sigtap': '0302050027', 'classificacao': '03', 'cid_sugestao': 'M998'}
        tabela['14'] = {'codigo_sigtap': '0302060014', 'classificacao': '03', 'cid_sugestao': 'G968'}
        tabela['30'] = {'codigo_sigtap': '0302060030', 'classificacao': '03', 'cid_sugestao': 'G839'}
        tabela['57'] = {'codigo_sigtap': '0302060057', 'classificacao': '03', 'cid_sugestao': 'Q878'}
        tabela['49'] = {'codigo_sigtap': '0302060049', 'classificacao': '03', 'cid_sugestao': 'F83'}
        tabela['530'] = {'codigo_sigtap': '0309050530', 'classificacao': '03', 'cid_sugestao': ''}
        tabela['23'] = {'codigo_sigtap': '0211030023', 'classificacao': '03', 'cid_sugestao': ''}
        tabela['31'] = {'codigo_sigtap': '0211030031', 'classificacao': '03', 'cid_sugestao': ''}
        
        # Reabilitação Intelectual - Serviço 135, Classificação 02
        tabela['24'] = {'codigo_sigtap': '0301070024', 'classificacao': '02', 'cid_sugestao': 'F83'}
        tabela['40'] = {'codigo_sigtap': '0301070040', 'classificacao': '02', 'cid_sugestao': 'F84'}
        tabela['59'] = {'codigo_sigtap': '0301070059', 'classificacao': '02', 'cid_sugestao': 'F84'}
        tabela['75'] = {'codigo_sigtap': '0301070075', 'classificacao': '02', 'cid_sugestao': 'F84'}
        tabela['261'] = {'codigo_sigtap': '0301070261', 'classificacao': '02', 'cid_sugestao': ''}
        tabela['13'] = {'codigo_sigtap': '0211100013', 'classificacao': '02', 'cid_sugestao': ''}
        
        # Reabilitação Visual - Serviço 135, Classificação 01
        tabela['38'] = {'codigo_sigtap': '0211060038', 'classificacao': '01', 'cid_sugestao': ''}
        tabela['20'] = {'codigo_sigtap': '0211060020', 'classificacao': '01', 'cid_sugestao': ''}
        tabela['54'] = {'codigo_sigtap': '0211060054', 'classificacao': '01', 'cid_sugestao': ''}
        tabela['100'] = {'codigo_sigtap': '0211060100', 'classificacao': '01', 'cid_sugestao': ''}
        tabela['1127'] = {'codigo_sigtap': '0211061127', 'classificacao': '01', 'cid_sugestao': ''}
        tabela['224'] = {'codigo_sigtap': '0211060224', 'classificacao': '01', 'cid_sugestao': ''}
        tabela['259'] = {'codigo_sigtap': '0211060259', 'classificacao': '01', 'cid_sugestao': ''}
        tabela['232'] = {'codigo_sigtap': '0211060232', 'classificacao': '01', 'cid_sugestao': ''}
        tabela['151'] = {'codigo_sigtap': '0211060151', 'classificacao': '01', 'cid_sugestao': ''}
        tabela['148'] = {'codigo_sigtap': '0301070148', 'classificacao': '01', 'cid_sugestao': ''}
        tabela['156'] = {'codigo_sigtap': '0301070156', 'classificacao': '01', 'cid_sugestao': 'H542'}
        tabela['164'] = {'codigo_sigtap': '0301070164', 'classificacao': '01', 'cid_sugestao': 'H542'}
        tabela['245'] = {'codigo_sigtap': '0301070245', 'classificacao': '01', 'cid_sugestao': ''}
        tabela['18'] = {'codigo_sigtap': '0302030018', 'classificacao': '01', 'cid_sugestao': 'H542'}
        tabela['26'] = {'codigo_sigtap': '0302030026', 'classificacao': '01', 'cid_sugestao': 'H519'}
        
        # Reabilitação Auditiva - Serviço 135, Classificação 05
        tabela['1113'] = {'codigo_sigtap': '0211051113', 'classificacao': '05', 'cid_sugestao': ''}
        tabela['25'] = {'codigo_sigtap': '0211070025', 'classificacao': '05', 'cid_sugestao': ''}
        tabela['33'] = {'codigo_sigtap': '0211070033', 'classificacao': '05', 'cid_sugestao': ''}
        tabela['41'] = {'codigo_sigtap': '0211070041', 'classificacao': '05', 'cid_sugestao': ''}
        tabela['50'] = {'codigo_sigtap': '0211070050', 'classificacao': '05', 'cid_sugestao': ''}
        tabela['106'] = {'codigo_sigtap': '0211070106', 'classificacao': '05', 'cid_sugestao': 'H919'}
        tabela['149'] = {'codigo_sigtap': '0211070149', 'classificacao': '05', 'cid_sugestao': ''}
        tabela['157'] = {'codigo_sigtap': '0211070157', 'classificacao': '05', 'cid_sugestao': ''}
        tabela['203'] = {'codigo_sigtap': '0211070203', 'classificacao': '05', 'cid_sugestao': ''}
        tabela['211'] = {'codigo_sigtap': '0211070211', 'classificacao': '05', 'cid_sugestao': ''}
        tabela['246'] = {'codigo_sigtap': '0211070246', 'classificacao': '05', 'cid_sugestao': ''}
        tabela['262'] = {'codigo_sigtap': '0211070262', 'classificacao': '05', 'cid_sugestao': ''}
        tabela['270'] = {'codigo_sigtap': '0211070270', 'classificacao': '05', 'cid_sugestao': ''}
        tabela['300'] = {'codigo_sigtap': '0211070300', 'classificacao': '05', 'cid_sugestao': 'H919'}
        tabela['319'] = {'codigo_sigtap': '0211070319', 'classificacao': '05', 'cid_sugestao': 'H919'}
        tabela['327'] = {'codigo_sigtap': '0211070327', 'classificacao': '05', 'cid_sugestao': ''}
        tabela['335'] = {'codigo_sigtap': '0211070335', 'classificacao': '05', 'cid_sugestao': ''}
        tabela['343'] = {'codigo_sigtap': '0211070343', 'classificacao': '05', 'cid_sugestao': ''}
        tabela['351'] = {'codigo_sigtap': '0211070351', 'classificacao': '05', 'cid_sugestao': ''}
        tabela['424'] = {'codigo_sigtap': '0211070424', 'classificacao': '05', 'cid_sugestao': ''}
        tabela['432'] = {'codigo_sigtap': '0211070432', 'classificacao': '05', 'cid_sugestao': ''}
        tabela['32'] = {'codigo_sigtap': '0301070032', 'classificacao': '05', 'cid_sugestao': 'H919'}
        tabela['253'] = {'codigo_sigtap': '0301070253', 'classificacao': '05', 'cid_sugestao': ''}
        
        return tabela
    
    def gerar_arquivo_txt(self, competencia, registros, caminho_arquivo):
        """Gera arquivo de texto no formato BPA"""
        try:
            # Converter competência para formato de extensão
            mes_num = int(competencia[-2:])
            meses = ['JAN', 'FEV', 'MAR', 'ABR', 'MAI', 'JUN', 'JUL', 'AGO', 'SET', 'OUT', 'NOV', 'DEZ']
            extensao = meses[mes_num - 1]  # -1 porque listas começam em 0
            
            # Ajustar o caminho do arquivo para usar a extensão correta
            nome_base = os.path.splitext(os.path.basename(caminho_arquivo))[0]
            diretorio = os.path.dirname(caminho_arquivo)
            caminho_arquivo_final = os.path.join(diretorio, f"{nome_base}.{extensao}")
            
            # Gerar o cabeçalho
            header = self.gerar_header_bpa(competencia, registros)
            
            with open(caminho_arquivo_final, 'w', newline='', encoding='latin-1') as f:
                # Escrever o cabeçalho
                linha_header = (
                    header['cbc_hdr_1'] + 
                    header['cbc_hdr_2'] + 
                    header['cbc_mvm'] + 
                    header['cbc_lin'] + 
                    header['cbc_flh'] + 
                    header['cbc_smt_vrf'] + 
                    header['cbc_rsp'] + 
                    header['cbc_sgl'] + 
                    header['cbc_cgccpf'] + 
                    header['cbc_dst'] + 
                    header['cbc_dst_in'] + 
                    header['cbc_versao']
                )
                f.write(linha_header + '\r\n')
                
                # Escrever os registros
                for reg in registros:
                    linha_reg = (
                        reg['prd_ident'] +
                        reg['prd_cnes'] +
                        reg['prd_cmp'] +
                        reg['prd_cnsmed'] +
                        reg['prd_cbo'] +
                        reg['prd_dtaten'] +
                        reg['prd_flh'] +
                        reg['prd_seq'] +
                        reg['prd_pa'] +
                        reg['prd_cnspac'] +
                        reg['prd_sexo'] +
                        reg['prd_ibge'] +
                        reg['prd_cid'] +
                        reg['prd_ldade'] +
                        reg['prd_qt'] +
                        reg['prd_caten'] +
                        reg['prd_naut'] +
                        reg['prd_org'] +
                        reg['prd_nmpac'] +
                        reg['prd_dtnasc'] +
                        reg['prd_raca'] +
                        reg['prd_etnia'] +
                        reg['prd_nac'] +
                        reg['prd_srv'] +
                        reg['prd_clf'] +
                        reg['prd_equipe_Seq'] +
                        reg['prd_equipe_Area'] +
                        reg['prd_cnpj'] +
                        reg['prd_cep_pcnte'] +
                        reg['prd_lograd_pcnte'] +
                        reg['prd_end_pcnte'] +
                        reg['prd_compl_pcnte'] +
                        reg['prd_num_pcnte'] +
                        reg['prd_bairro_pcnte'] +
                        reg['prd_ddtel_pcnte'] +
                        reg['prd_email_pcnte'] +
                        reg['prd_ine'] +
                        reg['prd_cpf_pcnte'] +
                        reg['prd_situacao_rua']
                    )
                    f.write(linha_reg + '\r\n')
                
            print(f"Arquivo BPA gerado com sucesso: {caminho_arquivo_final}")
            return True
        except Exception as e:
            print(f"Erro ao gerar arquivo BPA: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def gerar_arquivo_csv(self, registros, caminho_arquivo):
        """Gera arquivo CSV com os registros BPA"""
        try:
            # Converter a lista de dicionários para um DataFrame
            df = pd.DataFrame(registros)
            
            # Salvar como CSV
            df.to_csv(caminho_arquivo, index=False)
            
            print(f"Arquivo CSV gerado com sucesso: {caminho_arquivo}")
            return True
        except Exception as e:
            print(f"Erro ao gerar arquivo CSV: {str(e)}")
            return False
    
    def gerar_arquivo_xlsx(self, registros, caminho_arquivo):
        """Gera arquivo Excel com os registros BPA"""
        try:
            # Converter a lista de dicionários para um DataFrame
            df = pd.DataFrame(registros)
            
            # Salvar como XLSX
            df.to_excel(caminho_arquivo, index=False)
            
            print(f"Arquivo Excel gerado com sucesso: {caminho_arquivo}")
            return True
        except Exception as e:
            print(f"Erro ao gerar arquivo Excel: {str(e)}")
            return False

# Interface gráfica
class BPAExporterGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Exportador BPA-I")
        self.root.geometry("800x600")
        
        self.exporter = BPAExporter()
        
        # Criar frames
        self.frame_conexao = ttk.LabelFrame(root, text="Conexão ao Banco de Dados")
        self.frame_conexao.pack(fill="x", padx=10, pady=5)
        
        self.frame_filtros = ttk.LabelFrame(root, text="Filtros")
        self.frame_filtros.pack(fill="x", padx=10, pady=5)
        
        self.frame_config = ttk.LabelFrame(root, text="Configurações BPA")
        self.frame_config.pack(fill="x", padx=10, pady=5)
        
        self.frame_acoes = ttk.LabelFrame(root, text="Ações")
        self.frame_acoes.pack(fill="x", padx=10, pady=5)
        
        self.frame_log = ttk.LabelFrame(root, text="Log")
        self.frame_log.pack(fill="both", expand=True, padx=10, pady=5)
        
        # Elementos de conexão
        ttk.Label(self.frame_conexao, text="Banco:").grid(row=0, column=0, padx=5, pady=5, sticky="w")
        self.db_name = ttk.Entry(self.frame_conexao, width=20)
        self.db_name.insert(0, "bd0553")
        self.db_name.grid(row=0, column=1, padx=5, pady=5, sticky="w")
        
        ttk.Label(self.frame_conexao, text="Usuário:").grid(row=0, column=2, padx=5, pady=5, sticky="w")
        self.db_user = ttk.Entry(self.frame_conexao, width=20)
        self.db_user.insert(0, "postgres")
        self.db_user.grid(row=0, column=3, padx=5, pady=5, sticky="w")
        
        ttk.Label(self.frame_conexao, text="Senha:").grid(row=0, column=4, padx=5, pady=5, sticky="w")
        self.db_password = ttk.Entry(self.frame_conexao, width=20, show="*")
        self.db_password.insert(0, "postgres")
        self.db_password.grid(row=0, column=5, padx=5, pady=5, sticky="w")
        
        ttk.Label(self.frame_conexao, text="Host:").grid(row=1, column=0, padx=5, pady=5, sticky="w")
        self.db_host = ttk.Entry(self.frame_conexao, width=20)
        self.db_host.insert(0, "localhost")
        self.db_host.grid(row=1, column=1, padx=5, pady=5, sticky="w")
        
        ttk.Label(self.frame_conexao, text="Porta:").grid(row=1, column=2, padx=5, pady=5, sticky="w")
        self.db_port = ttk.Entry(self.frame_conexao, width=20)
        self.db_port.insert(0, "5432")
        self.db_port.grid(row=1, column=3, padx=5, pady=5, sticky="w")
        
        self.btn_conectar = ttk.Button(self.frame_conexao, text="Conectar", command=self.conectar_bd)
        self.btn_conectar.grid(row=1, column=5, padx=5, pady=5)
        
        # Elementos de filtro
        ttk.Label(self.frame_filtros, text="Data Início:").grid(row=0, column=0, padx=5, pady=5, sticky="w")
        self.data_inicio = DateEntry(self.frame_filtros, width=12, background='darkblue', foreground='white', date_pattern='dd/mm/yyyy')
        self.data_inicio.grid(row=0, column=1, padx=5, pady=5, sticky="w")
        
        ttk.Label(self.frame_filtros, text="Data Fim:").grid(row=0, column=2, padx=5, pady=5, sticky="w")
        self.data_fim = DateEntry(self.frame_filtros, width=12, background='darkblue', foreground='white', date_pattern='dd/mm/yyyy')
        self.data_fim.grid(row=0, column=3, padx=5, pady=5, sticky="w")
        
        ttk.Label(self.frame_filtros, text="Competência (AAAAMM):").grid(row=0, column=4, padx=5, pady=5, sticky="w")
        self.competencia = ttk.Entry(self.frame_filtros, width=8)
        self.competencia.insert(0, datetime.datetime.now().strftime("%Y%m"))
        self.competencia.grid(row=0, column=5, padx=5, pady=5, sticky="w")
        
        # Elementos de configuração
        ttk.Label(self.frame_config, text="Órgão Responsável:").grid(row=0, column=0, padx=5, pady=5, sticky="w")
        self.orgao_resp = ttk.Entry(self.frame_config, width=30)
        self.orgao_resp.insert(0, "NOME DA CLINICA")
        self.orgao_resp.grid(row=0, column=1, padx=5, pady=5, sticky="w")
        
        ttk.Label(self.frame_config, text="Sigla:").grid(row=0, column=2, padx=5, pady=5, sticky="w")
        self.sigla = ttk.Entry(self.frame_config, width=6)
        self.sigla.insert(0, "SIGLA")
        self.sigla.grid(row=0, column=3, padx=5, pady=5, sticky="w")
        
        ttk.Label(self.frame_config, text="CNPJ/CPF:").grid(row=1, column=0, padx=5, pady=5, sticky="w")
        self.cnpj_cpf = ttk.Entry(self.frame_config, width=14)
        self.cnpj_cpf.insert(0, "00000000000000")
        self.cnpj_cpf.grid(row=1, column=1, padx=5, pady=5, sticky="w")
        
        ttk.Label(self.frame_config, text="Órgão Destino:").grid(row=1, column=2, padx=5, pady=5, sticky="w")
        self.orgao_destino = ttk.Entry(self.frame_config, width=40)
        self.orgao_destino.insert(0, "SECRETARIA MUNICIPAL DE SAUDE")
        self.orgao_destino.grid(row=1, column=3, padx=5, pady=5, sticky="w")
        
        ttk.Label(self.frame_config, text="Indicador Destino:").grid(row=2, column=0, padx=5, pady=5, sticky="w")
        self.indicador_destino = ttk.Combobox(self.frame_config, values=["M", "E"], width=2)
        self.indicador_destino.current(0)
        self.indicador_destino.grid(row=2, column=1, padx=5, pady=5, sticky="w")
        
        ttk.Label(self.frame_config, text="Versão Sistema:").grid(row=2, column=2, padx=5, pady=5, sticky="w")
        self.versao = ttk.Entry(self.frame_config, width=10)
        self.versao.insert(0, "v1.0.0")
        self.versao.grid(row=2, column=3, padx=5, pady=5, sticky="w")
        
        ttk.Label(self.frame_config, text="CNES:").grid(row=3, column=0, padx=5, pady=5, sticky="w")
        self.cnes = ttk.Entry(self.frame_config, width=7)
        self.cnes.insert(0, "0000000")
        self.cnes.grid(row=3, column=1, padx=5, pady=5, sticky="w")
        
        # Elementos de ação
        self.btn_consultar = ttk.Button(self.frame_acoes, text="Consultar Dados", command=self.consultar_dados)
        self.btn_consultar.grid(row=0, column=0, padx=5, pady=5)
        
        self.btn_exportar_txt = ttk.Button(self.frame_acoes, text="Exportar TXT", command=self.exportar_txt)
        self.btn_exportar_txt.grid(row=0, column=1, padx=5, pady=5)
        
        self.btn_exportar_csv = ttk.Button(self.frame_acoes, text="Exportar CSV", command=self.exportar_csv)
        self.btn_exportar_csv.grid(row=0, column=2, padx=5, pady=5)
        
        self.btn_exportar_xlsx = ttk.Button(self.frame_acoes, text="Exportar XLSX", command=self.exportar_xlsx)
        self.btn_exportar_xlsx.grid(row=0, column=3, padx=5, pady=5)
        
        # Área de log
        self.log_text = tk.Text(self.frame_log, height=10)
        self.log_text.pack(fill="both", expand=True, padx=5, pady=5)
        
        # Scrollbar para o log
        scrollbar = ttk.Scrollbar(self.log_text, command=self.log_text.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text.config(yscrollcommand=scrollbar.set)
        
        # Desabilitar botões de exportação até que a consulta seja feita
        self.btn_exportar_txt.config(state="disabled")
        self.btn_exportar_csv.config(state="disabled")
        self.btn_exportar_xlsx.config(state="disabled")
        
        # Variáveis para armazenar dados consultados
        self.registros_bpa = []
        
    def log(self, mensagem):
        """Adiciona mensagem ao log"""
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        self.log_text.insert(tk.END, f"[{timestamp}] {mensagem}\n")
        self.log_text.see(tk.END)  # Rolar para o final
        
    def conectar_bd(self):
        """Conecta ao banco de dados com os parâmetros da interface"""
        try:
            db_name = self.db_name.get()
            user = self.db_user.get()
            password = self.db_password.get()
            host = self.db_host.get()
            port = self.db_port.get()
            
            # Atualizar configurações do BPA
            self.exporter.config['orgao_responsavel'] = self.orgao_resp.get()
            self.exporter.config['sigla_orgao'] = self.sigla.get()
            self.exporter.config['cgc_cpf'] = self.cnpj_cpf.get()
            self.exporter.config['orgao_destino'] = self.orgao_destino.get()
            self.exporter.config['indicador_destino'] = self.indicador_destino.get()
            self.exporter.config['versao_sistema'] = self.versao.get()
            self.exporter.config['cnes'] = self.cnes.get()
            
            # Tentar conectar
            if self.exporter.conectar_bd(db_name, user, password, host, port):
                self.log("Conexão com o banco de dados estabelecida com sucesso!")
                return True
            else:
                self.log("Erro ao conectar ao banco de dados.")
                messagebox.showerror("Erro de Conexão", "Falha ao conectar ao banco de dados.")
                return False
        except Exception as e:
            self.log(f"Erro ao conectar ao banco de dados: {str(e)}")
            messagebox.showerror("Erro de Conexão", f"Falha ao conectar ao banco de dados: {str(e)}")
            return False
    
    def consultar_dados(self):
        """Consulta os dados no banco para o período especificado"""
        try:
            # Verificar se está conectado
            if not self.exporter.conn:
                if not self.conectar_bd():
                    return
            
            # Obter datas de filtro
            data_inicio_str = self.data_inicio.get()
            data_fim_str = self.data_fim.get()
            
            # Converter para formato de data
            data_inicio = datetime.datetime.strptime(data_inicio_str, "%d/%m/%Y").date()
            data_fim = datetime.datetime.strptime(data_fim_str, "%d/%m/%Y").date()
            
            self.log(f"Consultando dados de {data_inicio_str} até {data_fim_str}...")
            
            # Consultar dados
            self.registros_bpa = self.exporter.consultar_dados(data_inicio, data_fim)
            
            # Exibir quantidade de registros encontrados
            if self.registros_bpa:
                self.log(f"Encontrados {len(self.registros_bpa)} registros.")
                # Habilitar botões de exportação
                self.btn_exportar_txt.config(state="normal")
                self.btn_exportar_csv.config(state="normal")
                self.btn_exportar_xlsx.config(state="normal")
            else:
                self.log("Nenhum registro encontrado para o período especificado.")
                messagebox.showinfo("Consulta", "Nenhum registro encontrado para o período especificado.")
                # Desabilitar botões de exportação
                self.btn_exportar_txt.config(state="disabled")
                self.btn_exportar_csv.config(state="disabled")
                self.btn_exportar_xlsx.config(state="disabled")
                
        except Exception as e:
            self.log(f"Erro ao consultar dados: {str(e)}")
            messagebox.showerror("Erro de Consulta", f"Falha ao consultar dados: {str(e)}")
    
    def exportar_txt(self):
        """Exporta os dados para arquivo TXT no formato BPA"""
        if not self.registros_bpa:
            self.log("Não há dados para exportar. Faça uma consulta primeiro.")
            return
        
        # Obter competência
        competencia = self.competencia.get()
        
        # Converter competência para formato de extensão
        try:
            mes_num = int(competencia[-2:])
            meses = ['JAN', 'FEV', 'MAR', 'ABR', 'MAI', 'JUN', 'JUL', 'AGO', 'SET', 'OUT', 'NOV', 'DEZ']
            extensao = meses[mes_num - 1]  # -1 porque listas começam em 0
        except (ValueError, IndexError):
            messagebox.showerror("Erro", "Competência inválida. Use o formato AAAAMM (ex: 202501)")
            return
        
        # Informar ao usuário o formato do arquivo
        self.log(f"Exportando arquivo no formato BPA com extensão .{extensao}")
        
        # Abrir diálogo para selecionar local de salvamento
        caminho_arquivo = filedialog.asksaveasfilename(
            initialfile=f"PAAPAE.{extensao}",
            filetypes=[(f"Arquivos BPA (.{extensao})", f"*.{extensao}")],
            title="Salvar Arquivo BPA"
        )
        
        if not caminho_arquivo:
            return  # Usuário cancelou
        
        # Exportar
        if self.exporter.gerar_arquivo_txt(competencia, self.registros_bpa, caminho_arquivo):
            caminho_final = os.path.splitext(caminho_arquivo)[0] + f".{extensao}"
            self.log(f"Arquivo BPA exportado com sucesso: {caminho_final}")
            messagebox.showinfo("Exportação", f"Arquivo BPA exportado com sucesso: {caminho_final}")
        else:
            self.log("Erro ao exportar arquivo BPA.")
            messagebox.showerror("Erro de Exportação", "Falha ao exportar arquivo BPA.")
    
    def exportar_csv(self):
        """Exporta os dados para arquivo CSV"""
        if not self.registros_bpa:
            self.log("Não há dados para exportar. Faça uma consulta primeiro.")
            return
        
        # Abrir diálogo para selecionar local de salvamento
        caminho_arquivo = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("Arquivos CSV", "*.csv")],
            title="Salvar Arquivo CSV"
        )
        
        if not caminho_arquivo:
            return  # Usuário cancelou
        
        # Exportar
        if self.exporter.gerar_arquivo_csv(self.registros_bpa, caminho_arquivo):
            self.log(f"Arquivo CSV exportado com sucesso: {caminho_arquivo}")
            messagebox.showinfo("Exportação", f"Arquivo CSV exportado com sucesso: {caminho_arquivo}")
        else:
            self.log("Erro ao exportar arquivo CSV.")
            messagebox.showerror("Erro de Exportação", "Falha ao exportar arquivo CSV.")
    
    def exportar_xlsx(self):
        """Exporta os dados para arquivo XLSX"""
        if not self.registros_bpa:
            self.log("Não há dados para exportar. Faça uma consulta primeiro.")
            return
        
        # Abrir diálogo para selecionar local de salvamento
        caminho_arquivo = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Arquivos Excel", "*.xlsx")],
            title="Salvar Arquivo Excel"
        )
        
        if not caminho_arquivo:
            return  # Usuário cancelou
        
        # Exportar
        if self.exporter.gerar_arquivo_xlsx(self.registros_bpa, caminho_arquivo):
            self.log(f"Arquivo Excel exportado com sucesso: {caminho_arquivo}")
            messagebox.showinfo("Exportação", f"Arquivo Excel exportado com sucesso: {caminho_arquivo}")
        else:
            self.log("Erro ao exportar arquivo Excel.")
            messagebox.showerror("Erro de Exportação", "Falha ao exportar arquivo Excel.")
    


# Ponto de entrada principal
def main():
    """Função principal"""
    try:
        # Verificar dependências
        import pandas as pd
        import sqlalchemy
        import tkcalendar
    except ImportError as e:
        print(f"Erro: Dependência não encontrada: {e}")
        print("Por favor, instale as dependências necessárias:")
        print("pip install pandas sqlalchemy psycopg2-binary tkcalendar")
        return
    
    # Iniciar a interface gráfica
    root = tk.Tk()
    app = BPAExporterGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()