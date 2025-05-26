import os
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, select, and_, func, text
from sqlalchemy.orm import sessionmaker
import datetime
import math
import configparser # Mantido, embora config seja gerenciada internamente na classe por enquanto
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkcalendar import DateEntry
import csv

class BPAExporter:
   
    def __init__(self):
        # Configurações iniciais
        self.engine = None
        self.metadata = None
        self.conn = None
        self.session = None
        self.mapeamentos_faltantes_log = set() # Para armazenar códigos curtos faltantes
        self.gui_log_callback = None # Placeholder para a função de log da GUI

        # Configurações do BPA (padrão, podem ser sobrescritas pela GUI)
        self.config = {
            'orgao_responsavel': 'NOME DA CLINICA',
            'sigla_orgao': 'SIGLA',
            'cgc_cpf': '00000000000000',
            'orgao_destino': 'SECRETARIA MUNICIPAL DE SAUDE',
            'indicador_destino': 'M', # 'M'unicipal ou 'E'stadual
            'versao_sistema': 'v1.0.0',
            'cnes': '0000000',
            'default_ibge_paciente': '000000', 
            'default_cep_paciente': '00000000', 
            'default_ine': '0000000000' 
        }
        
    def obter_cbo_por_funcao(self, tp_funcao):
        """Obtém o código CBO baseado no tipo de função do profissional"""
        
        if tp_funcao is None or str(tp_funcao).strip() == '':
            return "225142" 

        mapeamento_cbo = {
            1: "422105", 2: "351305", 3: "223208", 4: "225125", 5: "410105", 
            6: "223208", 7: "413115", 8: "223405", 9: "223415", 10: "251605", 
            11: "225270", 12: "411005", 13: "223710", 14: "521140", 15: "414105", 
            16: "514320", 17: "514320", 18: "142105", 19: "225320", 20: "317210", 
            21: "317210", 22: "225170", 23: "223810", 24: "223605", 25: "223905", 
            26: "251510", 27: "225150", 28: "225160", 29: "223910", 30: "225145",
            31: "223505", 32: "225135", 33: "225140", 34: "225130", 35: "224110",
            36: "239215", 37: "225125", 38: "322205",
        }
        
        try:
            if isinstance(tp_funcao, str) and tp_funcao.isdigit():
                tp_funcao_key = int(tp_funcao)
            elif isinstance(tp_funcao, (int, float)):
                 tp_funcao_key = int(tp_funcao)
            else: 
                tp_funcao_key = str(tp_funcao).strip()
        except (ValueError, TypeError):
            return "225142" 
        
        return mapeamento_cbo.get(tp_funcao_key, "225142") 
            
    def conectar_bd(self, db_name="bd0553", user="postgres", password="postgres", host="localhost", port="5432"):
        """Conecta ao banco de dados PostgreSQL"""
        try:
            connection_string = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
            self.engine = create_engine(connection_string)
            self.metadata = MetaData()
            self.conn = self.engine.connect() 
            Session = sessionmaker(bind=self.engine)
            self.session = Session()
            print("\nConexão com o banco de dados estabelecida com sucesso!")
            return True
        except Exception as e:
            print(f"Erro ao conectar ao banco de dados: {str(e)}")
            return False
    
    def calcular_controle(self, registros):
        """Calcula o campo de controle"""
        total = 0
        for reg in registros:
            proc_code_str = str(reg.get('prd_pa', '0')) 
            proc_code = ''.join(filter(str.isdigit, proc_code_str))
            try:
                proc_code_int = int(proc_code) if proc_code else 0
            except ValueError:
                proc_code_int = 0
            try:
                quantidade_str = str(reg.get('prd_qt', '0')).replace('.', '')
                quantidade = int(quantidade_str) if quantidade_str.strip() else 0
            except (ValueError, TypeError):
                quantidade = 0
            total += proc_code_int + quantidade
        resultado = (total % 1111) + 1111
        return resultado
    
    def gerar_header_bpa(self, competencia, registros):
        """Gera o cabeçalho do BPA conforme layout"""
        num_linhas = len(registros)
        num_folhas = math.ceil(num_linhas / 99) if num_linhas > 0 else 1 # CORRIGIDO para 99
        campo_controle = self.calcular_controle(registros)
        
        header = {
            'cbc_hdr_1': '01', 'cbc_hdr_2': '#BPA#', 'cbc_mvm': competencia, 
            'cbc_lin': str(num_linhas).zfill(6), 'cbc_flh': str(num_folhas).zfill(6), 
            'cbc_smt_vrf': str(campo_controle).zfill(4),
            'cbc_rsp': self.config.get('orgao_responsavel', '').ljust(30),
            'cbc_sgl': self.config.get('sigla_orgao', '').ljust(6),
            'cbc_cgccpf': self.config.get('cgc_cpf', '').zfill(14),
            'cbc_dst': self.config.get('orgao_destino', '').ljust(40),
            'cbc_dst_in': self.config.get('indicador_destino', 'M'), 
            'cbc_versao': self.config.get('versao_sistema', '').ljust(10),
        }
        return header

    def debug_datas_tabela(self, data_inicio, data_fim):
        # ... (código do debug_datas_tabela permanece o mesmo) ...
        if not self.conn:
            print("Erro no debug: Sem conexão com o banco de dados.")
            return None
        try:
            from sqlalchemy import text
            data_inicio_str = data_inicio.isoformat() if isinstance(data_inicio, datetime.date) else str(data_inicio)
            data_fim_str = data_fim.isoformat() if isinstance(data_fim, datetime.date) else str(data_fim)
            print(f"\n=== DEBUG DE DATAS (Tabela: sigh.lancamentos) ===")
            print(f"Período pesquisado: {data_inicio_str} a {data_fim_str}")
            print("\n1. Estrutura da tabela sigh.lancamentos:")
            struct_sql = text("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = 'sigh' AND table_name = 'lancamentos' ORDER BY ordinal_position")
            result = self.conn.execute(struct_sql)
            colunas = result.fetchall()
            colunas_data_potenciais = []
            if colunas:
                for col in colunas:
                    print(f"  - {col[0]} ({col[1]})")
                    if col[1] and ('date' in col[1].lower() or 'timestamp' in col[1].lower()):
                        colunas_data_potenciais.append(col[0])
            else:
                print("  Não foi possível obter a estrutura da tabela sigh.lancamentos.")
                return None
            print(f"\n2. Colunas que parecem ser de data/timestamp: {colunas_data_potenciais}")
            melhor_coluna = None
            max_registros_periodo = -1
            for coluna_teste in colunas_data_potenciais:
                try:
                    print(f"\n3. Testando coluna: {coluna_teste}")
                    count_total_sql = text(f"SELECT COUNT(*) FROM sigh.lancamentos WHERE {coluna_teste} IS NOT NULL")
                    total_na_coluna = self.conn.execute(count_total_sql).scalar_one_or_none() or 0
                    print(f"   Total de registros com {coluna_teste} preenchido: {total_na_coluna}")
                    if total_na_coluna > 0:
                        range_sql = text(f"SELECT MIN({coluna_teste}) as min_data, MAX({coluna_teste}) as max_data FROM sigh.lancamentos WHERE {coluna_teste} IS NOT NULL")
                        range_result = self.conn.execute(range_sql).fetchone()
                        if range_result: print(f"   Range de datas na coluna: {range_result[0]} a {range_result[1]}")
                        periodo_sql_str = f"SELECT COUNT(*) FROM sigh.lancamentos WHERE {coluna_teste} BETWEEN :inicio AND :fim"
                        periodo_sql = text(periodo_sql_str)
                        periodo_count = self.conn.execute(periodo_sql, {"inicio": data_inicio_str, "fim": data_fim_str}).scalar_one_or_none() or 0
                        print(f"   Registros no período ({data_inicio_str} a {data_fim_str}): {periodo_count}")
                        if periodo_count > 0 and periodo_count > max_registros_periodo :
                            max_registros_periodo = periodo_count
                            melhor_coluna = coluna_teste
                            print(f"   >>> {coluna_teste} é uma candidata melhor ({periodo_count} registros).")
                        if periodo_count > 0 and periodo_count >= (total_na_coluna * 0.01) and periodo_count > 10:
                            print(f"   Exemplos de dados em {coluna_teste} no período:")
                            exemplo_sql_str = f"SELECT {coluna_teste}, cod_proc, quantidade FROM sigh.lancamentos WHERE {coluna_teste} BETWEEN :inicio AND :fim ORDER BY {coluna_teste} LIMIT 3"
                            exemplo_sql = text(exemplo_sql_str)
                            exemplos = self.conn.execute(exemplo_sql, {"inicio": data_inicio_str, "fim": data_fim_str}).fetchall()
                            for ex_idx, ex_row in enumerate(exemplos): print(f"     Ex {ex_idx+1}: Data: {ex_row[0]}, Proc: {ex_row[1]}, Qtd: {ex_row[2]}")
                except Exception as e_col:
                    print(f"   Erro ao testar coluna {coluna_teste}: {str(e_col)}")
                    continue
            if melhor_coluna: print(f"\n*** Melhor coluna de data candidata encontrada: {melhor_coluna} ({max_registros_periodo} registros no período) ***")
            else: print("\nNenhuma coluna de data pareceu ideal no período especificado.")
            print("=== FIM DEBUG DE DATAS ===")
            return melhor_coluna
        except Exception as e_debug:
            print(f"Erro fatal no debug de datas: {str(e_debug)}")
            import traceback; traceback.print_exc()
            return None

    def consultar_dados_com_debug(self, data_inicio, data_fim, competencia=None, criterio_data="atendimento"):
        # ... (código do consultar_dados_com_debug sem alterações significativas na lógica central) ...
        if criterio_data == "lancamento":
            print("Executando debug para descobrir coluna de data correta...")
            coluna_data_correta = self.debug_datas_tabela(data_inicio, data_fim)
            if coluna_data_correta: print(f"Usando coluna descoberta: {coluna_data_correta} para a consulta principal.")
            else: print("Não foi possível descobrir a coluna de data. A consulta usará 'data' como padrão.")
        return [] # Função de debug, não retorna dados processados.
    
    def consultar_dados(self, data_inicio, data_fim, competencia=None, criterio_data="atendimento"):
        # ... (código do consultar_dados (versão simples) permanece o mesmo, pois não é o foco principal) ...
        # Esta função não será alterada pois a GUI usa consultar_dados_completo
        return []

    def _build_sql_completo(self, coluna_data_filtro, alias_tabela_filtro="l"):
        """Constrói a string SQL COMPLETA comum para diferentes critérios de data."""
        # Esta função já estava correta nas últimas versões, sem p.cpf, p.situacao_rua, fi.situacao_rua
        return f"""
        SELECT
            l.id_lancamento, l.cod_proc, l.quantidade, l.cod_cid AS lanc_cod_cid,
            l.cod_serv, l.data AS data_lancamento_original,
            {alias_tabela_filtro}.{coluna_data_filtro} AS data_filtro_usada,
            c.id_conta, c.cod_fia AS conta_cod_fia, c.dt_inicio AS conta_dt_inicio,
            c.dt_fim AS conta_dt_fim, c.competencia AS conta_competencia,
            c.data_fatura, c.status_conta, c.codigo_conta, c.numero_guia AS conta_numero_guia,
            fi.id_fia, fi.cod_paciente, fi.cod_medico, fi.diagnostico,
            fi.tipo_atend, fi.data_atendimento,
            fi.matricula AS cnspac_ficha,
            p.nm_paciente, p.data_nasc, p.cod_sexo AS sexo,
            p.email AS email_paciente,
            p.cod_raca_etnia AS cod_raca,
            p.cod_etnia_indigena AS cod_etnia_paciente,
            p.cod_municipio AS p_cod_municipio_fk, 
            p.cod_nacionalidade AS nacionalidade_paciente,
            p.fone_res_1, p.fone_cel_1,
            e.pac_cep AS e_pac_cep,
            e.pac_tp_logradouro AS e_pac_tp_logradouro,
            e.pac_logradouro AS e_pac_logradouro_nome,
            e.numero AS e_numero,
            e.complemento AS e_complemento,
            e.pac_bairro AS e_pac_bairro_nome,
            e.id_endereco AS e_id_endereco,
            mun_pac.num_ibge AS mun_num_ibge,
            mun_pac.nm_municipio AS mun_nm_municipio,
            pr.cns AS cns_med, pr.cod_tp_funcao AS tp_funcao, pr.nm_prestador
        FROM
            sigh.lancamentos AS l
        JOIN
            sigh.contas AS c ON l.cod_conta = c.id_conta
        JOIN
            sigh.ficha_amb_int AS fi ON c.cod_fia = fi.id_fia
        LEFT JOIN
            sigh.pacientes AS p ON fi.cod_paciente = p.id_paciente
        LEFT JOIN
            sigh.prestadores AS pr ON fi.cod_medico = pr.id_prestador
        LEFT JOIN (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY cod_paciente ORDER BY id_endereco DESC) as rn
            FROM sigh.enderecos
            WHERE ativo = 't'
        ) AS e ON p.id_paciente = e.cod_paciente AND e.rn = 1
        LEFT JOIN
            endereco_sigh.municipios AS mun_pac ON p.cod_municipio = mun_pac.id_municipio
        """

    def consultar_dados_completo(self, data_inicio, data_fim, competencia=None, criterio_data="lancamento"):
            """Consulta completa aplicando os filtros SIGH validados."""
            if not self.conn:
                log_msg = "Erro: Sem conexão com o banco de dados para consulta completa."
                print(log_msg)
                if hasattr(self, 'gui_log_callback') and callable(self.gui_log_callback):
                    self.gui_log_callback(log_msg)
                return []

            self.mapeamentos_faltantes_log.clear()

            try:
                print(f"\nIniciando consulta COMPLETA (filtros SIGH) para o período de {data_inicio} a {data_fim}")
                
                # Validação e formatação de competência (GUI continua AAAAMM)
                if competencia is None or len(competencia) != 6 or not competencia.isdigit():
                    competencia_gui = datetime.datetime.now().strftime("%Y%m") # Usado para processar_registros_bpa_i_completo
                    # print(f"Competência da GUI inválida ou ausente, usando default: {competencia_gui}")
                else:
                    competencia_gui = competencia
                
                # Formato da competência para o banco de dados (AAAA/MM) se o critério for competência
                competencia_bd_formatada = competencia_gui[:4] + "/" + competencia_gui[4:]

                from sqlalchemy import text
                
                data_inicio_str = data_inicio.isoformat() if isinstance(data_inicio, datetime.date) else str(data_inicio)
                data_fim_str = data_fim.isoformat() if isinstance(data_fim, datetime.date) else str(data_fim)
                
                # Coluna de data para o ALIAS 'data_filtro_usada' no SELECT e para o ORDER BY
                coluna_data_para_select_no_alias = "data" 
                alias_tabela_para_select = "l" # Default para 'lancamento'
                
                # Determina a coluna de data para o SELECT e ORDER BY baseado na seleção da GUI.
                # A cláusula WHERE principal usará a lógica de data confirmada do SIGH.
                if criterio_data == "lancamento": 
                    if self.conn: 
                        # debug_datas_tabela ajuda a escolher a melhor coluna 'data_lancamento' para SELECT/ORDER BY
                        coluna_data_para_select_no_alias = self.debug_datas_tabela(data_inicio, data_fim) or "data"
                    else:
                        coluna_data_para_select_no_alias = "data" 
                    alias_tabela_para_select = "l"
                    print(f"Coluna de data para SELECT/ORDER BY (GUI='{criterio_data}'): {alias_tabela_para_select}.{coluna_data_para_select_no_alias}")
                elif criterio_data == "conta":
                    coluna_data_para_select_no_alias = "dt_inicio"; alias_tabela_para_select = "c"
                    print(f"Coluna de data para SELECT/ORDER BY (GUI='{criterio_data}'): {alias_tabela_para_select}.{coluna_data_para_select_no_alias}")
                elif criterio_data == "atendimento": 
                    coluna_data_para_select_no_alias = "data_atendimento"; alias_tabela_para_select = "fi"
                    print(f"Coluna de data para SELECT/ORDER BY (GUI='{criterio_data}'): {alias_tabela_para_select}.{coluna_data_para_select_no_alias}")
                elif criterio_data == "competencia":
                    coluna_data_para_select_no_alias = "competencia"; alias_tabela_para_select = "c" 
                    print(f"Campo para SELECT 'data_filtro_usada' (GUI='{criterio_data}'): {alias_tabela_para_select}.{coluna_data_para_select_no_alias}")

                # _build_sql_completo monta o SELECT e os JOINs.
                # O JOIN com sigh.categorias é incluído por _build_sql_completo,
                # mas não será usado no WHERE principal se não for necessário para os filtros SIGH.
                sql_base = self._build_sql_completo(coluna_data_para_select_no_alias, alias_tabela_para_select)

                # --- Montando a Cláusula WHERE e Parâmetros ---
                condicoes_where_comuns_sigh = [
                    "c.ativo = 't'",
                    "c.status_conta = 'A'",
                    "c.codigo_conta IN (1, 2)", 
                    "l.cod_cc = 2",             
                    "l.cod_tp_ato = 56",        
                    "l.cod_proc IS NOT NULL"
                    # Adicione aqui filtros de fi.tipo_atend ou fi.cod_situacao_atendimento
                    # SE eles faziam parte da sua query que deu 6766/4516 DISTINCT id_lancamento.
                    # Ex:
                    # "fi.tipo_atend = 'AMB'", 
                    # "fi.cod_situacao_atendimento = 4" 
                ]
                
                params = {}
                if criterio_data == "competencia":
                    condicoes_especificas = ["c.competencia = :competencia_param"] + condicoes_where_comuns_sigh
                    where_clause_final = "WHERE " + " AND ".join(condicoes_especificas)
                    params = {"competencia_param": competencia_bd_formatada, 
                            "data_inicio": data_inicio_str, 
                            "data_fim": data_fim_str}
                    print(f"Usando critério GUI: COMPETÊNCIA DA CONTA ({competencia_bd_formatada}) com filtros SIGH.")
                else: 
                    # Para critérios de data da GUI (lancamento, conta, atendimento)
                    # Usamos a condição de data que você validou.
                    condicao_data_validada_sigh = f"((l.data_hora_criacao::date BETWEEN :data_inicio AND :data_fim) OR (l.data BETWEEN :data_inicio AND :data_fim))"
                    
                    condicoes_com_data_validada = [condicao_data_validada_sigh] + condicoes_where_comuns_sigh
                    where_clause_final = "WHERE " + " AND ".join(condicoes_com_data_validada)
                    params = {"data_inicio": data_inicio_str, "data_fim": data_fim_str}
                    print(f"Usando critério GUI: {criterio_data} com filtros SIGH validados (incluindo data SIGH).")
                
                # Ordenação
                order_by_data_field_para_ordenacao = "c.dt_inicio" # Default para competência
                if criterio_data != "competencia":
                    order_by_data_field_para_ordenacao = f"{alias_tabela_para_select}.{coluna_data_para_select_no_alias}"

                order_by_clause = f"ORDER BY pr.cns, {order_by_data_field_para_ordenacao}, l.cod_proc, l.id_lancamento, p.id_paciente"
                
                full_sql_query_str = sql_base + "\n" + where_clause_final + "\n" + order_by_clause
                
                print(f"SQL Final para buscar dados base:\n{full_sql_query_str}")
                print(f"Parâmetros: {params}")

                result = self.conn.execute(text(full_sql_query_str), params)
                registros_do_banco = [dict(row._mapping) for row in result.fetchall()]
                
                num_brutos = len(registros_do_banco)
                print(f"Encontrados {num_brutos} registros brutos na consulta SQL principal.")
                if hasattr(self, 'gui_log_callback') and callable(self.gui_log_callback):
                    self.gui_log_callback(f"Consulta SQL retornou {num_brutos} linhas brutas.")

                if registros_do_banco:
                    registros_processados = self.processar_registros_bpa_i_completo(registros_do_banco, competencia_gui)
                    
                    if self.mapeamentos_faltantes_log:
                        self._escrever_log_mapeamentos_faltantes()
                    
                    return registros_processados
                else: 
                    msg_nenhum_registro = "Nenhum registro encontrado no banco de dados para os critérios SIGH aplicados."
                    print(msg_nenhum_registro)
                    if hasattr(self, 'gui_log_callback') and callable(self.gui_log_callback):
                        self.gui_log_callback(msg_nenhum_registro)
                    return []
                        
            except Exception as e:
                error_message = f"Erro na consulta SQL COMPLETA ou processamento: {str(e)}"
                print(error_message)
                if hasattr(self, 'gui_log_callback') and callable(self.gui_log_callback):
                    self.gui_log_callback(error_message)
                import traceback
                traceback.print_exc()
                return []
                
    def debug_estrutura_tabelas(self):
        # ... (código do debug_estrutura_tabelas permanece o mesmo) ...
        return True # Adicionado para consistência

    def _obter_codigo_tipo_logradouro(self, valor_do_banco):
        # ... (código do _obter_codigo_tipo_logradouro permanece o mesmo) ...
        if not valor_do_banco: return "000"
        val_str = str(valor_do_banco).strip()
        if val_str.isdigit() and len(val_str) <= 3: return val_str.zfill(3)
        mapeamento = {"RUA": "001", "AVENIDA": "002", "TRAVESSA": "003", "PRACA": "004", "RODOVIA": "005"}
        return mapeamento.get(val_str.upper(), "000").zfill(3)

    def processar_registros_bpa_i_completo(self, registros_bd, competencia=None):
        """Processa os registros COMPLETOS para o formato BPA-I, SEM atribuir folha/sequência aqui."""
        if not registros_bd:
            return []

        if competencia is None:
            competencia = datetime.datetime.now().strftime("%Y%m")
        
        print(f"Processando {len(registros_bd)} registros BD (sem folha/seq) com competência: {competencia}")
        
        cod_procs_bd_unicos = list(set([reg.get('cod_proc') for reg in registros_bd if reg.get('cod_proc')]))
        mapeamento_proc = self.carregar_mapeamento_procedimentos(cod_procs_bd_unicos)
        tabela_proc_cid = self.carregar_tabela_procedimentos_cid()

        # A ORDENAÇÃO aqui é crucial para o método _atribuir_folha_sequencia_final
        registros_bd.sort(key=lambda r: (
            str(r.get('cns_med') or '').strip(),
            r.get('data_atendimento') or datetime.date.min, 
            r.get('data_lancamento_original') or datetime.date.min,
            str(r.get('nm_paciente') or '').strip(),
            r.get('id_lancamento') or 0
        ))

        registros_bpa_i_sem_numeracao = []

        for reg_data in registros_bd:
            # --- Início do processamento de cada campo do registro ---
            cns_med_val = str(reg_data.get('cns_med') or '').strip()
            cns_med = cns_med_val.ljust(15) if cns_med_val else ' '.ljust(15)

            tp_funcao = reg_data.get('tp_funcao')
            cbo_val = self.obter_cbo_por_funcao(tp_funcao)
            cbo = cbo_val.ljust(6) 

            nome_paciente_val = str(reg_data.get('nm_paciente') or 'PACIENTE NAO IDENTIFICADO').strip()
            nome_paciente = nome_paciente_val.ljust(30)[:30]
            
            data_nasc_obj = reg_data.get('data_nasc')
            data_nasc_str = data_nasc_obj.strftime('%Y%m%d') if data_nasc_obj else '19000101'

            data_atendimento_obj = reg_data.get('data_atendimento') or reg_data.get('conta_dt_inicio')
            data_atend_str = data_atendimento_obj.strftime('%Y%m%d') if data_atendimento_obj else competencia + "01"

            cnspac_val = str(reg_data.get('cnspac_paciente') or reg_data.get('cnspac_ficha') or '').strip()
            cnspac = cnspac_val.ljust(15) if cnspac_val else ' '.ljust(15)

            sexo_bd = str(reg_data.get('sexo') or '').strip() 
            sexo = 'F' if sexo_bd == '3' else 'M' 

            cod_ibge_paciente_val = str(reg_data.get('mun_num_ibge') or self.config.get('default_ibge_paciente', '000000')).strip()
            cod_ibge_paciente = cod_ibge_paciente_val.ljust(6)[:6]

            idade = 0
            if data_nasc_obj and data_atendimento_obj:
                idade = data_atendimento_obj.year - data_nasc_obj.year - \
                        ((data_atendimento_obj.month, data_atendimento_obj.day) < (data_nasc_obj.month, data_nasc_obj.day))
            idade_str = str(min(max(idade, 0), 130)).zfill(3)

            # Mapeamento de Raça/Cor CORRIGIDO e ATUALIZADO conforme sua tabela
            cod_cor_bd_val = str(reg_data.get('cod_raca') or '').strip() 
            
            mapeamento_raca_bd_para_bpa = {
                # Chave: "Valor Origem BD" (da sua tabela de-para)
                # Valor: "Valor Final" (código de 2 dígitos para o BPA-I)
                # Certifique-se que os VALORES ('01', '02', etc.) são os códigos OFICIAIS do BPA-I
                # (01-Branca, 02-Preta, 03-Parda, 04-Amarela, 05-Indígena, 99-Sem informação)
                '4': '01',  # BRANCA (Ex: BD valor '4' -> BPA '01')
                '33': '02', # PRETA  (Ex: BD valor '33' -> BPA '02')
                '22': '03', # PARDA  (Ex: BD valor '22' -> BPA '03')
                '16': '03', # MULATO -> Parda (Ex: BD valor '16' -> BPA '03')
                '27': '03', # PARDA (variação) -> Parda (Ex: BD valor '27' -> BPA '03')
                '20': '03', # MISTO -> Parda (Ex: BD valor '20' -> BPA '03', ou '99' se preferir)
                '29': '04', # AMARELA (Ex: BD valor '29' -> BPA '04')
                '26': '04', # AMAREL -> Amarela (Ex: BD valor '26' -> BPA '04')
                '19': '05', # INDIGENA (Ex: BD valor '19' -> BPA '05')
                '18': '05', # INDIA -> Indígena (Ex: BD valor '18' -> BPA '05')
                '7':  '02', # NEGRO -> Preta (Ex: BD valor '7' -> BPA '02')
                '31': '99', # NÃO INFORMADA (Ex: BD valor '31' -> BPA '99')
            }
            raca = mapeamento_raca_bd_para_bpa.get(cod_cor_bd_val, '99') # Default '99' se não mapeado
            raca = raca.ljust(2)


            etnia_val = str(reg_data.get('cod_etnia_paciente') or '').strip() if raca == '05' else ''
            etnia = etnia_val.zfill(4) if etnia_val else '    '

            # Nacionalidade do paciente - FIXO em '010' (Brasileiro)
            nacionalidade = '010'

            cpf_paciente_val = str(reg_data.get('cpf_paciente') or '').replace('.', '').replace('-', '').strip()
            cpf_paciente = cpf_paciente_val.ljust(11) if cpf_paciente_val else ' '.ljust(11)

            # Campo prd_situacao_rua fixo como espaço
            prd_situacao_rua_final = ' '

            cep_val = str(reg_data.get('e_pac_cep') or '').strip().replace('.', '').replace('-', '')
            cep = cep_val.zfill(8) if cep_val else self.config.get('default_cep_paciente', '00000000').ljust(8)
            logradouro_tipo_origem = reg_data.get('e_pac_tp_logradouro') 
            logradouro_tipo_cod = self._obter_codigo_tipo_logradouro(logradouro_tipo_origem)
            endereco_nome_val = str(reg_data.get('e_pac_logradouro_nome') or '').strip()
            endereco_nome = endereco_nome_val.ljust(30)[:30]
            complemento_val = str(reg_data.get('e_complemento') or '').strip()
            complemento = complemento_val.ljust(10)[:10]
            numero_val = str(reg_data.get('e_numero') or '').strip()
            numero = numero_val.ljust(5)[:5]
            bairro_val = str(reg_data.get('e_pac_bairro_nome') or '').strip()
            bairro = bairro_val.ljust(30)[:30]

            telefone_val_db = str(reg_data.get('fone_cel_1') or reg_data.get('fone_res_1') or '').strip()
            telefone_numeros = ''.join(filter(str.isdigit, telefone_val_db))
            telefone = telefone_numeros.ljust(11)[:11] if telefone_numeros else ' '.ljust(11)
            email_val = str(reg_data.get('email_paciente') or '').strip()
            email = email_val.ljust(40)[:40]

            # --- BLOCO DE MAPEAMENTO PARA PROCEDIMENTO E CID COM DEBUG ---
            cod_proc_bd = reg_data.get('cod_proc')
            id_lancamento_debug = reg_data.get('id_lancamento') 

            cod_proc_sigtap = '0301010013' 
            servico_val = '135' 
            classificacao_val = '001' 
            cid_sugestao_local = None

            # print(f"--- DEBUG Lanc. ID: {id_lancamento_debug}, cod_proc_bd: {cod_proc_bd} ---") 

            if cod_proc_bd and str(cod_proc_bd).strip():
                codigo_procedimento_mapeado = mapeamento_proc.get(str(cod_proc_bd))
                # print(f"  > codigo_procedimento_mapeado (de mapeamento_proc): {codigo_procedimento_mapeado}") 

                if not codigo_procedimento_mapeado:
                    # print(f"  ALERTA: cod_proc_bd '{cod_proc_bd}' NÃO ENCONTRADO em mapeamento_proc.")
                    pass

                if codigo_procedimento_mapeado:
                    proc_info = tabela_proc_cid.get(codigo_procedimento_mapeado)
                    # print(f"  > proc_info (de tabela_proc_cid usando '{codigo_procedimento_mapeado}'): {proc_info is not None}")

                    if not proc_info:
                         # print(f"  ALERTA: codigo_procedimento_mapeado '{codigo_procedimento_mapeado}' (de cod_proc_bd '{cod_proc_bd}') NÃO ENCONTRADO em tabela_proc_cid.")
                        if codigo_procedimento_mapeado != '72': # Exemplo de exclusão do log, ajuste se necessário
                            self.mapeamentos_faltantes_log.add(
                                (codigo_procedimento_mapeado, str(cod_proc_bd))
                            )

                    cid_obrigatorio_para_este_procedimento = False # Default
                    if proc_info:
                        cod_proc_sigtap = proc_info['codigo_sigtap']
                        servico_val = proc_info.get('servico', servico_val) 
                        classificacao_val = proc_info.get('classificacao', classificacao_val)
                        # Verifica a nova chave 'cid_obrigatorio'
                        cid_obrigatorio_para_este_procedimento = proc_info.get('cid_obrigatorio', False) # Default para False se não definido
                        if not (reg_data.get('lanc_cod_cid') or reg_data.get('diagnostico')):
                            cid_sugestao_local = proc_info.get('cid_sugestao')
            # else:
                 # print(f"  INFO: cod_proc_bd VAZIO ou NULO para Lanc. ID {id_lancamento_debug}. Usando defaults para PA.")
            
            cod_proc_sigtap = cod_proc_sigtap.ljust(10)
            # print(f"  > Final para Lanc. ID {id_lancamento_debug}: prd_pa={cod_proc_sigtap}, prd_srv={servico_val.zfill(3)}, prd_clf={classificacao_val.zfill(3)}")

            lanc_cod_cid_val = str(reg_data.get('lanc_cod_cid') or '').strip()
            diagnostico_val = str(reg_data.get('diagnostico') or '').strip()
            # print(f"  > Fontes CID para Lanc. ID {id_lancamento_debug}: lanc_cod_cid='{lanc_cod_cid_val}', diagnostico='{diagnostico_val}', cid_sugestao_local='{cid_sugestao_local}'")
            
            cid_final_fallback = 'Z000' # Default padrão se CID for obrigatório e não encontrado
            if not cid_obrigatorio_para_este_procedimento:
                cid_final_fallback = '0000' # Novo default para CID não obrigatório


            cid_val_db = (lanc_cod_cid_val or diagnostico_val or cid_sugestao_local or cid_final_fallback).upper()

            # Se mesmo com fallback '0000', alguma fonte primária (BD ou sugestão) preencheu, mantenha.
            # Apenas se todas as fontes + sugestão forem vazias E CID não é obrigatório, use '0000'.
            if not lanc_cod_cid_val and not diagnostico_val and not (cid_sugestao_local and cid_sugestao_local.strip()):
                if not cid_obrigatorio_para_este_procedimento:
                    cid_val_db = '0000'
                else:
                    cid_val_db = 'Z000' # Se obrigatório e tudo vazio, mantém Z000

            cid = cid_val_db.replace('.', '').ljust(4)[:4]
            if cid == '    ': # Se ficou apenas espaços (ex: cid_sugestao era ' ' e outras vazias)
                cid = cid_final_fallback.ljust(4)


            quantidade_val = 1 
            quantidade_formatada = str(quantidade_val).zfill(6)

            caracter_atendimento = '01' 
            numero_guia_val = str(reg_data.get('conta_numero_guia') or reg_data.get('numero_guia') or '').strip()
            prd_naut = numero_guia_val.ljust(13)[:13]
            prd_org = 'BPA'.ljust(3)
            prd_equipe_seq = ' '.ljust(8); prd_equipe_area = ' '.ljust(4)
            prd_ine = (str(reg_data.get('ine_da_equipe_no_banco') or self.config.get('default_ine', '0000000000'))).ljust(10)
            prd_cnpj_estab_val = self.config.get('cgc_cpf', '') if self.config.get('indicador_destino') == 'E' else ''
            prd_cnpj_estab = prd_cnpj_estab_val.ljust(14) if prd_cnpj_estab_val else ' '.ljust(14)
            
            registro_bpa_i = {
                'prd_ident': '03', 'prd_cnes': self.config.get('cnes', '0000000').ljust(7),
                'prd_cmp': competencia, 'prd_cnsmed': cns_med, 'prd_cbo': cbo,
                'prd_dtaten': data_atend_str,
                # prd_flh e prd_seq são atribuídos em _atribuir_folha_sequencia_final
                'prd_pa': cod_proc_sigtap, 'prd_cnspac': cnspac, 'prd_sexo': sexo,
                'prd_ibge': cod_ibge_paciente, 'prd_cid': cid, 'prd_ldade': idade_str,
                'prd_qt': quantidade_formatada,
                'prd_caten': caracter_atendimento, 'prd_naut': prd_naut,
                'prd_org': prd_org, 'prd_nmpac': nome_paciente, 'prd_dtnasc': data_nasc_str,
                'prd_raca': raca, 'prd_etnia': etnia, 'prd_nac': nacionalidade,
                'prd_srv': servico_val.zfill(3), 'prd_clf': classificacao_val.zfill(3),
                'prd_equipe_Seq': prd_equipe_seq, 'prd_equipe_Area': prd_equipe_area,
                'prd_cnpj': prd_cnpj_estab, 'prd_cep_pcnte': cep,
                'prd_lograd_pcnte': logradouro_tipo_cod, 'prd_end_pcnte': endereco_nome,
                'prd_compl_pcnte': complemento, 'prd_num_pcnte': numero,
                'prd_bairro_pcnte': bairro, 'prd_ddtel_pcnte': telefone,
                'prd_email_pcnte': email, 'prd_ine': prd_ine,
                'prd_cpf_pcnte': cpf_paciente,
                'prd_cid': cid,
                'prd_situacao_rua': prd_situacao_rua_final, # Linha corrigida
                '_id_lancamento_original': reg_data.get('id_lancamento') # Novo campo para deduplicação por ID original

            }
            registros_bpa_i_sem_numeracao.append(registro_bpa_i)
        
        print(f"Processados {len(registros_bpa_i_sem_numeracao)} registros BPA-I (sem folha/sequência ainda).")
        return registros_bpa_i_sem_numeracao


    def deduplicate_por_id_lancamento_original(self, registros_bpa_processados):
        """
        Deduplica mantendo apenas o primeiro registro encontrado para cada '_id_lancamento_original' único.
        Assume que prd_qt já é '000001'.
        """
        print(f"\nIniciando deduplicação por ID de Lançamento Original de {len(registros_bpa_processados)} registros...")
        if not registros_bpa_processados:
            return []

        ids_lancamento_vistos = set()
        registros_finais = []

        for registro in registros_bpa_processados:
            id_original = registro.get('_id_lancamento_original')
            if id_original is not None and id_original not in ids_lancamento_vistos:
                ids_lancamento_vistos.add(id_original)
                registros_finais.append(registro.copy()) # Adiciona uma cópia
            # else: Se id_original é None ou já foi visto, descarta.
        
        print(f"Deduplicação por ID de Lançamento Original concluída:")
        print(f"  - Registros originais processados: {len(registros_bpa_processados)}")
        print(f"  - Registros únicos finais (por id_lancamento): {len(registros_finais)}")
        
        return registros_finais


    def _escrever_log_mapeamentos_faltantes(self):
        """Escreve os códigos curtos de procedimentos não encontrados em tabela_proc_cid para um arquivo de log."""
        if not self.mapeamentos_faltantes_log:
            print("Nenhum mapeamento de procedimento faltante para registrar.")
            return

        nome_arquivo_log = "mapeamentos_procedimentos_faltantes.txt"
        try:
            with open(nome_arquivo_log, 'w', encoding='utf-8') as f:
                f.write(f"Relatório de Mapeamentos de Procedimentos Faltantes em tabela_proc_cid (gerado em: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')})\n")
                f.write("==================================================================================================================\n")
                f.write("Os seguintes 'Códigos Curtos' (vindos de sigh.procedimentos.codigo_procedimento) precisam ser adicionados\n")
                f.write("à função 'carregar_tabela_procedimentos_cid' no arquivo bpa_exporter.py com seus respectivos códigos SIGTAP.\n")
                f.write("O 'ID Original do BD' refere-se ao 'cod_proc' da tabela sigh.lancamentos (que é o 'id_procedimento' em sigh.procedimentos).\n\n")
                
                # Ordenar para facilitar a visualização
                sorted_faltantes = sorted(list(self.mapeamentos_faltantes_log))

                for codigo_curto, id_original_bd in sorted_faltantes:
                    f.write(f"Código Curto Faltante: '{codigo_curto}' (Originado do ID Original do BD: {id_original_bd})\n")
            
            print(f"Log de mapeamentos de procedimentos faltantes foi salvo em: {nome_arquivo_log}")
            # Informar o usuário via GUI também
            if hasattr(self, 'gui_log_callback') and callable(self.gui_log_callback): # Se houver um callback para logar na GUI
                 self.gui_log_callback(f"AVISO: Log de mapeamentos de procedimentos faltantes foi salvo em '{nome_arquivo_log}'. Verifique este arquivo para completar os mapeamentos em 'carregar_tabela_procedimentos_cid'.")

        except Exception as e:
            print(f"Erro ao escrever log de mapeamentos faltantes: {str(e)}")
            if hasattr(self, 'gui_log_callback') and callable(self.gui_log_callback):
                 self.gui_log_callback(f"ERRO ao escrever log de mapeamentos faltantes: {str(e)}")


    def _atribuir_folha_sequencia_final(self, lista_registros_processados):
        """
        Atribui os campos prd_flh e prd_seq à lista final de registros,
        agrupando por profissional e limitando a 99 registros por folha.
        """
        if not lista_registros_processados:
            return []

        # A ordenação já deve ter sido feita em processar_registros_bpa_i_completo.
        # Se a deduplicação puder alterar a ordem, re-ordenar aqui:
        # lista_registros_processados.sort(key=lambda r: (
        #     str(r.get('prd_cnsmed') or '').strip(), 
        #     r.get('prd_dtaten') or '',              
        #     str(r.get('prd_nmpac') or '').strip()   
        # ))

        cns_profissional_grupo_atual = None
        folha_para_profissional_atual = 0 
        sequencia_na_folha_atual = 0
        
        registros_numerados = []

        for registro in lista_registros_processados:
            cns_profissional_registro_corrente = registro.get('prd_cnsmed', ' ').strip()

            if cns_profissional_registro_corrente != cns_profissional_grupo_atual:
                cns_profissional_grupo_atual = cns_profissional_registro_corrente
                folha_para_profissional_atual = 1
                sequencia_na_folha_atual = 1
            else:
                sequencia_na_folha_atual += 1
                if sequencia_na_folha_atual > 99: # Limite de 99 por folha
                    folha_para_profissional_atual += 1
                    sequencia_na_folha_atual = 1
            
            registro_copia = registro.copy() 
            registro_copia['prd_flh'] = str(folha_para_profissional_atual).zfill(3)
            registro_copia['prd_seq'] = str(sequencia_na_folha_atual).zfill(2)
            registros_numerados.append(registro_copia)
            
        print(f"Atribuição final de folha/sequência para {len(registros_numerados)} registros concluída.")
        return registros_numerados
        
    def consultar_dados_alternativo(self, data_inicio, data_fim, competencia=None):
        # ... (código do consultar_dados_alternativo permanece o mesmo) ...
        return None, [] # Adicionado para consistência

    def carregar_mapeamento_procedimentos(self, cod_procs_bd_unicos):
        # ... (código do carregar_mapeamento_procedimentos permanece o mesmo) ...
        # Sua versão mais recente desta função está correta.
        mapeamento_proc = {}
        if not self.conn or not cod_procs_bd_unicos: return mapeamento_proc
        try:
            from sqlalchemy import table, column, select
            procedimentos_table = table("procedimentos", column("id_procedimento"), column("codigo_procedimento"), schema="sigh")
            cod_procs_list = [str(c) for c in cod_procs_bd_unicos]
            query = select(procedimentos_table.c.id_procedimento, procedimentos_table.c.codigo_procedimento).where(procedimentos_table.c.id_procedimento.in_(cod_procs_list))
            result = self.conn.execute(query)
            for row in result: mapeamento_proc[str(row.id_procedimento)] = str(row.codigo_procedimento) 
            print(f"Mapeamento de {len(mapeamento_proc)} procedimentos carregado (de {len(cod_procs_bd_unicos)} únicos).")
        except Exception as e: print(f"Erro ao carregar mapeamento de procedimentos: {str(e)}")
        return mapeamento_proc
        
    def carregar_tabela_procedimentos_cid(self):
        """Carrega a tabela de procedimentos (código curto) e seus respectivos SIGTAP, Serviço, Classificação e CID sugerido."""
        tabela = {} 
        tabela['105'] = {'codigo_sigtap': '0301070105', 'servico': '135', 'classificacao': '003', 'cid_sugestao': 'M638', 'cid_obrigatorio': True}
        tabela['121'] = {'codigo_sigtap': '0301070121', 'servico': '135', 'classificacao': '003', 'cid_sugestao': 'M638', 'cid_obrigatorio': True}
        tabela['237'] = {'codigo_sigtap': '0301070237', 'servico': '135', 'classificacao': '003', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['63'] = {'codigo_sigtap': '0301100063', 'servico': '135', 'classificacao': '003', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['210'] = {'codigo_sigtap': '0301070210', 'servico': '135', 'classificacao': '003', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['229'] = {'codigo_sigtap': '0301070229', 'servico': '135', 'classificacao': '003', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['19'] = {'codigo_sigtap': '0302050019', 'servico': '135', 'classificacao': '003', 'cid_sugestao': 'M968', 'cid_obrigatorio': True}
        tabela['27'] = {'codigo_sigtap': '0302050027', 'servico': '135', 'classificacao': '003', 'cid_sugestao': 'M998', 'cid_obrigatorio': True}
        tabela['14'] = {'codigo_sigtap': '0302060014', 'servico': '135', 'classificacao': '003', 'cid_sugestao': 'G968', 'cid_obrigatorio': True}
        tabela['30'] = {'codigo_sigtap': '0302060030', 'servico': '135', 'classificacao': '003', 'cid_sugestao': 'G839', 'cid_obrigatorio': True}
        tabela['57'] = {'codigo_sigtap': '0302060057', 'servico': '135', 'classificacao': '003', 'cid_sugestao': 'Q878', 'cid_obrigatorio': True}
        tabela['49'] = {'codigo_sigtap': '0302060049', 'servico': '135', 'classificacao': '003', 'cid_sugestao': 'F83', 'cid_obrigatorio': True}
        tabela['530'] = {'codigo_sigtap': '0309050530', 'servico': '135', 'classificacao': '003', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['23'] = {'codigo_sigtap': '0211030023', 'servico': '135', 'classificacao': '003', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['31'] = {'codigo_sigtap': '0211030031', 'servico': '135', 'classificacao': '003', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['24'] = {'codigo_sigtap': '0301070024', 'servico': '135', 'classificacao': '002', 'cid_sugestao': 'F83', 'cid_obrigatorio': True}
        tabela['40'] = {'codigo_sigtap': '0301070040', 'servico': '135', 'classificacao': '002', 'cid_sugestao': 'F84', 'cid_obrigatorio': True}
        tabela['59'] = {'codigo_sigtap': '0301070059', 'servico': '135', 'classificacao': '002', 'cid_sugestao': 'F84', 'cid_obrigatorio': True}
        tabela['75'] = {'codigo_sigtap': '0301070075', 'servico': '135', 'classificacao': '002', 'cid_sugestao': 'F84', 'cid_obrigatorio': True}
        tabela['261'] = {'codigo_sigtap': '0301070261', 'servico': '135', 'classificacao': '002', 'cid_sugestao': 'F84', 'cid_obrigatorio': True}
        tabela['13'] = {'codigo_sigtap': '0211100013', 'servico': '135', 'classificacao': '002', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['38'] = {'codigo_sigtap': '0211060038', 'servico': '135', 'classificacao': '001', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['20'] = {'codigo_sigtap': '0211060020', 'servico': '135', 'classificacao': '001', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['54'] = {'codigo_sigtap': '0211060054', 'servico': '135', 'classificacao': '001', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['100'] = {'codigo_sigtap': '0211060100', 'servico': '135', 'classificacao': '001', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['1127'] = {'codigo_sigtap': '0211061127', 'servico': '135', 'classificacao': '001', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['224'] = {'codigo_sigtap': '0211060224', 'servico': '135', 'classificacao': '001', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['259'] = {'codigo_sigtap': '0211060259', 'servico': '135', 'classificacao': '001', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['232'] = {'codigo_sigtap': '0211060232', 'servico': '135', 'classificacao': '001', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['151'] = {'codigo_sigtap': '0211060151', 'servico': '135', 'classificacao': '001', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['148'] = {'codigo_sigtap': '0301070148', 'servico': '135', 'classificacao': '001', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['156'] = {'codigo_sigtap': '0301070156', 'servico': '135', 'classificacao': '001', 'cid_sugestao': 'H542', 'cid_obrigatorio': True}
        tabela['164'] = {'codigo_sigtap': '0301070164', 'servico': '135', 'classificacao': '001', 'cid_sugestao': 'H542', 'cid_obrigatorio': True}
        tabela['245'] = {'codigo_sigtap': '0301070245', 'servico': '135', 'classificacao': '001', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['18'] = {'codigo_sigtap': '0302030018', 'servico': '135', 'classificacao': '001', 'cid_sugestao': 'H542', 'cid_obrigatorio': True}
        tabela['26'] = {'codigo_sigtap': '0302030026', 'servico': '135', 'classificacao': '001', 'cid_sugestao': 'H519', 'cid_obrigatorio': True}
        tabela['1113'] = {'codigo_sigtap': '0211051113', 'servico': '135', 'classificacao': '005', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['25'] = {'codigo_sigtap': '0211070025', 'servico': '135', 'classificacao': '005', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['33'] = {'codigo_sigtap': '0211070033', 'servico': '135', 'classificacao': '005', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['41'] = {'codigo_sigtap': '0211070041', 'servico': '135', 'classificacao': '005', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['50'] = {'codigo_sigtap': '0211070050', 'servico': '135', 'classificacao': '005', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['106'] = {'codigo_sigtap': '0211070106', 'servico': '135', 'classificacao': '005', 'cid_sugestao': 'H919', 'cid_obrigatorio': True}
        tabela['149'] = {'codigo_sigtap': '0211070149', 'servico': '135', 'classificacao': '005', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['157'] = {'codigo_sigtap': '0211070157', 'servico': '135', 'classificacao': '005', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['203'] = {'codigo_sigtap': '0211070203', 'servico': '135', 'classificacao': '005', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['211'] = {'codigo_sigtap': '0211070211', 'servico': '135', 'classificacao': '005', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['246'] = {'codigo_sigtap': '0211070246', 'servico': '135', 'classificacao': '005', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['262'] = {'codigo_sigtap': '0211070262', 'servico': '135', 'classificacao': '005', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['270'] = {'codigo_sigtap': '0211070270', 'servico': '135', 'classificacao': '005', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['300'] = {'codigo_sigtap': '0211070300', 'servico': '135', 'classificacao': '005', 'cid_sugestao': 'H919', 'cid_obrigatorio': True}
        tabela['319'] = {'codigo_sigtap': '0211070319', 'servico': '135', 'classificacao': '005', 'cid_sugestao': 'H919', 'cid_obrigatorio': True}
        tabela['327'] = {'codigo_sigtap': '0211070327', 'servico': '135', 'classificacao': '005', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['335'] = {'codigo_sigtap': '0211070335', 'servico': '135', 'classificacao': '005', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['343'] = {'codigo_sigtap': '0211070343', 'servico': '135', 'classificacao': '005', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['351'] = {'codigo_sigtap': '0211070351', 'servico': '135', 'classificacao': '005', 'cid_sugestao': '', 'cid_obrigatorio': False}        
        tabela['424'] = {'codigo_sigtap': '0211070424', 'servico': '135', 'classificacao': '005', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['432'] = {'codigo_sigtap': '0211070432', 'servico': '135', 'classificacao': '005', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['32'] = {'codigo_sigtap': '0301070032', 'servico': '135', 'classificacao': '005', 'cid_sugestao': 'H919', 'cid_obrigatorio': True}
        tabela['253'] = {'codigo_sigtap': '0301070253', 'servico': '135', 'classificacao': '005', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['520'] = {'codigo_sigtap': '0701050020', 'servico': '135', 'classificacao': '012', 'cid_sugestao': 'Z933', 'cid_obrigatorio': True}
        tabela['512'] = {'codigo_sigtap': '0701050012', 'servico': '135', 'classificacao': '012', 'cid_sugestao': 'Z933', 'cid_obrigatorio': True}
        tabela['72'] = {'codigo_sigtap': '0301010072', 'servico': '135', 'classificacao': '003', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['113'] = {'codigo_sigtap': '0301070113', 'servico': '135', 'classificacao': '002', 'cid_sugestao': 'H919', 'cid_obrigatorio': True}
        tabela['114'] = {'codigo_sigtap': '0211070114', 'servico': '135', 'classificacao': '005', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['124'] = {'codigo_sigtap': '0301070024', 'servico': '135', 'classificacao': '002', 'cid_sugestao': 'F840', 'cid_obrigatorio': True}
        tabela['143'] = {'codigo_sigtap': '0701030143', 'servico': '135', 'classificacao': '005', 'cid_sugestao': 'H919', 'cid_obrigatorio': True}
        tabela['160'] = {'codigo_sigtap': '0301080160', 'servico': '135', 'classificacao': '002', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['288'] = {'codigo_sigtap': '0301070288', 'servico': '135', 'classificacao': '002', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['296'] = {'codigo_sigtap': '0301070296', 'servico': '135', 'classificacao': '002', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['44'] = {'codigo_sigtap': '0301040044', 'servico': '135', 'classificacao': '002', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['48'] = {'codigo_sigtap': '0301010048', 'servico': '', 'classificacao': '', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['67'] = {'codigo_sigtap': '0301070067', 'servico': '135', 'classificacao': '002', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['76'] = {'codigo_sigtap': '0211070076', 'servico': '', 'classificacao': '', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['79'] = {'codigo_sigtap': '0301040079', 'servico': '', 'classificacao': '', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['84'] = {'codigo_sigtap': '0211070084', 'servico': '', 'classificacao': '', 'cid_sugestao': '', 'cid_obrigatorio': False}
        tabela['92'] = {'codigo_sigtap': '0211070092', 'servico': '135', 'classificacao': '005', 'cid_sugestao': 'H919', 'cid_obrigatorio': True}
        return tabela
    
    def gerar_arquivo_txt(self, competencia, registros_bpa, caminho_arquivo_base):
        # ... (código do gerar_arquivo_txt permanece o mesmo) ...
        # Certifique-se que newline='' está sendo usado
        if not registros_bpa: print("Não há registros processados para gerar o arquivo TXT."); return False
        try:
            print(f"\nGerando arquivo TXT para {len(registros_bpa)} registros com competência {competencia}")
            mes_num = int(competencia[-2:])
            extensoes_bpa = ['JAN', 'FEV', 'MAR', 'ABR', 'MAI', 'JUN', 'JUL', 'AGO', 'SET', 'OUT', 'NOV', 'DEZ']
            extensao_final = extensoes_bpa[mes_num - 1] if 0 < mes_num <= 12 else competencia[-2:]
            nome_base_sem_ext = os.path.splitext(os.path.basename(caminho_arquivo_base))[0]
            diretorio = os.path.dirname(caminho_arquivo_base)
            caminho_arquivo_final_com_ext = os.path.join(diretorio, f"{nome_base_sem_ext}.{extensao_final}")
            header_dict = self.gerar_header_bpa(competencia, registros_bpa)
            with open(caminho_arquivo_final_com_ext, 'w', newline='', encoding='latin-1') as f: # newline=''
                linha_header_str = ( header_dict['cbc_hdr_1'] + header_dict['cbc_hdr_2'] + header_dict['cbc_mvm'] + header_dict['cbc_lin'] + header_dict['cbc_flh'] + header_dict['cbc_smt_vrf'] + header_dict['cbc_rsp'] + header_dict['cbc_sgl'] + header_dict['cbc_cgccpf'] + header_dict['cbc_dst'] + header_dict['cbc_dst_in'] + header_dict['cbc_versao'] )
                f.write(linha_header_str + '\r\n')
                for reg_dict in registros_bpa:
                    linha_reg_str = (
                        str(reg_dict.get('prd_ident', '03')).ljust(2) + str(reg_dict.get('prd_cnes', ' ' * 7)).ljust(7) +
                        str(reg_dict.get('prd_cmp', ' ' * 6)).ljust(6) + str(reg_dict.get('prd_cnsmed', ' ' * 15)).ljust(15) +
                        str(reg_dict.get('prd_cbo', ' ' * 6)).ljust(6) + str(reg_dict.get('prd_dtaten', ' ' * 8)).ljust(8) +
                        str(reg_dict.get('prd_flh', '000')).zfill(3) + str(reg_dict.get('prd_seq', '00')).zfill(2) +
                        str(reg_dict.get('prd_pa', ' ' * 10)).ljust(10) + str(reg_dict.get('prd_cnspac', ' ' * 15)).ljust(15) +
                        str(reg_dict.get('prd_sexo', ' ')).ljust(1) + str(reg_dict.get('prd_ibge', ' ' * 6)).ljust(6) +
                        str(reg_dict.get('prd_cid', ' ' * 4)).ljust(4) + str(reg_dict.get('prd_ldade', '000')).zfill(3) +
                        str(reg_dict.get('prd_qt', '000000')).zfill(6) + str(reg_dict.get('prd_caten', '  ')).ljust(2) +
                        str(reg_dict.get('prd_naut', ' ' * 13)).ljust(13) + str(reg_dict.get('prd_org', '   ')).ljust(3) +
                        str(reg_dict.get('prd_nmpac', ' ' * 30)).ljust(30) + str(reg_dict.get('prd_dtnasc', ' ' * 8)).ljust(8) +
                        str(reg_dict.get('prd_raca', '  ')).ljust(2) + str(reg_dict.get('prd_etnia', '    ')).ljust(4) +
                        str(reg_dict.get('prd_nac', '   ')).ljust(3) + str(reg_dict.get('prd_srv', '   ')).ljust(3) +
                        str(reg_dict.get('prd_clf', '   ')).ljust(3) + str(reg_dict.get('prd_equipe_Seq', ' ' * 8)).ljust(8) +
                        str(reg_dict.get('prd_equipe_Area', ' ' * 4)).ljust(4) + str(reg_dict.get('prd_cnpj', ' ' * 14)).ljust(14) +
                        str(reg_dict.get('prd_cep_pcnte', ' ' * 8)).ljust(8) + str(reg_dict.get('prd_lograd_pcnte', '   ')).ljust(3) +
                        str(reg_dict.get('prd_end_pcnte', ' ' * 30)).ljust(30) + str(reg_dict.get('prd_compl_pcnte', ' ' * 10)).ljust(10) +
                        str(reg_dict.get('prd_num_pcnte', ' ' * 5)).ljust(5) + str(reg_dict.get('prd_bairro_pcnte', ' ' * 30)).ljust(30) +
                        str(reg_dict.get('prd_ddtel_pcnte', ' ' * 11)).ljust(11) + str(reg_dict.get('prd_email_pcnte', ' ' * 40)).ljust(40) +
                        str(reg_dict.get('prd_ine', ' ' * 10)).ljust(10) + str(reg_dict.get('prd_cpf_pcnte', ' ' * 11)).ljust(11) +
                        str(reg_dict.get('prd_situacao_rua', ' ')).ljust(1)
                    )
                    linha_reg_final = linha_reg_str.ljust(350)[:350]
                    f.write(linha_reg_final + '\r\n')
            print(f"Arquivo BPA gerado com sucesso: {caminho_arquivo_final_com_ext}")
            return True
        except Exception as e:
            print(f"Erro ao gerar arquivo BPA: {str(e)}"); import traceback; traceback.print_exc(); return False
            
    def gerar_arquivo_csv(self, registros_bpa, caminho_arquivo):
        # ... (código do gerar_arquivo_csv permanece o mesmo) ...
        if not registros_bpa: print("Não há dados para gerar CSV."); return False
        try:
            df = pd.DataFrame(registros_bpa)
            df.to_csv(caminho_arquivo, index=False, quoting=csv.QUOTE_ALL, encoding='utf-8-sig')
            print(f"Arquivo CSV gerado com sucesso: {caminho_arquivo}"); return True
        except Exception as e: print(f"Erro ao gerar arquivo CSV: {str(e)}"); return False
    
    def gerar_arquivo_xlsx(self, registros_bpa, caminho_arquivo):
        # ... (código do gerar_arquivo_xlsx permanece o mesmo) ...
        if not registros_bpa: print("Não há dados para gerar XLSX."); return False
        try:
            df = pd.DataFrame(registros_bpa)
            df.to_excel(caminho_arquivo, index=False)
            print(f"Arquivo Excel gerado com sucesso: {caminho_arquivo}"); return True
        except Exception as e: print(f"Erro ao gerar arquivo Excel: {str(e)}"); return False
            
    def processar_registros_bpa_i(self, registros_bd, competencia=None):
        # ... (código do processar_registros_bpa_i (versão simples) permanece o mesmo) ...
        # Esta função não é o foco principal das últimas correções.
        registros_bpa_i = []; # ...
        if not registros_bpa_i: print("Atenção: 'processar_registros_bpa_i' (simples) não foi totalmente implementada para todos os campos BPA-I.")
        return registros_bpa_i # Retorna lista vazia ou o que ela já fazia
    
    def aplicar_deduplicacao(self, registros_bpa_brutos, metodo="completo"):
        """Aplica deduplicação baseada no método escolhido. Não renumera folha/sequência aqui."""
        if not registros_bpa_brutos:
            return []
            
        if metodo == "nenhum":
            print("Deduplicação desabilitada - mantendo todos os registros brutos processados.")
            return registros_bpa_brutos
        elif metodo == "simples": 
            return self.deduplicate_por_criterio_alternativo(registros_bpa_brutos)
        elif metodo == "novo_manter_primeiro": 
            return self.deduplicate_novo_criterio_manter_primeiro(registros_bpa_brutos)
        elif metodo == "por_id_lancamento": # NOVA OPÇÃO
            return self.deduplicate_por_id_lancamento_original(registros_bpa_brutos)
        else: # "completo" ou default
            return self.deduplicate_registros_bpa(registros_bpa_brutos)
            
    def deduplicate_registros_bpa(self, registros_bpa_processados):
        """Remove registros duplicados da lista de registros BPA-I (Método Completo). Não renumera."""
        print(f"\nIniciando deduplicação (Método Completo) de {len(registros_bpa_processados)} registros...")
        if not registros_bpa_processados: return []
        campos_chave = [
            'prd_cnes', 'prd_cmp', 'prd_cnsmed', 'prd_cbo', 'prd_dtaten',
            'prd_pa', 'prd_cnspac', 'prd_cid', 
        ]
        registros_unicos_dict = {}; registros_duplicados_info = []
        for registro in registros_bpa_processados:
            chave_str_list = [str(registro.get(campo, '')).strip() for campo in campos_chave]
            chave_unica_tupla = tuple(chave_str_list)
            if chave_unica_tupla in registros_unicos_dict:
                registro_existente = registros_unicos_dict[chave_unica_tupla]
                try:
                    qtd_atual_str = str(registro.get('prd_qt', '0')).replace('.', '')
                    qtd_existente_str = str(registro_existente.get('prd_qt', '0')).replace('.', '')
                    qtd_atual = int(qtd_atual_str) if qtd_atual_str.isdigit() else 0
                    qtd_existente = int(qtd_existente_str) if qtd_existente_str.isdigit() else 0
                    qtd_total_somada = qtd_existente + qtd_atual
                    registro_existente['prd_qt'] = str(qtd_total_somada).zfill(6)
                    registros_duplicados_info.append({'chave': chave_unica_tupla, 'qtd_adic': qtd_atual, 'nova_qtd': qtd_total_somada})
                except (ValueError, TypeError) as e_qtd:
                    print(f"  Aviso: Erro ao somar quantidades para chave {chave_unica_tupla}: {e_qtd}. Mantendo o primeiro.")
                    registros_duplicados_info.append({'chave': chave_unica_tupla, 'motivo': 'Erro qtd'})
            else:
                registros_unicos_dict[chave_unica_tupla] = registro.copy() 
        registros_finais_deduplicados = list(registros_unicos_dict.values())
        print(f"Deduplicação (Método Completo) concluída: {len(registros_finais_deduplicados)} registros únicos finais.")
        return registros_finais_deduplicados

    def deduplicate_novo_criterio_manter_primeiro(self, registros_bpa_processados):
        """
        Deduplica mantendo apenas o primeiro registro para a chave:
        (CNES, Competência, CBO Profissional, Data Atendimento, CNS Paciente).
        A quantidade prd_qt não é somada, é mantida a do primeiro registro (que será '000001').
        """
        print(f"\nIniciando deduplicação (Novo Critério - Manter Primeiro por Pac/Prof/Dia) de {len(registros_bpa_processados)} registros...")
        if not registros_bpa_processados:
            return []

        registros_unicos_dict = {} 
        registros_finais = []

        campos_chave_novo = [
            'prd_cnes', 
            'prd_cmp', 
            'prd_cbo', 
            'prd_dtaten', 
            'prd_cnspac' 
        ]

        for registro in registros_bpa_processados:
            chave_str_list = [str(registro.get(campo, '')).strip() for campo in campos_chave_novo]
            chave_unica_tupla = tuple(chave_str_list)

            if chave_unica_tupla not in registros_unicos_dict:
                # Se a chave não foi vista antes, este é o primeiro registro para esta combinação.
                registros_unicos_dict[chave_unica_tupla] = True # Apenas marcar a chave como vista
                registros_finais.append(registro.copy()) # Adiciona o registro (com prd_qt='000001')
            # else:
                # Chave já existe, este é um registro subsequente para a mesma combinação. Ignora.
                # self._log_message(f"  DEBUG: Duplicata (Novo Critério) descartada para chave {chave_unica_tupla}, Pac: {registro.get('prd_nmpac')}, Proc Orig: {registro.get('prd_pa')}")
        
        print(f"Deduplicação (Novo Critério - Manter Primeiro) concluída:")
        print(f"  - Registros originais processados: {len(registros_bpa_processados)}")
        print(f"  - Registros únicos finais: {len(registros_finais)}")
        
        return registros_finais


    def deduplicate_por_criterio_alternativo(self, registros_bpa_processados):
        """Método alternativo de deduplicação (Simples). Não renumera."""
        print(f"\nIniciando deduplicação (Método Simples) de {len(registros_bpa_processados)} registros...")
        if not registros_bpa_processados: return []
        registros_unicos_agregados = {}
        for registro in registros_bpa_processados:
            chave_simples = ( str(registro.get('prd_cnspac', '')).strip(), str(registro.get('prd_pa', '')).strip(), str(registro.get('prd_dtaten', '')).strip() )
            if chave_simples in registros_unicos_agregados:
                registro_existente = registros_unicos_agregados[chave_simples]
                try:
                    qtd_atual_str = str(registro.get('prd_qt', '0')).replace('.', '')
                    qtd_existente_str = str(registro_existente.get('prd_qt', '0')).replace('.', '')
                    qtd_atual = int(qtd_atual_str) if qtd_atual_str.isdigit() else 0
                    qtd_existente = int(qtd_existente_str) if qtd_existente_str.isdigit() else 0
                    nova_qtd_total = qtd_existente + qtd_atual
                    registro_existente['prd_qt'] = str(nova_qtd_total).zfill(6)
                except (ValueError, TypeError) as e_qtd_simples:
                    print(f"  Aviso: Erro ao somar quantidades (Simples) para chave {chave_simples}: {e_qtd_simples}.")
            else:
                registros_unicos_agregados[chave_simples] = registro.copy()
        registros_finais_agrupados = list(registros_unicos_agregados.values())
        print(f"Deduplicação (Método Simples) concluída: {len(registros_finais_agrupados)} registros finais.")
        return registros_finais_agrupados

# --- Interface Gráfica (BPAExporterGUI) ---
class BPAExporterGUI:
    def __init__(self, root_window):
        self.root = root_window
        self.root.title("Exportador BPA-I (SIGH Profissional)")
        self.root.geometry("950x720") # Aumentei um pouco a altura para o novo label
        self.exporter = BPAExporter()
        self.exporter.gui_log_callback = self._log_message # Passa a função de log da GUI para o exporter

        
        style = ttk.Style()
        style.theme_use('clam') 
        style.configure("TLabel", padding=3, font=('Helvetica', 10))
        style.configure("TButton", padding=3, font=('Helvetica', 10))
        style.configure("TEntry", padding=3, font=('Helvetica', 10))
        style.configure("TCombobox", padding=3, font=('Helvetica', 10))
        style.configure("TLabelframe.Label", font=('Helvetica', 10, 'bold'))
        
        self.frame_conexao = ttk.LabelFrame(root_window, text="1. Conexão ao Banco de Dados")
        self.frame_conexao.pack(fill="x", padx=10, pady=5, ipady=5)
        
        
        self.frame_filtros = ttk.LabelFrame(root_window, text="2. Filtros e Deduplicação")
        self.frame_filtros.pack(fill="x", padx=10, pady=5, ipady=5)
        
        self.frame_config = ttk.LabelFrame(root_window, text="3. Configurações do Arquivo BPA-I")
        self.frame_config.pack(fill="x", padx=10, pady=5, ipady=5)
        
        self.frame_acoes = ttk.LabelFrame(root_window, text="4. Ações e Totais") # Nome do frame ajustado
        self.frame_acoes.pack(fill="x", padx=10, pady=5, ipady=5)
        
        self.frame_log = ttk.LabelFrame(root_window, text="Log de Eventos")
        self.frame_log.pack(fill="both", expand=True, padx=10, pady=5)

        conn_fields = [ ("Banco:", "db_name", "bd0553"), ("Usuário:", "db_user", "postgres"), ("Senha:", "db_password", "postgres", True), ("Host:", "db_host", "localhost"), ("Porta:", "db_port", "5432") ]
        for i, (label_text, attr_name, default_val, *is_password) in enumerate(conn_fields):
            row, col_offset = divmod(i, 3)
            ttk.Label(self.frame_conexao, text=label_text).grid(row=row, column=col_offset*2, padx=5, pady=3, sticky="w")
            entry = ttk.Entry(self.frame_conexao, width=18)
            if is_password and is_password[0]: entry.config(show="*")
            entry.insert(0, default_val); entry.grid(row=row, column=col_offset*2 + 1, padx=5, pady=3, sticky="ew"); setattr(self, attr_name, entry)
        self.btn_conectar = ttk.Button(self.frame_conexao, text="Conectar ao BD", command=self.conectar_bd)
        self.btn_conectar.grid(row=len(conn_fields)//3, column=6, padx=10, pady=5, rowspan=max(1, (len(conn_fields)-1)//3 + 1 - (len(conn_fields)//3) ), sticky="ew")
        ttk.Label(self.frame_filtros, text="Data Início:").grid(row=0, column=0, padx=5, pady=3, sticky="w")
        self.data_inicio_entry = DateEntry(self.frame_filtros, width=12, date_pattern='dd/mm/yyyy', font=('Helvetica', 10)); self.data_inicio_entry.grid(row=0, column=1, padx=5, pady=3, sticky="ew")
        ttk.Label(self.frame_filtros, text="Data Fim:").grid(row=0, column=2, padx=5, pady=3, sticky="w")
        self.data_fim_entry = DateEntry(self.frame_filtros, width=12, date_pattern='dd/mm/yyyy', font=('Helvetica', 10)); self.data_fim_entry.grid(row=0, column=3, padx=5, pady=3, sticky="ew")
        ttk.Label(self.frame_filtros, text="Competência (AAAAMM):").grid(row=0, column=4, padx=5, pady=3, sticky="w")
        self.competencia_entry = ttk.Entry(self.frame_filtros, width=10); self.competencia_entry.insert(0, datetime.datetime.now().strftime("%Y%m")); self.competencia_entry.grid(row=0, column=5, padx=5, pady=3, sticky="ew")
        ttk.Label(self.frame_filtros, text="Critério de Data para Filtro:").grid(row=1, column=0, padx=5, pady=3, sticky="w")
        self.criterio_data_combo = ttk.Combobox(self.frame_filtros, values=["Data do Lançamento (Recomendado)", "Data da Conta (Início)", "Competência da Conta", "Data do Atendimento (Ficha)"], width=30, state="readonly"); self.criterio_data_combo.current(0); self.criterio_data_combo.grid(row=1, column=1, columnspan=3, padx=5, pady=3, sticky="ew")
        ttk.Label(self.frame_filtros, text="Deduplicação de Registros:").grid(row=2, column=0, padx=5, pady=3, sticky="w")
        self.metodo_deduplicacao_combo = ttk.Combobox(self.frame_filtros, values=[
            "Método Completo (Agrega Qtde por Proc/Pac/etc)", # Descrição ajustada
            "Método Simples (Agrega Qtde por Proc/Pac/Data)", # Descrição ajustada
            "Novo: 1 Proc por Pac/Prof/Dia (Qtde=1)",      # Nova opção
            "SIGH: 1 por Lançamento Original do BD (Qtde=1)", # NOVA OPÇÃO
            "Sem Deduplicação (Qtde=1)"                       # Descrição ajustada
        ], width=45, state="readonly") # Aumentado width se necessário
        self.metodo_deduplicacao_combo.current(0) # Ou o default que você preferir
        self.metodo_deduplicacao_combo.grid(row=2, column=1, columnspan=3, padx=5, pady=3, sticky="ew")

        bpa_config_fields = [ ("Órgão Responsável:", "orgao_resp_entry", "APAE DE COLINAS DO TOCANTINS", 35), ("Sigla Órgão:", "sigla_orgao_entry", "APAE", 8), ("CNPJ/CPF Estab.:", "cgc_cpf_entry", "25062282000182", 18), ("Órgão Destino:", "orgao_destino_entry", "SECRETARIA MUNICIPAL DE SAUDE", 35), ("Indicador Destino (M/E):", "indicador_destino_combo", ["M", "E"], 5), ("Versão Sistema BPA:", "versao_sistema_entry", "V04.10", 10), ("CNES Estabelecimento:", "cnes_entry", "2560372", 10) ]
        for i, (label_text, attr_name, default_val, width) in enumerate(bpa_config_fields):
            row, col_offset = divmod(i, 2)
            ttk.Label(self.frame_config, text=label_text).grid(row=row, column=col_offset*2, padx=5, pady=3, sticky="w")
            if isinstance(default_val, list): combo = ttk.Combobox(self.frame_config, values=default_val, width=width, state="readonly"); combo.set(default_val[0]); combo.grid(row=row, column=col_offset*2 + 1, padx=5, pady=3, sticky="ew"); setattr(self, attr_name, combo)
            else: entry = ttk.Entry(self.frame_config, width=width); entry.insert(0, default_val); entry.grid(row=row, column=col_offset*2 + 1, padx=5, pady=3, sticky="ew"); setattr(self, attr_name, entry)
        self.btn_consultar_dados = ttk.Button(self.frame_acoes, text="Consultar Dados do BD", command=self.iniciar_consulta_dados, state="disabled")
        self.btn_consultar_dados.grid(row=0, column=0, padx=10, pady=10, ipady=5, sticky="ew")
        
        self.btn_exportar_txt = ttk.Button(self.frame_acoes, text="Exportar Arquivo BPA (.TXT)", command=self.exportar_arquivo_txt, state="disabled")
        self.btn_exportar_txt.grid(row=0, column=1, padx=10, pady=10, ipady=5, sticky="ew")
        
        self.btn_exportar_csv = ttk.Button(self.frame_acoes, text="Exportar para CSV", command=self.exportar_arquivo_csv, state="disabled")
        self.btn_exportar_csv.grid(row=0, column=2, padx=10, pady=10, ipady=5, sticky="ew")
        
        self.btn_exportar_xlsx = ttk.Button(self.frame_acoes, text="Exportar para Excel (.xlsx)", command=self.exportar_arquivo_xlsx, state="disabled")
        self.btn_exportar_xlsx.grid(row=0, column=3, padx=10, pady=10, ipady=5, sticky="ew")

        # NOVO: Labels para exibir totais
        ttk.Label(self.frame_acoes, text="Registros BPA-I Finais:").grid(row=1, column=0, padx=(10,0), pady=5, sticky="e")
        self.lbl_total_registros_valor = ttk.Label(self.frame_acoes, text="0", font=('Helvetica', 10, 'bold'))
        self.lbl_total_registros_valor.grid(row=1, column=1, padx=(0,10), pady=5, sticky="w")
        
        ttk.Label(self.frame_acoes, text="Total de Procedimentos (Qtde):").grid(row=1, column=2, padx=(10,0), pady=5, sticky="e")
        self.lbl_total_quantidade_valor = ttk.Label(self.frame_acoes, text="0", font=('Helvetica', 10, 'bold'))
        self.lbl_total_quantidade_valor.grid(row=1, column=3, padx=(0,10), pady=5, sticky="w")

        # Configurar colunas do frame_acoes para expandir igualmente
        for i_col in range(4): self.frame_acoes.columnconfigure(i_col, weight=1)

        self.log_text_area = tk.Text(self.frame_log, height=10, wrap=tk.WORD, font=('Courier New', 9)); self.log_text_area.pack(side=tk.LEFT, fill="both", expand=True, padx=(0,0))
        log_scrollbar = ttk.Scrollbar(self.frame_log, orient="vertical", command=self.log_text_area.yview); log_scrollbar.pack(side=tk.RIGHT, fill="y")
        self.log_text_area.config(yscrollcommand=log_scrollbar.set, state="disabled")
        self.registros_bpa_processados = []
        self._log_message("Interface iniciada. Preencha os dados de conexão e clique em 'Conectar'.")
        

    def _log_message(self, message):
        self.log_text_area.config(state="normal")
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.log_text_area.insert(tk.END, f"[{timestamp}] {message}\n")
        self.log_text_area.see(tk.END)
        self.log_text_area.config(state="disabled")
        self.root.update_idletasks()

    def conectar_bd(self):
        try:
            db_params = { "db_name": self.db_name.get(), "user": self.db_user.get(), "password": self.db_password.get(), "host": self.db_host.get(), "port": self.db_port.get() }
            self._log_message(f"Tentando conectar ao banco: {db_params['db_name']}@{db_params['host']}...")
            if self.exporter.conectar_bd(**db_params):
                self._log_message("Conexão com o banco de dados estabelecida com sucesso!")
                messagebox.showinfo("Conexão Bem-Sucedida", "Conexão ao banco de dados estabelecida!")
                self.btn_consultar_dados.config(state="normal")
            else:
                self._log_message("Falha ao conectar ao banco. Verifique as credenciais e o console.")
                messagebox.showerror("Erro de Conexão", "Não foi possível conectar ao banco de dados. Verifique o log.")
        except Exception as e:
            self._log_message(f"Erro inesperado durante a conexão: {str(e)}")
            messagebox.showerror("Erro Crítico", f"Ocorreu um erro crítico ao tentar conectar: {str(e)}")

    def _atualizar_config_exporter(self):
        self.exporter.config['orgao_responsavel'] = self.orgao_resp_entry.get()
        self.exporter.config['sigla_orgao'] = self.sigla_orgao_entry.get()
        self.exporter.config['cgc_cpf'] = self.cgc_cpf_entry.get()
        self.exporter.config['orgao_destino'] = self.orgao_destino_entry.get()
        self.exporter.config['indicador_destino'] = self.indicador_destino_combo.get()
        self.exporter.config['versao_sistema'] = self.versao_sistema_entry.get()
        self.exporter.config['cnes'] = self.cnes_entry.get()
        self.exporter.config['default_ibge_paciente'] = self.exporter.config.get('default_ibge_paciente', '000000')
        self.exporter.config['default_cep_paciente'] = self.exporter.config.get('default_cep_paciente', '00000000')
        self.exporter.config['default_ine'] = self.exporter.config.get('default_ine', '0000000000')

    def iniciar_consulta_dados(self):
        """Handler para o botão de consultar dados."""
        try:
            # Resetar labels de totais no início da consulta
            self.lbl_total_registros_valor.config(text="Calculando...")
            self.lbl_total_quantidade_valor.config(text="Calculando...")
            self.root.update_idletasks()

            data_inicio_val = self.data_inicio_entry.get_date()
            data_fim_val = self.data_fim_entry.get_date()
            competencia_val = self.competencia_entry.get()

            if not competencia_val or len(competencia_val) != 6 or not competencia_val.isdigit():
                messagebox.showerror("Entrada Inválida", "Competência deve estar no formato AAAAMM (ex: 202305).")
                self.lbl_total_registros_valor.config(text="0") # Reset em caso de erro de entrada
                self.lbl_total_quantidade_valor.config(text="0")
                return

            criterio_selecionado_gui = self.criterio_data_combo.get()
            map_criterio_gui_interno = {
                "Data do Lançamento (Recomendado)": "lancamento",
                "Data da Conta (Início)": "conta",
                "Competência da Conta": "competencia",
                "Data do Atendimento (Ficha)": "atendimento"
            }
            criterio_interno = map_criterio_gui_interno.get(criterio_selecionado_gui, "atendimento")
            
            metodo_dedup_gui = self.metodo_deduplicacao_combo.get()
            map_dedup_gui_interno = {
                "Método Completo (Agrega Qtde por Proc/Pac/etc)": "completo",
                "Método Simples (Agrega Qtde por Proc/Pac/Data)": "simples",
                "Novo: 1 Proc por Pac/Prof/Dia (Qtde=1)": "novo_manter_primeiro",
                "SIGH: 1 por Lançamento Original do BD (Qtde=1)": "por_id_lancamento", # <<< CORRIGIDO
                "Sem Deduplicação (Qtde=1)": "nenhum"
            }
            metodo_dedup_interno = map_dedup_gui_interno.get(metodo_dedup_gui, "completo")


            self._log_message(f"Iniciando consulta: Período {data_inicio_val.strftime('%d/%m/%Y')} a {data_fim_val.strftime('%d/%m/%Y')}, Competência {competencia_val}")
            self._log_message(f"Critério de data selecionado: {criterio_selecionado_gui} (interno: {criterio_interno})")
            self._log_message(f"Método de deduplicação: {metodo_dedup_gui} (interno: {metodo_dedup_interno})")
            
            self._atualizar_config_exporter()

            self.btn_consultar_dados.config(state="disabled")
            self.btn_exportar_txt.config(state="disabled")
            self.btn_exportar_csv.config(state="disabled")
            self.btn_exportar_xlsx.config(state="disabled")
            self.root.update_idletasks()

            registros_processados_sem_numeracao = self.exporter.consultar_dados_completo(
                data_inicio_val, data_fim_val, competencia_val, criterio_interno
            )
            
            if registros_processados_sem_numeracao:
                self._log_message(f"Consulta retornou {len(registros_processados_sem_numeracao)} registros processados (antes da deduplicação e numeração).")
                
                registros_deduplicados = self.exporter.aplicar_deduplicacao(
                    registros_processados_sem_numeracao, metodo_dedup_interno
                )
                self._log_message(f"Após deduplicação, {len(registros_deduplicados)} registros.")

                self.registros_bpa_processados = self.exporter._atribuir_folha_sequencia_final(registros_deduplicados)
                
                num_registros_finais = len(self.registros_bpa_processados)
                self._log_message(f"{num_registros_finais} registros finais com folha/sequência atribuídas para exportação.")
                
                # Calcular e exibir a soma das quantidades e o total de registros
                total_quantidade_procedimentos = 0
                if self.registros_bpa_processados: # Verifica se a lista não está vazia
                    for reg_dict in self.registros_bpa_processados:
                        try:
                            total_quantidade_procedimentos += int(reg_dict.get('prd_qt', '0'))
                        except ValueError:
                            self._log_message(f"Aviso: Valor inválido para prd_qt em um registro: {reg_dict.get('prd_qt')}")
                
                self.lbl_total_registros_valor.config(text=str(num_registros_finais))
                self.lbl_total_quantidade_valor.config(text=str(total_quantidade_procedimentos))
                self._log_message(f"Soma total de quantidades (prd_qt) dos procedimentos: {total_quantidade_procedimentos}")

                messagebox.showinfo("Consulta Concluída", f"Consulta finalizada. {num_registros_finais} registros prontos para exportar.")
                self.btn_exportar_txt.config(state="normal")
                self.btn_exportar_csv.config(state="normal")
                self.btn_exportar_xlsx.config(state="normal")
            else:
                self._log_message("Nenhum registro encontrado para os filtros aplicados.")
                messagebox.showwarning("Nenhum Registro", "A consulta não retornou registros.")
                self.lbl_total_registros_valor.config(text="0")
                self.lbl_total_quantidade_valor.config(text="0")

        except Exception as e:
            self._log_message(f"Erro durante a consulta de dados: {str(e)}")
            messagebox.showerror("Erro na Consulta", f"Ocorreu um erro: {str(e)}\nVerifique o log para detalhes.")
            import traceback
            self._log_message(traceback.format_exc())
            self.lbl_total_registros_valor.config(text="Erro") # Indicar erro nos labels
            self.lbl_total_quantidade_valor.config(text="Erro")
        finally:
            self.btn_consultar_dados.config(state="normal")

    def exportar_arquivo_txt(self):
        # ... (código do exportar_arquivo_txt permanece o mesmo) ...
        if not self.registros_bpa_processados: messagebox.showwarning("Sem Dados", "Não há dados consultados para exportar."); return
        competencia_val = self.competencia_entry.get()
        try:
            mes_num = int(competencia_val[-2:]); extensoes_bpa = ['JAN', ..., 'DEZ']; extensao_sugerida = extensoes_bpa[mes_num - 1] if 0 < mes_num <= 12 else competencia_val[-2:]
        except: extensao_sugerida = "TXT"
        cnes_str = self.cnes_entry.get().zfill(7); mes_comp_str = competencia_val[4:6]; ano_comp_ult_dig_str = competencia_val[3:4]
        nome_arquivo_sugerido = f"PA{cnes_str}{mes_comp_str}{ano_comp_ult_dig_str}"
        caminho_arquivo_selecionado = filedialog.asksaveasfilename(initialfile=f"{nome_arquivo_sugerido}.{extensao_sugerida}", defaultextension=f".{extensao_sugerida}", filetypes=[(f"Arquivos BPA (.{extensao_sugerida})", f"*.{extensao_sugerida}"), ("Todos os Arquivos", "*.*")], title="Salvar Arquivo BPA-I TXT" )
        if not caminho_arquivo_selecionado: self._log_message("Exportação TXT cancelada."); return
        self._log_message(f"Iniciando exportação para TXT: {caminho_arquivo_selecionado}"); self._atualizar_config_exporter()
        if self.exporter.gerar_arquivo_txt(competencia_val, self.registros_bpa_processados, caminho_arquivo_selecionado):
            self._log_message(f"Arquivo BPA TXT gerado: {caminho_arquivo_selecionado.replace(os.path.splitext(caminho_arquivo_selecionado)[1], '.'+extensao_sugerida) if '.' not in os.path.basename(caminho_arquivo_selecionado) else caminho_arquivo_selecionado}")
            messagebox.showinfo("Exportação TXT Concluída", "Arquivo BPA-I TXT gerado com sucesso!")
        else: self._log_message("Falha ao gerar arquivo BPA TXT."); messagebox.showerror("Erro na Exportação TXT", "Falha ao gerar arquivo TXT.")


    def exportar_arquivo_csv(self):
        # ... (código do exportar_arquivo_csv permanece o mesmo) ...
        if not self.registros_bpa_processados: messagebox.showwarning("Sem Dados", "Não há dados consultados para exportar."); return
        caminho_arquivo_selecionado = filedialog.asksaveasfilename(initialfile=f"BPA_export_{self.competencia_entry.get()}.csv", defaultextension=".csv", filetypes=[("Arquivos CSV", "*.csv"), ("Todos os Arquivos", "*.*")], title="Salvar Arquivo CSV")
        if not caminho_arquivo_selecionado: self._log_message("Exportação CSV cancelada."); return
        self._log_message(f"Iniciando exportação para CSV: {caminho_arquivo_selecionado}")
        if self.exporter.gerar_arquivo_csv(self.registros_bpa_processados, caminho_arquivo_selecionado):
            self._log_message(f"Arquivo CSV gerado: {caminho_arquivo_selecionado}"); messagebox.showinfo("Exportação CSV Concluída", "Arquivo CSV gerado!")
        else: self._log_message("Falha ao gerar CSV."); messagebox.showerror("Erro na Exportação CSV", "Falha ao gerar arquivo CSV.")

    def exportar_arquivo_xlsx(self):
        # ... (código do exportar_arquivo_xlsx permanece o mesmo) ...
        if not self.registros_bpa_processados: messagebox.showwarning("Sem Dados", "Não há dados consultados para exportar."); return
        caminho_arquivo_selecionado = filedialog.asksaveasfilename(initialfile=f"BPA_export_{self.competencia_entry.get()}.xlsx", defaultextension=".xlsx", filetypes=[("Arquivos Excel", "*.xlsx"), ("Todos os Arquivos", "*.*")], title="Salvar Arquivo Excel (.xlsx)")
        if not caminho_arquivo_selecionado: self._log_message("Exportação XLSX cancelada."); return
        self._log_message(f"Iniciando exportação para XLSX: {caminho_arquivo_selecionado}")
        if self.exporter.gerar_arquivo_xlsx(self.registros_bpa_processados, caminho_arquivo_selecionado):
            self._log_message(f"Arquivo Excel XLSX gerado: {caminho_arquivo_selecionado}"); messagebox.showinfo("Exportação XLSX Concluída", "Arquivo XLSX gerado!")
        else: self._log_message("Falha ao gerar XLSX."); messagebox.showerror("Erro na Exportação XLSX", "Falha ao gerar arquivo XLSX.")


def main():
    """Função principal para iniciar a GUI."""
    try:
        import pandas as pd; import sqlalchemy; import tkcalendar
    except ImportError as e:
        print(f"Erro: Dependência não encontrada: {e}\nPor favor, instale as dependências: pip install pandas sqlalchemy psycopg2-binary tkcalendar colorama")
        return
    
    app_root = tk.Tk()
    gui_app = BPAExporterGUI(app_root)
    app_root.mainloop()

if __name__ == "__main__":
    main()
