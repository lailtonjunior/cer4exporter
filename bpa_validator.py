#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Validador de arquivos BPA-I (Boletim de Produção Ambulatorial Individualizado)
Este script valida arquivos no formato BPA-I, verificando se estão de acordo 
com o layout oficial e as regras de negócio.
"""

import os
import sys
import argparse
import re
import datetime
import math
from colorama import init, Fore, Style

# Inicializar colorama para saída colorida no terminal
init(autoreset=True)

class BPAValidator:
    def __init__(self):
        # Definição do layout do header
        self.header_layout = {
            'cbc_hdr_1': {'inicio': 1, 'fim': 2, 'tipo': 'NUM', 'valor': '01', 'obrigatorio': True},
            'cbc_hdr_2': {'inicio': 3, 'fim': 7, 'tipo': 'ALFA', 'valor': '#BPA#', 'obrigatorio': True},
            'cbc_mvm': {'inicio': 8, 'fim': 13, 'tipo': 'NUM', 'pattern': r'^\d{6}$', 'obrigatorio': True}, # Competência AAAAMM
            'cbc_lin': {'inicio': 14, 'fim': 19, 'tipo': 'NUM', 'pattern': r'^\d{6}$', 'obrigatorio': True}, # Qtd Linhas
            'cbc_flh': {'inicio': 20, 'fim': 25, 'tipo': 'NUM', 'pattern': r'^\d{6}$', 'obrigatorio': True}, # Qtd Folhas
            'cbc_smt_vrf': {'inicio': 26, 'fim': 29, 'tipo': 'NUM', 'pattern': r'^\d{4}$', 'obrigatorio': True}, # Sequencial de remessa
            'cbc_rsp': {'inicio': 30, 'fim': 59, 'tipo': 'ALFA', 'tamanho': 30, 'obrigatorio': True}, # Nome do Responsável
            'cbc_sgl': {'inicio': 60, 'fim': 65, 'tipo': 'ALFA', 'tamanho': 6, 'obrigatorio': True}, # Sigla do Órgão
            'cbc_cgccpf': {'inicio': 66, 'fim': 79, 'tipo': 'NUM', 'pattern': r'^\d{14}$', 'obrigatorio': True}, # CGC/CPF
            'cbc_dst': {'inicio': 80, 'fim': 119, 'tipo': 'ALFA', 'tamanho': 40, 'obrigatorio': True}, # Órgão Destino
            'cbc_dst_in': {'inicio': 120, 'fim': 120, 'tipo': 'ALFA', 'valores': ['M', 'E'], 'obrigatorio': True}, # Indicador Destino (M-Municipal, E-Estadual)
            'cbc_versao': {'inicio': 121, 'fim': 130, 'tipo': 'ALFA', 'tamanho': 10, 'obrigatorio': True} # Versão do Sistema
        }
        
        # Definição do layout do registro BPA-I (Individualizado)
        self.registro_bpa_i_layout = {
            'prd_ident': {'inicio': 1, 'fim': 2, 'tipo': 'NUM', 'valor': '03', 'obrigatorio': True},
            'prd_cnes': {'inicio': 3, 'fim': 9, 'tipo': 'NUM', 'pattern': r'^\d{7}$', 'obrigatorio': True},
            'prd_cmp': {'inicio': 10, 'fim': 15, 'tipo': 'NUM', 'pattern': r'^\d{6}$', 'obrigatorio': True}, # Competência AAAAMM
            'prd_cnsmed': {'inicio': 16, 'fim': 30, 'tipo': 'NUM', 'pattern': r'^\d{15}$', 'obrigatorio': True}, # CNS do Profissional
            'prd_cbo': {'inicio': 31, 'fim': 36, 'tipo': 'ALFA', 'tamanho': 6, 'pattern': r'^[A-Z0-9]{6}$', 'obrigatorio': True}, # CBO (pode ser alfanumérico)
            'prd_dtaten': {'inicio': 37, 'fim': 44, 'tipo': 'NUM', 'pattern': r'^\d{8}$', 'obrigatorio': True}, # Data Atendimento AAAAMMDD
            'prd_flh': {'inicio': 45, 'fim': 47, 'tipo': 'NUM', 'pattern': r'^\d{3}$', 'obrigatorio': True}, # Folha
            'prd_seq': {'inicio': 48, 'fim': 49, 'tipo': 'NUM', 'pattern': r'^\d{2}$', 'obrigatorio': True}, # Sequência na Folha
            'prd_pa': {'inicio': 50, 'fim': 59, 'tipo': 'NUM', 'pattern': r'^\d{10}$', 'obrigatorio': True}, # Procedimento Ambulatorial
            'prd_cnspac': {'inicio': 60, 'fim': 74, 'tipo': 'NUM', 'pattern': r'^\d{15}$', 'obrigatorio': False}, # CNS do Paciente
            'prd_sexo': {'inicio': 75, 'fim': 75, 'tipo': 'ALFA', 'valores': ['M', 'F', 'I'], 'obrigatorio': True}, # Sexo (M/F/I - Ignorado)
            'prd_ibge': {'inicio': 76, 'fim': 81, 'tipo': 'NUM', 'pattern': r'^\d{6}$', 'obrigatorio': True}, # Código IBGE Município Residência
            'prd_cid': {'inicio': 82, 'fim': 85, 'tipo': 'ALFA', 'tamanho': 4, 'pattern': r'^[A-Z0-9]{3,4}$', 'obrigatorio': True}, # CID (pode ser 3 ou 4 chars)
            'prd_ldade': {'inicio': 86, 'fim': 88, 'tipo': 'NUM', 'pattern': r'^\d{3}$', 'obrigatorio': True}, # Idade
            'prd_qt': {'inicio': 89, 'fim': 94, 'tipo': 'NUM', 'pattern': r'^\d{6}$', 'obrigatorio': True}, # Quantidade
            'prd_caten': {'inicio': 95, 'fim': 96, 'tipo': 'NUM', 'pattern': r'^\d{2}$', 'obrigatorio': False}, # Caráter Atendimento
            'prd_naut': {'inicio': 97, 'fim': 109, 'tipo': 'NUM', 'pattern': r'^\d{13}$', 'obrigatorio': False}, # Número Autorização
            'prd_org': {'inicio': 110, 'fim': 112, 'tipo': 'ALFA', 'valor': 'BPA', 'obrigatorio': True}, # Origem (BPA,BPI,RAAS,APAC) - aqui fixo BPA
            'prd_nmpac': {'inicio': 113, 'fim': 142, 'tipo': 'ALFA', 'tamanho': 30, 'obrigatorio': True}, # Nome Paciente
            'prd_dtnasc': {'inicio': 143, 'fim': 150, 'tipo': 'NUM', 'pattern': r'^\d{8}$', 'obrigatorio': True}, # Data Nascimento Paciente AAAAMMDD
            'prd_raca': {'inicio': 151, 'fim': 152, 'tipo': 'NUM', 'pattern': r'^\d{2}$', 'obrigatorio': True}, # Raça/Cor
            'prd_etnia': {'inicio': 153, 'fim': 156, 'tipo': 'NUM', 'pattern': r'^\d{4}$', 'obrigatorio': False}, # Etnia Indígena
            'prd_nac': {'inicio': 157, 'fim': 159, 'tipo': 'NUM', 'pattern': r'^\d{3}$', 'obrigatorio': False}, # Nacionalidade
            'prd_srv': {'inicio': 160, 'fim': 162, 'tipo': 'NUM', 'pattern': r'^\d{3}$', 'obrigatorio': False}, # Código Serviço
            'prd_clf': {'inicio': 163, 'fim': 165, 'tipo': 'NUM', 'pattern': r'^\d{3}$', 'obrigatorio': False}, # Código Classificação do Serviço
            'prd_equipe_Seq': {'inicio': 166, 'fim': 173, 'tipo': 'NUM', 'pattern': r'^\d{8}$', 'obrigatorio': False}, # Sequencial da Equipe (INE)
            'prd_equipe_Area': {'inicio': 174, 'fim': 177, 'tipo': 'NUM', 'pattern': r'^\d{4}$', 'obrigatorio': False}, # Área da Equipe
            'prd_cnpj': {'inicio': 178, 'fim': 191, 'tipo': 'NUM', 'pattern': r'^\d{14}$', 'obrigatorio': False}, # CNPJ do Estabelecimento (se terceirizado)
            'prd_cep_pcnte': {'inicio': 192, 'fim': 199, 'tipo': 'NUM', 'pattern': r'^\d{8}$', 'obrigatorio': False},
            'prd_lograd_pcnte': {'inicio': 200, 'fim': 202, 'tipo': 'NUM', 'pattern': r'^\d{3}$', 'obrigatorio': False},
            'prd_end_pcnte': {'inicio': 203, 'fim': 232, 'tipo': 'ALFA', 'tamanho': 30, 'obrigatorio': False},
            'prd_compl_pcnte': {'inicio': 233, 'fim': 242, 'tipo': 'ALFA', 'tamanho': 10, 'obrigatorio': False},
            'prd_num_pcnte': {'inicio': 243, 'fim': 247, 'tipo': 'ALFA', 'tamanho': 5, 'obrigatorio': False}, # Pode ser S/N
            'prd_bairro_pcnte': {'inicio': 248, 'fim': 277, 'tipo': 'ALFA', 'tamanho': 30, 'obrigatorio': False},
            'prd_ddtel_pcnte': {'inicio': 278, 'fim': 288, 'tipo': 'NUM', 'pattern': r'^\d{10,11}$', 'obrigatorio': False}, # 10 ou 11 digitos
            'prd_email_pcnte': {'inicio': 289, 'fim': 328, 'tipo': 'ALFA', 'tamanho': 40, 'obrigatorio': False},
            'prd_ine': {'inicio': 329, 'fim': 338, 'tipo': 'NUM', 'pattern': r'^\d{10}$', 'obrigatorio': True}, # INE do profissional que realizou o atendimento
            'prd_cpf_pcnte': {'inicio': 339, 'fim': 349, 'tipo': 'NUM', 'pattern': r'^\d{11}$', 'obrigatorio': False},
            'prd_situacao_rua': {'inicio': 350, 'fim': 350, 'tipo': 'ALFA', 'valores':, 'obrigatorio': False}
        }
        
        self.stats = {} 
        self._reset_stats()

    def _reset_stats(self):
        """Inicializa ou reseta as estatísticas de validação."""
        self.stats = {
            'total_registros_lidos': 0,
            'total_registros_bpa_i': 0,
            'registros_validos': 0,
            'registros_invalidos': 0,
            'erros':,
            'competencia': 'N/A',
            'num_linhas_declarado_hdr': 0,
            'num_folhas_declarado_hdr': 0
        }

    def _validar_campo(self, valor_campo_bruto, config, num_linha=None, nome_campo_log=None):
        """Valida um único campo com base na sua configuração. Retorna uma lista de erros."""
        erros_campo =
        obrigatorio = config.get('obrigatorio', False)
        tipo = config.get('tipo', 'ALFA')
        
        valor_campo_proc = valor_campo_bruto.rstrip() if tipo == 'ALFA' else valor_campo_bruto
        prefixo_erro = f"Linha {num_linha}, Campo {nome_campo_log}: " if num_linha and nome_campo_log else f"Campo {nome_campo_log or 'Desconhecido'}: "

        if obrigatorio and valor_campo_bruto.strip() == '':
            erros_campo.append(f"{prefixo_erro}Campo obrigatório está vazio.")
            return erros_campo

        if not obrigatorio and valor_campo_bruto.strip() == '':
            return erros_campo # Campo opcional e vazio, sem mais validações

        if 'valor' in config:
            if valor_campo_bruto!= config['valor']:
                erros_campo.append(f"{prefixo_erro}valor '{valor_campo_bruto}' não corresponde ao esperado '{config['valor']}'")
        elif 'valores' in config:
            val_comp = valor_campo_bruto.strip() if len(valor_campo_bruto.strip()) > 0 else valor_campo_bruto
            if val_comp not in config['valores']:
                erros_campo.append(f"{prefixo_erro}valor '{valor_campo_bruto}' (comparado como '{val_comp}') não está entre os permitidos {config['valores']}")
        elif 'pattern' in config:
            if not re.fullmatch(config['pattern'], valor_campo_proc):
                erros_campo.append(f"{prefixo_erro}valor '{valor_campo_bruto}' (processado como '{valor_campo_proc}') não corresponde ao padrão '{config['pattern']}'")
        elif 'tamanho' in config and tipo == 'ALFA':
            if len(valor_campo_proc) > config['tamanho']:
                erros_campo.append(f"{prefixo_erro}conteúdo '{valor_campo_proc}' excede o limite de {config['tamanho']} caracteres.")
        
        # Validações específicas de formato de data e competência
        if nome_campo_log and valor_campo_bruto.strip().isdigit():
            if nome_campo_log in ['prd_dtaten', 'prd_dtnasc', 'cbc_dtprod_ini', 'cbc_dtprod_fim']: # Adicionar campos de data do header se houver
                try:
                    datetime.datetime.strptime(valor_campo_bruto.strip(), '%Y%m%d')
                except ValueError:
                    erros_campo.append(f"{prefixo_erro}data '{valor_campo_bruto.strip()}' é inválida (formato AAAAMMDD).")
            elif nome_campo_log in ['cbc_mvm', 'prd_cmp']:
                try:
                    ano = int(valor_campo_bruto[0:4])
                    mes = int(valor_campo_bruto[4:6])
                    if not (1900 <= ano <= datetime.datetime.now().year + 5 and 1 <= mes <= 12):
                        raise ValueError("Ano ou mês fora do intervalo aceitável.")
                    datetime.datetime.strptime(valor_campo_bruto.strip(), '%Y%m')
                except (ValueError, IndexError):
                    erros_campo.append(f"{prefixo_erro}competência '{valor_campo_bruto.strip()}' é inválida (formato AAAAMM e data válida).")
        return erros_campo

    def validar_header(self, linha):
        """Valida a linha de cabeçalho. Retorna (True/False, lista_de_erros)."""
        erros =
        min_len_header = max(c['fim'] for c in self.header_layout.values())
        if len(linha) < min_len_header:
            erros.append(f"Tamanho da linha de cabeçalho ({len(linha)}) é menor que o esperado ({min_len_header} caracteres).")
            return False, erros
            
        for nome_campo, config in self.header_layout.items():
            inicio, fim = config['inicio'] - 1, config['fim']
            valor_campo_bruto = linha[inicio:fim] if len(linha) >= fim else ""
            if valor_campo_bruto == "" and config.get('obrigatorio'):
                 erros.append(f"Campo Cabeçalho {nome_campo}: Ausente ou truncado (linha curta demais).")
                 continue
            erros.extend(self._validar_campo(valor_campo_bruto, config, num_linha=1, nome_campo_log=nome_campo))
        return len(erros) == 0, erros

    def validar_registro_bpa_i(self, linha, num_linha):
        """Valida uma linha de registro BPA-I. Retorna (True/False, lista_de_erros)."""
        erros =
        min_len_registro = max(c['fim'] for c in self.registro_bpa_i_layout.values())

        if len(linha) < 2 or linha[0:2]!= '03': # Checagem básica do identificador
            erros.append(f"Linha {num_linha}: Identificador de registro inválido. Esperado '03', encontrado '{linha[0:2] if len(linha) >=2 else 'N/A'}'.")
            return False, erros # Erro fundamental

        if len(linha) < min_len_registro:
            erros.append(f"Linha {num_linha}: Tamanho da linha ({len(linha)}) é menor que o esperado ({min_len_registro} caracteres). Alguns campos podem estar ausentes ou truncados.")
            # Continua a validar os campos possíveis mesmo com linha curta, erros serão adicionados por _validar_campo

        for nome_campo, config in self.registro_bpa_i_layout.items():
            inicio, fim = config['inicio'] - 1, config['fim']
            
            if len(linha) < fim: # Linha curta demais para este campo
                if config.get('obrigatorio', False):
                    erros.append(f"Linha {num_linha}, Campo {nome_campo}: Ausente devido à linha ser curta (comprimento {len(linha)}, esperado até {fim}).")
                continue 

            valor_campo_bruto = linha[inicio:fim]
            erros.extend(self._validar_campo(valor_campo_bruto, config, num_linha=num_linha, nome_campo_log=nome_campo))
        return len(erros) == 0, erros

    def validar_arquivo(self, caminho_arquivo):
        """Valida o arquivo BPA-I completo, linha por linha."""
        self._reset_stats()
        print(f"\n{Fore.BLUE}Iniciando validação do arquivo: {caminho_arquivo}{Style.RESET_ALL}")

        try:
            with open(caminho_arquivo, 'r', encoding='latin-1') as f:
                for num_linha_atual, linha_raw in enumerate(f, 1):
                    self.stats['total_registros_lidos'] += 1
                    linha = linha_raw.rstrip('\r\n')

                    if num_linha_atual == 1: # Processar cabeçalho
                        if not linha:
                            msg = "Erro Crítico: Arquivo iniciado com linha de cabeçalho vazia."
                            print(f"{Fore.RED}{msg}{Style.RESET_ALL}")
                            self.stats['erros'].append(msg)
                            return False 

                        header_valido, erros_hdr = self.validar_header(linha)
                        if not header_valido:
                            print(f"{Fore.RED}Erros encontrados no Cabeçalho (Linha 1):{Style.RESET_ALL}")
                            for erro in erros_hdr: print(f"  - {erro}")
                            self.stats['erros'].extend([f"Cabeçalho (Linha 1): {e}" for e in erros_hdr])
                        
                        # Extrair informações do cabeçalho para estatísticas e consistência
                        if len(linha) >= 13: self.stats['competencia'] = linha[7:13]
                        if len(linha) >= 19 and linha[13:19].isdigit(): self.stats['num_linhas_declarado_hdr'] = int(linha[13:19])
                        if len(linha) >= 25 and linha[19:25].isdigit(): self.stats['num_folhas_declarado_hdr'] = int(linha[19:25])
                        continue 

                    # Validar registros BPA-I (linhas de dados)
                    if len(linha) >= 2 and linha[0:2] == '03':
                        self.stats['total_registros_bpa_i'] += 1
                        reg_valido, erros_reg = self.validar_registro_bpa_i(linha, num_linha_atual)
                        if reg_valido:
                            self.stats['registros_validos'] += 1
                        else:
                            self.stats['registros_invalidos'] += 1
                            self.stats['erros'].extend(erros_reg)
                            if len(erros_reg) > 3:
                                print(f"{Fore.YELLOW}Linha {num_linha_atual} (Registro BPA-I): {len(erros_reg)} erros (exibindo os 3 primeiros){Style.RESET_ALL}")
                                for erro in erros_reg[:3]: print(f"  - {erro}")
                            else:
                                print(f"{Fore.YELLOW}Linha {num_linha_atual} (Registro BPA-I): {len(erros_reg)} erros{Style.RESET_ALL}")
                                for erro in erros_reg: print(f"  - {erro}")
                    elif linha.strip() == "": # Linha em branco
                        print(f"{Fore.CYAN}Linha {num_linha_atual}: Linha em branco ignorada.{Style.RESET_ALL}")
                    elif linha: # Linha não vazia, mas não é '03' e não é cabeçalho
                        msg = f"Linha {num_linha_atual}: Tipo de registro desconhecido ou inválido (não inicia com '03'). Conteúdo: '{linha[:60]}...'"
                        print(f"{Fore.RED}{msg}{Style.RESET_ALL}")
                        self.stats['erros'].append(msg)
                        # Não incrementa registros_invalidos aqui, pois não é um BPA-I malformado, mas algo inesperado.
            
            if self.stats['total_registros_lidos'] == 0:
                msg = "Erro Crítico: Arquivo vazio (sem cabeçalho ou registros)."
                print(f"{Fore.RED}{msg}{Style.RESET_ALL}")
                self.stats['erros'].append(msg)
                return False

            # Verificações de consistência final
            if self.stats['total_registros_bpa_i']!= self.stats['num_linhas_declarado_hdr']:
                msg = (f"Consistência: Número de registros BPA-I encontrados ({self.stats['total_registros_bpa_i']}) "
                       f"difere do declarado no cabeçalho ({self.stats['num_linhas_declarado_hdr']}).")
                print(f"{Fore.RED}{msg}{Style.RESET_ALL}")
                self.stats['erros'].append(msg)
            
            if self.stats['num_linhas_declarado_hdr'] == 0:
                num_folhas_calc = 0 # Se 0 linhas declaradas, 0 folhas esperadas
            else:
                # Se houver linhas declaradas, mas nenhuma encontrada, ainda calcula baseado no que deveria ter.
                # A regra original era: math.ceil(total_registros_bpa_i / 99) if total_registros_bpa_i > 0 else 1
                # Ajustando: se 0 registros encontrados, mas >0 declarados, ainda pode ser 1 folha (vazia).
                # Se o BPA-I sempre tem pelo menos 1 folha se há registros declarados:
                registros_para_calculo_folhas = self.stats['total_registros_bpa_i']
                num_folhas_calc = math.ceil(registros_para_calculo_folhas / 99) if registros_para_calculo_folhas > 0 else 1
                if self.stats['num_linhas_declarado_hdr'] > 0 and registros_para_calculo_folhas == 0 : # Declarou linhas, mas não achou nenhuma
                    num_folhas_calc = 1 # Assume que a folha existe, mas está vazia (ou deveria ter registros)

            if num_folhas_calc!= self.stats['num_folhas_declarado_hdr']:
                msg = (f"Consistência: Número de folhas calculado ({num_folhas_calc}) "
                       f"difere do declarado no cabeçalho ({self.stats['num_folhas_declarado_hdr']}). "
                       f"(Cálculo baseado em {self.stats['total_registros_bpa_i']} registros encontrados / 99 por folha).")
                print(f"{Fore.RED}{msg}{Style.RESET_ALL}")
                self.stats['erros'].append(msg)

        except FileNotFoundError:
            msg = f"Erro Crítico: Arquivo '{caminho_arquivo}' não encontrado."
            print(f"{Fore.RED}{msg}{Style.RESET_ALL}")
            self.stats['erros'].append(msg)
            return False
        except Exception as e:
            msg = f"Erro Inesperado durante a validação: {str(e)}"
            print(f"{Fore.RED}{msg}{Style.RESET_ALL}")
            import traceback
            traceback.print_exc()
            self.stats['erros'].append(msg)
            return False
        finally:
            print(f"\n{Fore.GREEN}--- Resumo Final da Validação ---{Style.RESET_ALL}")
            print(f"Arquivo Processado: {caminho_arquivo}")
            print(f"Competência (do Header): {self.stats.get('competencia', 'N/A')}")
            print(f"Linhas Declaradas (Header): {self.stats.get('num_linhas_declarado_hdr', 'N/A')}")
            print(f"Folhas Declaradas (Header): {self.stats.get('num_folhas_declarado_hdr', 'N/A')}")
            print(f"Total de Linhas Lidas do Arquivo: {self.stats.get('total_registros_lidos', 'N/A')}")
            print(f"Total de Registros BPA-I Encontrados: {self.stats.get('total_registros_bpa_i', 'N/A')}")
            print(f"Registros BPA-I Válidos: {self.stats.get('registros_validos', 'N/A')}")
            print(f"Registros BPA-I Inválidos (ou com erros): {self.stats.get('registros_invalidos', 'N/A')}")
            print(f"Total de Erros Detalhados Acumulados: {len(self.stats.get('erros',))}")
            
            if not self.stats.get('erros'):
                print(f"\n{Fore.GREEN}SUCESSO: O arquivo parece estar em conformidade com o layout BPA-I.{Style.RESET_ALL}")
                return True
            else:
                print(f"\n{Fore.RED}FALHA: O arquivo contém erros. Verifique os detalhes acima e o relatório HTML (se gerado).{Style.RESET_ALL}")
                return False

    def gerar_relatorio_com_stats(self, nome_arquivo_original, caminho_saida):
        """Gera um relatório de validação em formato HTML usando os self.stats existentes."""
        try:
            if not self.stats or self.stats.get('total_registros_lidos', 0) == 0:
                 print(f"{Fore.YELLOW}Atenção: Estatísticas de validação não disponíveis ou arquivo não processado. Execute a validação primeiro para gerar um relatório completo.{Style.RESET_ALL}")
                 return False # Não gerar relatório se não houve validação
            
            agora = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            html_erros = ""
            if self.stats.get('erros'):
                lista_erros_html = "".join([f"<li>{erro}</li>" for erro in self.stats['erros']])
                html_erros = f"""
                <h2>Detalhes dos Erros Encontrados ({len(self.stats['erros'])})</h2>
                <ul class="errors-list">
                    {lista_erros_html}
                </ul>
                """
            
            status_classe = 'success' if not self.stats.get('erros') else 'error'
            status_mensagem = ('O arquivo está em conformidade com o layout BPA-I.' if not self.stats.get('erros') 
                               else 'O arquivo contém erros. Corrija-os e tente novamente.')

            html = f"""
            <!DOCTYPE html>
            <html lang="pt-br">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>Relatório de Validação BPA-I</title>
                <style>
                    body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 20px; line-height: 1.6; color: #333; }}
                   .container {{ max-width: 900px; margin: auto; background: #fff; padding: 20px; box-shadow: 0 0 15px rgba(0,0,0,0.1); border-radius: 8px; }}
                    h1 {{ color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; text-align: center; }}
                    h2 {{ color: #3498db; margin-top: 30px; border-bottom: 1px solid #eee; padding-bottom: 5px; }}
                   .summary-table {{ width: 100%; border-collapse: collapse; margin-bottom: 20px; }}
                   .summary-table td {{ padding: 10px; border: 1px solid #ddd; }}
                   .summary-table td:first-child {{ font-weight: bold; background-color: #f9f9f9; width: 40%; }}
                   .status-box {{ padding: 15px; margin-top: 20px; border-radius: 5px; text-align: center; font-size: 1.1em; font-weight: bold; }}
                   .success {{ background-color: #e8f5e9; color: #2e7d32; border: 1px solid #a5d6a7; }}
                   .error {{ background-color: #ffebee; color: #c62828; border: 1px solid #ef9a9a; }}
                   .errors-list {{ list-style-type: none; padding-left: 0; }}
                   .errors-list li {{ background-color: #fff9f9; border-left: 3px solid #e57373; padding: 8px; margin-bottom: 5px; font-size: 0.95em; }}
                    footer {{ text-align: center; margin-top: 30px; font-size: 0.9em; color: #777; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>Relatório de Validação BPA-I</h1>
                    <p><strong>Arquivo:</strong> {os.path.basename(nome_arquivo_original)}</p>
                    <p><strong>Data/Hora da Validação:</strong> {agora}</p>
                    
                    <h2>Resumo da Validação</h2>
                    <table class="summary-table">
                        <tr><td>Competência (do Header):</td><td>{self.stats.get('competencia', 'N/A')}</td></tr>
                        <tr><td>Linhas Declaradas (Header):</td><td>{self.stats.get('num_linhas_declarado_hdr', 'N/A')}</td></tr>
                        <tr><td>Folhas Declaradas (Header):</td><td>{self.stats.get('num_folhas_declarado_hdr', 'N/A')}</td></tr>
                        <tr><td>Total de Linhas Lidas do Arquivo:</td><td>{self.stats.get('total_registros_lidos', 'N/A')}</td></tr>
                        <tr><td>Total de Registros BPA-I Encontrados:</td><td>{self.stats.get('total_registros_bpa_i', 'N/A')}</td></tr>
                        <tr><td>Registros BPA-I Válidos:</td><td>{self.stats.get('registros_validos', 'N/A')}</td></tr>
                        <tr><td>Registros BPA-I Inválidos (ou com erros):</td><td>{self.stats.get('registros_invalidos', 'N/A')}</td></tr>
                    </table>
                    
                    <div class="status-box {status_classe}">{status_mensagem}</div>
                    {html_erros}
                    <footer>Relatório gerado por BPAValidator.</footer>
                </div>
            </body>
            </html>
            """
            
            with open(caminho_saida, 'w', encoding='utf-8') as f:
                f.write(html)
            print(f"\n{Fore.GREEN}Relatório HTML gerado com sucesso: {caminho_saida}{Style.RESET_ALL}")
            return True
        except Exception as e:
            print(f"{Fore.RED}Erro crítico ao gerar relatório HTML: {str(e)}{Style.RESET_ALL}")
            import traceback
            traceback.print_exc()
            return False

def main():
    parser = argparse.ArgumentParser(
        description='Validador de arquivos BPA-I (Boletim de Produção Ambulatorial Individualizado).',
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('arquivo', help='Caminho para o arquivo BPA-I a ser validado.')
    parser.add_argument(
        '-r', '--relatorio', 
        help='Gerar relatório HTML de validação.', 
        action='store_true'
    )
    parser.add_argument(
        '-o', '--output', 
        help='Caminho para o arquivo de saída do relatório HTML.\n'
             'Padrão: <nome_arquivo_original>_validacao.html no mesmo diretório do arquivo de entrada.'
    )
    
    args = parser.parse_args()
    
    if not os.path.isfile(args.arquivo):
        print(f"{Fore.RED}Erro: O arquivo especificado '{args.arquivo}' não existe ou não é um arquivo.{Style.RESET_ALL}")
        sys.exit(2) # Código de saída para erro de arquivo não encontrado
    
    validador = BPAValidator()
    resultado_validacao_ok = validador.validar_arquivo(args.arquivo)
    
    if args.relatorio:
        if args.output:
            output_path = args.output
        else:
            base, ext = os.path.splitext(args.arquivo)
            output_path = base + '_validacao.html'
        
        validador.gerar_relatorio_com_stats(args.arquivo, output_path)
    
    sys.exit(0 if resultado_validacao_ok else 1) # 0 para sucesso, 1 para erros de validação

if __name__ == "__main__":
    main()