#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Validador de arquivos BPA-I
Este script valida arquivos no formato BPA-I, verificando se estão de acordo com o layout oficial.
"""

import os
import sys
import argparse
import re
import datetime
from colorama import init, Fore, Style

# Inicializar colorama para saída colorida no terminal
init()

class BPAValidator:
    def __init__(self):
        # Definição do layout do header
        self.header_layout = {
            'cbc_hdr_1': {'inicio': 1, 'fim': 2, 'tipo': 'NUM', 'valor': '01', 'obrigatorio': True},
            'cbc_hdr_2': {'inicio': 3, 'fim': 7, 'tipo': 'ALFA', 'valor': '#BPA#', 'obrigatorio': True},
            'cbc_mvm': {'inicio': 8, 'fim': 13, 'tipo': 'NUM', 'pattern': r'^\d{6}$', 'obrigatorio': True},
            'cbc_lin': {'inicio': 14, 'fim': 19, 'tipo': 'NUM', 'pattern': r'^\d{6}$', 'obrigatorio': True},
            'cbc_flh': {'inicio': 20, 'fim': 25, 'tipo': 'NUM', 'pattern': r'^\d{6}$', 'obrigatorio': True},
            'cbc_smt_vrf': {'inicio': 26, 'fim': 29, 'tipo': 'NUM', 'pattern': r'^\d{4}$', 'obrigatorio': True},
            'cbc_rsp': {'inicio': 30, 'fim': 59, 'tipo': 'ALFA', 'tamanho': 30, 'obrigatorio': True},
            'cbc_sgl': {'inicio': 60, 'fim': 65, 'tipo': 'ALFA', 'tamanho': 6, 'obrigatorio': True},
            'cbc_cgccpf': {'inicio': 66, 'fim': 79, 'tipo': 'NUM', 'pattern': r'^\d{14}$', 'obrigatorio': True},
            'cbc_dst': {'inicio': 80, 'fim': 119, 'tipo': 'ALFA', 'tamanho': 40, 'obrigatorio': True},
            'cbc_dst_in': {'inicio': 120, 'fim': 120, 'tipo': 'ALFA', 'valores': ['M', 'E'], 'obrigatorio': True},
            'cbc_versao': {'inicio': 121, 'fim': 130, 'tipo': 'ALFA', 'tamanho': 10, 'obrigatorio': True}
        }
        
        # Definição do layout do registro BPA-I
        self.registro_bpa_i_layout = {
            'prd_ident': {'inicio': 1, 'fim': 2, 'tipo': 'NUM', 'valor': '03', 'obrigatorio': True},
            'prd_cnes': {'inicio': 3, 'fim': 9, 'tipo': 'NUM', 'pattern': r'^\d{7}$', 'obrigatorio': True},
            'prd_cmp': {'inicio': 10, 'fim': 15, 'tipo': 'NUM', 'pattern': r'^\d{6}', 'obrigatorio': True},
            'prd_cnsmed': {'inicio': 16, 'fim': 30, 'tipo': 'NUM', 'pattern': r'^\d{15}', 'obrigatorio': True},
            'prd_cbo': {'inicio': 31, 'fim': 36, 'tipo': 'ALFA', 'tamanho': 6, 'obrigatorio': True},
            'prd_dtaten': {'inicio': 37, 'fim': 44, 'tipo': 'NUM', 'pattern': r'^\d{8}', 'obrigatorio': True},
            'prd_flh': {'inicio': 45, 'fim': 47, 'tipo': 'NUM', 'pattern': r'^\d{3}', 'obrigatorio': True},
            'prd_seq': {'inicio': 48, 'fim': 49, 'tipo': 'NUM', 'pattern': r'^\d{2}', 'obrigatorio': True},
            'prd_pa': {'inicio': 50, 'fim': 59, 'tipo': 'NUM', 'pattern': r'^\d{10}', 'obrigatorio': True},
            'prd_cnspac': {'inicio': 60, 'fim': 74, 'tipo': 'NUM', 'pattern': r'^\d{15}', 'obrigatorio': False},
            'prd_sexo': {'inicio': 75, 'fim': 75, 'tipo': 'ALFA', 'valores': ['M', 'F'], 'obrigatorio': True},
            'prd_ibge': {'inicio': 76, 'fim': 81, 'tipo': 'NUM', 'pattern': r'^\d{6}', 'obrigatorio': True},
            'prd_cid': {'inicio': 82, 'fim': 85, 'tipo': 'ALFA', 'tamanho': 4, 'obrigatorio': True},
            'prd_ldade': {'inicio': 86, 'fim': 88, 'tipo': 'NUM', 'pattern': r'^\d{3}', 'obrigatorio': True},
            'prd_qt': {'inicio': 89, 'fim': 94, 'tipo': 'NUM', 'pattern': r'^\d{6}', 'obrigatorio': True},
            'prd_caten': {'inicio': 95, 'fim': 96, 'tipo': 'NUM', 'pattern': r'^\d{2}', 'obrigatorio': False},
            'prd_naut': {'inicio': 97, 'fim': 109, 'tipo': 'NUM', 'pattern': r'^\d{13}', 'obrigatorio': False},
            'prd_org': {'inicio': 110, 'fim': 112, 'tipo': 'ALFA', 'valor': 'BPA', 'obrigatorio': True},
            'prd_nmpac': {'inicio': 113, 'fim': 142, 'tipo': 'ALFA', 'tamanho': 30, 'obrigatorio': True},
            'prd_dtnasc': {'inicio': 143, 'fim': 150, 'tipo': 'NUM', 'pattern': r'^\d{8}', 'obrigatorio': True},
            'prd_raca': {'inicio': 151, 'fim': 152, 'tipo': 'NUM', 'pattern': r'^\d{2}', 'obrigatorio': True},
            'prd_etnia': {'inicio': 153, 'fim': 156, 'tipo': 'NUM', 'pattern': r'^\d{4}', 'obrigatorio': False},
            'prd_nac': {'inicio': 157, 'fim': 159, 'tipo': 'NUM', 'pattern': r'^\d{3}', 'obrigatorio': False},
            'prd_srv': {'inicio': 160, 'fim': 162, 'tipo': 'NUM', 'pattern': r'^\d{3}', 'obrigatorio': False},
            'prd_clf': {'inicio': 163, 'fim': 165, 'tipo': 'NUM', 'pattern': r'^\d{3}', 'obrigatorio': False},
            'prd_equipe_Seq': {'inicio': 166, 'fim': 173, 'tipo': 'NUM', 'pattern': r'^\d{8}', 'obrigatorio': False},
            'prd_equipe_Area': {'inicio': 174, 'fim': 177, 'tipo': 'NUM', 'pattern': r'^\d{4}', 'obrigatorio': False},
            'prd_cnpj': {'inicio': 178, 'fim': 191, 'tipo': 'NUM', 'pattern': r'^\d{14}', 'obrigatorio': False},
            'prd_cep_pcnte': {'inicio': 192, 'fim': 199, 'tipo': 'NUM', 'pattern': r'^\d{8}', 'obrigatorio': False},
            'prd_lograd_pcnte': {'inicio': 200, 'fim': 202, 'tipo': 'NUM', 'pattern': r'^\d{3}', 'obrigatorio': False},
            'prd_end_pcnte': {'inicio': 203, 'fim': 232, 'tipo': 'ALFA', 'tamanho': 30, 'obrigatorio': False},
            'prd_compl_pcnte': {'inicio': 233, 'fim': 242, 'tipo': 'ALFA', 'tamanho': 10, 'obrigatorio': False},
            'prd_num_pcnte': {'inicio': 243, 'fim': 247, 'tipo': 'ALFA', 'tamanho': 5, 'obrigatorio': False},
            'prd_bairro_pcnte': {'inicio': 248, 'fim': 277, 'tipo': 'ALFA', 'tamanho': 30, 'obrigatorio': False},
            'prd_ddtel_pcnte': {'inicio': 278, 'fim': 288, 'tipo': 'NUM', 'pattern': r'^\d{11}', 'obrigatorio': False},
            'prd_email_pcnte': {'inicio': 289, 'fim': 328, 'tipo': 'ALFA', 'tamanho': 40, 'obrigatorio': False},
            'prd_ine': {'inicio': 329, 'fim': 338, 'tipo': 'NUM', 'pattern': r'^\d{10}', 'obrigatorio': True},
            'prd_cpf_pcnte': {'inicio': 339, 'fim': 349, 'tipo': 'NUM', 'pattern': r'^\d{11}', 'obrigatorio': False},
            'prd_situacao_rua': {'inicio': 350, 'fim': 350, 'tipo': 'ALFA', 'valores': ['N', 'S'], 'obrigatorio': False}
        }
        
        # Estatísticas de validação
        self.stats = {
            'total_registros': 0,
            'registros_validos': 0,
            'registros_invalidos': 0,
            'erros': []
        }
    
    def validar_header(self, linha):
        """Valida o cabeçalho do arquivo BPA-I"""
        erros = []
        
        # Verificar tamanho mínimo da linha
        if len(linha) < 130:
            erros.append(f"Tamanho da linha de cabeçalho inválido: {len(linha)}, esperado 130 caracteres")
            return False, erros
            
        # Validar cada campo do cabeçalho
        for campo, config in self.header_layout.items():
            inicio = config['inicio'] - 1  # Ajustar para índice base 0
            fim = config['fim']
            valor_campo = linha[inicio:fim]
            
            # Verificar valor fixo
            if 'valor' in config:
                if valor_campo != config['valor']:
                    erros.append(f"Campo {campo}: valor '{valor_campo}' não corresponde ao esperado '{config['valor']}'")
            
            # Verificar valores permitidos
            elif 'valores' in config:
                if valor_campo not in config['valores']:
                    erros.append(f"Campo {campo}: valor '{valor_campo}' não está entre os valores permitidos {config['valores']}")
            
            # Verificar padrão regex
            elif 'pattern' in config:
                if not re.match(config['pattern'], valor_campo):
                    erros.append(f"Campo {campo}: valor '{valor_campo}' não corresponde ao padrão esperado")
            
            # Verificar tamanho específico
            elif 'tamanho' in config:
                if len(valor_campo.rstrip()) > config['tamanho']:
                    erros.append(f"Campo {campo}: tamanho do valor excede o limite de {config['tamanho']} caracteres")
        
        return len(erros) == 0, erros
    
    def validar_arquivo(self, caminho_arquivo):
        """Valida um arquivo BPA-I completo"""
        try:
            print(f"\n{Fore.BLUE}Validando arquivo: {caminho_arquivo}{Style.RESET_ALL}")
            
            # Resetar estatísticas
            self.stats = {
                'total_registros': 0,
                'registros_validos': 0,
                'registros_invalidos': 0,
                'erros': []
            }
            
            with open(caminho_arquivo, 'r', encoding='latin-1') as f:
                linhas = f.readlines()
                
            if not linhas:
                print(f"{Fore.RED}Erro: Arquivo vazio{Style.RESET_ALL}")
                return False
                
            # Validar cabeçalho (primeira linha)
            header_valido, erros_header = self.validar_header(linhas[0].rstrip('\r\n'))
            if not header_valido:
                print(f"{Fore.RED}Erros no cabeçalho:{Style.RESET_ALL}")
                for erro in erros_header:
                    print(f"  - {erro}")
                self.stats['erros'].extend(erros_header)
                
            # Extrair informações do cabeçalho
            competencia = linhas[0][7:13] if len(linhas[0]) > 13 else "??????"
            num_linhas_declarado = int(linhas[0][13:19]) if len(linhas[0]) > 19 and linhas[0][13:19].isdigit() else 0
            num_folhas_declarado = int(linhas[0][19:25]) if len(linhas[0]) > 25 and linhas[0][19:25].isdigit() else 0
            
            # Validar registros BPA-I
            total_registros_bpa_i = 0
            registros_validos = 0
            registros_invalidos = 0
            
            for i, linha in enumerate(linhas[1:], 1):
                linha = linha.rstrip('\r\n')
                
                # Verificar se é um registro BPA-I (começa com '03')
                if len(linha) >= 2 and linha[0:2] == '03':
                    total_registros_bpa_i += 1
                    registro_valido, erros_registro = self.validar_registro_bpa_i(linha, i+1)
                    
                    if registro_valido:
                        registros_validos += 1
                    else:
                        registros_invalidos += 1
                        self.stats['erros'].extend(erros_registro)
                        
                        # Limitar quantidade de erros exibidos
                        if len(erros_registro) > 3:
                            print(f"{Fore.YELLOW}Linha {i+1}: {len(erros_registro)} erros encontrados (exibindo os 3 primeiros){Style.RESET_ALL}")
                            for erro in erros_registro[:3]:
                                print(f"  - {erro}")
                        else:
                            print(f"{Fore.YELLOW}Linha {i+1}: {len(erros_registro)} erros encontrados{Style.RESET_ALL}")
                            for erro in erros_registro:
                                print(f"  - {erro}")
            
            # Atualizar estatísticas
            self.stats['total_registros'] = total_registros_bpa_i
            self.stats['registros_validos'] = registros_validos
            self.stats['registros_invalidos'] = registros_invalidos
            
            # Verificar se o número de registros está correto
            if total_registros_bpa_i != num_linhas_declarado:
                erro_msg = f"Número de registros BPA-I ({total_registros_bpa_i}) não corresponde ao declarado no cabeçalho ({num_linhas_declarado})"
                print(f"{Fore.RED}{erro_msg}{Style.RESET_ALL}")
                self.stats['erros'].append(erro_msg)
            
            # Verificar número de folhas
            num_folhas_calculado = (total_registros_bpa_i + 19) // 20  # 20 registros por folha, arredondando para cima
            if num_folhas_calculado != num_folhas_declarado:
                erro_msg = f"Número de folhas calculado ({num_folhas_calculado}) não corresponde ao declarado no cabeçalho ({num_folhas_declarado})"
                print(f"{Fore.RED}{erro_msg}{Style.RESET_ALL}")
                self.stats['erros'].append(erro_msg)
            
            # Exibir resumo
            print(f"\n{Fore.GREEN}Resumo da validação:{Style.RESET_ALL}")
            print(f"Arquivo: {caminho_arquivo}")
            print(f"Competência: {competencia}")
            print(f"Total de registros: {total_registros_bpa_i}")
            print(f"Registros válidos: {registros_validos}")
            print(f"Registros inválidos: {registros_invalidos}")
            print(f"Total de erros: {len(self.stats['erros'])}")
            
            if len(self.stats['erros']) == 0:
                print(f"\n{Fore.GREEN}O arquivo está em conformidade com o layout BPA-I.{Style.RESET_ALL}")
                return True
            else:
                print(f"\n{Fore.RED}O arquivo contém erros. Corrija-os e tente novamente.{Style.RESET_ALL}")
                return False
                
        except Exception as e:
            print(f"{Fore.RED}Erro ao validar arquivo: {str(e)}{Style.RESET_ALL}")
            import traceback
            traceback.print_exc()
            return False
    
    def gerar_relatorio(self, caminho_arquivo, caminho_saida):
        """Gera um relatório de validação em formato HTML"""
        try:
            # Validar o arquivo primeiro
            self.validar_arquivo(caminho_arquivo)
            
            # Criar relatório HTML
            agora = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            
            html = f"""
            <!DOCTYPE html>
            <html lang="pt-br">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>Relatório de Validação BPA-I</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; }}
                    h1 {{ color: #2c3e50; }}
                    h2 {{ color: #3498db; }}
                    .success {{ color: green; }}
                    .error {{ color: red; }}
                    .warning {{ color: orange; }}
                    table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
                    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                    th {{ background-color: #f2f2f2; }}
                    tr:nth-child(even) {{ background-color: #f9f9f9; }}
                </style>
            </head>
            <body>
                <h1>Relatório de Validação BPA-I</h1>
                <p><strong>Arquivo:</strong> {os.path.basename(caminho_arquivo)}</p>
                <p><strong>Data/Hora:</strong> {agora}</p>
                
                <h2>Resumo</h2>
                <p><strong>Total de registros:</strong> {self.stats['total_registros']}</p>
                <p><strong>Registros válidos:</strong> {self.stats['registros_validos']}</p>
                <p><strong>Registros inválidos:</strong> {self.stats['registros_invalidos']}</p>
                <p><strong>Total de erros:</strong> {len(self.stats['erros'])}</p>
                
                <h2>Status</h2>
                <p class="{('success' if len(self.stats['erros']) == 0 else 'error')}">
                    {('O arquivo está em conformidade com o layout BPA-I.' if len(self.stats['erros']) == 0 else 'O arquivo contém erros. Corrija-os e tente novamente.')}
                </p>
            """
            
            # Adicionar lista de erros, se houver
            if self.stats['erros']:
                html += """
                <h2>Erros Encontrados</h2>
                <table>
                    <tr>
                        <th>#</th>
                        <th>Descrição</th>
                    </tr>
                """
                
                for i, erro in enumerate(self.stats['erros'], 1):
                    html += f"""
                    <tr>
                        <td>{i}</td>
                        <td>{erro}</td>
                    </tr>
                    """
                
                html += "</table>"
            
            # Fechar tags HTML
            html += """
            </body>
            </html>
            """
            
            # Salvar relatório
            with open(caminho_saida, 'w', encoding='utf-8') as f:
                f.write(html)
                
            print(f"\n{Fore.GREEN}Relatório gerado com sucesso: {caminho_saida}{Style.RESET_ALL}")
            return True
            
        except Exception as e:
            print(f"{Fore.RED}Erro ao gerar relatório: {str(e)}{Style.RESET_ALL}")
            return False

def main():
    """Função principal"""
    # Configurar argumentos de linha de comando
    parser = argparse.ArgumentParser(description='Validador de arquivos BPA-I')
    parser.add_argument('arquivo', help='Caminho para o arquivo BPA-I a ser validado')
    parser.add_argument('-r', '--relatorio', help='Gerar relatório HTML de validação', action='store_true')
    parser.add_argument('-o', '--output', help='Caminho para o arquivo de saída do relatório')
    
    args = parser.parse_args()
    
    # Verificar se o arquivo existe
    if not os.path.isfile(args.arquivo):
        print(f"{Fore.RED}Erro: O arquivo {args.arquivo} não existe.{Style.RESET_ALL}")
        return 1
    
    # Criar validador
    validador = BPAValidator()
    
    # Validar arquivo
    resultado = validador.validar_arquivo(args.arquivo)
    
    # Gerar relatório, se solicitado
    if args.relatorio:
        output_path = args.output if args.output else os.path.splitext(args.arquivo)[0] + '_validacao.html'
        validador.gerar_relatorio(args.arquivo, output_path)
    
    return 0 if resultado else 1

if __name__ == "__main__":
    sys.exit(main())
    
    def validar_registro_bpa_i(self, linha, num_linha):
        """Valida um registro BPA-I"""
        erros = []
        
        # Verificar tamanho mínimo da linha
        if len(linha) < 350:
            erros.append(f"Linha {num_linha}: Tamanho inválido: {len(linha)}, esperado 350 caracteres")
            return False, erros
            
        # Validar cada campo do registro
        for campo, config in self.registro_bpa_i_layout.items():
            inicio = config['inicio'] - 1  # Ajustar para índice base 0
            fim = config['fim']
            
            # Garantir que a linha tenha comprimento suficiente
            if len(linha) < fim:
                erros.append(f"Linha {num_linha}: Tamanho insuficiente para o campo {campo}")
                continue
                
            valor_campo = linha[inicio:fim]
            
            # Verificar campo obrigatório
            if config['obrigatorio']:
                if valor_campo.strip() == '':
                    erros.append(f"Linha {num_linha}: Campo obrigatório {campo} está vazio")
                    continue
            
            # Se o campo não é obrigatório e está vazio, pular validações adicionais
            if not config['obrigatorio'] and valor_campo.strip() == '':
                continue
            
            # Verificar valor fixo
            if 'valor' in config:
                if valor_campo != config['valor']:
                    erros.append(f"Linha {num_linha}, Campo {campo}: valor '{valor_campo}' não corresponde ao esperado '{config['valor']}'")
            
            # Verificar valores permitidos
            elif 'valores' in config:
                if valor_campo not in config['valores']:
                    erros.append(f"Linha {num_linha}, Campo {campo}: valor '{valor_campo}' não está entre os valores permitidos {config['valores']}")
            
            # Verificar padrão regex
            elif 'pattern' in config:
                if not re.match(config['pattern'], valor_campo.strip()):
                    erros.append(f"Linha {num_linha}, Campo {campo}: valor '{valor_campo}' não corresponde ao padrão esperado")
            
            # Verificar tamanho específico
            elif 'tamanho' in config:
                if len(valor_campo.rstrip()) > config['tamanho']:
                    erros.append(f"Linha {num_linha}, Campo {campo}: tamanho do valor excede o limite de {config['tamanho']} caracteres")
        
        return len(erros) == 0, erros