
# Importando Bibliotecas Necessárias
from sqlalchemy import create_engine
import pandas as pd
import os
import zipfile
import time
import io


# Criando função para download de arquivos
def download_file(url: str, dest_file: str):
    import requests
    req = requests.get(url)
    file = open(dest_file, 'wb')
    for chunk in req.iter_content(100000):
        file.write(chunk)
    file.close()
    return True


# Criando engine para do postgresql (utilizando o schema cnpj)
engine = create_engine(
    'postgresql://user:pass@ip-server:port/postgres?options=-csearch_path%3Dcnpj')


# Definindo Variáveis
url = 'http://200.152.38.155/CNPJ/'
directory = 'C:/Users/neymo/Downloads/dados_receita/'


# Baixando dados da Empresa
files = [f'K3241.K03200Y{i}.D10814.EMPRECSV.zip' for i in range(10)]
for i in files:
    download_file(url + i, directory + i)


# Baixando dados do Estabelecimento
files = [f'K3241.K03200Y{i}.D10814.ESTABELE.zip' for i in range(10)]
for i in files:
    download_file(url + i, directory + i)


# Baixando dados dos Socios
files = [f'K3241.K03200Y{i}.D10814.SOCIOCSV.zip' for i in range(10)]
for i in files:
    download_file(url + i, directory + i)


# Baixando Demais arquivos Auxiliares
files = ['F.K03200$Z.D10814.QUALSCSV.zip', 'F.K03200$Z.D10814.MOTICSV.zip', 'F.K03200$Z.D10814.CNAECSV.zip', 'F.K03200$W.SIMPLES.CSV.D10814.zip', 'F.K03200$Z.D10814.MUNICCSV.zip',
         'F.K03200$Z.D10814.NATJUCSV.zip', 'F.K03200$Z.D10814.PAISCSV.zip']
for i in files:
    download_file(url + i, directory + i)


# Definindo Layout das bases para carga no Banco de dados
layout_files = {'EMPRE': {'columns':
                          {'st_cnpj_base': str, 'st_razao_social': str, 'cd_natureza_juridica': str, 'cd_qualificacao': str,
                              'vl_capital_social': str, 'cd_porte_empresa': str, 'st_ente_federativo': str},
                          'table_name_db': 'tb_empresa'},
                'ESTABELE': {'columns':
                             {'st_cnpj_base': str, 'st_cnpj_ordem': str, 'st_cnpj_dv': str, 'cd_matriz_filial': str, 'st_nome_fantasia': str, 'cd_situacao_cadastral': str,
                              'dt_situacao_cadastral': str, 'cd_motivo_situacao_cadastral': str, 'st_cidade_exterior': str, 'cd_pais': str, 'dt_inicio_atividade': str,
                              'cd_cnae_principal': str, 'cd_cnae_secundario': str, 'st_tipo_logradouro': str, 'st_logradouro': str, 'st_numero': str, 'st_complemento': str,
                              'st_bairro': str, 'st_cep': str, 'st_uf': str, 'cd_municipio': str, 'st_ddd1': str, 'st_telefone1': str, 'st_ddd2': str, 'st_telefone2': str,
                              'st_ddd_fax': str, 'st_fax': str, 'st_email': str, 'st_situacao_especial': str, 'dt_situacao_especial': str
                              }, 'table_name_db': 'tb_estabelecimento'},
                'SIMPLES': {'columns':
                            {'st_cnpj_base': str, 'st_opcao_simples': str, 'dt_opcao_simples': str, 'dt_exclusao_simples': str,
                             'st_opcao_mei': str, 'dt_opcao_mei': str, 'dt_exclusao_mei': str
                             }, 'table_name_db': 'tb_dados_simples'},
                'SOCIO': {'columns':
                           {'st_cnpj_base': str, 'cd_tipo': str, 'st_nome': str, 'st_cpf_cnpj': str, 'cd_qualificacao': str, 'dt_entrada': str,
                            'cd_pais': str, 'st_representante': str, 'st_nome_representante': str, 'cd_qualificacao_representante': str, 'cd_faixa_etaria': str},
                          'table_name_db': 'tb_socio'},
                'PAIS': {'columns': {'cd_pais': str, 'st_pais': str}, 'table_name_db': 'tb_pais'},
                'MUNIC': {'columns': {'cd_municipio': str, 'st_municipio': str}, 'table_name_db': 'tb_municipio'},
                'QUALS': {'columns': {'cd_qualificacao': str, 'st_qualificacao': str}, 'table_name_db': 'tb_qualificacao_socio'},
                'NATJU': {'columns': {'cd_natureza_juridica': str, 'st_natureza_juridica': str}, 'table_name_db': 'tb_natureza_juridica'},
                'MOTI': {'columns': {'cd_motivo_situacao_cadastral': str, 'st_motivo_situacao_cadastral': str}, 'table_name_db': 'tb_motivo_situacao_cadastral'},
                'CNAE': {'columns': {'cd_cnae': str, 'st_cnae': str}, 'table_name_db': 'tb_cnae'}
                }


# Listar arquivos do diretório
files = os.listdir(directory)
uploaded = []


for file in files:
    # Verificando arquivos já carregados
    if file in uploaded:
        continue


    temp_file = io.BytesIO()


    # Selecionando Base para captura de Layout e nome do Arquivo
    model = file.replace('.zip', '').split('.')[-1].replace('CSV', '') if file.find('SIMPLES') < 0 else 'SIMPLES'


    # Descompactando Arquivo Zip na Memória
    with zipfile.ZipFile(directory + file, 'r') as zip_ref:
        temp_file.write(zip_ref.read(zip_ref.namelist()[0]))


    # Após Gravação, voltando indicador do arquivo para o Inicio
    temp_file.seek(0)


    # Fazendo leitura do CSV em partes
    for chunk in pd.read_csv(temp_file, delimiter=';', header=None, chunksize=65000, names=list(layout_files[model]['columns'].keys()), iterator=True, dtype=str, encoding="ISO-8859-1"):
        # Formatando Colunas de Datas
        for i in chunk.columns[chunk.columns.str.contains('dt_')]:
            chunk.loc[chunk[i] == '00000000', i] = None
            chunk.loc[chunk[i] == '0', i] = None
            chunk[i] = pd.to_datetime(
                chunk[i], format='%Y%m%d', errors='coerce')


        # Usando Try para tentativas de conexão, caso perca a conexão espera 60 segundos para tentar novamente o envio
        try:
            chunk.to_sql(layout_files[model]['table_name_db'],
                         engine, if_exists="append", index=False)
        except:
            time.sleep(60)
            chunk.to_sql(layout_files[model]['table_name_db'],
                         engine, if_exists="append", index=False)


    # Armazenando nome dos arquivos processados
    uploaded.append(file)