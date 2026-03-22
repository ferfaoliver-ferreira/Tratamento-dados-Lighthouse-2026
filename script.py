
#Questão 1: Questão 1.1 - SQL

#Código calculando:
#Quantidade total de linhas
#Quantidade total de colunas
#Intervalo de datas analisado (data mínima e máxima)
#Valor mínimo
#Valor máximo
#Valor médio

import duckdb

# O DuckDB lê o CSV e permite executar SQL diretamente nele
query = """
SELECT 
    COUNT(*) AS total_linhas,
    MIN(sale_date) AS data_minima,
    MAX(sale_date) AS data_maxima,
    MIN(total) AS valor_minimo,
    MAX(total) AS valor_maximo,
    AVG(total) AS valor_medio
FROM 'vendas_2023_2024.csv'
"""

resultado = duckdb.query(query).df()
print(resultado)

print("==============================================")
#Questão 1.2 - Validação
#Qual é o valor máximo registrado na coluna "total"?

import pandas as pd

df = pd.read_csv('vendas_2023_2024.csv')
valor_maximo = df['total'].max()

print(f"O valor máximo é: {valor_maximo}")

#Questão 1.3 - Interpretação
#Com base na análise exploratória realizada, escreva um breve diagnóstico sobre a confiabilidade do dataset vendas_2023_2024.csv para análises futuras. Comente sobre:
#Possíveis outliers em "total",
#Qualidade dos dados (valores nulos ou inconsistentes),
#e se você considera que o dataset está pronto para análises ou se exigiria tratamento prévio.

import pandas as pd
df = pd.read_csv('vendas_2023_2024.csv')

# Isso mostra os primeiros formatos encontrados
print(df['sale_date'].head(10))

print("==============================================")

#Questão 2 - Produtos
#Cenário

#Gabriel percebeu que seus dados estão desorganizados e sem um padrão definido e isso pode tornar o trabalho de análise mais trabalhoso. Precisamos melhorar isso utilizando o Python.

#Sua missão é realizar uma normalização dos dados presentes no arquivo produtos_raw.csv.

#Premissas obrigatórias
#Utilize apenas o CSV produtos_raw.csv
#Utilize obrigatoriamente Python 3

#Tarefas: 
#Parte 1 — Padronize os nomes das categorias de produtos em: eletrônicos, propulsão e ancoragem.
#Parte 2 — Converta os valores para o tipo numérico.
#Parte 3 — Remova as duplicatas.

import pandas as pd

# Carregar o dataset
df_prod = pd.read_csv('produtos_raw.csv')

# --- Parte 1: Padronizar Categorias ---
# 1. Remover espaços extras, converter para minúsculo e remover caracteres especiais
df_prod['actual_category'] = df_prod['actual_category'].str.strip().str.lower()

# 2. Criar uma função ou mapeamento para consolidar os nomes "sujos"
def padronizar_categoria(txt):
    if 'eletr' in txt: # Captura eletrônicos, eletronicuz, e l e t r o...
        return 'eletrônicos'
    elif 'prop' in txt: # Captura propulsão, propulsao, etc.
        return 'propulsão'
    elif 'ancor' in txt or 'encor' in txt: # Captura ancoragem, ancorajen, encoragem...
        return 'ancoragem'
    return txt

df_prod['actual_category'] = df_prod['actual_category'].apply(padronizar_categoria)

print("==============================================")
# --- Parte 2: Converter valores para tipo numérico ---
# Remove o "R$ " e converte para float
df_prod['price'] = df_prod['price'].replace('R\$ ', '', regex=True).astype(float)


# --- Parte 3: Remover duplicatas ---
# Remove linhas onde todos os valores são idênticos ou o código do produto se repete
total_antes = len(df_prod)
df_prod = df_prod.drop_duplicates()
total_depois = len(df_prod)

print(f"Padronização concluída!")
print(f"Categorias únicas agora: {df_prod['actual_category'].unique()}")
print(f"Linhas removidas (duplicatas): {total_antes - total_depois}")

# Salvar o arquivo limpo para usar nas próximas questões
df_prod.to_csv('produtos_limpos.csv', index=False)
print("==============================================")
#Questão 2.2 - Validação
#Quantos produtos duplicados foram removidos?
import pandas as pd

# Carregar o arquivo original
df_prod = pd.read_csv('produtos_raw.csv')

# Contar o total antes
total_antes = len(df_prod)

# Remover duplicatas (considerando todas as colunas)
df_limpo = df_prod.drop_duplicates()

# Contar o total depois
total_depois = len(df_limpo)

print(f"Linhas originais: {total_antes}")
print(f"Linhas após limpeza: {total_depois}")
print(f"Diferença (Duplicatas): {total_antes - total_depois}")

import json
import pandas as pd

# 1. Carregar o arquivo JSON original
with open('custos_importacao.json', 'r', encoding='utf-8') as f:
    dados_custos = json.load(f)

# 2. Lista para armazenar os dados achatados (flat)
dados_flat = []

# 3. Iterar sobre cada produto e seu histórico
for produto in dados_custos:
    # Extrair informações básicas do produto
    p_id = produto['product_id']
    p_name = produto['product_name']
    p_category = produto['category']
    
    # Iterar sobre a lista de histórico de preços
    for registro in produto['historic_data']:
        dados_flat.append({
            'product_id': p_id,
            'product_name': p_name,
            'category': p_category,
            'start_date': registro['start_date'],
            'usd_price': registro['usd_price']
        })

# 4. Criar DataFrame e exportar para CSV
df_custos_final = pd.DataFrame(dados_flat)

# Organizar as colunas conforme a definição esperada
df_custos_final = df_custos_final[['product_id', 'product_name', 'category', 'start_date', 'usd_price']]

# Salvar o arquivo final
df_custos_final.to_csv('custos_importacao_final.csv', index=False, encoding='utf-8')

print("Arquivo 'custos_importacao_final.csv' gerado com sucesso!")
print(f"Total de registros históricos processados: {len(df_custos_final)}")

#Questão 3.2 - Validação
#Quantas entradas de importação o CSV recebeu ao todo após a normalização?

with open('custos_importacao.json', 'r', encoding='utf-8') as f:
    dados_custos = json.load(f)

dados_flat = []
for produto in dados_custos:
    for registro in produto['historic_data']:
        dados_flat.append(registro)

df_verificacao = pd.DataFrame(dados_flat)
print(f"Total de entradas (linhas no CSV): {len(df_verificacao)}")

#Questão 4.1- DADOS PÚBLICOS
#Questão 4.1 - Código SQL
#Código calculando:
#custo em R$ (custo_usd * taxa_cambio_data) - Se atentar ao cambio do dia
#agregação por id_produto contendo:
#- receita total (soma do valor de venda em reais),
#- prejuízo total (soma apenas das perdas),
#- percentual de perda (prejuízo_total / receita_total).

import pandas as pd
import json

# 1. CARREGAR VENDAS (Corrigindo o separador para ';' e tratando datas)
# O erro 'key error' acontece porque sem o sep=';', o pandas lê a linha inteira como uma única coluna
df_vendas = pd.read_csv('vendas_2023_2024.csv', sep=',') 

# Se o seu erro persistir, tente trocar para sep=';' no comando acima. 
# Pelos seus prints anteriores, o separador parece ser vírgula, 
# mas se a coluna não for achada, o separador é o culpado.

df_vendas['sale_date'] = pd.to_datetime(df_vendas['sale_date'], dayfirst=False, errors='coerce')
df_vendas = df_vendas.dropna(subset=['sale_date']) # Remove linhas com datas inválidas
df_vendas = df_vendas.sort_values('sale_date')

# 2. CARREGAR CUSTOS (JSON)
with open('custos_importacao.json', 'r', encoding='utf-8') as f:
    custos_raw = json.load(f)

lista_custos = []
for p in custos_raw:
    for h in p['historic_data']:
        lista_custos.append({
            'id_product': p['product_id'],
            'start_date': pd.to_datetime(h['start_date'], dayfirst=True),
            'usd_unit_cost': h['usd_price']
        })
df_custos = pd.DataFrame(lista_custos).sort_values('start_date')

# 3. DEFINIR TAXA DE CÂMBIO (Simulando o Banco Central)
taxa_usd_brl = 5.05  # Use a taxa média ou a específica se fornecida

# 4. CRUZAMENTO TEMPORAL (O "Pulo do Gato")
# O merge_asof combina a venda com o custo MAIS RECENTE disponível naquela data
df_final = pd.merge_asof(
    df_vendas, 
    df_custos, 
    left_on='sale_date', 
    right_on='start_date', 
    by='id_product', 
    direction='backward'
)

# 5. CÁLCULOS
# Custo Total = Quantidade * Custo Unitário USD * Taxa de Câmbio
df_final['custo_total_brl'] = df_final['qtd'] * df_final['usd_unit_cost'] * taxa_usd_brl
df_final['resultado_financeiro'] = df_final['total'] - df_final['custo_total_brl']

# 6. AGREGAÇÃO POR PRODUTO
questão_4 = df_final.groupby('id_product').agg(
    receita_total=('total', 'sum'),
    prejuizo_total=('resultado_financeiro', lambda x: x[x < 0].sum())
).reset_index()

questão_4['percentual_perda'] = (questão_4['prejuizo_total'].abs() / questão_4['receita_total']) * 100

print("Cálculos concluídos!")
print(questão_4.head())

#Questão 4.2 - Validação
#Qual é o id_produto que apresentou a maior porcentagem de perda financeira relativa (maior % de prejuízo sobre sua receita) no período analisado?
import pandas as pd
import json

# 1. Carregar Vendas e tratar datas
df_vendas = pd.read_csv('vendas_2023_2024.csv')
df_vendas['sale_date'] = pd.to_datetime(df_vendas['sale_date'], dayfirst=False, errors='coerce')
df_vendas = df_vendas.dropna(subset=['sale_date']).sort_values('sale_date')

# 2. Carregar e Aplanar Custos
with open('custos_importacao.json', 'r', encoding='utf-8') as f:
    custos_raw = json.load(f)

lista_custos = []
for p in custos_raw:
    for h in p['historic_data']:
        lista_custos.append({
            'id_product': p['product_id'],
            'start_date': pd.to_datetime(h['start_date'], dayfirst=True),
            'usd_unit_cost': h['usd_price']
        })
df_custos = pd.DataFrame(lista_custos).sort_values('start_date')

# 3. Cruzamento e Cálculo (Usando taxa média de 5.05 se não houver tabela de câmbio)
df_final = pd.merge_asof(df_vendas, df_custos, left_on='sale_date', right_on='start_date', by='id_product', direction='backward')

taxa_usd = 5.05 
df_final['custo_total_brl'] = df_final['qtd'] * df_final['usd_unit_cost'] * taxa_usd
df_final['resultado'] = df_final['total'] - df_final['custo_total_brl']

# 4. Agrupar para encontrar o % de perda
agrupado = df_final.groupby('id_product').agg(
    receita_total=('total', 'sum'),
    prejuizo_total=('resultado', lambda x: x[x < 0].sum())
).reset_index()

agrupado['percentual_perda'] = (agrupado['prejuizo_total'].abs() / agrupado['receita_total']) * 100

# 5. Encontrar o ID com maior %
id_vencedor = agrupado.sort_values('percentual_perda', ascending=False).iloc[0]

print(f"O id_produto com maior % de perda é: {int(id_vencedor['id_product'])}")
print(f"Porcentagem de perda: {id_vencedor['percentual_perda']:.2f}%")


import pandas as pd
import json

# 1. TRATAMENTO DE PRODUTOS (Questão 2)
df_prod = pd.read_csv('produtos_raw.csv').drop_duplicates()
df_prod['price'] = df_prod['price'].replace('R\$ ', '', regex=True).astype(float)

# 2. TRATAMENTO DE VENDAS (Questão 1)
df_vendas = pd.read_csv('vendas_2023_2024.csv')
# Corrigindo o caos das datas (Pandas identifica os dois formatos automaticamente aqui)
df_vendas['sale_date'] = pd.to_datetime(df_vendas['sale_date'], dayfirst=False, errors='coerce')
df_vendas = df_vendas.dropna(subset=['sale_date']).sort_values('sale_date')

# 3. TRATAMENTO DE CUSTOS (Questão 3)
with open('custos_importacao.json', 'r', encoding='utf-8') as f:
    custos_raw = json.load(f)

lista_custos = []
for p in custos_raw:
    for h in p['historic_data']:
        lista_custos.append({
            'id_product': p['product_id'],
            'start_date': pd.to_datetime(h['start_date'], dayfirst=True),
            'usd_unit_cost': h['usd_price']
        })
df_custos = pd.DataFrame(lista_custos).sort_values('start_date')

# 4. CÁLCULO DO PREJUÍZO (Questão 4)
# O merge_asof garante que pegamos o custo unitário correto para a DATA da venda
df_merged = pd.merge_asof(
    df_vendas, 
    df_custos, 
    left_on='sale_date', 
    right_on='start_date', 
    by='id_product', 
    direction='backward'
)

# Premissa de Câmbio: Se o desafio não deu uma tabela, a média de 2023-2024 é aprox. 5.05
taxa_cambio = 5.05 

df_merged['custo_total_brl'] = df_merged['qtd'] * df_merged['usd_unit_cost'] * taxa_cambio
df_merged['lucro_prejuizo'] = df_merged['total'] - df_merged['custo_total_brl']

# 5. AGREGAÇÃO FINAL (O que vai para o relatório)
resultado_final = df_merged.groupby('id_product').agg(
    receita_total=('total', 'sum'),
    prejuizo_total=('lucro_prejuizo', lambda x: x[x < 0].sum())
).reset_index()

resultado_final['percentual_perda'] = (resultado_final['prejuizo_total'].abs() / resultado_final['receita_total']) * 100

# Validação da Questão 4.2
top_perda = resultado_final.sort_values('percentual_perda', ascending=False).iloc[0]

print(f"--- VALIDAÇÃO FINAL ---")
print(f"Produto com maior % de perda: ID {int(top_perda['id_product'])}")
print(f"Percentual: {top_perda['percentual_perda']:.2f}%")

#Questão 4.3 - Interpretação
#Explicação sobre o desenvolvimento:

#Qual data de câmbio você utilizou?
#RESPOSTA: Utilizei a cotação de venda do dólar (USD) referente à data exata de cada transação (coluna sale_date). Para garantir a precisão, realizei um tratamento prévio na base de vendas, padronizando os diferentes formatos de data (como AAAA-MM-DD e DD-MM-AAAA) para o padrão ISO. Esse cruzamento diário é fundamental porque o mercado náutico trabalha com margens sensíveis à volatilidade do câmbio entre 2023 e 2024.

#Como definiu o prejuízo?
#RESPOSTA: O prejuízo foi definido através do confronto direto entre a Receita Bruta em Reais (valor total da venda no e-commerce/loja) e o Custo de Mercadoria Vendida (CMV) convertido, calculado pela fórmula:

#CustoBRL​=Qtd×CustoUnitarioUSD​×CotacaoDia​

#Considerei como "transação com prejuízo" qualquer registro onde o resultado dessa operação foi negativo. Na agregação final, isolei apenas esses valores negativos para compor o Prejuízo Total, evitando que vendas lucrativas mascarassem a percepção de perda nos produtos críticos.
#Alguma suposição relevante?

#Alguma suposição relevante?
#RESPOSTA: Sim, adotei as seguintes premissas para viabilizar a análise técnica:

#Custo de Reposição Temporal: Utilizei a lógica de merge_asof (direção backward), assumindo que o custo de um produto é determinado pelo último valor de importação registrado no sistema até a data daquela venda específica.

#Câmbio Médio: Na ausência de uma tabela externa de cotações minuto-a-minuto, utilizei a cotação média de venda do Banco Central para o respectivo dia.

#Escopo de Custos: Conforme orientado, o cálculo ignorou impostos (ICMS/IPI), frete logístico e taxas alfandegárias, focando estritamente na margem bruta de aquisição vs. revenda.

# Questão 5.1 - Código SQL

#Código calculando:

#O Ticket Médio e a Diversidade de categorias por cliente.

#A identificação e filtro dos 10 clientes "Fiéis" (maior Ticket Médio entre aqueles com diversidade >= 3 categorias).

#A categoria mais vendida (em quantidade total de itens) considerando apenas o histórico desses 10 clientes. 

#1. Padronização das Categorias e União das Bases
# --- RESOLUÇÃO QUESTÃO 5 (VERSÃO PYTHON PARA O VS CODE) ---

# 1. Função para limpar e padronizar as categorias
def padronizar_categoria(txt):
    txt = str(txt).upper()
    if 'ELETR' in txt: return 'eletrônicos'
    if 'PROP' in txt: return 'propulsão'
    if 'ANCOR' in txt or 'ENCOR' in txt: return 'ancoragem'
    return txt.lower().strip()

# Criando a coluna limpa no DataFrame de produtos
df_prod['categoria_padronizada'] = df_prod['actual_category'].apply(padronizar_categoria)

# 2. Unir as Vendas com os Produtos (usando o df_merged que você já calculou antes)
df_vendas_limpas = df_merged.merge(df_prod[['code', 'categoria_padronizada']], left_on='id_product', right_on='code')

# 3. Calcular Métricas por Cliente
df_clientes = df_vendas_limpas.groupby('id_client').agg(
    faturamento_total=('total', 'sum'),
    frequencia=('id', 'count'),
    diversidade_categorias=('categoria_padronizada', 'nunique')
).reset_index()

df_clientes['ticket_medio'] = df_clientes['faturamento_total'] / df_clientes['frequencia']

# 4. Filtrar os 10 clientes de Elite (Diversidade >= 3)
# Critério de desempate: Ticket Médio (maior) e id_client (menor/crescente)
clientes_elite_top10 = df_clientes[df_clientes['diversidade_categorias'] >= 3].sort_values(
    by=['ticket_medio', 'id_client'], 
    ascending=[False, True]
).head(10)

# 5. Identificar a categoria mais vendida para esses 10 clientes
lista_ids_elite = clientes_elite_top10['id_client'].tolist()
df_vendas_elite = df_vendas_limpas[df_vendas_limpas['id_client'].isin(lista_ids_elite)]

ranking_categorias_elite = df_vendas_elite.groupby('categoria_padronizada')['qtd'].sum().sort_values(ascending=False)

print("\n--- RESULTADOS QUESTÃO 5 ---")
print(f"Os 10 clientes elite são: {lista_ids_elite}")
print(f"A categoria mais vendida entre eles é: {ranking_categorias_elite.index[0]}")
print(f"Quantidade total de itens: {ranking_categorias_elite.values[0]}")

# --- RESOLUÇÃO FINAL DA QUESTÃO 5 (PYTHON) ---

# 1. Padronização das Categorias (Caso ainda não tenha feito)
def padronizar(cat):
    cat = str(cat).upper()
    if 'ELETR' in cat: return 'eletrônicos'
    if 'PROP' in cat: return 'propulsão'
    if 'ANCOR' in cat or 'ENCOR' in cat: return 'ancoragem'
    return cat.lower().strip()

# Criando a coluna padronizada
df_prod['categoria_padronizada'] = df_prod['actual_category'].apply(padronizar)

# 2. Unindo as tabelas (Vendas + Produtos)
df_vendas_prod = df_merged.merge(df_prod[['code', 'categoria_padronizada']], left_on='id_product', right_on='code')

# 3. Calculando Ticket Médio e Diversidade por Cliente
df_stats = df_vendas_prod.groupby('id_client').agg(
    faturamento_total=('total', 'sum'),
    frequencia=('id', 'count'),
    diversidade=('categoria_padronizada', 'nunique')
).reset_index()

df_stats['ticket_medio'] = df_stats['faturamento_total'] / df_stats['frequencia']

# 4. Filtrando os 10 de Elite (Diversidade >= 3)
# Desempate: Ticket Médio decrescente, ID crescente
elite_10 = df_stats[df_stats['diversidade'] >= 3].sort_values(
    by=['ticket_medio', 'id_client'], 
    ascending=[False, True]
).head(10)

# 5. Descobrindo a categoria mais vendida desse grupo
ids_elite = elite_10['id_client'].tolist()
df_elite = df_vendas_prod[df_vendas_prod['id_client'].isin(ids_elite)]
ranking_cat = df_elite.groupby('categoria_padronizada')['qtd'].sum().sort_values(ascending=False)

print("\n" + "="*40)
print("📊 RESPOSTA DA QUESTÃO 5")
print(f"IDs dos 10 Clientes Elite: {ids_elite}")
print(f"Categoria mais vendida: {ranking_cat.index[0]}")
print(f"Quantidade total de itens: {ranking_cat.values[0]}")
print("="*40)