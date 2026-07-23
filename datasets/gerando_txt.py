import random
from datetime import datetime, timedelta

def data_venda_aleatoria():
    """Gera datas de venda aleatórias durante o ano de 2023."""
    inicio = datetime(2023, 1, 1)
    fim = datetime(2023, 12, 31)
    dias_diferenca = (fim - inicio).days
    data_sorteada = inicio + timedelta(days=random.randint(0, dias_diferenca))
    return data_sorteada.strftime("%Y%m%d")  # Formato YYYYMMDD (8 caracteres)

# Garantir reprodutibilidade dos dados de teste
random.seed(42)

# Configurações para o volume de 10.000 linhas
total_vendas = 10000
total_clientes = 300  # Relacionado com as 300 linhas do clientes.csv
produtos_disponiveis = [10001, 10002, 10003, 10004, 10005, 10006, 10007, 10008, 10009, 10010]

caminho_arquivo = "/content/datasets/vendas.txt"

print(f"🚀 Gerando {total_vendas:,} linhas posicionais em '{caminho_arquivo}'...")

with open(caminho_arquivo, mode="w", encoding="utf-8") as f:
    buffer = []
    
    for i in range(1, total_vendas + 1):
        # 1. venda_id: posições 1 a 5 (5 dígitos com zero à esquerda)
        # Nota: Para lidar com IDs acima de 99999 no futuro, usamos mod ou formatação dinâmica.
        # Mantendo 5 dígitos exatos como pede o layout do teste técnico.
        venda_id_str = f"{(i % 100000):05d}"
        
        # 2. cliente_id: posições 6 a 10 (5 dígitos)
        cliente_id = random.randint(1, total_clientes)
        cliente_id_str = f"{cliente_id:05d}"
        
        # 3. produto_id: posições 11 a 15 (5 dígitos)
        produto_id = random.choice(produtos_disponiveis)
        produto_id_str = f"{produto_id:05d}"
        
        # 4. valor: posições 16 a 23 (8 dígitos, 2 decimais implícitos, ex: R$ 150.50 -> 00015050)
        valor_float = round(random.uniform(5.0, 1500.00), 2)
        valor_inteiro = int(valor_float * 100)
        valor_str = f"{valor_inteiro:08d}"
        
        # 5. data_venda: posições 24 a 31 (8 dígitos YYYYMMDD)
        data_venda_str = data_venda_aleatoria()
        
        # Linha montada com exatos 31 caracteres
        linha = f"{venda_id_str}{cliente_id_str}{produto_id_str}{valor_str}{data_venda_str}\n"
        buffer.append(linha)
        
        # Grava em disco a cada 2.000 linhas para otimizar uso de memória
        if len(buffer) >= 2000:
            f.writelines(buffer)
            buffer.clear()
            
    # Escreve o restante se houver
    if buffer:
        f.writelines(buffer)

print(f"✅ Arquivo '{caminho_arquivo}' criado com sucesso contendo {total_vendas:,} registros de 31 caracteres cada!")