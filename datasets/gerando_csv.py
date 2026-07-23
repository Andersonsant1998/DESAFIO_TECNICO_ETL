import csv
import random
from datetime import datetime, timedelta


# Listas para geração aleatória de nomes compostos
primeiros_nomes = [
    "Ana", "Joao", "Maria", "Carlos", "Lucas", "Mariana", "Pedro", "Juliana",
    "Gabriel", "Fernanda", "Rodrigo", "Camila", "Mateus", "Beatriz", "Guilherme",
    "Aline", "Bruno", "Patricia", "Felipe", "Larissa", "Thiago", "Amanda",
    "Rafael", "Leticia", "Diego", "Vanessa", "Gustavo", "Jessica", "Luan", "Bruna"
]

sobrenomes = [
    "Silva", "Souza", "Costa", "Santos", "Oliveira", "Pereira", "Rodrigues",
    "Almeida", "Nascimento", "Lima", "Araujo", "Fernandes", "Carvalho", "Gomes",
    "Martins", "Rocha", "Ribeiro", "Alves", "Monteiro", "Mendes", "Barros",
    "Freitas", "Barbosa", "Pinto", "Moura", "Cavalcanti", "Dias", "Castro"
]

def data_nascimento_aleatoria():
    """Gera datas de nascimento entre 1960 e 2005."""
    inicio = datetime(1960, 1, 1)
    fim = datetime(2005, 12, 31)
    dias_diferenca = (fim - inicio).days
    data_sorteada = inicio + timedelta(days=random.randint(0, dias_diferenca))
    return data_sorteada.strftime("%Y-%m-%d")

# Garantir reprodutibilidade (mesmos dados sempre que rodar)
random.seed(42)

# Geração das 300 linhas
linhas = [["cliente_id", "nome", "data_nascimento"]]

for i in range(1, 301):
    nome_completo = f"{random.choice(primeiros_nomes)} {random.choice(sobrenomes)}"
    data_nasc = data_nascimento_aleatoria()
    linhas.append([i, nome_completo, data_nasc])


# Salvando em data/clientes.csv
caminho_arquivo = "/content/datasets/clientes.csv"
with open(caminho_arquivo, mode="w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerows(linhas)

print(f"✅ Arquivo '{caminho_arquivo}' gerado com sucesso com {len(linhas)-1} registros!")