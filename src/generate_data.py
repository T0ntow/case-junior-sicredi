
import csv
import random
import logging
from pathlib import Path
from datetime import date, timedelta
from decimal import Decimal

# CONFIGURAÇÕES

random.seed(42)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "data_sample"

TOTAL_CLIENTES = 30
TOTAL_CONTAS = 35
TOTAL_TRANSACOES = 200

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# FUNÇÕES AUXILIARES

def write_csv(filename, fieldnames, records):
    """Grava uma lista de registros em um arquivo CSV."""

    filepath = OUTPUT_DIR / filename

    try:
        with open(filepath, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(
                file,
                fieldnames=fieldnames,
                delimiter=","
            )

            writer.writeheader()
            writer.writerows(records)

        logger.info(
            "%s gerado com %d registros.",
            filename,
            len(records)
        )

    except OSError:
        logger.exception("Erro ao gerar o arquivo %s.", filename)
        raise


def random_date(start_date, end_date):
    """Gera uma data aleatória entre duas datas."""

    days = (end_date - start_date).days

    return start_date + timedelta(days=random.randint(0, days))

# GERAÇÃO DE CPFs
def generate_fake_cpf():
    """Gera CPF fictício com 11 dígitos."""
    digits = [random.randint(0, 9) for _ in range(9)]

    total = sum(
        digit * weight
        for digit, weight in zip(digits, range(10, 1, -1))
    )

    remainder = (total * 10) % 11
    first_check = 0 if remainder == 10 else remainder

    digits.append(first_check)

    total = sum(
        digit * weight
        for digit, weight in zip(digits, range(11, 1, -1))
    )

    remainder = (total * 10) % 11
    second_check = 0 if remainder == 10 else remainder

    digits.append(second_check)

    # Altera o último dígito para invalidar o checksum.
    digits[-1] = (digits[-1] + 1) % 10

    return "".join(map(str, digits))


# GERAÇÃO DE CLIENTES

def generate_clients():
    clients = []

    first_names = [
        "Ana", "Carlos", "Mariana", "João", "Pedro",
        "Lucas", "Fernanda", "Camila", "Rafael", "Juliana",
        "Gabriel", "Beatriz", "Matheus", "Larissa", "Bruno"
    ]

    last_names = [
        "Silva", "Santos", "Oliveira", "Souza", "Pereira",
        "Costa", "Rodrigues", "Almeida", "Nascimento", "Lima"
    ]

    cities = [
        ("Vitória da Conquista", "BA"),
        ("Salvador", "BA"),
        ("São Paulo", "SP"),
        ("Rio de Janeiro", "RJ"),
        ("Belo Horizonte", "MG"),
        ("Curitiba", "PR"),
        ("Recife", "PE"),
        ("Fortaleza", "CE"),
        ("Brasília", "DF"),
        ("Porto Alegre", "RS")
    ]

    for i in range(1, TOTAL_CLIENTES + 1):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)

        name = f"{first_name} {last_name}"
        city, state = random.choice(cities)

        birth_date = random_date(
            date(1960, 1, 1),
            date(2005, 12, 31)
        )

        # CPFs fictícios com 11 dígitos numéricos.
        cpf = generate_fake_cpf()

        clients.append({
            "id_cliente": i,
            "nome": name,
            "cpf": cpf,
            "data_nascimento": birth_date.isoformat(),
            "cidade": city,
            "estado": state
        })

    # Nomes com espaços extras para testar trim em uma camada refinada.
    clients[0]["nome"] = "  Ana Silva  "
    clients[1]["nome"] = "Carlos   Santos "

    # Gerar CPFs inválidos por tamanho ou caracteres.
    # Criado especificamente para tratar posteriormente
    clients[2]["cpf"] = "12345"
    clients[3]["cpf"] = "ABC12345678"
    clients[4]["cpf"] = "123456789012"

    # Duplicidade de chave de negócio.
    duplicate_client = clients[5].copy()
    duplicate_client["nome"] = "  " + duplicate_client["nome"] + "  "
    clients.append(duplicate_client)

    return clients


# GERAÇÃO DE CONTAS

def generate_accounts(clients):
    accounts = []

    account_types = [
        "CORRENTE",
        "POUPANCA",
        "SALARIO"
    ]

    for i in range(1, TOTAL_CONTAS + 1):
        client = clients[(i - 1) % TOTAL_CLIENTES]

        opening_date = random_date(
            date(2018, 1, 1),
            date(2026, 1, 1)
        )

        status = random.choice([
            "ATIVA",
            "ATIVA",
            "ENCERRADA"
        ])

        accounts.append({
            "id_conta": i,
            "id_cliente": client["id_cliente"],
            "tipo_conta": random.choice(account_types),
            "data_abertura": opening_date.isoformat(),
            "status_conta": status
        })

    # Introdução de inconsistências para teste de validação.
    accounts[0]["tipo_conta"] = "  corrente "
    accounts[1]["status_conta"] = " ativa "

    return accounts


# GERAÇÃO DE TRANSAÇÕES PIX

def generate_transactions(accounts):
    transactions = []

    movements = [
        "ENVIO",
        "RECEBIMENTO"
    ]

    descriptions = [
        "Pagamento",
        "Transferência",
        "Compra",
        "Recebimento de serviço",
        "Transferência entre contas",
        "Pagamento de fornecedor"
    ]

    for i in range(1, TOTAL_TRANSACOES + 1):
        account = random.choice(accounts)

        transaction_date = random_date(
            date(2025, 1, 1),
            date(2026, 10, 31)
        )

        value = Decimal(random.randint(100, 500000)) / Decimal("100")

        transactions.append({
            "id_transacao": i,
            "id_conta": account["id_conta"],
            "data_transacao": transaction_date.isoformat(),
            "valor": str(value),
            "tipo_movimento": random.choice(movements),
            "descricao": random.choice(descriptions)
        })

    ## Introdução de inconsistências para teste de validação.
    # Valores nulos.
    transactions[10]["valor"] = ""
    transactions[20]["valor"] = ""

    # Datas inválidas.
    transactions[30]["data_transacao"] = "2026-15-45"
    transactions[40]["data_transacao"] = "data_invalida"

    # Descrição com espaços extras.
    transactions[0]["descricao"] = "  Pagamento de serviço  "

    # Duplicidades de id_transacao.
    transactions[195]["id_transacao"] = transactions[0]["id_transacao"]
    transactions[196]["id_transacao"] = transactions[1]["id_transacao"]
    transactions[197]["id_transacao"] = transactions[2]["id_transacao"]
    transactions[198]["id_transacao"] = transactions[3]["id_transacao"]

    # Referência a uma conta inexistente.
    transactions[199]["id_conta"] = 9999

    return transactions


# EXECUÇÃO PRINCIPAL

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Iniciando geração da massa de dados.")

    clients = generate_clients()
    accounts = generate_accounts(clients)
    transactions = generate_transactions(accounts)

    write_csv(
        "clientes.csv",
        [
            "id_cliente",
            "nome",
            "cpf",
            "data_nascimento",
            "cidade",
            "estado"
        ],
        clients
    )

    write_csv(
        "contas.csv",
        [
            "id_conta",
            "id_cliente",
            "tipo_conta",
            "data_abertura",
            "status_conta"
        ],
        accounts
    )

    write_csv(
        "transacoes_pix.csv",
        [
            "id_transacao",
            "id_conta",
            "data_transacao",
            "valor",
            "tipo_movimento",
            "descricao"
        ],
        transactions
    )

    logger.info("Geração da massa de dados concluída.")
    logger.info("Arquivos disponíveis em: %s", OUTPUT_DIR)


if __name__ == "__main__":
    main()
