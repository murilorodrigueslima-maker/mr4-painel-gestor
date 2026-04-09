"""
Funções de acesso a cada endpoint do GestãoClick.
Cada função retorna dados já normalizados e prontos para salvar no banco.
"""

from datetime import date, timedelta
from . import client


# ─── VENDAS ────────────────────────────────────────────────────────────────────

def get_vendas(data_inicio: str = None, data_fim: str = None) -> list:
    params = {}
    if data_inicio:
        params["data_inicio"] = data_inicio
    if data_fim:
        params["data_fim"] = data_fim
    return client.get_all("vendas", params)


def get_vendas_hoje() -> list:
    hoje = date.today().isoformat()
    return get_vendas(data_inicio=hoje, data_fim=hoje)


def get_vendas_mes_atual() -> list:
    hoje = date.today()
    inicio = hoje.replace(day=1).isoformat()
    return get_vendas(data_inicio=inicio, data_fim=hoje.isoformat())


def get_vendas_periodo(dias: int = 30) -> list:
    fim = date.today()
    inicio = fim - timedelta(days=dias)
    return get_vendas(data_inicio=inicio.isoformat(), data_fim=fim.isoformat())


def get_venda_by_id(venda_id: str) -> dict:
    result = client.get(f"vendas/{venda_id}")
    return result.get("data", {})


# ─── PRODUTOS ──────────────────────────────────────────────────────────────────

def get_produtos(apenas_ativos: bool = True) -> list:
    params = {"ativo": "1"} if apenas_ativos else {}
    return client.get_all("produtos", params)


def get_produto_by_id(produto_id: str) -> dict:
    result = client.get(f"produtos/{produto_id}")
    return result.get("data", {})


# ─── CLIENTES ──────────────────────────────────────────────────────────────────

def get_clientes(apenas_ativos: bool = True) -> list:
    params = {"ativo": "1"} if apenas_ativos else {}
    return client.get_all("clientes", params)


def get_cliente_by_id(cliente_id: str) -> dict:
    result = client.get(f"clientes/{cliente_id}")
    return result.get("data", {})


# ─── FORNECEDORES ──────────────────────────────────────────────────────────────

def get_fornecedores() -> list:
    return client.get_all("fornecedores")


# ─── FINANCEIRO / PAGAMENTOS ───────────────────────────────────────────────────

def get_pagamentos(data_inicio: str = None, data_fim: str = None) -> list:
    """
    Retorna lançamentos financeiros.
    entidade='I' → entradas (contas a receber)
    entidade='O' → saídas (contas a pagar)
    """
    params = {}
    if data_inicio:
        params["data_inicio"] = data_inicio
    if data_fim:
        params["data_fim"] = data_fim
    return client.get_all("pagamentos", params)


def get_contas_receber(data_inicio: str = None, data_fim: str = None) -> list:
    lancamentos = get_pagamentos(data_inicio, data_fim)
    return [l for l in lancamentos if l.get("entidade") == "I"]


def get_contas_pagar(data_inicio: str = None, data_fim: str = None) -> list:
    lancamentos = get_pagamentos(data_inicio, data_fim)
    return [l for l in lancamentos if l.get("entidade") == "O"]


def get_contas_receber_vencidas() -> list:
    hoje = date.today().isoformat()
    lancamentos = get_pagamentos(data_fim=hoje)
    return [
        l for l in lancamentos
        if l.get("entidade") == "I"
        and l.get("liquidado") == "0"
        and l.get("data_vencimento", "9999") < hoje
    ]


def get_contas_vencendo(dias: int = 7) -> list:
    hoje = date.today()
    limite = (hoje + timedelta(days=dias)).isoformat()
    lancamentos = get_pagamentos(data_inicio=hoje.isoformat(), data_fim=limite)
    return [l for l in lancamentos if l.get("liquidado") == "0"]


# ─── HELPERS GERAIS ────────────────────────────────────────────────────────────

def extrair_itens_venda(venda: dict) -> list:
    """Extrai e normaliza os itens (produtos) de uma venda."""
    itens = []
    for item in venda.get("produtos", []):
        p = item.get("produto", item)
        itens.append({
            "venda_id": venda["id"],
            "produto_id": p.get("produto_id", ""),
            "variacao_id": p.get("variacao_id", ""),
            "nome_produto": p.get("nome_produto", ""),
            "quantidade": float(p.get("quantidade", 0)),
            "valor_custo": float(p.get("valor_custo", 0) or 0),
            "valor_venda": float(p.get("valor_venda", 0) or 0),
            "desconto_valor": float(p.get("desconto_valor", 0) or 0),
            "sigla_unidade": p.get("sigla_unidade", ""),
        })
    return itens


def extrair_pagamentos_venda(venda: dict) -> list:
    """Extrai as formas de pagamento de uma venda."""
    pagamentos = []
    for item in venda.get("pagamentos", []):
        p = item.get("pagamento", item)
        pagamentos.append({
            "venda_id": venda["id"],
            "forma_pagamento_id": p.get("forma_pagamento_id", ""),
            "nome_forma_pagamento": p.get("nome_forma_pagamento", ""),
            "valor": float(p.get("valor", 0) or 0),
            "data_vencimento": p.get("data_vencimento", ""),
        })
    return pagamentos
