"""
Módulo de KPIs — calcula todos os indicadores de gestão da MR4 a partir do banco local.
Todos os cálculos são feitos via SQL para máxima performance.
"""

from datetime import date, timedelta
from database import get_connection


def _query(sql: str, params: tuple = ()) -> list:
    conn = get_connection()
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def _scalar(sql: str, params: tuple = (), default=0):
    conn = get_connection()
    row = conn.execute(sql, params).fetchone()
    conn.close()
    if row is None:
        return default
    val = row[0]
    return val if val is not None else default


# ─── VENDAS ────────────────────────────────────────────────────────────────────

def faturamento_periodo(data_inicio: str, data_fim: str) -> dict:
    sql = """
        SELECT
            COUNT(*)            AS qtd_vendas,
            SUM(valor_total)    AS faturamento,
            SUM(valor_custo)    AS custo_total,
            SUM(margem_bruta)   AS margem_bruta,
            AVG(valor_total)    AS ticket_medio
        FROM vendas
        WHERE data BETWEEN ? AND ?
          AND nome_situacao NOT IN ('Cancelada', 'Orçamento')
    """
    row = _query(sql, (data_inicio, data_fim))
    r = row[0] if row else {}
    fat = r.get("faturamento") or 0
    custo = r.get("custo_total") or 0
    margem = r.get("margem_bruta") or 0
    return {
        "qtd_vendas": r.get("qtd_vendas") or 0,
        "faturamento": round(fat, 2),
        "custo_total": round(custo, 2),
        "margem_bruta": round(margem, 2),
        "margem_percentual": round((margem / fat * 100) if fat > 0 else 0, 2),
        "ticket_medio": round(r.get("ticket_medio") or 0, 2),
    }


def faturamento_hoje() -> dict:
    hoje = date.today().isoformat()
    return faturamento_periodo(hoje, hoje)


def faturamento_mes_atual() -> dict:
    hoje = date.today()
    inicio = hoje.replace(day=1).isoformat()
    return faturamento_periodo(inicio, hoje.isoformat())


def faturamento_mes_anterior() -> dict:
    hoje = date.today()
    primeiro_mes = hoje.replace(day=1)
    ultimo_mes_ant = primeiro_mes - timedelta(days=1)
    inicio = ultimo_mes_ant.replace(day=1).isoformat()
    fim = ultimo_mes_ant.isoformat()
    return faturamento_periodo(inicio, fim)


def faturamento_ultimos_dias(dias: int = 30) -> dict:
    fim = date.today()
    inicio = fim - timedelta(days=dias)
    return faturamento_periodo(inicio.isoformat(), fim.isoformat())


def comparativo_mensal() -> dict:
    atual = faturamento_mes_atual()
    anterior = faturamento_mes_anterior()
    fat_atual = atual["faturamento"]
    fat_ant = anterior["faturamento"]
    variacao = ((fat_atual - fat_ant) / fat_ant * 100) if fat_ant > 0 else 0
    return {
        "mes_atual": atual,
        "mes_anterior": anterior,
        "variacao_percentual": round(variacao, 2),
        "variacao_valor": round(fat_atual - fat_ant, 2),
    }


def faturamento_por_dia(dias: int = 30) -> list:
    fim = date.today()
    inicio = fim - timedelta(days=dias)
    return _query("""
        SELECT
            data,
            COUNT(*)          AS qtd_vendas,
            SUM(valor_total)  AS faturamento,
            SUM(margem_bruta) AS margem_bruta,
            AVG(valor_total)  AS ticket_medio
        FROM vendas
        WHERE data BETWEEN ? AND ?
          AND nome_situacao NOT IN ('Cancelada', 'Orçamento')
        GROUP BY data
        ORDER BY data DESC
    """, (inicio.isoformat(), fim.isoformat()))


def faturamento_por_vendedor(dias: int = 30) -> list:
    """Retorna apenas a equipe comercial (Ademir e Fabiana). Murilo e Swyanne são administrativo."""
    fim = date.today()
    inicio = (fim - timedelta(days=dias)).isoformat()
    return _query("""
        SELECT
            nome_vendedor,
            COUNT(*)          AS qtd_vendas,
            SUM(valor_total)  AS faturamento,
            SUM(margem_bruta) AS margem_bruta,
            AVG(valor_total)  AS ticket_medio
        FROM vendas
        WHERE data >= ?
          AND nome_situacao NOT IN ('Cancelada', 'Orçamento')
          AND LOWER(nome_vendedor) IN ('ademir lopes furtado', 'fabiana andrade da silva')
        GROUP BY nome_vendedor
        ORDER BY faturamento DESC
    """, (inicio,))


def faturamento_por_canal(dias: int = 30) -> list:
    fim = date.today()
    inicio = (fim - timedelta(days=dias)).isoformat()
    return _query("""
        SELECT
            nome_canal_venda,
            COUNT(*)          AS qtd_vendas,
            SUM(valor_total)  AS faturamento
        FROM vendas
        WHERE data >= ?
          AND nome_situacao NOT IN ('Cancelada', 'Orçamento')
        GROUP BY nome_canal_venda
        ORDER BY faturamento DESC
    """, (inicio,))


def faturamento_por_forma_pagamento(dias: int = 30) -> list:
    fim = date.today()
    inicio = (fim - timedelta(days=dias)).isoformat()
    return _query("""
        SELECT
            pv.nome_forma_pagamento,
            COUNT(DISTINCT pv.venda_id) AS qtd_vendas,
            SUM(pv.valor)               AS total
        FROM pagamentos_venda pv
        JOIN vendas v ON v.id = pv.venda_id
        WHERE v.data >= ?
          AND v.nome_situacao NOT IN ('Cancelada', 'Orçamento')
          AND pv.nome_forma_pagamento != ''
        GROUP BY pv.nome_forma_pagamento
        ORDER BY total DESC
    """, (inicio,))


# ─── PRODUTOS ──────────────────────────────────────────────────────────────────

def top_produtos_vendidos(limite: int = 10, dias: int = 30) -> list:
    fim = date.today()
    inicio = (fim - timedelta(days=dias)).isoformat()
    return _query("""
        SELECT
            iv.nome_produto,
            iv.produto_id,
            SUM(iv.quantidade)  AS qtd_vendida,
            SUM(iv.subtotal)    AS receita,
            SUM(iv.margem)      AS margem_total,
            COUNT(DISTINCT iv.venda_id) AS num_vendas
        FROM itens_venda iv
        JOIN vendas v ON v.id = iv.venda_id
        WHERE v.data >= ?
          AND v.nome_situacao NOT IN ('Cancelada', 'Orçamento')
          AND iv.nome_produto != ''
        GROUP BY iv.produto_id, iv.nome_produto
        ORDER BY receita DESC
        LIMIT ?
    """, (inicio, limite))


def produtos_parados(dias: int = 60) -> list:
    corte = (date.today() - timedelta(days=dias)).isoformat()
    return _query("""
        SELECT
            p.id,
            p.nome,
            p.codigo_interno,
            p.estoque,
            p.valor_custo,
            p.valor_venda,
            ROUND(p.estoque * p.valor_custo, 2) AS capital_parado,
            MAX(v.data) AS ultima_venda
        FROM produtos p
        LEFT JOIN itens_venda iv ON iv.produto_id = p.id
        LEFT JOIN vendas v ON v.id = iv.venda_id
            AND v.nome_situacao NOT IN ('Cancelada', 'Orçamento')
        WHERE p.ativo = '1'
          AND p.estoque > 0
          AND p.movimenta_estoque = '1'
        GROUP BY p.id
        HAVING ultima_venda < ? OR ultima_venda IS NULL
        ORDER BY capital_parado DESC
    """, (corte,))


def produtos_abaixo_minimo() -> list:
    return _query("""
        SELECT
            id, nome, codigo_interno,
            estoque, estoque_minimo,
            valor_custo,
            ROUND(estoque_minimo - estoque, 2) AS falta
        FROM produtos
        WHERE ativo = '1'
          AND movimenta_estoque = '1'
          AND estoque_minimo > 0
          AND estoque < estoque_minimo
        ORDER BY falta DESC
    """)


def valor_total_estoque() -> dict:
    row = _query("""
        SELECT
            COUNT(*)                            AS qtd_produtos,
            SUM(estoque * valor_custo)          AS valor_custo_total,
            SUM(estoque * valor_venda)          AS valor_venda_total,
            SUM(CASE WHEN estoque <= 0 THEN 1 ELSE 0 END) AS sem_estoque
        FROM produtos
        WHERE ativo = '1' AND movimenta_estoque = '1'
    """)
    return row[0] if row else {}


def giro_estoque(dias: int = 30) -> list:
    fim = date.today()
    inicio = (fim - timedelta(days=dias)).isoformat()
    return _query("""
        SELECT
            p.nome,
            p.estoque,
            COALESCE(SUM(iv.quantidade), 0) AS vendas_periodo,
            CASE
                WHEN p.estoque > 0
                THEN ROUND(COALESCE(SUM(iv.quantidade), 0) / p.estoque, 2)
                ELSE 0
            END AS giro
        FROM produtos p
        LEFT JOIN itens_venda iv ON iv.produto_id = p.id
        LEFT JOIN vendas v ON v.id = iv.venda_id
            AND v.data >= ?
            AND v.nome_situacao NOT IN ('Cancelada', 'Orçamento')
        WHERE p.ativo = '1' AND p.movimenta_estoque = '1'
        GROUP BY p.id
        ORDER BY giro DESC
    """, (inicio,))


# ─── CLIENTES ──────────────────────────────────────────────────────────────────

def top_clientes(limite: int = 10, dias: int = 30) -> list:
    fim = date.today()
    inicio = (fim - timedelta(days=dias)).isoformat()
    return _query("""
        SELECT
            c.nome,
            c.celular,
            c.email,
            COUNT(v.id)       AS qtd_compras,
            SUM(v.valor_total) AS total_gasto,
            AVG(v.valor_total) AS ticket_medio,
            MAX(v.data)        AS ultima_compra
        FROM clientes c
        JOIN vendas v ON v.cliente_id = c.id
        WHERE v.data >= ?
          AND v.nome_situacao NOT IN ('Cancelada', 'Orçamento')
        GROUP BY c.id
        ORDER BY total_gasto DESC
        LIMIT ?
    """, (inicio, limite))


def clientes_inativos(dias: int = 90) -> list:
    corte = (date.today() - timedelta(days=dias)).isoformat()
    return _query("""
        SELECT
            c.nome,
            c.celular,
            c.email,
            c.ultima_compra,
            c.total_compras,
            c.qtd_compras,
            CAST(julianday('now') - julianday(c.ultima_compra) AS INTEGER) AS dias_sem_comprar
        FROM clientes c
        WHERE c.ativo = '1'
          AND c.ultima_compra IS NOT NULL
          AND c.ultima_compra < ?
          AND c.qtd_compras > 0
        ORDER BY c.total_compras DESC
    """, (corte,))


def novos_clientes_mes() -> int:
    hoje = date.today()
    inicio = hoje.replace(day=1).isoformat()
    return _scalar("""
        SELECT COUNT(*) FROM clientes
        WHERE cadastrado_em >= ?
    """, (inicio,))


def indice_recompra() -> dict:
    total = _scalar("SELECT COUNT(*) FROM clientes WHERE qtd_compras > 0")
    recompra = _scalar("SELECT COUNT(*) FROM clientes WHERE qtd_compras >= 2")
    perc = round((recompra / total * 100) if total > 0 else 0, 2)
    return {"total_compradores": total, "com_recompra": recompra, "indice_percentual": perc}


# ─── FINANCEIRO ────────────────────────────────────────────────────────────────

def contas_vencidas() -> dict:
    hoje = date.today().isoformat()
    rows = _query("""
        SELECT
            COUNT(*)       AS qtd,
            SUM(valor_total) AS valor_total
        FROM lancamentos
        WHERE entidade = 'I'
          AND liquidado = '0'
          AND data_vencimento < ?
    """, (hoje,))
    r = rows[0] if rows else {}
    return {
        "qtd": r.get("qtd") or 0,
        "valor": round(r.get("valor_total") or 0, 2),
    }


def contas_vencendo(dias: int = 7) -> list:
    hoje = date.today()
    limite = (hoje + timedelta(days=dias)).isoformat()
    return _query("""
        SELECT
            descricao, nome_cliente, nome_forma_pagamento,
            data_vencimento, valor_total, entidade
        FROM lancamentos
        WHERE liquidado = '0'
          AND data_vencimento BETWEEN ? AND ?
        ORDER BY data_vencimento, entidade
    """, (hoje.isoformat(), limite))


def fluxo_caixa_projetado(dias: int = 30) -> list:
    hoje = date.today()
    fim = (hoje + timedelta(days=dias)).isoformat()
    return _query("""
        SELECT
            data_vencimento AS data,
            SUM(CASE WHEN entidade='I' THEN valor_total ELSE 0 END) AS entradas,
            SUM(CASE WHEN entidade='O' THEN valor_total ELSE 0 END) AS saidas,
            SUM(CASE WHEN entidade='I' THEN valor_total ELSE -valor_total END) AS saldo_dia
        FROM lancamentos
        WHERE liquidado = '0'
          AND data_vencimento BETWEEN ? AND ?
        GROUP BY data_vencimento
        ORDER BY data_vencimento
    """, (hoje.isoformat(), fim))


def inadimplencia() -> dict:
    total_receber = _scalar("""
        SELECT SUM(valor_total) FROM lancamentos
        WHERE entidade='I' AND liquidado='0'
    """)
    vencido = _scalar("""
        SELECT SUM(valor_total) FROM lancamentos
        WHERE entidade='I' AND liquidado='0' AND data_vencimento < date('now')
    """)
    perc = round((vencido / total_receber * 100) if total_receber > 0 else 0, 2)
    return {
        "total_a_receber": round(total_receber, 2),
        "total_vencido": round(vencido, 2),
        "percentual_inadimplencia": perc,
    }


# ─── DASHBOARD RESUMO ──────────────────────────────────────────────────────────

def dashboard_completo() -> dict:
    hoje = date.today().isoformat()
    return {
        "data": hoje,
        "vendas": {
            "hoje": faturamento_hoje(),
            "mes_atual": faturamento_mes_atual(),
            "comparativo_mensal": comparativo_mensal(),
            "por_vendedor_30d": faturamento_por_vendedor(30),
            "por_canal_30d": faturamento_por_canal(30),
            "por_forma_pagamento_30d": faturamento_por_forma_pagamento(30),
        },
        "produtos": {
            "top10_30d": top_produtos_vendidos(10, 30),
            "parados_60d": len(produtos_parados(60)),
            "abaixo_minimo": len(produtos_abaixo_minimo()),
            "estoque": valor_total_estoque(),
        },
        "clientes": {
            "top10_30d": top_clientes(10, 30),
            "inativos_90d": len(clientes_inativos(90)),
            "novos_mes": novos_clientes_mes(),
            "recompra": indice_recompra(),
        },
        "financeiro": {
            "inadimplencia": inadimplencia(),
            "contas_vencidas": contas_vencidas(),
            "vencendo_7d": contas_vencendo(7),
            "fluxo_30d": fluxo_caixa_projetado(30),
        },
    }


if __name__ == "__main__":
    import json
    print(json.dumps(dashboard_completo(), indent=2, ensure_ascii=False, default=str))
