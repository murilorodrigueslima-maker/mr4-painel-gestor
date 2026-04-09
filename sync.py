"""
Módulo de sincronização — puxa dados do GestãoClick e salva no banco local.
Suporta sync completo (histórico) e sync incremental (só o que mudou).
"""

import logging
import sqlite3
from datetime import datetime, date, timedelta

from gestaoclick import endpoints
from database import get_connection, upsert_many, init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ─── CONTROLE DE SYNC ──────────────────────────────────────────────────────────

def get_last_sync(endpoint: str):
    conn = get_connection()
    row = conn.execute(
        "SELECT last_sync_at FROM sync_control WHERE endpoint = ?", (endpoint,)
    ).fetchone()
    conn.close()
    return row["last_sync_at"] if row else None


def set_last_sync(endpoint: str, total: int):
    conn = get_connection()
    conn.execute("""
        INSERT INTO sync_control (endpoint, last_sync_at, total_records, status)
        VALUES (?, ?, ?, 'ok')
        ON CONFLICT(endpoint) DO UPDATE SET
            last_sync_at = excluded.last_sync_at,
            total_records = excluded.total_records,
            status = 'ok'
    """, (endpoint, datetime.now().isoformat(), total))
    conn.commit()
    conn.close()


# ─── SYNC DE CLIENTES ──────────────────────────────────────────────────────────

def sync_clientes(full: bool = False):
    logger.info("Sincronizando clientes...")
    raw = endpoints.get_clientes(apenas_ativos=False)

    records = []
    for c in raw:
        enderecos = c.get("enderecos", [])
        cidade = estado = ""
        if enderecos:
            end = enderecos[0].get("endereco", enderecos[0])
            cidade = end.get("cidade", "")
            estado = end.get("estado", "")

        records.append({
            "id": c["id"],
            "tipo_pessoa": c.get("tipo_pessoa", ""),
            "nome": c.get("nome", ""),
            "razao_social": c.get("razao_social", ""),
            "cpf": c.get("cpf", ""),
            "cnpj": c.get("cnpj", ""),
            "telefone": c.get("telefone", ""),
            "celular": c.get("celular", ""),
            "email": c.get("email", ""),
            "cidade": cidade,
            "estado": estado,
            "ativo": c.get("ativo", "1"),
            "vendedor_id": c.get("vendedor_id", ""),
            "nome_vendedor": c.get("nome_vendedor", ""),
            "cadastrado_em": c.get("cadastrado_em", ""),
            "modificado_em": c.get("modificado_em", ""),
        })

    conn = get_connection()
    upsert_many("clientes", records, conn)
    conn.commit()
    conn.close()

    set_last_sync("clientes", len(records))
    logger.info(f"  ✓ {len(records)} clientes sincronizados")
    return len(records)


# ─── SYNC DE FORNECEDORES ──────────────────────────────────────────────────────

def sync_fornecedores():
    logger.info("Sincronizando fornecedores...")
    raw = endpoints.get_fornecedores()

    records = [{
        "id": f["id"],
        "tipo_pessoa": f.get("tipo_pessoa", ""),
        "nome": f.get("nome", ""),
        "razao_social": f.get("razao_social", ""),
        "cnpj": f.get("cnpj", ""),
        "cpf": f.get("cpf", ""),
        "telefone": f.get("telefone", ""),
        "celular": f.get("celular", ""),
        "email": f.get("email", ""),
        "ativo": f.get("ativo", "1"),
        "cadastrado_em": f.get("cadastrado_em", ""),
        "modificado_em": f.get("modificado_em", ""),
    } for f in raw]

    conn = get_connection()
    upsert_many("fornecedores", records, conn)
    conn.commit()
    conn.close()

    set_last_sync("fornecedores", len(records))
    logger.info(f"  ✓ {len(records)} fornecedores sincronizados")
    return len(records)


# ─── SYNC DE PRODUTOS ──────────────────────────────────────────────────────────

def sync_produtos():
    logger.info("Sincronizando produtos...")
    raw = endpoints.get_produtos(apenas_ativos=False)

    records = []
    for p in raw:
        estoque_info = p.get("estoque", {})
        if isinstance(estoque_info, dict):
            qtd_estoque = float(estoque_info.get("quantidade", 0) or 0)
            estoque_min = float(estoque_info.get("estoque_minimo", 0) or 0)
        else:
            qtd_estoque = float(estoque_info or 0)
            estoque_min = 0

        valor_venda = 0
        valores = p.get("valores", [])
        if valores:
            v = valores[0].get("valor", valores[0]) if isinstance(valores[0], dict) else {}
            valor_venda = float(v.get("valor_venda", 0) or 0)

        records.append({
            "id": p["id"],
            "nome": p.get("nome", ""),
            "codigo_interno": p.get("codigo_interno", ""),
            "codigo_barra": p.get("codigo_barra", ""),
            "grupo_id": p.get("grupo_id", ""),
            "nome_grupo": p.get("nome_grupo", ""),
            "ativo": p.get("ativo", "1"),
            "movimenta_estoque": p.get("movimenta_estoque", "1"),
            "possui_variacao": p.get("possui_variacao", "0"),
            "peso": float(p.get("peso", 0) or 0),
            "valor_custo": float(p.get("valor_custo", 0) or 0),
            "valor_venda": valor_venda,
            "estoque": qtd_estoque,
            "estoque_minimo": estoque_min,
            "descricao": p.get("descricao", ""),
            "cadastrado_em": p.get("cadastrado_em", ""),
            "modificado_em": p.get("modificado_em", ""),
        })

    conn = get_connection()
    upsert_many("produtos", records, conn)
    conn.commit()
    conn.close()

    set_last_sync("produtos", len(records))
    logger.info(f"  ✓ {len(records)} produtos sincronizados")
    return len(records)


# ─── SYNC DE VENDAS ────────────────────────────────────────────────────────────

def sync_vendas(data_inicio: str = None, data_fim: str = None):
    """
    Sincroniza vendas. Se não informar datas, pega os últimos 7 dias.
    Para sync histórico completo, passe data_inicio='2020-01-01'.
    """
    if not data_inicio:
        data_inicio = (date.today() - timedelta(days=7)).isoformat()
    if not data_fim:
        data_fim = date.today().isoformat()

    logger.info(f"Sincronizando vendas de {data_inicio} a {data_fim}...")
    raw = endpoints.get_vendas(data_inicio=data_inicio, data_fim=data_fim)

    vendas_records = []
    itens_records = []
    pgto_records = []

    for v in raw:
        valor_total = float(v.get("valor_total", 0) or 0)
        valor_custo = float(v.get("valor_custo", 0) or 0)
        margem = valor_total - valor_custo

        vendas_records.append({
            "id": v["id"],
            "codigo": v.get("codigo", ""),
            "cliente_id": v.get("cliente_id", ""),
            "nome_cliente": v.get("nome_cliente", ""),
            "vendedor_id": v.get("vendedor_id", ""),
            "nome_vendedor": v.get("nome_vendedor", ""),
            "data": v.get("data", ""),
            "prazo_entrega": v.get("prazo_entrega", ""),
            "situacao_id": v.get("situacao_id", ""),
            "nome_situacao": v.get("nome_situacao", ""),
            "valor_total": valor_total,
            "valor_produtos": float(v.get("valor_produtos", 0) or 0),
            "valor_servicos": float(v.get("valor_servicos", 0) or 0),
            "valor_custo": valor_custo,
            "valor_frete": float(v.get("valor_frete", 0) or 0),
            "desconto_valor": float(v.get("desconto_valor", 0) or 0),
            "desconto_porcentagem": float(v.get("desconto_porcentagem", 0) or 0),
            "margem_bruta": margem,
            "condicao_pagamento": v.get("condicao_pagamento", ""),
            "situacao_financeiro": v.get("situacao_financeiro", ""),
            "situacao_estoque": v.get("situacao_estoque", ""),
            "nome_canal_venda": v.get("nome_canal_venda", ""),
            "nome_loja": v.get("nome_loja", ""),
            "numero_parcelas": int(v.get("numero_parcelas", 1) or 1),
            "cadastrado_em": v.get("cadastrado_em", ""),
            "modificado_em": v.get("modificado_em", ""),
        })

        for item in endpoints.extrair_itens_venda(v):
            subtotal = item["quantidade"] * item["valor_venda"]
            custo_total = item["quantidade"] * item["valor_custo"]
            item["subtotal"] = subtotal
            item["margem"] = subtotal - custo_total
            itens_records.append(item)

        pgto_records.extend(endpoints.extrair_pagamentos_venda(v))

    conn = get_connection()

    # Upsert vendas
    upsert_many("vendas", vendas_records, conn)

    # Para itens e pagamentos: deleta e reinsere (não têm ID único da API)
    if raw:
        ids = [v["id"] for v in raw]
        placeholders = ",".join(["?" for _ in ids])
        conn.execute(f"DELETE FROM itens_venda WHERE venda_id IN ({placeholders})", ids)
        conn.execute(f"DELETE FROM pagamentos_venda WHERE venda_id IN ({placeholders})", ids)

    if itens_records:
        conn.executemany("""
            INSERT INTO itens_venda
            (venda_id, produto_id, variacao_id, nome_produto, quantidade,
             valor_custo, valor_venda, desconto_valor, subtotal, margem, sigla_unidade)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, [(r["venda_id"], r["produto_id"], r["variacao_id"], r["nome_produto"],
               r["quantidade"], r["valor_custo"], r["valor_venda"], r["desconto_valor"],
               r["subtotal"], r["margem"], r["sigla_unidade"]) for r in itens_records])

    if pgto_records:
        conn.executemany("""
            INSERT INTO pagamentos_venda
            (venda_id, forma_pagamento_id, nome_forma_pagamento, valor, data_vencimento)
            VALUES (?,?,?,?,?)
        """, [(r["venda_id"], r["forma_pagamento_id"], r["nome_forma_pagamento"],
               r["valor"], r["data_vencimento"]) for r in pgto_records])

    conn.commit()
    conn.close()

    set_last_sync("vendas", len(vendas_records))
    logger.info(f"  ✓ {len(vendas_records)} vendas | {len(itens_records)} itens | {len(pgto_records)} pagamentos")
    return len(vendas_records)


# ─── SYNC DE FINANCEIRO ────────────────────────────────────────────────────────

def sync_financeiro(data_inicio: str = None, data_fim: str = None):
    if not data_inicio:
        data_inicio = (date.today() - timedelta(days=30)).isoformat()
    if not data_fim:
        data_fim = (date.today() + timedelta(days=60)).isoformat()

    logger.info(f"Sincronizando lançamentos financeiros...")
    raw = endpoints.get_pagamentos(data_inicio=data_inicio, data_fim=data_fim)

    records = [{
        "id": l["id"],
        "codigo": l.get("codigo", ""),
        "descricao": l.get("descricao", ""),
        "entidade": l.get("entidade", ""),
        "valor": float(l.get("valor", 0) or 0),
        "juros": float(l.get("juros", 0) or 0),
        "desconto": float(l.get("desconto", 0) or 0),
        "valor_total": float(l.get("valor_total", 0) or 0),
        "liquidado": l.get("liquidado", "0"),
        "data_vencimento": l.get("data_vencimento", ""),
        "data_liquidacao": l.get("data_liquidacao", ""),
        "data_competencia": l.get("data_competencia", ""),
        "forma_pagamento_id": l.get("forma_pagamento_id", ""),
        "nome_forma_pagamento": l.get("nome_forma_pagamento", ""),
        "plano_contas_id": l.get("plano_contas_id", ""),
        "nome_plano_conta": l.get("nome_plano_conta", ""),
        "conta_bancaria_id": l.get("conta_bancaria_id", ""),
        "nome_conta_bancaria": l.get("nome_conta_bancaria", ""),
        "cliente_id": l.get("cliente_id", ""),
        "nome_cliente": l.get("nome_cliente", ""),
        "fornecedor_id": l.get("fornecedor_id", ""),
        "nome_fornecedor": l.get("nome_fornecedor", ""),
        "nome_loja": l.get("nome_loja", ""),
        "cadastrado_em": l.get("cadastrado_em", ""),
        "modificado_em": l.get("modificado_em", ""),
    } for l in raw]

    conn = get_connection()
    upsert_many("lancamentos", records, conn)
    conn.commit()
    conn.close()

    set_last_sync("financeiro", len(records))
    entradas = sum(1 for r in records if r["entidade"] == "I")
    saidas = sum(1 for r in records if r["entidade"] == "O")
    logger.info(f"  ✓ {len(records)} lançamentos ({entradas} entradas, {saidas} saídas)")
    return len(records)


# ─── ATUALIZA ESTATÍSTICAS DE CLIENTES ────────────────────────────────────────

def atualiza_stats_clientes():
    logger.info("Atualizando estatísticas de clientes...")
    conn = get_connection()
    conn.execute("""
        UPDATE clientes SET
            ultima_compra  = sub.ultima_compra,
            total_compras  = sub.total_compras,
            qtd_compras    = sub.qtd_compras
        FROM (
            SELECT
                cliente_id,
                MAX(data)         AS ultima_compra,
                SUM(valor_total)  AS total_compras,
                COUNT(*)          AS qtd_compras
            FROM vendas
            WHERE nome_situacao NOT IN ('Cancelada', 'Orçamento')
            GROUP BY cliente_id
        ) sub
        WHERE clientes.id = sub.cliente_id
    """)
    conn.commit()
    conn.close()
    logger.info("  ✓ Estatísticas de clientes atualizadas")


# ─── SYNC COMPLETO ─────────────────────────────────────────────────────────────

def sync_full(data_inicio_vendas: str = None):
    """Sync completo: histórico inteiro. Use na primeira instalação."""
    logger.info("=" * 50)
    logger.info("INICIANDO SYNC COMPLETO — MR4 DISTRIBUIDORA")
    logger.info("=" * 50)

    init_db()

    sync_clientes()
    sync_fornecedores()
    sync_produtos()

    inicio = data_inicio_vendas or "2020-01-01"
    fim = date.today().isoformat()
    sync_vendas(data_inicio=inicio, data_fim=fim)
    sync_financeiro(data_inicio=inicio, data_fim=(date.today() + timedelta(days=90)).isoformat())
    atualiza_stats_clientes()

    logger.info("=" * 50)
    logger.info("SYNC COMPLETO FINALIZADO")
    logger.info("=" * 50)


def sync_diario():
    """Sync incremental: últimos 2 dias + financeiro próximos 90 dias."""
    logger.info("SYNC DIÁRIO — MR4 DISTRIBUIDORA")
    ontem = (date.today() - timedelta(days=2)).isoformat()
    hoje = date.today().isoformat()
    fim_fin = (date.today() + timedelta(days=90)).isoformat()

    sync_clientes()
    sync_produtos()
    sync_vendas(data_inicio=ontem, data_fim=hoje)
    sync_financeiro(data_inicio=ontem, data_fim=fim_fin)
    atualiza_stats_clientes()
    logger.info("SYNC DIÁRIO FINALIZADO")


if __name__ == "__main__":
    import sys
    if "--full" in sys.argv:
        inicio = None
        for arg in sys.argv:
            if arg.startswith("--desde="):
                inicio = arg.split("=")[1]
        sync_full(data_inicio_vendas=inicio)
    else:
        sync_diario()
