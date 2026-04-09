"""
Schema e inicialização do banco SQLite.
Cria todas as tabelas necessárias para a MR4 Distribuidora.
"""

import sqlite3
import os

DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(__file__), "mr4.db"))


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.executescript("""
    -- ─── CONTROLE DE SYNC ───────────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS sync_control (
        endpoint        TEXT PRIMARY KEY,
        last_sync_at    TEXT,
        total_records   INTEGER DEFAULT 0,
        status          TEXT DEFAULT 'ok'
    );

    -- ─── CLIENTES ───────────────────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS clientes (
        id              TEXT PRIMARY KEY,
        tipo_pessoa     TEXT,
        nome            TEXT,
        razao_social    TEXT,
        cpf             TEXT,
        cnpj            TEXT,
        telefone        TEXT,
        celular         TEXT,
        email           TEXT,
        cidade          TEXT,
        estado          TEXT,
        ativo           TEXT,
        vendedor_id     TEXT,
        nome_vendedor   TEXT,
        cadastrado_em   TEXT,
        modificado_em   TEXT,
        -- campos calculados
        ultima_compra   TEXT,
        total_compras   REAL DEFAULT 0,
        qtd_compras     INTEGER DEFAULT 0
    );

    -- ─── FORNECEDORES ───────────────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS fornecedores (
        id              TEXT PRIMARY KEY,
        tipo_pessoa     TEXT,
        nome            TEXT,
        razao_social    TEXT,
        cnpj            TEXT,
        cpf             TEXT,
        telefone        TEXT,
        celular         TEXT,
        email           TEXT,
        ativo           TEXT,
        cadastrado_em   TEXT,
        modificado_em   TEXT
    );

    -- ─── PRODUTOS ───────────────────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS produtos (
        id                  TEXT PRIMARY KEY,
        nome                TEXT,
        codigo_interno      TEXT,
        codigo_barra        TEXT,
        grupo_id            TEXT,
        nome_grupo          TEXT,
        ativo               TEXT,
        movimenta_estoque   TEXT,
        possui_variacao     TEXT,
        peso                REAL,
        valor_custo         REAL DEFAULT 0,
        valor_venda         REAL DEFAULT 0,
        estoque             REAL DEFAULT 0,
        estoque_minimo      REAL DEFAULT 0,
        descricao           TEXT,
        cadastrado_em       TEXT,
        modificado_em       TEXT
    );

    -- ─── VENDAS ─────────────────────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS vendas (
        id                      TEXT PRIMARY KEY,
        codigo                  TEXT,
        cliente_id              TEXT,
        nome_cliente            TEXT,
        vendedor_id             TEXT,
        nome_vendedor           TEXT,
        data                    TEXT,
        prazo_entrega           TEXT,
        situacao_id             TEXT,
        nome_situacao           TEXT,
        valor_total             REAL DEFAULT 0,
        valor_produtos          REAL DEFAULT 0,
        valor_servicos          REAL DEFAULT 0,
        valor_custo             REAL DEFAULT 0,
        valor_frete             REAL DEFAULT 0,
        desconto_valor          REAL DEFAULT 0,
        desconto_porcentagem    REAL DEFAULT 0,
        margem_bruta            REAL DEFAULT 0,
        condicao_pagamento      TEXT,
        situacao_financeiro     TEXT,
        situacao_estoque        TEXT,
        nome_canal_venda        TEXT,
        nome_loja               TEXT,
        numero_parcelas         INTEGER DEFAULT 1,
        cadastrado_em           TEXT,
        modificado_em           TEXT,
        FOREIGN KEY (cliente_id) REFERENCES clientes(id)
    );

    -- ─── ITENS DE VENDA ─────────────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS itens_venda (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        venda_id        TEXT NOT NULL,
        produto_id      TEXT,
        variacao_id     TEXT,
        nome_produto    TEXT,
        quantidade      REAL DEFAULT 0,
        valor_custo     REAL DEFAULT 0,
        valor_venda     REAL DEFAULT 0,
        desconto_valor  REAL DEFAULT 0,
        subtotal        REAL DEFAULT 0,
        margem          REAL DEFAULT 0,
        sigla_unidade   TEXT,
        FOREIGN KEY (venda_id) REFERENCES vendas(id)
    );
    CREATE INDEX IF NOT EXISTS idx_itens_venda_id ON itens_venda(venda_id);
    CREATE INDEX IF NOT EXISTS idx_itens_produto_id ON itens_venda(produto_id);

    -- ─── PAGAMENTOS DE VENDA ────────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS pagamentos_venda (
        id                      INTEGER PRIMARY KEY AUTOINCREMENT,
        venda_id                TEXT NOT NULL,
        forma_pagamento_id      TEXT,
        nome_forma_pagamento    TEXT,
        valor                   REAL DEFAULT 0,
        data_vencimento         TEXT,
        FOREIGN KEY (venda_id) REFERENCES vendas(id)
    );
    CREATE INDEX IF NOT EXISTS idx_pgto_venda_id ON pagamentos_venda(venda_id);

    -- ─── LANÇAMENTOS FINANCEIROS ────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS lancamentos (
        id                      TEXT PRIMARY KEY,
        codigo                  TEXT,
        descricao               TEXT,
        entidade                TEXT,   -- 'I'=entrada, 'O'=saída
        valor                   REAL DEFAULT 0,
        juros                   REAL DEFAULT 0,
        desconto                REAL DEFAULT 0,
        valor_total             REAL DEFAULT 0,
        liquidado               TEXT,   -- '1'=liquidado, '0'=pendente
        data_vencimento         TEXT,
        data_liquidacao         TEXT,
        data_competencia        TEXT,
        forma_pagamento_id      TEXT,
        nome_forma_pagamento    TEXT,
        plano_contas_id         TEXT,
        nome_plano_conta        TEXT,
        conta_bancaria_id       TEXT,
        nome_conta_bancaria     TEXT,
        cliente_id              TEXT,
        nome_cliente            TEXT,
        fornecedor_id           TEXT,
        nome_fornecedor         TEXT,
        nome_loja               TEXT,
        cadastrado_em           TEXT,
        modificado_em           TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_lanc_vencimento ON lancamentos(data_vencimento);
    CREATE INDEX IF NOT EXISTS idx_lanc_entidade ON lancamentos(entidade);
    CREATE INDEX IF NOT EXISTS idx_lanc_liquidado ON lancamentos(liquidado);

    -- ─── FOLLOW-UP FABIANA ──────────────────────────────────────────────────────
    -- Rastreia clientes que compraram apenas 1x e precisam de follow-up
    CREATE TABLE IF NOT EXISTS followup_fabiana (
        cliente_id              TEXT PRIMARY KEY,
        nome_cliente            TEXT,
        telefone                TEXT,
        celular                 TEXT,
        cidade                  TEXT,
        estado                  TEXT,
        primeira_venda_id       TEXT,
        data_primeira_compra    TEXT,
        valor_primeira_compra   REAL DEFAULT 0,
        produtos_comprados      TEXT,   -- JSON com lista de produtos
        qtd_compras_total       INTEGER DEFAULT 1,
        status                  TEXT DEFAULT 'pendente',
        -- pendente | contatado_7d | contatado_25d | recomprou | perdido
        data_followup_7d        TEXT,
        data_followup_25d       TEXT,
        obs                     TEXT,
        atualizado_em           TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_followup_status ON followup_fabiana(status);
    CREATE INDEX IF NOT EXISTS idx_followup_data ON followup_fabiana(data_primeira_compra);

    -- ─── EQUIVALÊNCIAS DE PRODUTOS ─────────────────────────────────────────────
    -- Agrupa produtos de fabricantes diferentes que têm a mesma aplicação.
    -- Todos os produtos do mesmo grupo_id são considerados equivalentes.
    CREATE TABLE IF NOT EXISTS equivalencias (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        grupo_id    TEXT NOT NULL,
        produto_id  TEXT NOT NULL,
        criado_em   TEXT DEFAULT (datetime('now')),
        FOREIGN KEY (produto_id) REFERENCES produtos(id),
        UNIQUE(grupo_id, produto_id)
    );
    CREATE INDEX IF NOT EXISTS idx_equiv_grupo ON equivalencias(grupo_id);
    CREATE INDEX IF NOT EXISTS idx_equiv_produto ON equivalencias(produto_id);

    -- ─── KPIs DIÁRIOS (snapshot) ────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS kpis_diarios (
        data                        TEXT PRIMARY KEY,
        faturamento                 REAL DEFAULT 0,
        custo_total                 REAL DEFAULT 0,
        margem_bruta                REAL DEFAULT 0,
        margem_percentual           REAL DEFAULT 0,
        qtd_vendas                  INTEGER DEFAULT 0,
        ticket_medio                REAL DEFAULT 0,
        novos_clientes              INTEGER DEFAULT 0,
        clientes_inativos_90d       INTEGER DEFAULT 0,
        produtos_abaixo_minimo      INTEGER DEFAULT 0,
        contas_vencidas_valor       REAL DEFAULT 0,
        contas_vencidas_qtd         INTEGER DEFAULT 0,
        gerado_em                   TEXT
    );
    """)

    conn.commit()
    conn.close()
    print(f"Banco de dados inicializado: {DB_PATH}")


def upsert(table: str, data: dict, conn: sqlite3.Connection = None):
    """Insert ou Update por chave primária 'id'."""
    close = conn is None
    if conn is None:
        conn = get_connection()

    cols = list(data.keys())
    placeholders = ", ".join(["?" for _ in cols])
    updates = ", ".join([f"{c}=excluded.{c}" for c in cols if c != "id"])
    sql = f"""
        INSERT INTO {table} ({', '.join(cols)})
        VALUES ({placeholders})
        ON CONFLICT(id) DO UPDATE SET {updates}
    """
    conn.execute(sql, list(data.values()))

    if close:
        conn.commit()
        conn.close()


def upsert_many(table: str, records: list, conn: sqlite3.Connection = None):
    """Upsert em lote — muito mais rápido para grandes volumes."""
    if not records:
        return
    close = conn is None
    if conn is None:
        conn = get_connection()

    cols = list(records[0].keys())
    placeholders = ", ".join(["?" for _ in cols])
    updates = ", ".join([f"{c}=excluded.{c}" for c in cols if c != "id"])
    sql = f"""
        INSERT INTO {table} ({', '.join(cols)})
        VALUES ({placeholders})
        ON CONFLICT(id) DO UPDATE SET {updates}
    """
    conn.executemany(sql, [list(r.values()) for r in records])

    if close:
        conn.commit()
        conn.close()


if __name__ == "__main__":
    init_db()
