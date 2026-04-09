"""
Painel do Gestor — MR4 Distribuidora
Comando: streamlit run integracoes/painel.py
"""

import sys
from pathlib import Path
import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from datetime import date, timedelta, datetime
from zoneinfo import ZoneInfo

_TZ_BR = ZoneInfo("America/Fortaleza")

def _agora_br():
    return datetime.now(_TZ_BR)

def _hoje_br():
    return _agora_br().date()

_HERE = Path(__file__).parent.resolve()
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import uuid
from database import get_connection, init_db
from sync import sync_produtos, sync_vendas, sync_financeiro, atualiza_stats_clientes

# ── garante que o banco e as tabelas existem ───────────────────────────────────
init_db()

# ── página ─────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Painel do Gestor · MR4",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
  /* ── Base ── */
  body, .stApp,
  [data-testid="stAppViewContainer"],
  [data-testid="stHeader"],
  section[data-testid="stSidebar"] { background-color: #f5f6fa !important; font-size: 15px !important; }
  .block-container { padding: 1.5rem 2rem; }
  * { color: inherit; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }

  /* ── Tipografia geral ── */
  html, body, .stApp, p, div, span, td, th, li, input, select, textarea, button {
    font-size: 15px !important;
    line-height: 1.5 !important;
  }
  h1 { color: #1a1a2e !important; font-size: 1.75rem !important; font-weight: 800 !important; }
  h2 { color: #1a1a2e !important; font-size: 0.85rem !important;
       text-transform: uppercase; letter-spacing: .08em;
       font-weight: 700 !important; margin-top: 1.5rem !important; }
  h3 { color: #1a1a2e !important; font-size: 1.1rem !important; font-weight: 700 !important; }
  p, span, div, label { color: #212529; }

  /* ── Streamlit widgets com texto maior ── */
  div[data-testid="stTabs"] button { font-size: 14px !important; }
  div[data-testid="stRadio"] label { font-size: 15px !important; }
  div[data-testid="stSelectbox"] div { font-size: 15px !important; }
  div[data-testid="stMultiSelect"] div { font-size: 15px !important; }
  div[data-testid="stExpander"] summary { font-size: 15px !important; }
  div[data-testid="stExpander"] summary p { font-size: 15px !important; }
  div[data-testid="stMarkdown"] p { font-size: 15px !important; }
  div[data-testid="stMarkdown"] li { font-size: 15px !important; }
  [data-testid="stText"] { font-size: 15px !important; }
  .stButton button { font-size: 15px !important; }

  /* ── Cards ── */
  .card {
    background: #ffffff;
    border-radius: 10px;
    padding: 18px 22px;
    margin-bottom: 10px;
    border: 1px solid #e9ecef;
    box-shadow: 0 1px 4px rgba(0,0,0,.06);
  }
  .card-red    { border-left: 4px solid #dc3545; }
  .card-green  { border-left: 4px solid #28a745; }
  .card-yellow { border-left: 4px solid #fd7e14; }
  .card-blue   { border-left: 4px solid #0066cc; }
  .card-purple { border-left: 4px solid #6f42c1; }

  /* ── Números grandes ── */
  .big-number { font-size: 2.2rem !important; font-weight: 800; line-height: 1.1; color: #1a1a2e; }
  .big-label  { font-size: 0.85rem !important; color: #6c757d; text-transform: uppercase;
                letter-spacing: .08em; margin-bottom: 4px; }

  /* ── Tags / badges ── */
  .tag-red    { background:#fde8ea; color:#c0392b; border:1px solid #f5c6cb;
                border-radius:6px; padding:3px 10px; font-size:.85rem !important; display:inline-block; }
  .tag-green  { background:#d4edda; color:#155724; border:1px solid #c3e6cb;
                border-radius:6px; padding:3px 10px; font-size:.85rem !important; display:inline-block; }
  .tag-yellow { background:#fff3cd; color:#856404; border:1px solid #ffeeba;
                border-radius:6px; padding:3px 10px; font-size:.85rem !important; display:inline-block; }
  .tag-blue   { background:#cce5ff; color:#004085; border:1px solid #b8daff;
                border-radius:6px; padding:3px 10px; font-size:.85rem !important; display:inline-block; }

  /* ── Tabelas (DataFrames) — estilo GestãoClick ── */
  [data-testid="stDataFrame"] {
    border-radius: 8px !important;
    overflow: hidden !important;
    border: 1px solid #dee2e6 !important;
    box-shadow: 0 1px 4px rgba(0,0,0,.06);
  }
  /* Cabeçalho */
  .ag-header, .ag-header-row {
    background-color: #f8f9fa !important;
    border-bottom: 2px solid #dee2e6 !important;
  }
  .ag-header-cell-label, .ag-header-cell-text {
    color: #212529 !important;
    font-weight: 700 !important;
    font-size: .82rem !important;
    text-transform: uppercase !important;
    letter-spacing: .04em !important;
  }
  /* Linhas */
  .ag-root-wrapper, .ag-body-viewport, .ag-center-cols-container {
    background-color: #ffffff !important;
  }
  .ag-row {
    background-color: #ffffff !important;
    color: #212529 !important;
    border-bottom: 1px solid #f0f0f0 !important;
    font-size: .88rem !important;
  }
  .ag-row-odd {
    background-color: #f8f9fa !important;
  }
  .ag-row:hover, .ag-row-hover {
    background-color: #e8f4fd !important;
  }
  .ag-cell {
    color: #212529 !important;
    padding: 8px 12px !important;
  }
  /* Scrollbar da tabela */
  .ag-body-horizontal-scroll-viewport::-webkit-scrollbar { height: 6px; }
  .ag-body-horizontal-scroll-viewport::-webkit-scrollbar-thumb {
    background: #dee2e6; border-radius: 3px;
  }

  /* ── Métricas Streamlit ── */
  div[data-testid="stMetric"] {
    background: #ffffff !important;
    border-radius: 10px !important;
    padding: 14px 18px !important;
    border: 1px solid #e9ecef !important;
    box-shadow: 0 1px 4px rgba(0,0,0,.06);
  }
  div[data-testid="stMetric"] label { color: #6c757d !important; font-size: .75rem !important; }
  div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
    font-size: 1.6rem !important; color: #1a1a2e !important; font-weight: 800 !important;
  }

  /* ── Checkboxes ── */
  div[data-testid="stCheckbox"] label { font-size: 15px !important; color: #212529 !important; }

  /* ── Divisores ── */
  hr { border-color: #dee2e6 !important; margin: 1rem 0 !important; }

  /* ── Expander (corrige fundo preto) ── */
  div[data-testid="stExpander"] {
    background: #ffffff !important;
    border: 1px solid #dee2e6 !important;
    border-radius: 8px !important;
  }
  div[data-testid="stExpander"] > details { background: #ffffff !important; }
  div[data-testid="stExpander"] summary {
    background: #ffffff !important;
    color: #212529 !important;
    font-size: 15px !important;
    padding: 10px 14px !important;
  }

  /* ── Multiselect / Selectbox (corrige fundo preto) ── */
  div[data-testid="stMultiSelect"] > div,
  div[data-testid="stSelectbox"] > div {
    background: #ffffff !important;
    border: 1px solid #ced4da !important;
    border-radius: 6px !important;
    color: #212529 !important;
    font-size: 15px !important;
  }
  div[data-baseweb="select"] { background: #ffffff !important; }
  div[data-baseweb="select"] * { color: #212529 !important; background: #ffffff !important; }

  /* ── Dropdown popup (corrige fundo preto da lista) ── */
  div[data-baseweb="popover"],
  div[data-baseweb="popover"] > div,
  div[data-baseweb="popover"] ul,
  div[data-baseweb="menu"],
  div[data-baseweb="menu"] > ul {
    background: #ffffff !important;
    color: #212529 !important;
  }
  div[data-baseweb="menu"] li,
  div[data-baseweb="menu"] [role="option"],
  ul[role="listbox"] li,
  ul[role="listbox"] [role="option"] {
    background: #ffffff !important;
    color: #212529 !important;
    font-size: 15px !important;
  }
  div[data-baseweb="menu"] li:hover,
  div[data-baseweb="menu"] [role="option"]:hover,
  ul[role="listbox"] li:hover,
  ul[role="listbox"] [aria-selected="true"] {
    background: #e8f4fd !important;
    color: #212529 !important;
  }

  /* ── Estoque badges ── */
  .estoque-row {
    display: flex; justify-content: space-between; align-items: center;
    padding: 12px 0; border-bottom: 1px solid #e9ecef;
  }
  .estoque-badge {
    font-size: 13px !important; font-weight: 700; text-transform: uppercase;
    letter-spacing: .06em; padding: 3px 10px; border-radius: 6px;
    display: inline-block; margin-bottom: 3px;
  }
</style>
""", unsafe_allow_html=True)


# ── helpers ────────────────────────────────────────────────────────────────────
def tabela_html(df: "pd.DataFrame", altura: int = None) -> None:
    """Renderiza DataFrame como tabela HTML estilo GestãoClick."""
    style_table = (
        "width:100%;border-collapse:collapse;font-size:14px;"
        "font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;"
    )
    style_wrap = "overflow-x:auto;border-radius:8px;border:1px solid #dee2e6;box-shadow:0 1px 4px rgba(0,0,0,.06);"
    if altura:
        style_wrap += f"max-height:{altura}px;overflow-y:auto;"

    th_style = (
        "background:#f8f9fa;color:#495057;font-weight:700;font-size:13px;"
        "text-transform:uppercase;letter-spacing:.05em;padding:12px 16px;"
        "border-bottom:2px solid #dee2e6;white-space:nowrap;text-align:left;"
        "position:sticky;top:0;z-index:1;"
    )
    rows_html = ""
    for i, (_, row) in enumerate(df.iterrows()):
        bg = "#ffffff" if i % 2 == 0 else "#f8f9fa"
        tds = ""
        for val in row:
            td_style = (
                f"background:{bg};color:#212529;padding:11px 16px;"
                "border-bottom:1px solid #e9ecef;white-space:nowrap;font-size:14px;"
            )
            tds += f"<td style='{td_style}'>{val}</td>"
        rows_html += f"<tr style='transition:background .1s;'>{tds}</tr>"

    headers = "".join(f"<th style='{th_style}'>{c}</th>" for c in df.columns)
    html = f"""
    <div style="{style_wrap}">
      <table style="{style_table}">
        <thead><tr>{headers}</tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>
    """
    st.html(html)


def brl(v):
    if v is None: return "R$ 0,00"
    return f"R$ {v:,.2f}".replace(",","X").replace(".",",").replace("X",".")

def pct_bar(valor, meta, cor="#8b5cf6"):
    p = min(valor / meta * 100, 100) if meta else 0
    return f"""
    <div style="background:#e2e8f0;border-radius:99px;height:10px;margin:6px 0;">
      <div style="background:{cor};border-radius:99px;height:10px;width:{p:.0f}%;
           transition:width .4s;"></div>
    </div>
    <span style="font-size:.8rem;color:#64748b;">{p:.0f}% da meta</span>
    """

def ultimo_sync():
    """Retorna data/hora do último sync bem-sucedido."""
    try:
        conn = get_connection()
        row = conn.execute(
            "SELECT MAX(last_sync_at) as ts FROM sync_control WHERE status='ok'"
        ).fetchone()
        conn.close()
        if row and row["ts"]:
            ts = row["ts"][:16].replace("T", " ")
            return ts
    except Exception:
        pass
    return None

def rodar_sync():
    """Sync incremental: produtos + vendas recentes + financeiro."""
    hoje = _hoje_br().isoformat()
    ontem = (_hoje_br() - timedelta(days=2)).isoformat()
    fim_fin = (_hoje_br() + timedelta(days=90)).isoformat()
    sync_produtos()
    sync_vendas(data_inicio=ontem, data_fim=hoje)
    sync_financeiro(data_inicio=ontem, data_fim=fim_fin)
    atualiza_stats_clientes()

def margem_cor(m):
    if m >= 30: return "#10b981"
    if m >= 20: return "#f59e0b"
    return "#ef4444"

def estoque_badge(estoque, minimo):
    if estoque < 0:
        return "<span class='estoque-badge' style='background:#7f1d1d;color:#fca5a5;'>Negativo</span>"
    if estoque == 0:
        return "<span class='estoque-badge' style='background:#7f1d1d;color:#fca5a5;'>Zerado</span>"
    if minimo > 0 and estoque < minimo:
        return "<span class='estoque-badge' style='background:#78350f;color:#fde68a;'>Abaixo do mín.</span>"
    return "<span class='estoque-badge' style='background:#064e3b;color:#6ee7b7;'>OK</span>"


# ── dados: visão geral ─────────────────────────────────────────────────────────
def carregar_dados():
    conn = get_connection()
    hoje = _hoje_br()
    ontem = hoje - timedelta(days=1)
    semana_inicio = hoje - timedelta(days=hoje.weekday())
    mes_str = hoje.strftime('%Y-%m')

    fat_ontem = conn.execute("""
        SELECT COALESCE(SUM(valor_total),0) as fat, COUNT(*) as qtd
        FROM vendas WHERE date(data)=? AND nome_situacao NOT IN ('Cancelado','cancelado')
    """, (str(ontem),)).fetchone()

    fat_hoje = conn.execute("""
        SELECT COALESCE(SUM(valor_total),0) as fat, COUNT(*) as qtd
        FROM vendas WHERE date(data)=? AND nome_situacao NOT IN ('Cancelado','cancelado')
    """, (str(hoje),)).fetchone()

    fat_semana = conn.execute("""
        SELECT COALESCE(SUM(valor_total),0) as fat, COUNT(*) as qtd,
               COALESCE(SUM(margem_bruta),0) as margem
        FROM vendas WHERE date(data)>=? AND date(data)<=?
        AND nome_situacao NOT IN ('Cancelado','cancelado')
    """, (str(semana_inicio), str(hoje))).fetchone()

    fat_mes = conn.execute("""
        SELECT COALESCE(SUM(valor_total),0) as fat, COUNT(*) as qtd,
               COALESCE(SUM(margem_bruta),0) as margem,
               COALESCE(AVG(valor_total),0) as ticket
        FROM vendas WHERE strftime('%Y-%m',data)=?
        AND nome_situacao NOT IN ('Cancelado','cancelado')
    """, (mes_str,)).fetchone()

    vendedores_hoje = conn.execute("""
        SELECT nome_vendedor,
               COALESCE(SUM(valor_total),0) as fat,
               COUNT(*) as qtd
        FROM vendas WHERE date(data)=?
        AND nome_situacao NOT IN ('Cancelado','cancelado')
        AND LOWER(nome_vendedor) IN ('ademir lopes furtado','fabiana andrade da silva')
        GROUP BY nome_vendedor
    """, (str(hoje),)).fetchall()

    vendedores_semana = conn.execute("""
        SELECT nome_vendedor,
               COALESCE(SUM(valor_total),0) as fat,
               COUNT(*) as qtd,
               COALESCE(AVG(valor_total),0) as ticket
        FROM vendas WHERE date(data)>=? AND date(data)<=?
        AND nome_situacao NOT IN ('Cancelado','cancelado')
        AND LOWER(nome_vendedor) IN ('ademir lopes furtado','fabiana andrade da silva')
        GROUP BY nome_vendedor ORDER BY fat DESC
    """, (str(semana_inicio), str(hoje))).fetchall()

    fat_por_dia = conn.execute("""
        SELECT date(data) as data, COALESCE(SUM(valor_total),0) as fat
        FROM vendas WHERE date(data)>=? AND date(data)<=?
        AND nome_situacao NOT IN ('Cancelado','cancelado')
        GROUP BY date(data) ORDER BY date(data)
    """, (str(semana_inicio), str(hoje))).fetchall()

    est_negativo = conn.execute("""
        SELECT COUNT(*) as qtd FROM produtos
        WHERE estoque < 0 AND ativo='1'
    """).fetchone()['qtd']

    sem_estoque = conn.execute("""
        SELECT p.nome, p.estoque
        FROM produtos p
        WHERE p.ativo='1' AND p.estoque <= 0
        AND p.id IN (
            SELECT vi.produto_id FROM itens_venda vi
            JOIN vendas v ON v.id=vi.venda_id
            WHERE v.data >= date('now','-30 days')
            GROUP BY vi.produto_id ORDER BY SUM(vi.subtotal) DESC LIMIT 5
        )
    """).fetchall()

    contas_vencendo = conn.execute("""
        SELECT COUNT(*) as qtd, COALESCE(SUM(valor_total),0) as total
        FROM lancamentos
        WHERE data_vencimento BETWEEN date('now') AND date('now','+3 days')
        AND liquidado='0'
        AND entidade IN ('F','O','U')
    """).fetchone()

    contas_vencidas = conn.execute("""
        SELECT COUNT(*) as qtd, COALESCE(SUM(valor_total),0) as total
        FROM lancamentos
        WHERE data_vencimento < date('now')
        AND liquidado='0'
        AND entidade IN ('F','O','U')
    """).fetchone()

    parados = conn.execute("""
        SELECT COUNT(*) as qtd, COALESCE(SUM(p.estoque*p.valor_custo),0) as capital
        FROM produtos p
        WHERE p.ativo='1' AND p.estoque > 0
        AND p.id NOT IN (
            SELECT DISTINCT vi.produto_id FROM itens_venda vi
            JOIN vendas v ON v.id=vi.venda_id
            WHERE v.data >= date('now','-60 days')
            AND v.nome_situacao NOT IN ('Cancelado','cancelado')
        )
    """).fetchone()

    fat_7d = conn.execute("""
        SELECT date(data) as data, COALESCE(SUM(valor_total),0) as fat
        FROM vendas WHERE date(data) >= date('now','-6 days')
        AND nome_situacao NOT IN ('Cancelado','cancelado')
        GROUP BY date(data) ORDER BY date(data)
    """).fetchall()

    fat_mes_ant = conn.execute("""
        SELECT COALESCE(SUM(valor_total),0) as fat
        FROM vendas WHERE strftime('%Y-%m',data)=?
        AND nome_situacao NOT IN ('Cancelado','cancelado')
    """, ((hoje.replace(day=1) - timedelta(days=1)).strftime('%Y-%m'),)).fetchone()['fat']

    conn.close()

    dia_mes = hoje.day or 1
    projecao = fat_mes['fat'] / dia_mes * 31 if dia_mes > 0 else 0

    return {
        "hoje": _hoje_br(),
        "fat_ontem": dict(fat_ontem),
        "fat_hoje": dict(fat_hoje),
        "fat_semana": dict(fat_semana),
        "fat_mes": dict(fat_mes),
        "fat_mes_ant": fat_mes_ant,
        "projecao": projecao,
        "vendedores_hoje": [dict(r) for r in vendedores_hoje],
        "vendedores_semana": [dict(r) for r in vendedores_semana],
        "fat_por_dia": [dict(r) for r in fat_por_dia],
        "fat_7d": [dict(r) for r in fat_7d],
        "est_negativo": est_negativo,
        "sem_estoque": [dict(r) for r in sem_estoque],
        "contas_vencendo": dict(contas_vencendo),
        "contas_vencidas": dict(contas_vencidas),
        "parados": dict(parados),
        "dia_mes": dia_mes,
    }


# ── dados: estoque ─────────────────────────────────────────────────────────────
def carregar_estoque():
    conn = get_connection()

    # KPIs gerais de estoque
    kpis = conn.execute("""
        SELECT
            COUNT(*) as total_ativos,
            SUM(CASE WHEN estoque < 0 THEN 1 ELSE 0 END) as negativos,
            SUM(CASE WHEN estoque_minimo > 0 AND estoque < estoque_minimo AND estoque >= 0 THEN 1 ELSE 0 END) as abaixo_minimo,
            SUM(CASE WHEN estoque > 0 THEN estoque * valor_custo ELSE 0 END) as capital_total
        FROM produtos WHERE ativo='1'
    """).fetchone()

    # Produtos abaixo do estoque mínimo
    abaixo_minimo = conn.execute("""
        SELECT nome, nome_grupo,
               ROUND(estoque,2) as estoque,
               ROUND(estoque_minimo,2) as estoque_minimo,
               valor_custo, valor_venda,
               CASE WHEN valor_venda > 0
                    THEN (valor_venda - valor_custo) / valor_venda * 100
                    ELSE 0 END as margem_pct,
               ROUND(estoque_minimo - estoque, 2) as qtd_repor,
               ROUND((estoque_minimo - estoque) * valor_custo, 2) as custo_repor
        FROM produtos
        WHERE ativo='1' AND estoque_minimo > 0
          AND estoque < estoque_minimo AND estoque >= 0
        ORDER BY custo_repor DESC
        LIMIT 50
    """).fetchall()

    # Sem giro > 90 dias com estoque positivo
    sem_giro = conn.execute("""
        SELECT p.nome, p.nome_grupo,
               ROUND(p.estoque, 2) as estoque,
               p.valor_custo, p.valor_venda,
               CASE WHEN p.valor_venda > 0
                    THEN (p.valor_venda - p.valor_custo) / p.valor_venda * 100
                    ELSE 0 END as margem_pct,
               ROUND(p.estoque * p.valor_custo, 2) as capital_imobilizado
        FROM produtos p
        WHERE p.ativo='1' AND p.estoque > 0
          AND p.id NOT IN (
              SELECT DISTINCT vi.produto_id FROM itens_venda vi
              JOIN vendas v ON v.id = vi.venda_id
              WHERE v.data >= date('now','-90 days')
                AND v.nome_situacao NOT IN ('Cancelado','cancelado')
                AND vi.produto_id IS NOT NULL
          )
        ORDER BY capital_imobilizado DESC
        LIMIT 50
    """).fetchall()

    # Todos os produtos com custo e venda para filtro de margem
    todos_margem = conn.execute("""
        SELECT nome, nome_grupo,
               ROUND(estoque, 2) as estoque,
               valor_custo, valor_venda,
               CASE WHEN valor_venda > 0
                    THEN (valor_venda - valor_custo) / valor_venda * 100
                    ELSE 0 END as margem_pct
        FROM produtos
        WHERE ativo='1' AND valor_venda > 0 AND valor_custo > 0
        ORDER BY (valor_venda - valor_custo) / valor_venda * 100 ASC
    """).fetchall()

    # Para promoção: sem giro 90d + estoque significativo
    para_promocao = conn.execute("""
        SELECT p.nome, p.nome_grupo,
               ROUND(p.estoque, 2) as estoque,
               p.valor_custo, p.valor_venda,
               CASE WHEN p.valor_venda > 0
                    THEN (p.valor_venda - p.valor_custo) / p.valor_venda * 100
                    ELSE 0 END as margem_pct,
               ROUND(p.estoque * p.valor_custo, 2) as capital_imobilizado
        FROM produtos p
        WHERE p.ativo='1' AND p.estoque >= 3
          AND p.id NOT IN (
              SELECT DISTINCT vi.produto_id FROM itens_venda vi
              JOIN vendas v ON v.id = vi.venda_id
              WHERE v.data >= date('now','-90 days')
                AND v.nome_situacao NOT IN ('Cancelado','cancelado')
                AND vi.produto_id IS NOT NULL
          )
        ORDER BY capital_imobilizado DESC
        LIMIT 30
    """).fetchall()

    # Gestão de compras: produtos com necessidade de reposição
    compras = conn.execute("""
        SELECT p.nome, p.nome_grupo,
               ROUND(p.estoque, 2) as estoque,
               ROUND(p.estoque_minimo, 2) as estoque_minimo,
               p.valor_custo, p.valor_venda,
               COALESCE(v90.qtd_vendida, 0) as vendas_90d,
               ROUND(COALESCE(v90.qtd_vendida, 0) / 90.0 * 30, 1) as media_mensal,
               ROUND(MAX(0.0, p.estoque_minimo - p.estoque), 2) as deficit,
               ROUND(MAX(0.0, COALESCE(v90.qtd_vendida, 0) / 90.0 * 60 - p.estoque), 0) as sugestao_compra,
               ROUND(MAX(0.0, COALESCE(v90.qtd_vendida, 0) / 90.0 * 60 - p.estoque) * p.valor_custo, 2) as custo_sugestao
        FROM produtos p
        LEFT JOIN (
            SELECT vi.produto_id, SUM(vi.quantidade) as qtd_vendida
            FROM itens_venda vi
            JOIN vendas v ON v.id = vi.venda_id
            WHERE v.data >= date('now','-90 days')
              AND v.nome_situacao NOT IN ('Cancelado','cancelado')
              AND vi.produto_id IS NOT NULL
            GROUP BY vi.produto_id
        ) v90 ON v90.produto_id = p.id
        WHERE p.ativo='1'
          AND (
              (p.estoque_minimo > 0 AND p.estoque < p.estoque_minimo)
              OR (COALESCE(v90.qtd_vendida, 0) > 0
                  AND p.estoque <= COALESCE(v90.qtd_vendida, 0) / 90.0 * 15)
          )
          AND COALESCE(v90.qtd_vendida, 0) > 0
        ORDER BY deficit * p.valor_custo DESC, custo_sugestao DESC
        LIMIT 40
    """).fetchall()

    conn.close()

    capital_sem_giro = sum(r['capital_imobilizado'] for r in sem_giro)

    return {
        "kpis": dict(kpis),
        "abaixo_minimo": [dict(r) for r in abaixo_minimo],
        "sem_giro": [dict(r) for r in sem_giro],
        "todos_margem": [dict(r) for r in todos_margem],
        "para_promocao": [dict(r) for r in para_promocao],
        "compras": [dict(r) for r in compras],
        "capital_sem_giro": capital_sem_giro,
    }


# ── carregar ───────────────────────────────────────────────────────────────────
d  = carregar_dados()
de = carregar_estoque()

hoje = d["hoje"]
hora = _agora_br().strftime("%H:%M")
dia_semana_pt = ["Segunda","Terça","Quarta","Quinta","Sexta","Sábado","Domingo"]
nome_dia = dia_semana_pt[hoje.weekday()]

# ── metas ──────────────────────────────────────────────────────────────────────
META_DIA    = 5_750.0
META_SEMANA = 40_000.0
META_MES    = 161_000.0

# ── cabeçalho ──────────────────────────────────────────────────────────────────
ts_sync = ultimo_sync()
sync_label = f"Sync: {ts_sync}" if ts_sync else "Sync: nunca"

col_h1, col_h2, col_h3 = st.columns([3, 2, 1])
with col_h1:
    st.markdown(f"# 🎯 Painel do Gestor")
    st.markdown(
        f"<span style='color:#64748b;font-size:.95rem;'>"
        f"{nome_dia}, {hoje.strftime('%d/%m/%Y')} · {hora}"
        f"</span>"
        f"&nbsp;&nbsp;<span style='color:#475569;font-size:.8rem;'>· {sync_label}</span>",
        unsafe_allow_html=True
    )
with col_h2:
    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
    if st.button("🔄 Buscar dados do sistema", use_container_width=True):
        _erro = None
        _msg  = []
        with st.spinner("Sincronizando com GestãoClick..."):
            try:
                from datetime import date as _d, timedelta as _td
                _hoje  = _d.today().isoformat()
                _inicio = (_d.today() - _td(days=7)).isoformat()
                _fim_fin = (_d.today() + _td(days=90)).isoformat()

                n_prod = sync_produtos()
                _msg.append(f"✅ Produtos atualizados")

                n_vend = sync_vendas(data_inicio=_inicio, data_fim=_hoje)
                _msg.append(f"✅ {n_vend} vendas sincronizadas")

                sync_financeiro(data_inicio=_inicio, data_fim=_fim_fin)
                _msg.append(f"✅ Financeiro atualizado")

                atualiza_stats_clientes()
            except Exception as e:
                _erro = str(e)

        if _erro:
            st.error(f"Erro ao sincronizar: {_erro}")
            st.stop()
        else:
            for m in _msg:
                st.toast(m)
            st.rerun()
with col_h3:
    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
    if st.button("⚙️ Metas", use_container_width=True):
        st.session_state["show_metas"] = not st.session_state.get("show_metas", False)

if st.session_state.get("show_metas"):
    with st.expander("⚙️ Ajustar Metas", expanded=True):
        c1, c2, c3 = st.columns(3)
        META_DIA    = c1.number_input("Meta diária (R$)",    value=META_DIA,    step=500.0)
        META_SEMANA = c2.number_input("Meta semanal (R$)",   value=META_SEMANA, step=1000.0)
        META_MES    = c3.number_input("Meta mensal (R$)",    value=META_MES,    step=5000.0)

st.markdown("<hr/>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# ABAS PRINCIPAIS
# ══════════════════════════════════════════════════════════════════════════════
tab_geral, tab_estoque = st.tabs(["📊 Visão Geral", "📦 Estoque & Compras"])

# ══════════════════════════════════════════════════════════════════════════════
# ABA 1 — VISÃO GERAL (conteúdo original)
# ══════════════════════════════════════════════════════════════════════════════
with tab_geral:

    st.markdown("## 📊 Situação de Hoje")

    k1, k2, k3, k4, k5 = st.columns(5)

    fat_h = d["fat_hoje"]["fat"]
    fat_o = d["fat_ontem"]["fat"]

    k1.metric("Vendas Hoje",
              brl(fat_h),
              f"{d['fat_hoje']['qtd']} pedidos",
              delta_color="normal")

    pct_dia = fat_h / META_DIA * 100 if META_DIA else 0
    cor_dia = "#10b981" if pct_dia >= 80 else "#f59e0b" if pct_dia >= 50 else "#ef4444"
    k2.metric("Meta Diária",
              f"{pct_dia:.0f}%",
              brl(META_DIA),
              delta_color="normal" if pct_dia >= 80 else "inverse")

    fat_s = d["fat_semana"]["fat"]
    pct_sem = fat_s / META_SEMANA * 100 if META_SEMANA else 0
    k3.metric("Semana Atual",
              brl(fat_s),
              f"{pct_sem:.0f}% da meta",
              delta_color="normal" if pct_sem >= (d["dia_mes"] / 6 * 100) else "inverse")

    fat_m = d["fat_mes"]["fat"]
    pct_mes = fat_m / META_MES * 100 if META_MES else 0
    k4.metric("Mês Atual",
              brl(fat_m),
              f"{pct_mes:.0f}% da meta",
              delta_color="normal" if pct_mes >= (d["dia_mes"] / 31 * 100) else "inverse")

    k5.metric("Projeção do Mês",
              brl(d["projecao"]),
              f"{((d['projecao']/d['fat_mes_ant'])-1)*100:+.1f}% vs mês ant." if d["fat_mes_ant"] else "—",
              delta_color="normal" if d["projecao"] >= d["fat_mes_ant"] else "inverse")

    b1, b2, b3 = st.columns(3)
    with b1:
        st.markdown(f"**Meta do dia** · {brl(META_DIA)}")
        st.markdown(pct_bar(fat_h, META_DIA, cor_dia), unsafe_allow_html=True)
    with b2:
        st.markdown(f"**Meta da semana** · {brl(META_SEMANA)}")
        st.markdown(pct_bar(fat_s, META_SEMANA, "#3b82f6"), unsafe_allow_html=True)
    with b3:
        st.markdown(f"**Meta do mês** · {brl(META_MES)}")
        st.markdown(pct_bar(fat_m, META_MES, "#8b5cf6"), unsafe_allow_html=True)

    st.markdown("<hr/>", unsafe_allow_html=True)

    col_alert, col_check = st.columns([1, 1])

    with col_alert:
        st.markdown("## 🚨 Alertas")

        alertas = []

        if fat_h == 0 and hoje.weekday() < 6:
            alertas.append(("red", "VENDAS", f"Nenhuma venda registrada hoje"))

        if _agora_br().hour >= 12 and pct_dia < 50:
            alertas.append(("yellow", "META", f"Abaixo de 50% da meta ({pct_dia:.0f}%) — já é meio-dia"))

        if d["contas_vencidas"]["total"] > 0:
            alertas.append(("red", "FINANCEIRO",
                            f"{d['contas_vencidas']['qtd']} conta(s) vencida(s) · {brl(d['contas_vencidas']['total'])}"))

        if d["contas_vencendo"]["total"] > 0:
            alertas.append(("yellow", "FINANCEIRO",
                            f"{d['contas_vencendo']['qtd']} conta(s) vencendo em 3 dias · {brl(d['contas_vencendo']['total'])}"))

        if d["est_negativo"] > 0:
            alertas.append(("red", "ESTOQUE",
                            f"{d['est_negativo']} produto(s) com estoque negativo"))

        for p in d["sem_estoque"]:
            alertas.append(("red", "RUPTURA",
                            f"{p['nome'][:45]}... → zerado"))

        if d["parados"]["capital"] > 50_000:
            alertas.append(("yellow", "ESTOQUE",
                            f"{d['parados']['qtd']} produtos parados · {brl(d['parados']['capital'])} imobilizados"))

        kpis_est = de["kpis"]
        if kpis_est["abaixo_minimo"] and kpis_est["abaixo_minimo"] > 0:
            alertas.append(("yellow", "ESTOQUE",
                            f"{kpis_est['abaixo_minimo']} produto(s) abaixo do estoque mínimo"))

        if not alertas:
            st.markdown("""
            <div class='card card-green'>
              <div class='big-label'>Status</div>
              <div style='font-size:1.1rem;color:#10b981;'>✅ Nenhum alerta crítico no momento</div>
            </div>
            """, unsafe_allow_html=True)
        else:
            cores = {"red": "#ef4444", "yellow": "#f59e0b", "green": "#10b981"}
            icones = {"red": "🔴", "yellow": "🟡", "green": "🟢"}
            for nivel, cat, msg in alertas:
                st.markdown(f"""
                <div class='card card-{"red" if nivel=="red" else "yellow"}' style='margin-bottom:8px;'>
                  <span style='font-size:.7rem;color:{cores[nivel]};font-weight:700;
                               text-transform:uppercase;letter-spacing:.06em;'>
                    {icones[nivel]} {cat}
                  </span><br/>
                  <span style='font-size:.95rem;'>{msg}</span>
                </div>
                """, unsafe_allow_html=True)

    with col_check:
        st.markdown("## ✅ Checklist do Dia")

        chave = f"check_{hoje}"
        if chave not in st.session_state:
            st.session_state[chave] = {i: False for i in range(10)}

        checks = st.session_state[chave]

        def item(idx, emoji, texto, obrig=False):
            label = f"{emoji} {texto}" + (" ⚡" if obrig else "")
            checks[idx] = st.checkbox(label, value=checks[idx], key=f"ck_{hoje}_{idx}")

        st.markdown("**🌅 Manhã (antes das 9h)**")
        item(0, "📊", "Abrir painel e ver faturamento de ontem", obrig=True)
        item(1, "💰", "Verificar contas que vencem hoje", obrig=True)
        item(2, "👥", "Alinhar meta do dia com Ademir e Fabiana", obrig=True)

        st.markdown("**☀️ Meio do dia (12h)**")
        item(3, "📈", "Checar ritmo de vendas: estamos na meta?")
        item(4, "📦", "Algum produto top zerou estoque?")

        st.markdown("**🌆 Final do dia (17h)**")
        item(5, "💵", "Confirmar depósitos e PIX do dia")
        item(6, "📋", "Ver o que ficou pendente para amanhã")
        item(7, "🔔", "Responder clientes importantes em aberto")

        st.markdown("**📅 Semanal (somente segunda)**")
        item(8, "🎯", "Definir meta da semana e comunicar equipe")
        item(9, "🧾", "Revisar contas a pagar da semana")

        feitos = sum(1 for v in checks.values() if v)
        total  = len(checks)
        pct_ck = feitos / total * 100
        cor_ck = "#10b981" if pct_ck >= 80 else "#f59e0b" if pct_ck >= 40 else "#ef4444"
        st.markdown(f"""
        <div style='margin-top:12px;background:#ffffff;border-radius:8px;padding:12px;'>
          <span style='color:{cor_ck};font-size:1.4rem;font-weight:800;'>{feitos}/{total}</span>
          <span style='color:#64748b;font-size:.85rem;'> itens concluídos hoje</span>
          <div style='background:#e2e8f0;border-radius:99px;height:8px;margin-top:8px;'>
            <div style='background:{cor_ck};border-radius:99px;height:8px;width:{pct_ck:.0f}%;'></div>
          </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<hr/>", unsafe_allow_html=True)

    col_eq, col_graf = st.columns([1, 2])

    with col_eq:
        st.markdown("## 👥 Equipe — Semana")

        nomes_curtos = {
            "ademir lopes furtado": "Ademir",
            "fabiana andrade da silva": "Fabiana",
        }
        fat_total_semana = sum(v["fat"] for v in d["vendedores_semana"]) or 1

        for v in d["vendedores_semana"]:
            nome  = nomes_curtos.get(v["nome_vendedor"].lower(), v["nome_vendedor"])
            pct_v = v["fat"] / fat_total_semana * 100
            cor_v = "#8b5cf6" if "ademir" in v["nome_vendedor"].lower() else "#3b82f6"
            st.markdown(f"""
            <div class='card' style='margin-bottom:10px;border-left:4px solid {cor_v};'>
              <div style='display:flex;justify-content:space-between;align-items:center;'>
                <span style='font-size:1rem;font-weight:700;'>{nome}</span>
                <span style='font-size:.75rem;color:#64748b;'>{pct_v:.0f}% do total</span>
              </div>
              <div style='font-size:1.5rem;font-weight:800;color:{cor_v};margin:4px 0;'>
                {brl(v["fat"])}
              </div>
              <div style='font-size:.8rem;color:#64748b;'>
                {v["qtd"]} vendas · ticket {brl(v["ticket"])}
              </div>
              <div style='background:#e2e8f0;border-radius:99px;height:6px;margin-top:8px;'>
                <div style='background:{cor_v};border-radius:99px;height:6px;
                            width:{min(v["fat"]/META_SEMANA*100,100):.0f}%;'></div>
              </div>
              <div style='font-size:.72rem;color:#64748b;margin-top:3px;'>
                {min(v["fat"]/META_SEMANA*100,100):.0f}% da meta semanal
              </div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("**Hoje:**")
        if d["vendedores_hoje"]:
            for v in d["vendedores_hoje"]:
                nome = nomes_curtos.get(v["nome_vendedor"].lower(), v["nome_vendedor"])
                st.markdown(f"&nbsp;&nbsp;• **{nome}**: {brl(v['fat'])} ({v['qtd']} pedidos)")
        else:
            st.markdown("<span style='color:#f59e0b;'>Nenhuma venda registrada hoje ainda</span>",
                        unsafe_allow_html=True)

    with col_graf:
        st.markdown("## 📈 Vendas — Últimos 7 dias")

        if d["fat_7d"]:
            df7 = pd.DataFrame(d["fat_7d"])
            df7["data"] = pd.to_datetime(df7["data"])
            df7["dia"] = df7["data"].dt.strftime("%a %d/%m")
            df7["cor"] = df7["fat"].apply(
                lambda x: "#10b981" if x >= META_DIA else "#f59e0b" if x >= META_DIA * 0.6 else "#ef4444"
            )

            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=df7["dia"], y=df7["fat"],
                marker_color=df7["cor"].tolist(),
                text=[brl(v) for v in df7["fat"]],
                textposition="outside",
                textfont=dict(size=11, color="#e2e8f0"),
                hovertemplate="<b>%{x}</b><br>R$ %{y:,.2f}<extra></extra>"
            ))
            fig.add_hline(
                y=META_DIA, line_dash="dash",
                line_color="#94a3b8", line_width=1.5,
                annotation_text=f"Meta diária: {brl(META_DIA)}",
                annotation_font_color="#94a3b8", annotation_font_size=11
            )
            fig.update_layout(
                plot_bgcolor="#ffffff", paper_bgcolor="rgba(0,0,0,0)",
                xaxis=dict(gridcolor="#e9ecef", tickfont=dict(color="#6c757d")),
                yaxis=dict(gridcolor="#e9ecef", tickfont=dict(color="#6c757d"),
                           tickprefix="R$ ", tickformat=",.0f"),
                height=320, margin=dict(l=10, r=10, t=30, b=10),
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sem dados para os últimos 7 dias.")

    st.markdown("<hr/>", unsafe_allow_html=True)

    col_fin, col_prio = st.columns([1, 1])

    with col_fin:
        st.markdown("## 💰 Financeiro")

        conn = get_connection()
        prox_pagamentos = conn.execute("""
            SELECT descricao, data_vencimento, valor_total,
                   COALESCE(nome_fornecedor, nome_cliente, 'Outros') as nome,
                   liquidado,
                   CAST(julianday(data_vencimento) - julianday('now') AS INT) as dias_para_vencer
            FROM lancamentos
            WHERE data_vencimento BETWEEN date('now','-1 days') AND date('now','+14 days')
            AND liquidado='0'
            AND entidade IN ('F','O','U')
            ORDER BY data_vencimento ASC
            LIMIT 8
        """).fetchall()
        conn.close()

        if not prox_pagamentos:
            st.markdown("<div class='card card-green'>Nenhum pagamento nos próximos 14 dias</div>",
                        unsafe_allow_html=True)
        else:
            for p in prox_pagamentos:
                p = dict(p)
                d_vencer = p["dias_para_vencer"]
                if d_vencer < 0:
                    tag = f"<span class='tag-red'>Vencido {abs(d_vencer)}d atrás</span>"
                elif d_vencer == 0:
                    tag = "<span class='tag-red'>Vence HOJE</span>"
                elif d_vencer <= 3:
                    tag = f"<span class='tag-yellow'>Vence em {d_vencer}d</span>"
                else:
                    tag = f"<span style='color:#64748b;font-size:.75rem;'>em {d_vencer} dias</span>"

                nome_curto = p["nome"][:30] + "..." if len(p["nome"]) > 30 else p["nome"]
                st.markdown(f"""
                <div style='display:flex;justify-content:space-between;align-items:center;
                            padding:10px 0;border-bottom:1px solid #f1f5f9;'>
                  <div>
                    {tag}
                    <div style='font-size:.9rem;margin-top:3px;'>{nome_curto}</div>
                    <div style='font-size:.75rem;color:#64748b;'>{p["data_vencimento"]}</div>
                  </div>
                  <div style='font-size:1rem;font-weight:700;color:#1a1a2e;text-align:right;'>
                    {brl(p["valor_total"])}
                  </div>
                </div>
                """, unsafe_allow_html=True)

    with col_prio:
        st.markdown("## 🎯 Prioridade do Dia")

        chave_prio = f"prio_{hoje}"
        chave_obs  = f"obs_{hoje}"

        st.markdown("**Qual é a sua UMA prioridade de hoje?**")
        prio = st.text_input(
            "Escreva aqui:",
            value=st.session_state.get(chave_prio, ""),
            placeholder="Ex: Ligar para os 5 clientes inativos da Fabiana",
            key=chave_prio, label_visibility="collapsed"
        )

        if prio:
            st.markdown(f"""
            <div class='card card-purple' style='margin-top:10px;'>
              <div class='big-label'>Foco de hoje</div>
              <div style='font-size:1.1rem;font-weight:700;color:#c4b5fd;'>→ {prio}</div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("<div style='margin-top:16px;'><b>Anotações do dia</b></div>",
                    unsafe_allow_html=True)
        obs = st.text_area(
            "Anotações:",
            value=st.session_state.get(chave_obs, ""),
            placeholder="Pendências, recados, decisões do dia...",
            height=130, key=chave_obs, label_visibility="collapsed"
        )

        st.markdown("<div style='margin-top:16px;'><b>Acesso rápido</b></div>",
                    unsafe_allow_html=True)
        ca, cb = st.columns(2)
        ca.page_link("http://localhost:8501", label="📊 Dashboard Completo", icon="📊")
        cb.markdown(f"""
        <div style='background:#ffffff;border:1px solid #e9ecef;border-radius:8px;padding:10px;text-align:center;
                    margin-top:4px;font-size:.85rem;color:#6c757d;'>
          📦 Estoque parado<br/>
          <span style='color:#f59e0b;font-size:1.1rem;font-weight:700;'>
            {d['parados']['qtd']} produtos
          </span><br/>
          <span style='font-size:.8rem;'>{brl(d['parados']['capital'])}</span>
        </div>
        """, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# ABA 2 — ESTOQUE & COMPRAS
# ══════════════════════════════════════════════════════════════════════════════
with tab_estoque:

    kpis_est = de["kpis"]

    # ── KPIs de estoque ────────────────────────────────────────────────────────
    st.markdown("## 📦 Resumo do Estoque")

    ke1, ke2, ke3, ke4 = st.columns(4)

    ke1.metric("Produtos Ativos", f"{kpis_est['total_ativos']:,}")

    abaixo_min_qtd = kpis_est["abaixo_minimo"] or 0
    ke2.metric(
        "Abaixo do Mínimo",
        str(abaixo_min_qtd),
        "Necessitam reposição",
        delta_color="inverse" if abaixo_min_qtd > 0 else "normal"
    )

    qtd_sem_giro = len(de["sem_giro"])
    ke3.metric(
        "Sem Giro +90 dias",
        str(qtd_sem_giro),
        brl(de["capital_sem_giro"]) + " imobilizado",
        delta_color="inverse" if qtd_sem_giro > 0 else "normal"
    )

    ke4.metric(
        "Capital Total em Estoque",
        brl(kpis_est["capital_total"] or 0),
    )

    st.markdown("<hr/>", unsafe_allow_html=True)

    # ── Sub-abas de estoque ────────────────────────────────────────────────────
    sub1, sub2, sub3, sub4, sub5, sub6 = st.tabs([
        f"🔴 Estoque Baixo ({abaixo_min_qtd})",
        f"🕒 Sem Giro +90d ({qtd_sem_giro})",
        "📉 Margens Baixas",
        f"🏷️ Para Promoção ({len(de['para_promocao'])})",
        "🛒 Compras / Reposição",
        "🔗 Equivalências",
    ])

    # ── SUB 1: Estoque Baixo ───────────────────────────────────────────────────
    with sub1:
        st.markdown("### Produtos abaixo do estoque mínimo")
        st.caption("Produtos ativos com estoque atual menor que o mínimo cadastrado.")

        dados_ab = de["abaixo_minimo"]
        if not dados_ab:
            st.success("Nenhum produto abaixo do estoque mínimo.")
        else:
            total_repor = sum(r["custo_repor"] for r in dados_ab)
            st.markdown(f"**{len(dados_ab)} produtos** precisam de reposição · "
                        f"custo estimado: **{brl(total_repor)}**")

            df_ab = pd.DataFrame(dados_ab)
            df_ab = df_ab.rename(columns={
                "nome": "Produto",
                "nome_grupo": "Grupo",
                "estoque": "Estoque Atual",
                "estoque_minimo": "Mínimo",
                "qtd_repor": "Qtd. Repor",
                "valor_custo": "Custo Unit.",
                "valor_venda": "Preço Venda",
                "margem_pct": "Margem %",
                "custo_repor": "Custo Reposição",
            })
            df_ab["Custo Unit."] = df_ab["Custo Unit."].apply(brl)
            df_ab["Preço Venda"] = df_ab["Preço Venda"].apply(brl)
            df_ab["Custo Reposição"] = df_ab["Custo Reposição"].apply(brl)
            df_ab["Margem %"] = df_ab["Margem %"].apply(lambda x: f"{x:.1f}%")
            df_ab = df_ab[["Produto", "Grupo", "Estoque Atual", "Mínimo", "Qtd. Repor",
                           "Custo Unit.", "Preço Venda", "Margem %", "Custo Reposição"]]
            tabela_html(df_ab, altura=400)

    # ── SUB 2: Sem Giro ────────────────────────────────────────────────────────
    with sub2:
        st.markdown("### Produtos sem giro nos últimos 90 dias")
        st.caption("Produtos com estoque positivo que não tiveram nenhuma saída em 90 dias.")

        dados_sg = de["sem_giro"]
        if not dados_sg:
            st.success("Nenhum produto parado há mais de 90 dias.")
        else:
            st.markdown(f"**{len(dados_sg)} produtos** parados · "
                        f"**{brl(de['capital_sem_giro'])}** imobilizado")

            df_sg = pd.DataFrame(dados_sg)
            df_sg = df_sg.rename(columns={
                "nome": "Produto",
                "nome_grupo": "Grupo",
                "estoque": "Estoque",
                "valor_custo": "Custo Unit.",
                "valor_venda": "Preço Venda",
                "margem_pct": "Margem %",
                "capital_imobilizado": "Capital Imobilizado",
            })
            df_sg["Custo Unit."] = df_sg["Custo Unit."].apply(brl)
            df_sg["Preço Venda"] = df_sg["Preço Venda"].apply(brl)
            df_sg["Capital Imobilizado"] = df_sg["Capital Imobilizado"].apply(brl)
            df_sg["Margem %"] = df_sg["Margem %"].apply(lambda x: f"{x:.1f}%")
            df_sg = df_sg[["Produto", "Grupo", "Estoque", "Custo Unit.",
                           "Preço Venda", "Margem %", "Capital Imobilizado"]]
            tabela_html(df_sg, altura=400)

    # ── SUB 3: Margens Baixas ──────────────────────────────────────────────────
    with sub3:
        st.markdown("### Produtos com margem abaixo do limite")

        limite_margem = st.slider(
            "Limite de margem (%)", min_value=5, max_value=40, value=20, step=1,
            help="Exibe produtos com margem abaixo deste valor"
        )

        dados_mg = [r for r in de["todos_margem"] if r["margem_pct"] < limite_margem]

        if not dados_mg:
            st.success(f"Nenhum produto com margem abaixo de {limite_margem}%.")
        else:
            st.caption(f"**{len(dados_mg)} produtos** com margem < {limite_margem}%")

            df_mg = pd.DataFrame(dados_mg)
            df_mg = df_mg.rename(columns={
                "nome": "Produto",
                "nome_grupo": "Grupo",
                "estoque": "Estoque",
                "valor_custo": "Custo Unit.",
                "valor_venda": "Preço Venda",
                "margem_pct": "Margem %",
            })
            df_mg["Custo Unit."] = df_mg["Custo Unit."].apply(brl)
            df_mg["Preço Venda"] = df_mg["Preço Venda"].apply(brl)
            df_mg["Margem %"] = df_mg["Margem %"].apply(lambda x: f"{x:.1f}%")
            df_mg = df_mg[["Produto", "Grupo", "Estoque", "Custo Unit.", "Preço Venda", "Margem %"]]
            tabela_html(df_mg, altura=400)

            # Gráfico dos piores
            top20 = sorted(dados_mg, key=lambda x: x["margem_pct"])[:20]
            if top20:
                fig_mg = go.Figure(go.Bar(
                    x=[r["margem_pct"] for r in top20],
                    y=[r["nome"][:35] for r in top20],
                    orientation="h",
                    marker_color=[
                        "#ef4444" if r["margem_pct"] < 10 else "#f59e0b"
                        for r in top20
                    ],
                    text=[f"{r['margem_pct']:.1f}%" for r in top20],
                    textposition="outside",
                    textfont=dict(color="#e2e8f0", size=11),
                ))
                fig_mg.update_layout(
                    plot_bgcolor="#ffffff", paper_bgcolor="rgba(0,0,0,0)",
                    xaxis=dict(gridcolor="#e9ecef", tickfont=dict(color="#6c757d"),
                               ticksuffix="%", title="Margem %"),
                    yaxis=dict(gridcolor="#e9ecef", tickfont=dict(color="#6c757d")),
                    height=max(300, len(top20) * 28),
                    margin=dict(l=10, r=60, t=20, b=10),
                    showlegend=False,
                    title=dict(text="Top 20 menores margens", font=dict(color="#94a3b8", size=13))
                )
                st.plotly_chart(fig_mg, use_container_width=True)

    # ── SUB 4: Para Promoção ───────────────────────────────────────────────────
    with sub4:
        st.markdown("### Produtos candidatos a promoção")
        st.caption(
            "Produtos com estoque ≥ 3 unidades que não tiveram saída nos últimos 90 dias. "
            "Bons candidatos para promoção ou queima de estoque."
        )

        dados_promo = de["para_promocao"]
        if not dados_promo:
            st.success("Nenhum produto para promoção no momento.")
        else:
            capital_promo = sum(r["capital_imobilizado"] for r in dados_promo)
            st.markdown(f"**{len(dados_promo)} produtos** · "
                        f"**{brl(capital_promo)}** imobilizado")

            # Cards por produto com sugestão de desconto
            for i in range(0, len(dados_promo), 3):
                cols = st.columns(3)
                for j, col in enumerate(cols):
                    if i + j >= len(dados_promo):
                        break
                    p = dados_promo[i + j]
                    margem = p["margem_pct"]
                    # Sugere desconto de até 50% da margem
                    desc_sugerido = min(margem * 0.5, 15) if margem > 0 else 0
                    preco_promo = p["valor_venda"] * (1 - desc_sugerido / 100)
                    with col:
                        st.markdown(f"""
                        <div class='card card-blue' style='margin-bottom:8px;'>
                          <div class='big-label'>{(p['nome_grupo'] or 'Sem grupo')[:20]}</div>
                          <div style='font-size:.95rem;font-weight:700;margin-bottom:6px;'>
                            {p['nome'][:40]}
                          </div>
                          <div style='font-size:.8rem;color:#64748b;'>
                            Estoque: <b style='color:#1e293b;'>{p['estoque']:.0f} un</b> ·
                            Margem: <b style='color:{margem_cor(margem)};'>{margem:.1f}%</b>
                          </div>
                          <div style='font-size:.8rem;margin-top:4px;'>
                            Preço atual: <b>{brl(p['valor_venda'])}</b>
                          </div>
                          <div style='font-size:.8rem;color:#93c5fd;'>
                            💡 Promo -{desc_sugerido:.0f}%: <b>{brl(preco_promo)}</b>
                          </div>
                          <div style='font-size:.75rem;color:#64748b;margin-top:4px;'>
                            Capital: {brl(p['capital_imobilizado'])}
                          </div>
                        </div>
                        """, unsafe_allow_html=True)

    # ── SUB 5: Compras / Reposição ─────────────────────────────────────────────
    with sub5:
        st.markdown("### Lista de Compras / Reposição")
        st.caption(
            "Produtos com giro nos últimos 90 dias que estão abaixo do mínimo ou com "
            "menos de 15 dias de estoque. Sugestão calculada para cobrir 60 dias de vendas."
        )

        dados_comp = de["compras"]
        if not dados_comp:
            st.info("Nenhum produto identificado para reposição no momento.")
        else:
            total_custo_compras = sum(r["custo_sugestao"] for r in dados_comp)

            col_c1, col_c2, col_c3 = st.columns(3)
            col_c1.metric("Itens para Comprar", str(len(dados_comp)))
            col_c2.metric("Investimento Estimado", brl(total_custo_compras))
            col_c3.metric("Cobertura sugerida", "60 dias")

            st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

            # Tabela detalhada
            df_comp = pd.DataFrame(dados_comp)
            df_comp = df_comp.rename(columns={
                "nome": "Produto",
                "nome_grupo": "Grupo",
                "estoque": "Estoque Atual",
                "estoque_minimo": "Mínimo",
                "vendas_90d": "Vendas 90d",
                "media_mensal": "Média/Mês",
                "deficit": "Déficit",
                "sugestao_compra": "Sugestão Compra",
                "valor_custo": "Custo Unit.",
                "valor_venda": "Preço Venda",
                "custo_sugestao": "Custo Total",
            })
            df_comp["Custo Unit."] = df_comp["Custo Unit."].apply(brl)
            df_comp["Preço Venda"] = df_comp["Preço Venda"].apply(brl)
            df_comp["Custo Total"] = df_comp["Custo Total"].apply(brl)
            df_comp["Sugestão Compra"] = df_comp["Sugestão Compra"].apply(lambda x: f"{x:.0f} un")
            df_comp["Média/Mês"] = df_comp["Média/Mês"].apply(lambda x: f"{x:.1f}")
            df_comp = df_comp[[
                "Produto", "Grupo", "Estoque Atual", "Mínimo",
                "Vendas 90d", "Média/Mês", "Déficit",
                "Sugestão Compra", "Custo Unit.", "Custo Total"
            ]]
            tabela_html(df_comp, altura=450)

            # Gráfico top 15 por custo
            top15 = sorted(dados_comp, key=lambda x: x["custo_sugestao"], reverse=True)[:15]
            if top15:
                fig_comp = go.Figure(go.Bar(
                    x=[r["custo_sugestao"] for r in top15],
                    y=[r["nome"][:35] for r in top15],
                    orientation="h",
                    marker_color="#3b82f6",
                    text=[brl(r["custo_sugestao"]) for r in top15],
                    textposition="outside",
                    textfont=dict(color="#e2e8f0", size=10),
                ))
                fig_comp.update_layout(
                    plot_bgcolor="#ffffff", paper_bgcolor="rgba(0,0,0,0)",
                    xaxis=dict(gridcolor="#e9ecef", tickfont=dict(color="#6c757d"),
                               tickprefix="R$ ", tickformat=",.0f"),
                    yaxis=dict(gridcolor="#e9ecef", tickfont=dict(color="#6c757d")),
                    height=max(300, len(top15) * 30),
                    margin=dict(l=10, r=80, t=20, b=10),
                    showlegend=False,
                    title=dict(text="Top 15 por custo de reposição", font=dict(color="#94a3b8", size=13))
                )
                st.plotly_chart(fig_comp, use_container_width=True)


    # ── SUB 6: Equivalências ──────────────────────────────────────────────────
    with sub6:
        st.markdown("### 🔗 Gerenciar Equivalências de Produtos")
        st.caption(
            "Agrupe produtos de fabricantes diferentes que servem à mesma aplicação. "
            "O painel de alertas usa o estoque consolidado do grupo para evitar compras duplicadas."
        )

        # ── Funções locais de equivalência ────────────────────────────────────
        def _init_equiv():
            c = get_connection()
            c.execute("""CREATE TABLE IF NOT EXISTS equivalencias (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                grupo_id TEXT NOT NULL, produto_id TEXT NOT NULL,
                criado_em TEXT DEFAULT (datetime('now')),
                UNIQUE(grupo_id, produto_id))""")
            c.execute("CREATE INDEX IF NOT EXISTS idx_equiv_grupo ON equivalencias(grupo_id)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_equiv_produto ON equivalencias(produto_id)")
            c.commit(); c.close()

        @st.cache_data(ttl=60)
        def _load_equiv():
            c = get_connection()
            try:
                df = pd.read_sql("""
                    SELECT e.grupo_id, e.produto_id, p.nome, p.codigo_interno,
                           p.estoque, p.valor_custo
                    FROM equivalencias e JOIN produtos p ON p.id = e.produto_id
                    ORDER BY e.grupo_id, p.nome""", c)
            except Exception:
                df = pd.DataFrame(columns=["grupo_id","produto_id","nome",
                                           "codigo_interno","estoque","valor_custo"])
            c.close()
            return df

        @st.cache_data(ttl=60)
        def _load_prods_equiv():
            c = get_connection()
            df = pd.read_sql("""SELECT id, nome, codigo_interno, estoque
                FROM produtos WHERE ativo='1' AND movimenta_estoque='1'
                ORDER BY nome""", c)
            c.close()
            return df

        def _criar_grupo(ids):
            c = get_connection()
            placeholders = ",".join(["?"]*len(ids))
            exist = c.execute(
                f"SELECT grupo_id FROM equivalencias WHERE produto_id IN ({placeholders}) LIMIT 1",
                ids).fetchone()
            gid = exist["grupo_id"] if exist else str(uuid.uuid4())[:8]
            for pid in ids:
                c.execute("INSERT OR IGNORE INTO equivalencias (grupo_id, produto_id) VALUES (?,?)",
                          (gid, pid))
            c.commit(); c.close()
            return gid

        def _remover_membro(produto_id):
            c = get_connection()
            row = c.execute("SELECT grupo_id FROM equivalencias WHERE produto_id=?",
                            (produto_id,)).fetchone()
            if not row:
                c.close(); return
            gid = row["grupo_id"]
            c.execute("DELETE FROM equivalencias WHERE produto_id=?", (produto_id,))
            restantes = c.execute("SELECT COUNT(*) as n FROM equivalencias WHERE grupo_id=?",
                                  (gid,)).fetchone()["n"]
            if restantes < 2:
                c.execute("DELETE FROM equivalencias WHERE grupo_id=?", (gid,))
            c.commit(); c.close()

        def _add_membro(gid, pid):
            c = get_connection()
            c.execute("INSERT OR IGNORE INTO equivalencias (grupo_id, produto_id) VALUES (?,?)",
                      (gid, pid))
            c.commit(); c.close()

        _init_equiv()

        # ── Criar novo grupo ──────────────────────────────────────────────────
        prods_eq = _load_prods_equiv()
        opcoes_eq = dict(zip(
            prods_eq["nome"] + "  [" + prods_eq["codigo_interno"].fillna("") + "]",
            prods_eq["id"]
        ))

        with st.expander("➕ Criar novo grupo de equivalência", expanded=True):
            sel = st.multiselect(
                "Selecione 2 ou mais produtos equivalentes:",
                options=list(opcoes_eq.keys()),
                placeholder="Digite nome ou código...",
                key="equiv_sel"
            )
            if st.button("🔗 Criar Grupo", type="primary",
                         disabled=len(sel) < 2, key="btn_criar_equiv"):
                ids_sel = [opcoes_eq[s] for s in sel]
                gid = _criar_grupo(ids_sel)
                st.success(f"✅ Grupo `{gid}` criado com {len(ids_sel)} produtos!")
                st.cache_data.clear()
                st.rerun()

        st.markdown("---")

        # ── Grupos existentes ─────────────────────────────────────────────────
        equiv_df = _load_equiv()

        if equiv_df.empty:
            st.info("Nenhum grupo cadastrado ainda.")
        else:
            st.markdown(f"**{equiv_df['grupo_id'].nunique()} grupo(s) cadastrado(s)**")

            for gid, grp in equiv_df.groupby("grupo_id"):
                est_total = int(grp["estoque"].sum())
                codigos = "  ↔  ".join(
                    str(c) for c in grp["codigo_interno"].fillna("—").tolist()
                )
                nomes_curtos = "  ↔  ".join(
                    str(n)[:30] for n in grp["nome"].tolist()
                )

                with st.expander(
                    f"🔗  {codigos}   ·   Est. consolidado: {est_total} un.  "
                    f"  —  {nomes_curtos}", expanded=False
                ):
                    for _, mb in grp.iterrows():
                        c1, c2, c3 = st.columns([5, 2, 1])
                        with c1:
                            st.markdown(f"**{mb['nome']}**")
                            st.caption(mb["codigo_interno"] or "—")
                        with c2:
                            est_mb = int(mb["estoque"])
                            cor_mb = "#10b981" if est_mb > 0 else "#ef4444"
                            st.markdown(
                                f"<span style='color:{cor_mb};font-weight:700;font-size:1.05rem;'>"
                                f"{est_mb} un.</span>", unsafe_allow_html=True)
                        with c3:
                            if st.button("❌", key=f"rm_{gid}_{mb['produto_id']}",
                                         help="Remover do grupo"):
                                _remover_membro(mb["produto_id"])
                                st.cache_data.clear()
                                st.rerun()

                    st.markdown("---")
                    ids_no_grupo = list(grp["produto_id"])
                    opcoes_add = {k: v for k, v in opcoes_eq.items()
                                  if v not in ids_no_grupo}
                    col_add1, col_add2 = st.columns([4, 1])
                    with col_add1:
                        novo_mb = st.selectbox("Adicionar produto:",
                            options=[""] + list(opcoes_add.keys()),
                            key=f"add_sel_{gid}", label_visibility="collapsed")
                    with col_add2:
                        if st.button("➕ Adicionar", key=f"btn_add_{gid}",
                                     disabled=not novo_mb):
                            _add_membro(gid, opcoes_add[novo_mb])
                            st.cache_data.clear()
                            st.rerun()

# ── rodapé ─────────────────────────────────────────────────────────────────────
st.markdown("<hr/>", unsafe_allow_html=True)
st.markdown(
    f"<p style='text-align:center;color:#475569;font-size:.75rem;'>"
    f"MR4 Distribuidora · Painel do Gestor · Atualizado às {hora}</p>",
    unsafe_allow_html=True
)
