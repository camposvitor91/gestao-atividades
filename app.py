import sqlite3
from contextlib import contextmanager
from datetime import date, timedelta

import pandas as pd
import streamlit as st


# =========================================================
# CONFIGURAÇÃO DA PÁGINA
# =========================================================
st.set_page_config(
    page_title="Sistema de Gestão de Atividades",
    page_icon="📋",
    layout="wide",
)


# =========================================================
# ESTILO
# =========================================================
st.markdown(
    """
    <style>
        :root {
            --bg:       #0b0e14;
            --surface:  #111520;
            --card:     #161b27;
            --border:   #1f2535;
            --accent:   #4c7cf3;
            --text:     #dde1f0;
            --muted:    #5e6484;
            --danger:   #c94f4f;
            --danger-bg:#1f1117;
        }
        .block-container {
            padding-top: 3rem !important;
            padding-bottom: 0.35rem;
            max-width: 96%;
        }
        hr {
            margin-top: 0.25rem !important;
            margin-bottom: 0.25rem !important;
            border-color: var(--border) !important;
        }
        div.stButton > button {
            background-color: var(--card) !important;
            color: var(--text) !important;
            border: 1px solid var(--border) !important;
            border-radius: 6px !important;
            font-size: 0.82rem !important;
            letter-spacing: 0.04em !important;
            transition: background 0.2s, border-color 0.2s !important;
        }
        div.stButton > button:hover {
            background-color: var(--accent) !important;
            border-color: var(--accent) !important;
            color: #fff !important;
        }
        .stExpander {
            background-color: var(--card) !important;
            border: 1px solid var(--border) !important;
            border-radius: 6px !important;
            margin-bottom: 0.15rem !important;
        }
        div[data-testid="stSelectbox"] > div > div {
            background-color: var(--card) !important;
            border-color: var(--border) !important;
            color: var(--text) !important;
        }
        .stDataFrame { font-size: 10.5px !important; }
        h3 {
            color: var(--text) !important;
            font-weight: 500 !important;
            letter-spacing: 0.05em !important;
            margin-top: 0.5rem !important;
            margin-bottom: 0.5rem !important;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("📋 SISTEMA DE GESTÃO DE ATIVIDADES")


# =========================================================
# CONSTANTES
# =========================================================
DB_NAME = "gestao_atividades.db"

PESO_CRITICIDADE = {"BAIXA": 1, "MÉDIA": 2, "ALTA": 3}
PESO_FREQUENCIA  = {"ISOLADA": 1, "MENSAL": 2, "QUINZENAL": 3, "SEMANAL": 4, "DIÁRIA": 5}
PESO_STATUS      = {"NÃO INICIADA": 3, "EM ANDAMENTO": 2, "PENDENTE": 4, "PENDENTE APROVAÇÃO": 4, "CONCLUÍDA": 1}

OPCOES_TIPO        = ["ESTRATÉGIA", "OPERACIONAL", "EXTERNO"]
OPCOES_CRITICIDADE = ["BAIXA", "MÉDIA", "ALTA"]
OPCOES_FREQUENCIA  = ["DIÁRIA", "SEMANAL", "QUINZENAL", "MENSAL", "ISOLADA"]
OPCOES_STATUS      = ["NÃO INICIADA", "EM ANDAMENTO", "CONCLUÍDA"]
OPCOES_CRIADO_POR  = ["GESTOR", "COLABORADOR"]

STATUS_CONCLUIDOS = {"CONCLUÍDA", "CONCLUIDA"}


# =========================================================
# BANCO DE DADOS
# =========================================================
def conectar() -> sqlite3.Connection:
    return sqlite3.connect(DB_NAME, check_same_thread=False)


@contextmanager
def get_conn():
    conn = conectar()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def criar_tabela() -> None:
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS atividades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tipo TEXT, macro_acao TEXT, micro_acao TEXT,
                criticidade TEXT, frequencia TEXT, data_referencia TEXT,
                tempo REAL, prazo TEXT, status TEXT, responsavel TEXT,
                origem TEXT, criado_por TEXT, aprovacao_gestor TEXT,
                aprovacao_conclusao TEXT, percentual_conclusao REAL
            )""")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS historico (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                atividade_id INTEGER NOT NULL, data_hora TEXT NOT NULL,
                acao TEXT NOT NULL, campo TEXT, valor_anterior TEXT,
                valor_novo TEXT, feito_por TEXT
            )""")


def registrar_historico(atividade_id: int, acao: str, campo: str = "",
                        valor_anterior: str = "", valor_novo: str = "",
                        feito_por: str = "Sistema") -> None:
    from datetime import datetime as _dt
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO historico (atividade_id,data_hora,acao,campo,valor_anterior,valor_novo,feito_por) VALUES(?,?,?,?,?,?,?)",
            (atividade_id, _dt.now().strftime("%d/%m/%Y %H:%M"), acao, campo, valor_anterior, valor_novo, feito_por),
        )


def listar_historico(atividade_id: int) -> pd.DataFrame:
    conn = conectar()
    df = pd.read_sql_query("SELECT * FROM historico WHERE atividade_id=? ORDER BY id DESC", conn, params=(atividade_id,))
    conn.close()
    return df


def inserir_linha(dados: tuple) -> None:
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO atividades (tipo,macro_acao,micro_acao,criticidade,frequencia,
            data_referencia,tempo,prazo,status,responsavel,origem,criado_por,
            aprovacao_gestor,aprovacao_conclusao,percentual_conclusao)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", dados)
    listar.clear()


@st.cache_data(ttl=5)
def listar() -> pd.DataFrame:
    conn = conectar()
    df = pd.read_sql_query("SELECT * FROM atividades ORDER BY id DESC", conn)
    conn.close()
    if "percentual_conclusao" not in df.columns:
        df["percentual_conclusao"] = 0
    for coluna in ["origem","criado_por","aprovacao_gestor","aprovacao_conclusao","data_referencia"]:
        if coluna not in df.columns:
            df[coluna] = ""
    return df


def limpar_tabela() -> None:
    with get_conn() as conn:
        conn.execute("DELETE FROM atividades")
    listar.clear()


def apagar_atividade(id_atividade: int) -> None:
    with get_conn() as conn:
        conn.execute("DELETE FROM atividades WHERE id=?", (id_atividade,))
    listar.clear()


def listar_responsaveis() -> list[str]:
    df = listar()
    if df.empty or "responsavel" not in df.columns:
        return []
    serie = df["responsavel"].fillna("").astype(str).str.strip()
    valores = serie.unique().tolist()
    return sorted(valores, key=lambda x: (x != "", x.lower()))


def normalizar_colunas_excel(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(col).strip().upper() for col in df.columns]
    return df


def obter_valor_linha(row, *nomes, default=""):
    for nome in nomes:
        if nome in row and pd.notna(row[nome]):
            return row[nome]
    return default


# =========================================================
# IMPORTAÇÃO DO EXCEL
# =========================================================
def importar_excel(arquivo) -> int:
    xl = pd.ExcelFile(arquivo)
    total = 0
    for aba in xl.sheet_names:
        try:
            df = pd.read_excel(arquivo, sheet_name=aba, header=0)
        except Exception:
            continue
        if df.empty:
            continue
        df = normalizar_colunas_excel(df)
        for _, row in df.iterrows():
            micro_acao = obter_valor_linha(row, "Micro ação", "MICRO ACAO", default="")
            if pd.isna(micro_acao) or str(micro_acao).strip() == "":
                continue
            try:    tempo_valor = float(obter_valor_linha(row,"TEMPO",default=0))
            except: tempo_valor = 0.0
            try:    percentual_valor = float(obter_valor_linha(row,"% CONCLUSÃO","% CONCLUSAO",default=0))
            except: percentual_valor = 0.0
            prazo_valor = obter_valor_linha(row,"Prazo",default="")
            if pd.notna(prazo_valor) and str(prazo_valor).strip():
                try:    prazo_valor = pd.to_datetime(prazo_valor).strftime("%d/%m/%Y")
                except: prazo_valor = str(prazo_valor).strip()
            else:
                prazo_valor = ""
            responsavel_valor = obter_valor_linha(row,"RESPONSÁVEL 1","RESPONSAVEL 1","Responsável","RESPONSAVEL",default="")
            inserir_linha((
                str(obter_valor_linha(row,"Tipo",default="")),
                str(obter_valor_linha(row,"Macro ação","MACRO ACAO",default="")),
                str(micro_acao),
                str(obter_valor_linha(row,"Criticidade",default="")),
                str(obter_valor_linha(row,"Frequência","FREQUENCIA",default="")),
                "", tempo_valor, prazo_valor,
                str(obter_valor_linha(row,"Status",default="")),
                str(responsavel_valor).strip(), aba, "IMPORTAÇÃO EXCEL",
                "APROVADA", "NÃO SE APLICA", percentual_valor,
            ))
            total += 1
    return total


# =========================================================
# REGRAS DE NEGÓCIO
# =========================================================
def calcular_proximo_prazo(frequencia: str, data_ref: date) -> date:
    mapa = {"DIÁRIA": timedelta(days=1), "SEMANAL": timedelta(days=7),
            "QUINZENAL": timedelta(days=15), "MENSAL": timedelta(days=30)}
    return data_ref + mapa.get(frequencia, timedelta(0))


def gerar_recorrencia(id_atividade: int) -> bool:
    df = listar()
    row = df[df["id"] == id_atividade]
    if row.empty:
        return False
    r = row.iloc[0]
    frequencia = str(r["frequencia"]).upper().strip()
    if frequencia == "ISOLADA" or not frequencia:
        return False
    try:
        from datetime import datetime as _dt
        prazo_atual_dt = _dt.strptime(str(r["prazo"]).strip(), "%d/%m/%Y").date()
    except Exception:
        return False
    proximo_prazo = calcular_proximo_prazo(frequencia, prazo_atual_dt)
    proximo_prazo_str = proximo_prazo.strftime("%d/%m/%Y")
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO atividades(tipo,macro_acao,micro_acao,criticidade,frequencia,
            data_referencia,tempo,prazo,status,responsavel,origem,criado_por,
            aprovacao_gestor,aprovacao_conclusao,percentual_conclusao)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (str(r["tipo"]),str(r["macro_acao"]),str(r["micro_acao"]),str(r["criticidade"]),
             frequencia,str(r["prazo"]),float(r["tempo"]),proximo_prazo_str,"NÃO INICIADA",
             str(r["responsavel"]),"RECORRÊNCIA AUTOMÁTICA",str(r["criado_por"]),
             "APROVADA","NÃO SE APLICA",0))
        novo_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    registrar_historico(novo_id,"Recorrência automática","origem",
                        f"gerada a partir da atividade #{id_atividade}",proximo_prazo_str,"Sistema")
    listar.clear()
    return True


def aprovar_inicio(id_atividade: int) -> None:
    with get_conn() as conn:
        conn.execute("UPDATE atividades SET aprovacao_gestor='APROVADA',status='NÃO INICIADA' WHERE id=?", (id_atividade,))
    registrar_historico(id_atividade,"Aprovação de início","status","PENDENTE APROVAÇÃO","NÃO INICIADA","Gestor")
    listar.clear()


def reprovar_inicio(id_atividade: int) -> None:
    with get_conn() as conn:
        conn.execute("UPDATE atividades SET aprovacao_gestor='REPROVADA',status='PENDENTE APROVAÇÃO' WHERE id=?", (id_atividade,))
    registrar_historico(id_atividade,"Reprovação de início","aprovacao_gestor","PENDENTE APROVAÇÃO","REPROVADA","Gestor")
    listar.clear()


def aprovar_conclusao(id_atividade: int) -> None:
    with get_conn() as conn:
        conn.execute("UPDATE atividades SET aprovacao_conclusao='APROVADA',status='CONCLUÍDA',percentual_conclusao=100 WHERE id=?", (id_atividade,))
    registrar_historico(id_atividade,"Aprovação de conclusão","status","AGUARDANDO APROVAÇÃO","CONCLUÍDA","Gestor")
    listar.clear()
    gerar_recorrencia(id_atividade)


def reprovar_conclusao(id_atividade: int) -> None:
    with get_conn() as conn:
        conn.execute("UPDATE atividades SET aprovacao_conclusao='REPROVADA',status='EM ANDAMENTO' WHERE id=?", (id_atividade,))
    registrar_historico(id_atividade,"Reprovação de conclusão","status","AGUARDANDO APROVAÇÃO","EM ANDAMENTO","Gestor")
    listar.clear()


def editar_atividade(id_atividade: int, dados: dict) -> None:
    df_atual = listar()
    row_atual = df_atual[df_atual["id"] == id_atividade]
    with get_conn() as conn:
        conn.execute("""UPDATE atividades SET tipo=?,macro_acao=?,micro_acao=?,criticidade=?,
            frequencia=?,tempo=?,prazo=?,responsavel=? WHERE id=?""",
            (dados["tipo"],dados["macro_acao"],dados["micro_acao"],dados["criticidade"],
             dados["frequencia"],dados["tempo"],dados["prazo"],dados["responsavel"],id_atividade))
    if not row_atual.empty:
        for campo in ["tipo","macro_acao","micro_acao","criticidade","frequencia","tempo","prazo","responsavel"]:
            ant = str(row_atual[campo].values[0])
            nov = str(dados[campo])
            if ant != nov:
                registrar_historico(id_atividade,"Edição de campo",campo,ant,nov)
    listar.clear()


def gerar_excel(df: pd.DataFrame) -> bytes:
    import io
    colunas = ["id","responsavel","tipo","macro_acao","micro_acao","criticidade","frequencia",
               "tempo","prazo","status","percentual_conclusao","score","horas_em_aberto"]
    cols_ex = [c for c in colunas if c in df.columns]
    df_ex = df[cols_ex].copy()
    df_ex.columns = [c.replace("_"," ").upper() for c in df_ex.columns]
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df_ex.to_excel(w, index=False, sheet_name="Atividades")
        ws = w.sheets["Atividades"]
        for col in ws.columns:
            mx = max(len(str(c.value or "")) for c in col)
            ws.column_dimensions[col[0].column_letter].width = min(mx+4, 60)
    return buf.getvalue()


def atualizar_status(id_atividade: int, novo_status: str, percentual: float) -> None:
    aprovacao = "AGUARDANDO APROVAÇÃO" if novo_status == "CONCLUÍDA" else "NÃO SE APLICA"
    df_atual = listar()
    row_atual = df_atual[df_atual["id"] == id_atividade]
    status_ant = str(row_atual["status"].values[0]) if not row_atual.empty else ""
    perc_ant   = str(row_atual["percentual_conclusao"].values[0]) if not row_atual.empty else ""
    with get_conn() as conn:
        conn.execute("UPDATE atividades SET status=?,percentual_conclusao=?,aprovacao_conclusao=? WHERE id=?",
                     (novo_status, percentual, aprovacao, id_atividade))
    registrar_historico(id_atividade,"Atualização de status","status",status_ant,novo_status)
    if str(int(percentual)) != str(int(float(perc_ant or 0))):
        registrar_historico(id_atividade,"Atualização de %","% conclusão",perc_ant,str(int(percentual)))
    listar.clear()


# =========================================================
# PRAZO / SCORE
# =========================================================
def converter_prazo_misto(serie: pd.Series) -> tuple[pd.Series, pd.Series]:
    prazo_texto = serie.fillna("").astype(str).str.strip()
    prazo_dt    = pd.to_datetime(prazo_texto, format="%d/%m/%Y", errors="coerce")
    mask = prazo_dt.isna() & prazo_texto.ne("")
    if mask.any():
        prazo_dt.loc[mask] = pd.to_datetime(prazo_texto.loc[mask], errors="coerce")
    prazo_final = prazo_texto.copy()
    ok = prazo_dt.notna()
    prazo_final.loc[ok] = prazo_dt.loc[ok].dt.strftime("%d/%m/%Y")
    return prazo_dt, prazo_final


def preparar_dashboard(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df["tempo"]                = pd.to_numeric(df["tempo"],                errors="coerce").fillna(0)
    df["percentual_conclusao"] = pd.to_numeric(df["percentual_conclusao"], errors="coerce").fillna(0)
    df["prazo_dt"], df["prazo"] = converter_prazo_misto(df["prazo"])
    hoje = pd.Timestamp.today().normalize()
    status_upper = df["status"].fillna("").astype(str).str.upper()
    df["atrasada"]        = (df["prazo_dt"] < hoje) & (~status_upper.isin(STATUS_CONCLUIDOS))
    df["horas_em_aberto"] = df["tempo"] * (100 - df["percentual_conclusao"]) / 100
    df.loc[status_upper.isin(STATUS_CONCLUIDOS), "horas_em_aberto"] = 0
    df["horas_em_aberto"] = df["horas_em_aberto"].round(2)
    df["peso_criticidade"] = df["criticidade"].fillna("").astype(str).str.upper().map(PESO_CRITICIDADE).fillna(1)
    df["peso_frequencia"]  = df["frequencia"].fillna("").astype(str).str.upper().map(PESO_FREQUENCIA).fillna(1)
    df["peso_status"]      = df["status"].fillna("").astype(str).str.upper().map(PESO_STATUS).fillna(1)
    dias_para_prazo = (df["prazo_dt"] - hoje).dt.days.fillna(9999)
    df["peso_prazo"] = pd.cut(dias_para_prazo, bins=[-float("inf"),0,1,3,7,float("inf")],
                               labels=[5,4,3,2,1], right=True).astype(float)
    df["peso_tempo"] = (df["tempo"]/8).clip(lower=1)
    df["score"] = (df["peso_criticidade"]*3 + df["peso_frequencia"]*2 + df["peso_status"]*2
                   + df["peso_prazo"]*3 + df["peso_tempo"]).round(2)
    return df


def formatar_df_numeros(df: pd.DataFrame, colunas_numericas: list[str]) -> pd.DataFrame:
    df = df.copy()
    for col in colunas_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


# =========================================================
# INICIALIZAÇÃO
# =========================================================
def fazer_backup() -> None:
    import shutil, os
    from datetime import datetime as _dt
    if not os.path.exists(DB_NAME):
        return
    pasta = "backups"
    os.makedirs(pasta, exist_ok=True)
    nome = f"{pasta}/gestao_atividades_{_dt.today().strftime('%Y%m%d')}.db"
    if not os.path.exists(nome):
        shutil.copy2(DB_NAME, nome)
        arqs = sorted([f for f in os.listdir(pasta) if f.endswith(".db")], reverse=True)
        for a in arqs[7:]:
            os.remove(os.path.join(pasta, a))


criar_tabela()
fazer_backup()


# =========================================================
# AUTENTICAÇÃO
# =========================================================
USUARIOS = dict(st.secrets.get("usuarios", {
    "gestor":      "gestor123",
    "colaborador": "colab123",
}))


def tela_login():
    _, col, _ = st.columns([1, 1.2, 1])
    with col:
        st.markdown(
            """
            <div style="background:#161b27;border:1px solid #1f2535;border-radius:10px;
                        padding:36px 40px 20px 40px;margin-top:5rem;text-align:center;">
                <div style="font-size:2.2rem;margin-bottom:8px;">📋</div>
                <div style="font-size:1rem;font-weight:700;color:#dde1f0;letter-spacing:.04em;">
                    Sistema de Gestão de Atividades
                </div>
                <div style="font-size:0.72rem;color:#5e6484;margin-top:6px;">
                    Acesso restrito — faça login para continuar
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        with st.form("form_login", clear_on_submit=False):
            usuario = st.text_input("Usuário", key="login_usuario")
            senha   = st.text_input("Senha",   key="login_senha", type="password")
            entrar  = st.form_submit_button("Entrar", use_container_width=True)
        if entrar:
            if usuario in USUARIOS and USUARIOS[usuario] == senha:
                st.session_state["autenticado"]    = True
                st.session_state["usuario_logado"] = usuario
                st.rerun()
            else:
                st.error("Usuário ou senha incorretos.")


if not st.session_state.get("autenticado"):
    tela_login()
    st.stop()


# =========================================================
# NAVEGAÇÃO
# =========================================================
if "pagina" not in st.session_state:
    st.session_state.pagina = "Dashboard"

for _chave, _padrao in [
    ("dash_filtro_resp",   "TODOS"),
    ("dash_filtro_status", "TODOS"),
    ("dash_filtro_crit",   "TODOS"),
]:
    if _chave not in st.session_state:
        st.session_state[_chave] = _padrao

_pag_ativa      = st.session_state.get("pagina", "Dashboard")
_usuario_logado = st.session_state.get("usuario_logado", "")

_df_nav = listar()
_pendencias = 0
if not _df_nav.empty:
    _pendencias = int(
        (_df_nav["aprovacao_gestor"].fillna("").astype(str).str.upper()    == "PENDENTE APROVAÇÃO").sum() +
        (_df_nav["aprovacao_conclusao"].fillna("").astype(str).str.upper() == "AGUARDANDO APROVAÇÃO").sum()
    )
_label_aprov = f"Aprovações ({_pendencias})" if _pendencias > 0 else "Aprovações do Gestor"

st.markdown(
    f'<div style="font-size:0.65rem;color:#5e6484;margin-bottom:4px;">'
    f'&#128100; <b style="color:#dde1f0;">{_usuario_logado.upper()}</b>'
    f'&nbsp;&nbsp;<a href="?sair=1" target="_self" style="color:#c94f4f;text-decoration:none;font-weight:600;">sair</a>'
    f'</div>',
    unsafe_allow_html=True,
)

if st.query_params.get("sair") == "1":
    for _k in ["autenticado", "usuario_logado", "pagina"]:
        st.session_state.pop(_k, None)
    st.query_params.clear()
    st.rerun()

col1, col2, col3, col4, col5, col6 = st.columns(6)

with col1:
    if st.button("Dashboard",            use_container_width=True,
                 type="primary" if _pag_ativa == "Dashboard"  else "secondary"):
        st.session_state.pagina = "Dashboard"
with col2:
    if st.button(_label_aprov,           use_container_width=True,
                 type="primary" if _pag_ativa == "Aprovações" else "secondary"):
        st.session_state.pagina = "Aprovações"
with col3:
    if st.button("Atualizar Atividades", use_container_width=True,
                 type="primary" if _pag_ativa == "Atualizar"  else "secondary"):
        st.session_state.pagina = "Atualizar"
with col4:
    if st.button("Nova Atividade",       use_container_width=True,
                 type="primary" if _pag_ativa == "Nova"       else "secondary"):
        st.session_state.pagina = "Nova"
with col5:
    if st.button("Simular Atividade",    use_container_width=True,
                 type="primary" if _pag_ativa == "Simulador"  else "secondary"):
        st.session_state.pagina = "Simulador"
with col6:
    with st.popover("Outros  ▾", use_container_width=True):
        if st.button("📥 Importar Excel", use_container_width=True, key="nav_importar"):
            st.session_state.pagina = "Importar"
            st.rerun()
        if st.button("📖 Definições", use_container_width=True, key="nav_definicoes"):
            st.session_state.pagina = "Definições"
            st.rerun()

st.markdown(
    """<style>
    div[data-testid="stHorizontalBlock"] > div:last-child [data-testid="stPopover"] > button {
        height: 2.4rem !important; min-height: 0 !important;
        margin-top: 0 !important; width: 100% !important;
    }
    </style>""",
    unsafe_allow_html=True,
)


# =========================================================
# DASHBOARD
# =========================================================
if st.session_state.pagina == "Dashboard":
    df = listar()
    df = preparar_dashboard(df)

    if df.empty:
        st.info("Nenhuma atividade cadastrada.")
    else:
        if st.session_state.pop("_reset_filtros", False):
            st.session_state.dash_filtro_resp   = "TODOS"
            st.session_state.dash_filtro_status = "TODOS"
            st.session_state.dash_filtro_crit   = "TODOS"
            st.session_state.dash_busca         = ""

        responsaveis_opts = ["TODOS"] + sorted(df["responsavel"].dropna().astype(str).unique().tolist())
        status_opts       = ["TODOS"] + sorted(df["status"].dropna().astype(str).unique().tolist())
        crit_opts         = ["TODOS"] + sorted(df["criticidade"].dropna().astype(str).unique().tolist())

        if "dash_busca" not in st.session_state:
            st.session_state.dash_busca = ""

        if st.session_state.dash_filtro_resp   not in responsaveis_opts:
            st.session_state.dash_filtro_resp   = "TODOS"
        if st.session_state.dash_filtro_status not in status_opts:
            st.session_state.dash_filtro_status = "TODOS"
        if st.session_state.dash_filtro_crit   not in crit_opts:
            st.session_state.dash_filtro_crit   = "TODOS"

        st.markdown("### Filtros")
        f1, f2, f3, f4, f5 = st.columns([2, 2, 2, 2, 1])
        with f1:
            filtro_resp = st.selectbox("Responsável", responsaveis_opts,
                index=responsaveis_opts.index(st.session_state.dash_filtro_resp), key="dash_filtro_resp")
        with f2:
            filtro_status = st.selectbox("Status", status_opts,
                index=status_opts.index(st.session_state.dash_filtro_status), key="dash_filtro_status")
        with f3:
            filtro_crit = st.selectbox("Criticidade", crit_opts,
                index=crit_opts.index(st.session_state.dash_filtro_crit), key="dash_filtro_crit")
        with f4:
            busca = st.text_input("🔍 Buscar", placeholder="Macro ação, micro ação...", key="dash_busca")
        with f5:
            st.markdown('<p style="font-size:0.875rem;margin-bottom:4px;visibility:hidden;">_</p>', unsafe_allow_html=True)
            if st.button("🧹 Limpar", use_container_width=True):
                st.session_state["_reset_filtros"] = True
                st.rerun()

        df_filtrado = df.copy()
        if filtro_resp   != "TODOS": df_filtrado = df_filtrado[df_filtrado["responsavel"] == filtro_resp]
        if filtro_status != "TODOS": df_filtrado = df_filtrado[df_filtrado["status"]      == filtro_status]
        if filtro_crit   != "TODOS": df_filtrado = df_filtrado[df_filtrado["criticidade"] == filtro_crit]
        if busca.strip():
            _t = busca.strip().lower()
            df_filtrado = df_filtrado[
                df_filtrado["macro_acao"].fillna("").astype(str).str.lower().str.contains(_t) |
                df_filtrado["micro_acao"].fillna("").astype(str).str.lower().str.contains(_t)
            ]

        df_filtrado  = df_filtrado.sort_values("score", ascending=False)
        status_upper = df_filtrado["status"].fillna("").astype(str).str.upper()

        st.divider()
        st.markdown("### Indicadores")

        total        = len(df_filtrado)
        em_andamento = len(df_filtrado[status_upper == "EM ANDAMENTO"])
        atrasadas    = int(df_filtrado["atrasada"].sum())
        concluidas   = len(df_filtrado[status_upper.isin(STATUS_CONCLUIDOS)])

        st.markdown(
            '<div style="font-size:0.72rem;font-weight:600;color:#5e6484;text-transform:uppercase;'
            'letter-spacing:.08em;margin-bottom:6px;">Resumo geral</div>',
            unsafe_allow_html=True,
        )
        _cor_at = "c94f4f" if atrasadas > 0 else "dde1f0"
        st.markdown(
            f'<div style="display:flex;gap:0.6rem;margin-bottom:0.5rem;">'
            f'<div style="flex:1;background:#161b27;border:1px solid #1f2535;border-radius:6px;padding:8px 12px;text-align:center;">'
            f'<div style="font-size:0.65rem;color:#5e6484;text-transform:uppercase;letter-spacing:.08em;margin-bottom:2px;">Total</div>'
            f'<div style="font-size:1.4rem;font-weight:700;color:#dde1f0;">{total}</div></div>'
            f'<div style="flex:1;background:#161b27;border:1px solid #1f2535;border-radius:6px;padding:8px 12px;text-align:center;">'
            f'<div style="font-size:0.65rem;color:#5e6484;text-transform:uppercase;letter-spacing:.08em;margin-bottom:2px;">Em Andamento</div>'
            f'<div style="font-size:1.4rem;font-weight:700;color:#4c7cf3;">{em_andamento}</div></div>'
            f'<div style="flex:1;background:#161b27;border:1px solid #1f2535;border-radius:6px;padding:8px 12px;text-align:center;">'
            f'<div style="font-size:0.65rem;color:#5e6484;text-transform:uppercase;letter-spacing:.08em;margin-bottom:2px;">Atrasadas</div>'
            f'<div style="font-size:1.4rem;font-weight:700;color:#{_cor_at};">{atrasadas}</div></div>'
            f'<div style="flex:1;background:#161b27;border:1px solid #1f2535;border-radius:6px;padding:8px 12px;text-align:center;">'
            f'<div style="font-size:0.65rem;color:#5e6484;text-transform:uppercase;letter-spacing:.08em;margin-bottom:2px;">Concluídas</div>'
            f'<div style="font-size:1.4rem;font-weight:700;color:#3ecf8e;">{concluidas}</div></div>'
            f'</div>',
            unsafe_allow_html=True,
        )

        st.markdown(
            '<div style="font-size:0.72rem;font-weight:600;color:#5e6484;text-transform:uppercase;'
            'letter-spacing:.08em;margin-top:0.8rem;margin-bottom:6px;">Comprometimento da equipe — próximas 4 semanas</div>',
            unsafe_allow_html=True,
        )

        CAPACIDADE_REF = 40.0
        _hoje = pd.Timestamp.today().normalize()
        df_todas = preparar_dashboard(listar())

        _n_pessoas     = df_todas["responsavel"].dropna().nunique()
        _cap_total_sem = _n_pessoas * CAPACIDADE_REF
        _horas_abertas = float(df_todas[
            ~df_todas["status"].fillna("").astype(str).str.upper().isin(STATUS_CONCLUIDOS)
        ]["horas_em_aberto"].sum())
        _semanas_backlog = (_horas_abertas / _cap_total_sem) if _cap_total_sem > 0 else 0
        _sobrecarregados = 0
        _max_sem_val  = 0.0
        _max_sem_resp = "—"
        for _rp in df_todas["responsavel"].dropna().unique():
            _grp_rp    = df_todas[df_todas["responsavel"] == _rp]
            _st_rp     = _grp_rp["status"].fillna("").astype(str).str.upper()
            _aberto_rp = _grp_rp[~_st_rp.isin(STATUS_CONCLUIDOS)]
            _h_ab      = float(_aberto_rp["horas_em_aberto"].sum())
            _sem_rp    = _h_ab / CAPACIDADE_REF
            _at_rp     = int(_aberto_rp["atrasada"].sum())
            _cr_rp     = int(_aberto_rp["criticidade"].fillna("").astype(str).str.upper().isin(["ALTA"]).sum())
            _idx_rp    = min((_sem_rp * 100) + _at_rp * 15 + _cr_rp * 8, 500)
            if _idx_rp > 100: _sobrecarregados += 1
            if _sem_rp > _max_sem_val:
                _max_sem_val  = _sem_rp
                _max_sem_resp = str(_rp)
        _pct_sobrec = (_sobrecarregados / _n_pessoas * 100) if _n_pessoas > 0 else 0
        _tx_atraso  = (atrasadas / total * 100) if total > 0 else 0
        _cor_diag   = "#c94f4f" if _pct_sobrec >= 50 else "#e07040" if _pct_sobrec >= 25 else "#e0a040"
        _icon_diag  = "🔴" if _pct_sobrec >= 50 else "🟠" if _pct_sobrec >= 25 else "🟡"

        _semanas = []
        for _w in range(4):
            _ini = _hoje + pd.Timedelta(days=_w * 7)
            _fim = _ini + pd.Timedelta(days=6)
            _semanas.append((_ini, _fim))

        def _label_semana(ini, fim): return f"{ini.strftime('%d/%m')} – {fim.strftime('%d/%m')}"
        def _cor_idx(v):
            if v <= 70:  return "#3ecf8e"
            if v <= 90:  return "#e0a040"
            if v <= 120: return "#e07040"
            return "#c94f4f"
        def _label_idx(v):
            if v <= 70:  return "Disponível"
            if v <= 90:  return "Moderado"
            if v <= 120: return "Alto"
            return "Sobrecarregado"

        _resp_list = sorted([r for r in df_todas["responsavel"].dropna().unique() if str(r).strip()], key=lambda x: x.lower())

        _th_semanas = "".join(
            f'<th style="padding:6px 10px;text-align:center;border-bottom:1px solid #1f2535;'
            f'color:#5e6484;font-size:0.68rem;font-weight:500;text-transform:uppercase;white-space:nowrap;">'
            f'Sem {i+1}<br><span style="font-size:0.6rem;font-weight:400;">{_label_semana(ini,fim)}</span></th>'
            for i, (ini, fim) in enumerate(_semanas)
        )
        _html_heat = (
            '<table style="width:100%;border-collapse:collapse;"><thead><tr style="background:#111520;">'
            '<th style="padding:6px 10px;text-align:left;border-bottom:1px solid #1f2535;'
            'color:#5e6484;font-size:0.68rem;font-weight:500;text-transform:uppercase;">Responsável</th>'
            + _th_semanas + '</tr></thead><tbody>'
        )

        for _resp in _resp_list:
            _grp        = df_todas[df_todas["responsavel"] == _resp]
            _st_up      = _grp["status"].fillna("").astype(str).str.upper()
            _grp_aberto = _grp[~_st_up.isin(STATUS_CONCLUIDOS)].copy()
            _horas_por_semana   = [0.0] * 4
            _atraso_por_semana  = [0]   * 4
            _critica_por_semana = [0]   * 4

            for _, _t in _grp_aberto.iterrows():
                _h    = float(_t["horas_em_aberto"])
                _pdt  = _t["prazo_dt"]
                _atrs = bool(_t.get("atrasada", False))
                _crit = str(_t.get("criticidade","")).upper() in ["ALTA"]
                _sdt  = []
                for _wi, (_ini, _fim) in enumerate(_semanas):
                    if pd.isna(_pdt) or _pdt > _fim: _sdt.append(_wi)
                    elif _pdt >= _ini:                _sdt.append(_wi)
                if not _sdt: _sdt = [0]
                _hps = _h / len(_sdt)
                for _wi in _sdt:
                    _horas_por_semana[_wi]   += _hps
                    if _atrs: _atraso_por_semana[_wi]   += 1
                    if _crit: _critica_por_semana[_wi]  += 1

            _html_heat += (
                f'<tr><td style="padding:7px 10px;border-bottom:1px solid #1a1f2e;'
                f'font-size:0.78rem;color:#dde1f0;font-weight:600;text-transform:uppercase;">{_resp}</td>'
            )
            for _wi in range(4):
                _idx = min((_horas_por_semana[_wi]/CAPACIDADE_REF)*100 + _atraso_por_semana[_wi]*15 + _critica_por_semana[_wi]*8, 200)
                _cor = _cor_idx(_idx)
                _lbl = _label_idx(_idx)
                _pct = min(_idx, 100)
                _html_heat += (
                    f'<td style="padding:6px 10px;border-bottom:1px solid #1a1f2e;min-width:130px;">'
                    f'<div style="display:flex;justify-content:space-between;margin-bottom:3px;">'
                    f'<span style="font-size:0.65rem;color:#5e6484;">{_lbl}</span>'
                    f'<span style="font-size:0.65rem;color:{_cor};font-weight:700;">{_idx:.0f}%</span></div>'
                    f'<div style="background:#1f2535;border-radius:99px;height:4px;">'
                    f'<div style="width:{_pct:.0f}%;background:{_cor};height:4px;border-radius:99px;"></div></div>'
                    f'<div style="font-size:0.6rem;color:#5e6484;margin-top:2px;">{_horas_por_semana[_wi]:.1f}h comprometidas</div>'
                    f'</td>'
                )
            _html_heat += '</tr>'

        _html_heat += '</tbody></table>'
        st.markdown(_html_heat, unsafe_allow_html=True)

        # Diagnóstico
        _df_aberto   = df_todas[~df_todas["status"].fillna("").astype(str).str.upper().isin(STATUS_CONCLUIDOS)]
        _deficit_cap = max(_horas_abertas - _cap_total_sem * 4, 0)
        _criticas_ab = int(_df_aberto["criticidade"].fillna("").astype(str).str.upper().isin(["ALTA"]).sum())
        _cor_backlog = "#c94f4f" if _semanas_backlog > 8 else "#e0a040" if _semanas_backlog > 4 else "#3ecf8e"
        _cor_deficit = "#c94f4f" if _deficit_cap > 0 else "#3ecf8e"
        _cor_atraso  = "#c94f4f" if _tx_atraso > 30 else "#e0a040" if _tx_atraso > 15 else "#dde1f0"

        def _met(titulo, valor, sub, cor="#dde1f0"):
            return (f'<div style="min-width:120px;">'
                    f'<div style="font-size:0.62rem;color:#5e6484;text-transform:uppercase;letter-spacing:.05em;margin-bottom:2px;">{titulo}</div>'
                    f'<div style="font-size:1.1rem;font-weight:700;color:{cor};">{valor}</div>'
                    f'<div style="font-size:0.6rem;color:#5e6484;">{sub}</div></div>')

        _diag_html = (
            f'<div style="background:#161b27;border:1px solid {_cor_diag};border-left:3px solid {_cor_diag};'
            f'border-radius:6px;padding:14px 16px;margin-top:0.6rem;">'
            f'<div style="font-size:0.75rem;font-weight:700;color:{_cor_diag};text-transform:uppercase;'
            f'letter-spacing:.08em;margin-bottom:12px;">{_icon_diag} Diagnóstico da equipe</div>'
            f'<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(140px,1fr));gap:1rem 2rem;">'
            + _met("Horas em aberto",         f"{_horas_abertas:.0f}h",             f"cap. {_cap_total_sem:.0f}h/sem · {_n_pessoas} pessoas")
            + _met("Déficit de capacidade",    f"{_deficit_cap:.0f}h",               "horas além das próximas 4 semanas", _cor_deficit)
            + _met("Semanas p/ zerar backlog", f"{_semanas_backlog:.1f}",             "sem novas demandas", _cor_backlog)
            + _met("Equipe em sobrecarga",     f"{_sobrecarregados} de {_n_pessoas}", f"{_pct_sobrec:.0f}% acima de 100%", _cor_diag)
            + _met("Mais sobrecarregada",      _max_sem_resp.upper(),                 f"{_max_sem_val:.1f} semanas de trabalho em aberto", "#c94f4f" if _max_sem_val > 8 else "#e07040")
            + _met("Taxa de atraso",           f"{_tx_atraso:.0f}%",                 f"{atrasadas} atividades fora do prazo", _cor_atraso)
            + _met("Críticas em aberto",       str(_criticas_ab),                    "alta ou crítica, não concluídas", "#e07040" if _criticas_ab > 5 else "#dde1f0")
            + '</div></div>'
        )
        st.markdown(_diag_html, unsafe_allow_html=True)

        st.divider()
        st.markdown("### Carga por responsável")

        carga = (df_filtrado.groupby("responsavel", dropna=False)
                 .agg(**{"QUANTIDADE DE ATIVIDADES": ("id","count"), "HORAS EM ABERTO": ("horas_em_aberto","sum")})
                 .reset_index().rename(columns={"responsavel":"Responsável"})
                 .sort_values("Responsável", key=lambda s: s.fillna("").astype(str).str.lower()))
        carga = formatar_df_numeros(carga, ["QUANTIDADE DE ATIVIDADES","HORAS EM ABERTO"])

        def carga_para_html(df: pd.DataFrame) -> str:
            linhas = ""
            for _, row in df.iterrows():
                resp = row["Responsável"] if pd.notna(row["Responsável"]) and str(row["Responsável"]).strip() else "—"
                qtd  = f"{int(row['QUANTIDADE DE ATIVIDADES']):,}".replace(",",".")
                hrs  = f"{row['HORAS EM ABERTO']:,.2f}".replace(",","X").replace(".",",").replace("X",".")
                linhas += (f'<tr><td style="padding:5px 12px;border-bottom:1px solid #1f2535;text-align:center;color:#dde1f0;">{resp}</td>'
                           f'<td style="padding:5px 12px;border-bottom:1px solid #1f2535;text-align:center;font-weight:600;color:#dde1f0;">{qtd}</td>'
                           f'<td style="padding:5px 12px;border-bottom:1px solid #1f2535;text-align:center;font-weight:600;color:#dde1f0;">{hrs}</td></tr>')
            return (f'<table style="width:100%;border-collapse:collapse;font-size:13px;"><thead>'
                    f'<tr style="background:#111520;">'
                    f'<th style="padding:7px 12px;text-align:center;border-bottom:1px solid #1f2535;color:#5e6484;font-weight:500;text-transform:uppercase;letter-spacing:.07em;">Responsável</th>'
                    f'<th style="padding:7px 12px;text-align:center;border-bottom:1px solid #1f2535;color:#5e6484;font-weight:500;text-transform:uppercase;letter-spacing:.07em;">Quantidade de Atividades</th>'
                    f'<th style="padding:7px 12px;text-align:center;border-bottom:1px solid #1f2535;color:#5e6484;font-weight:500;text-transform:uppercase;letter-spacing:.07em;">Horas em Aberto</th>'
                    f'</tr></thead><tbody>{linhas}</tbody></table>')

        st.markdown(carga_para_html(carga), unsafe_allow_html=True)

        st.divider()
        from datetime import datetime as _dt
        import base64 as _b64
        _nome_arquivo = f"atividades_{_dt.today().strftime('%Y%m%d')}.xlsx"
        _excel_bytes  = gerar_excel(df_filtrado)
        _excel_b64    = _b64.b64encode(_excel_bytes).decode()
        _mime         = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

        st.markdown(
            f'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:-0.5rem;">'
            f'<h3 style="margin:0;">Atividades</h3>'
            f'<a href="data:{_mime};base64,{_excel_b64}" download="{_nome_arquivo}"'
            f' style="color:#5e6484;font-size:0.75rem;text-decoration:none;white-space:nowrap;">📤 Exportar para Excel</a>'
            f'</div>',
            unsafe_allow_html=True,
        )

        colunas_exibir = ["id","responsavel","tipo","macro_acao","micro_acao","criticidade","frequencia","tempo","prazo","status","percentual_conclusao","score","atrasada"]
        tabela_exibir = (df_filtrado[colunas_exibir].copy().rename(columns={
            "id":"ID","responsavel":"Responsável","tipo":"Tipo","macro_acao":"MACRO_AÇÃO","micro_acao":"MICRO_AÇÃO",
            "criticidade":"Criticidade","frequencia":"Frequência","tempo":"TEMPO","prazo":"Prazo",
            "status":"Status","percentual_conclusao":"%_CONCLUSÃO","score":"SCORE","atrasada":"ATRASADA"}))
        tabela_exibir = formatar_df_numeros(tabela_exibir, ["ID","TEMPO","%_CONCLUSÃO","SCORE"])

        def badge(texto, cor): return f'<span style="background:{cor};color:#fff;font-size:0.62rem;padding:2px 8px;border-radius:3px;font-weight:600;white-space:nowrap;">{texto}</span>'
        def barra(pct):
            pct = max(0, min(100, pct))
            cor = "#3ecf8e" if pct == 100 else "#4c7cf3"
            return (f'<div style="display:flex;align-items:center;gap:6px;">'
                    f'<div style="flex:1;background:#1f2535;border-radius:99px;height:4px;">'
                    f'<div style="width:{pct:.0f}%;background:{cor};height:4px;border-radius:99px;"></div></div>'
                    f'<span style="font-size:0.65rem;color:#5e6484;white-space:nowrap;">{pct:.0f}%</span></div>')
        def cel(val, align="center", color="#dde1f0"):
            v = str(val) if str(val) not in ("nan","None","") else "—"
            return f'<td style="padding:5px 10px;border-bottom:1px solid #1a1f2e;text-align:{align};color:{color};font-size:0.75rem;">{v}</td>'

        COR_STATUS = {"NÃO INICIADA":"#5e6484","EM ANDAMENTO":"#4c7cf3","CONCLUÍDA":"#3ecf8e","PENDENTE APROVAÇÃO":"#e0a040","PENDENTE":"#e0a040"}
        COR_CRIT   = {"BAIXA":"#3ecf8e","MÉDIA":"#e0a040","ALTA":"#e07040"}

        cabecalhos = ["ID","Tipo","Macro ação","Micro ação","Criticidade","FREQ.","TEMPO","Prazo","Status","% CONCLUSÃO","SCORE"]
        ths = "".join(f'<th style="padding:7px 10px;text-align:center;border-bottom:1px solid #1f2535;color:#5e6484;font-size:0.68rem;font-weight:500;text-transform:uppercase;letter-spacing:.06em;white-space:nowrap;">{h}</th>' for h in cabecalhos)

        responsaveis_ordenados = sorted(tabela_exibir["Responsável"].fillna("—").unique().tolist(), key=lambda x: x.lower())
        html_completo = '<div style="overflow-x:auto;margin-top:-0.5rem;">'

        for resp in responsaveis_ordenados:
            grupo          = tabela_exibir[tabela_exibir["Responsável"].fillna("—") == resp]
            total_resp     = len(grupo)
            atrasadas_resp = int(grupo["ATRASADA"].sum()) if "ATRASADA" in grupo.columns else 0
            html_completo += (
                f'<div style="margin-top:1.2rem;margin-bottom:0.4rem;display:flex;align-items:center;gap:10px;">'
                f'<span style="font-size:0.95rem;font-weight:600;color:#dde1f0;text-transform:uppercase;letter-spacing:.07em;">👤 {resp}</span>'
                f'<span style="background:#1f2535;color:#dde1f0;font-size:0.75rem;padding:1px 8px;border-radius:99px;">{total_resp} atividade{"s" if total_resp!=1 else ""}</span>'
                + (f'<span style="background:#c94f4f;color:#fff;font-size:0.75rem;padding:1px 8px;border-radius:99px;">{atrasadas_resp} atrasada{"s" if atrasadas_resp!=1 else ""}</span>' if atrasadas_resp > 0 else "")
                + f'</div><div style="margin-bottom:0.5rem;"><table style="width:100%;border-collapse:collapse;"><thead><tr style="background:#111520;">{ths}</tr></thead><tbody>'
            )
            for _, row in grupo.iterrows():
                atrasada  = bool(row.get("ATRASADA", False))
                bg        = "#1f1117" if atrasada else "transparent"
                borda_esq = "#c94f4f" if atrasada else "#1f2535"
                status    = str(row["Status"])
                crit      = str(row["Criticidade"]).upper()
                pct       = float(row["%_CONCLUSÃO"]) if pd.notna(row["%_CONCLUSÃO"]) else 0.0
                prazo_cor = "#c94f4f" if atrasada else "#dde1f0"
                html_completo += (
                    f'<tr style="background:{bg};border-left:2px solid {borda_esq};">'
                    + cel(int(float(row["ID"])))
                    + cel(row["Tipo"])
                    + cel(row["MACRO_AÇÃO"], "left")
                    + cel(row["MICRO_AÇÃO"], "left")
                    + f'<td style="padding:5px 10px;border-bottom:1px solid #1a1f2e;text-align:center;">{badge(crit, COR_CRIT.get(crit,"#5e6484"))}</td>'
                    + cel(row["Frequência"])
                    + cel(f'{float(row["TEMPO"]):.1f}h')
                    + cel(row["Prazo"], color=prazo_cor)
                    + f'<td style="padding:5px 10px;border-bottom:1px solid #1a1f2e;text-align:center;">{badge(status, COR_STATUS.get(status,"#5e6484"))}</td>'
                    + f'<td style="padding:5px 10px;border-bottom:1px solid #1a1f2e;min-width:90px;">{barra(pct)}</td>'
                    + cel(f'{float(row["SCORE"]):.1f}', color="#4c7cf3")
                    + '</tr>'
                )
            html_completo += '</tbody></table></div>'
        html_completo += '</div>'
        st.markdown(html_completo, unsafe_allow_html=True)


# =========================================================
# APROVAÇÕES DO GESTOR
# =========================================================
elif st.session_state.pagina == "Aprovações":
    df = listar()
    COR_CRIT_APR = {"BAIXA":"#3ecf8e","MÉDIA":"#e0a040","ALTA":"#e07040"}

    def secao_aprovacao(pendentes, titulo, chave_aprovar, fn_aprovar, chave_reprovar, fn_reprovar, extra_campo=None):
        st.markdown(
            f'<div style="font-size:0.95rem;font-weight:600;color:#dde1f0;text-transform:uppercase;'
            f'letter-spacing:.1em;margin-bottom:0.5rem;">{titulo}'
            f'<span style="background:#1f2535;color:#dde1f0;font-size:0.85rem;padding:1px 8px;'
            f'border-radius:99px;margin-left:8px;">{len(pendentes)}</span></div>',
            unsafe_allow_html=True,
        )
        if pendentes.empty:
            st.markdown('<div style="font-size:1rem;color:#dde1f0;padding:8px 0;">Nenhuma atividade pendente.</div>', unsafe_allow_html=True)
            return
        for _, row in pendentes.iterrows():
            crit     = str(row["criticidade"]).upper()
            cor_crit = COR_CRIT_APR.get(crit, "#5e6484")
            with st.expander(f"ID {row['id']}  ·  {row['micro_acao']}"):
                st.markdown(
                    f"""<div style="font-size:1rem;line-height:2;margin-bottom:0.5rem;">
                        <span style="color:#5e6484;">Responsável:</span> <strong>{row['responsavel']}</strong>
                        &nbsp;·&nbsp;<span style="color:#5e6484;">Tipo:</span> {row['tipo']}
                        &nbsp;·&nbsp;<span style="color:#5e6484;">Prazo:</span> {row['prazo']}
                        &nbsp;·&nbsp;<span style="color:#5e6484;">Criticidade:</span>
                        <span style="background:{cor_crit};color:#fff;font-size:0.82rem;padding:2px 9px;border-radius:3px;font-weight:600;">{crit}</span>
                        <br><span style="color:#5e6484;">Macro ação:</span> {row['macro_acao']}
                        {f'<br><span style="color:#5e6484;">% Conclusão:</span> <strong>{row["percentual_conclusao"]}%</strong>' if extra_campo else ''}
                    </div>""",
                    unsafe_allow_html=True,
                )
                _c1, _c2 = st.columns(2)
                with _c1:
                    if st.button("✅ Aprovar", key=f"{chave_aprovar}_{row['id']}", use_container_width=True):
                        fn_aprovar(int(row["id"])); st.rerun()
                with _c2:
                    if st.button("❌ Reprovar", key=f"{chave_reprovar}_{row['id']}", use_container_width=True):
                        fn_reprovar(int(row["id"])); st.rerun()

    if df.empty:
        st.info("Nenhuma atividade cadastrada.")
    else:
        pendentes_inicio    = df[df["aprovacao_gestor"].fillna("").astype(str).str.upper()    == "PENDENTE APROVAÇÃO"]
        pendentes_conclusao = df[df["aprovacao_conclusao"].fillna("").astype(str).str.upper() == "AGUARDANDO APROVAÇÃO"]
        secao_aprovacao(pendentes_inicio,    "Aprovação de Início",    "aprovar_inicio",    aprovar_inicio,    "reprovar_inicio",    reprovar_inicio)
        st.divider()
        secao_aprovacao(pendentes_conclusao, "Aprovação de Conclusão", "aprovar_conclusao", aprovar_conclusao, "reprovar_conclusao", reprovar_conclusao, extra_campo=True)


# =========================================================
# ATUALIZAR ATIVIDADES
# =========================================================
elif st.session_state.pagina == "Atualizar":
    df           = listar()
    responsaveis = listar_responsaveis()
    mapa_responsaveis   = {}
    opcoes_responsaveis = []
    for resp in responsaveis:
        if str(resp).strip() == "":
            mapa_responsaveis["(Sem responsável)"] = ""
            opcoes_responsaveis.append("(Sem responsável)")
        else:
            mapa_responsaveis[resp] = resp
            opcoes_responsaveis.append(resp)

    if not opcoes_responsaveis:
        st.info("Nenhum responsável encontrado na base.")
    else:
        escolha_responsavel = st.selectbox("Selecione o responsável", opcoes_responsaveis)
        responsavel_filtro  = mapa_responsaveis[escolha_responsavel]
        df_filtrado = df[df["responsavel"].fillna("").astype(str).str.strip() == responsavel_filtro].copy()

        if df_filtrado.empty:
            st.warning("Nenhuma atividade encontrada para esse responsável.")
        else:
            st.write(f"Total de atividades encontradas: {len(df_filtrado)}")
            st.divider()

            for _, row in df_filtrado.iterrows():
                _cor_badge = {"NÃO INICIADA":"#5e6484","EM ANDAMENTO":"#4c7cf3","CONCLUÍDA":"#3ecf8e","PENDENTE APROVAÇÃO":"#e0a040","PENDENTE":"#e0a040"}.get(str(row["status"]).strip(),"#5e6484")
                _col_exp, _col_badge = st.columns([0.87, 0.13])
                with _col_badge:
                    st.markdown(f'<div style="display:flex;align-items:center;justify-content:center;height:2.1rem;margin-top:0.1rem;">'
                                f'<span style="background:{_cor_badge};color:#fff;font-size:0.65rem;padding:4px 0;border-radius:4px;font-weight:600;letter-spacing:0.05em;text-align:center;display:block;width:100%;">{row["status"]}</span></div>',
                                unsafe_allow_html=True)
                with _col_exp:
                    with st.expander(f"ID {row['id']} | {row['micro_acao']}"):
                        modo_edicao = st.session_state.get(f"editar_{row['id']}", False)
                        if not modo_edicao:
                            st.markdown(f"""<div style="line-height:1.8;margin-bottom:0.4rem;">
                                <div><strong>Tipo:</strong> {row['tipo']}</div>
                                <div><strong>Macro ação:</strong> {row['macro_acao']}</div>
                                <div><strong>Criticidade:</strong> {row['criticidade']}</div>
                                <div><strong>Frequência:</strong> {row['frequencia']}</div>
                                <div><strong>Tempo estimado (h):</strong> {row['tempo']}</div>
                                <div><strong>Prazo:</strong> {row['prazo']}</div>
                                <div><strong>Origem:</strong> {row['origem']}</div>
                            </div>""", unsafe_allow_html=True)
                        else:
                            st.markdown("**✏️ Modo edição**")
                            _e1, _e2 = st.columns(2)
                            with _e1:
                                ed_tipo  = st.selectbox("Tipo", OPCOES_TIPO, index=OPCOES_TIPO.index(row["tipo"]) if row["tipo"] in OPCOES_TIPO else 0, key=f"ed_tipo_{row['id']}")
                                ed_macro = st.text_input("Macro ação", value=str(row["macro_acao"]), key=f"ed_macro_{row['id']}")
                                ed_micro = st.text_area("Micro ação", value=str(row["micro_acao"]), key=f"ed_micro_{row['id']}")
                                ed_resp  = st.text_input("Responsável", value=str(row["responsavel"]), key=f"ed_resp_{row['id']}")
                            with _e2:
                                ed_crit  = st.selectbox("Criticidade", OPCOES_CRITICIDADE, index=OPCOES_CRITICIDADE.index(row["criticidade"]) if row["criticidade"] in OPCOES_CRITICIDADE else 0, key=f"ed_crit_{row['id']}")
                                ed_freq  = st.selectbox("Frequência", OPCOES_FREQUENCIA, index=OPCOES_FREQUENCIA.index(row["frequencia"]) if row["frequencia"] in OPCOES_FREQUENCIA else 0, key=f"ed_freq_{row['id']}")
                                ed_tempo = st.number_input("Tempo (horas)", min_value=0.0, step=0.5, value=float(row["tempo"]) if pd.notna(row["tempo"]) else 0.0, key=f"ed_tempo_{row['id']}")
                                ed_prazo = st.text_input("Prazo (dd/mm/aaaa)", value=str(row["prazo"]), key=f"ed_prazo_{row['id']}")
                            _s1, _s2 = st.columns(2)
                            with _s1:
                                if st.button("💾 Salvar edição", key=f"salvar_ed_{row['id']}", use_container_width=True):
                                    editar_atividade(int(row["id"]), {"tipo":ed_tipo,"macro_acao":ed_macro,"micro_acao":ed_micro,"criticidade":ed_crit,"frequencia":ed_freq,"tempo":ed_tempo,"prazo":ed_prazo,"responsavel":ed_resp})
                                    st.session_state[f"editar_{row['id']}"] = False
                                    st.session_state[f"msg_ok_{row['id']}"] = True
                                    st.rerun()
                            with _s2:
                                if st.button("✖ Cancelar edição", key=f"cancel_ed_{row['id']}", use_container_width=True):
                                    st.session_state[f"editar_{row['id']}"] = False
                                    st.rerun()

                        status_atual = str(row["status"]) if pd.notna(row["status"]) else "NÃO INICIADA"
                        if status_atual not in OPCOES_STATUS: status_atual = "NÃO INICIADA"

                        if not modo_edicao:
                            st.divider()
                            novo_status = st.selectbox(f"Novo status — atividade {row['id']}", OPCOES_STATUS,
                                                       index=OPCOES_STATUS.index(status_atual), key=f"status_{row['id']}")
                            chave_perc = f"perc_{row['id']}"
                            if novo_status == "CONCLUÍDA": st.session_state[chave_perc] = 100
                            percentual = st.slider(f"% de conclusão — atividade {row['id']}", 0, 100, key=chave_perc)
                            c1, c2, c3 = st.columns(3)
                            with c1:
                                if st.button(f"Atualizar atividade {row['id']}", key=f"btn_{row['id']}", use_container_width=True):
                                    atualizar_status(int(row["id"]), novo_status, float(percentual))
                                    st.session_state[f"msg_ok_{row['id']}"] = True
                                    st.rerun()
                            with c2:
                                if st.button("✏️ Editar campos", key=f"edit_{row['id']}", use_container_width=True):
                                    st.session_state[f"editar_{row['id']}"] = True
                                    st.rerun()
                            with c3:
                                chave_confirmar = f"confirmar_del_{row['id']}"
                                if not st.session_state.get(chave_confirmar, False):
                                    if st.button(f"Apagar atividade {row['id']}", key=f"del_{row['id']}", use_container_width=True):
                                        st.session_state[chave_confirmar] = True
                                        st.rerun()
                                else:
                                    st.warning("Tem certeza? Essa ação não pode ser desfeita.")
                                    cc1, cc2 = st.columns(2)
                                    with cc1:
                                        if st.button("Sim, apagar", key=f"confirm_sim_{row['id']}"):
                                            apagar_atividade(int(row["id"]))
                                            st.session_state.pop(chave_confirmar, None)
                                            st.success(f"Atividade {row['id']} apagada.")
                                            st.rerun()
                                    with cc2:
                                        if st.button("Cancelar", key=f"confirm_nao_{row['id']}"):
                                            st.session_state.pop(chave_confirmar, None)
                                            st.rerun()

                        if st.session_state.pop(f"msg_ok_{row['id']}", False):
                            st.success(f"✅ Atividade {row['id']} atualizada com sucesso.")

                        hist = listar_historico(int(row["id"]))
                        if not hist.empty:
                            st.divider()
                            st.markdown('<div style="font-size:0.72rem;font-weight:600;color:#5e6484;text-transform:uppercase;letter-spacing:.07em;margin-bottom:4px;">Histórico</div>', unsafe_allow_html=True)
                            for _, h in hist.iterrows():
                                st.markdown(
                                    f'<div style="font-size:0.72rem;color:#5e6484;line-height:1.7;">'
                                    f'<span style="color:#dde1f0;">{h["data_hora"]}</span> · '
                                    f'<span style="color:#4c7cf3;">{h["acao"]}</span>'
                                    + (f' · <span style="color:#dde1f0;">{h["campo"]}</span>: '
                                       f'<span style="text-decoration:line-through;">{h["valor_anterior"]}</span> → '
                                       f'<span style="color:#3ecf8e;">{h["valor_novo"]}</span>' if h["campo"] else "")
                                    + '</div>', unsafe_allow_html=True)


# =========================================================
# NOVA ATIVIDADE
# =========================================================
elif st.session_state.pagina == "Nova":

    if st.session_state.get("nova_sucesso"):
        st.success(st.session_state.pop("nova_sucesso"))

    responsaveis_base           = listar_responsaveis()
    responsaveis_base_sem_vazio = [r for r in responsaveis_base if str(r).strip() != ""]
    opcoes_responsavel          = responsaveis_base_sem_vazio + ["Novo responsável", "(Sem responsável)"]

    if "nova_pendente" not in st.session_state:
        st.session_state.nova_pendente = False

    if not st.session_state.nova_pendente:
        n1, n2, n3 = st.columns(3)
        with n1:
            tipo        = st.selectbox("Tipo",        OPCOES_TIPO,        key="nova_tipo")
            criticidade = st.selectbox("Criticidade", OPCOES_CRITICIDADE, key="nova_crit")
            frequencia  = st.selectbox("Frequência",  OPCOES_FREQUENCIA,  key="nova_freq")
        with n2:
            macro = st.text_input("Macro ação", key="nova_macro")
            micro = st.text_input("Micro ação", key="nova_micro")
            tempo = st.number_input("Tempo (horas)", min_value=1, step=1, format="%d", key="nova_tempo")
        with n3:
            escolha   = st.selectbox("Responsável", opcoes_responsavel, key="nova_resp")
            nome_novo = ""
            if escolha == "Novo responsável":
                nome_novo = st.text_input("Nome do novo responsável", key="nova_nome_novo")
            criado_por = st.selectbox("Criado por", OPCOES_CRIADO_POR, key="nova_criado_por")
            if frequencia != "ISOLADA":
                data_ref     = st.date_input("Data de referência", format="DD/MM/YYYY", key="nova_data_ref")
                prazo_manual = None
            else:
                data_ref     = None
                prazo_manual = st.date_input("Prazo", format="DD/MM/YYYY", key="nova_prazo")

        if st.button("Salvar atividade", type="primary"):
            if escolha == "Novo responsável" and not nome_novo.strip():
                st.error("Digite o nome do novo responsável antes de salvar.")
                st.stop()
            st.session_state.nova_dados = {
                "tipo": tipo, "macro": macro, "micro": micro,
                "criticidade": criticidade, "frequencia": frequencia,
                "tempo": tempo, "data_ref": data_ref, "prazo_manual": prazo_manual,
                "escolha": escolha, "nome_novo": nome_novo, "criado_por": criado_por,
            }
            st.session_state.nova_pendente = True
            st.rerun()

    else:
        d = st.session_state.nova_dados

        if d["escolha"] == "Novo responsável":  responsavel_final = d["nome_novo"].strip()
        elif d["escolha"] == "(Sem responsável)": responsavel_final = ""
        else:                                     responsavel_final = d["escolha"]

        prazo_obj = calcular_proximo_prazo(d["frequencia"], d["data_ref"]) if d["frequencia"] != "ISOLADA" and d["data_ref"] else d["prazo_manual"]
        prazo_str = prazo_obj.strftime("%d/%m/%Y") if prazo_obj else "—"

        _hoje_nova  = pd.Timestamp.today().normalize()
        _prazo_ts   = pd.Timestamp(prazo_obj) if prazo_obj else _hoje_nova + pd.Timedelta(days=30)
        _dias_imp   = (_prazo_ts - _hoje_nova).days
        _df_impacto = preparar_dashboard(listar())

        _tem_impacto = (responsavel_final and not _df_impacto.empty and
                        responsavel_final in _df_impacto["responsavel"].values)

        if _tem_impacto:
            _grp_imp = _df_impacto[_df_impacto["responsavel"] == responsavel_final]
            _st_imp  = _grp_imp["status"].fillna("").astype(str).str.upper()
            _ab_imp  = _grp_imp[~_st_imp.isin(STATUS_CONCLUIDOS)]
            _h_at    = float(_ab_imp["horas_em_aberto"].sum())
            _at_at   = int(_ab_imp["atrasada"].sum())
            _cr_at   = int(_ab_imp["criticidade"].fillna("").astype(str).str.upper().isin(["ALTA","CRÍTICA"]).sum())
            _idx_at  = min((_h_at/40.0)*100 + _at_at*15 + _cr_at*8, 500)
            _h_nv    = float(d["tempo"])
            _cr_nv   = 1 if d["criticidade"] in ["ALTA","CRÍTICA"] else 0
            _h_dp    = _h_at + _h_nv
            _idx_dp  = min((_h_dp/40.0)*100 + _at_at*15 + (_cr_at+_cr_nv)*8, 500)
            _delta   = _idx_dp - _idx_at

            _peso_pr  = 5 if _dias_imp<0 else 4 if _dias_imp<=1 else 3 if _dias_imp<=3 else 2 if _dias_imp<=7 else 1
            _peso_fr  = PESO_FREQUENCIA.get(d["frequencia"].upper(), 1)
            _peso_cr  = PESO_CRITICIDADE.get(d["criticidade"], 1)
            _score_nv = round(_peso_cr*3 + _peso_fr*2 + 3*2 + _peso_pr*3 + max(_h_nv/8,1), 2)
            _fila_imp = _ab_imp.sort_values("score", ascending=False)
            _pos_nv   = int((_fila_imp["score"] > _score_nv).sum()) + 1
            _total_nv = len(_fila_imp) + 1

            def _ci(v):
                if v<=70:  return "#3ecf8e","Disponível"
                if v<=90:  return "#e0a040","Moderado"
                if v<=120: return "#e07040","Alto"
                return "#c94f4f","Sobrecarregado"

            _ca,_la = _ci(_idx_at)
            _cd,_ld = _ci(_idx_dp)
            _cd_d   = "#c94f4f" if _delta>20 else "#e0a040" if _delta>0 else "#3ecf8e"
            _sinal  = f"+{_delta:.0f}%" if _delta>=0 else f"{_delta:.0f}%"
            _data_fmt = _prazo_ts.strftime("%d/%m/%Y")

            from datetime import datetime as _dtnow
            _agora       = _dtnow.now().strftime("%d/%m/%Y %H:%M")
            _prazo_cor_c = "#e0a040" if 0<=_dias_imp<=7 else "#dde1f0"
            _prazo_tag   = (
                '<span style="font-size:0.7rem;color:#c94f4f;font-weight:600;">&#9888; Prazo já vencido!</span>'
                if _dias_imp < 0 else
                f'<span style="font-size:0.7rem;color:#5e6484;">&#9632; Dias até o prazo: <b style="color:{_prazo_cor_c};">{_dias_imp}d</b></span>'
            )

            _atividades_antes = int((_fila_imp["score"] > _score_nv).sum())
            _horas_antes      = float(_fila_imp[_fila_imp["score"] > _score_nv]["horas_em_aberto"].sum())
            _data_inicio_est  = _hoje_nova + pd.Timedelta(days=_horas_antes/8.0)
            _data_concl_est   = _data_inicio_est + pd.Timedelta(days=float(d["tempo"])/8.0)
            _data_concl_str   = _data_concl_est.strftime("%d/%m/%Y")
            _data_inicio_str  = _data_inicio_est.strftime("%d/%m/%Y")

            _prazos_validos = _fila_imp["prazo_dt"].dropna()
            _fim_fila       = _prazos_validos.max() if not _prazos_validos.empty else _hoje_nova
            _fim_fila_str   = _fim_fila.strftime("%d/%m/%Y") if not _prazos_validos.empty else "sem prazo"
            _duracao_fila   = int((_fim_fila - _hoje_nova).days) if not _prazos_validos.empty else 0
            _periodo_fila   = f"{_hoje_nova.strftime('%d/%m/%Y')} a {_fim_fila_str} ({_duracao_fila} dias)"

            _mapa_freq_txt = {
                "DIÁRIA":    "diária — a cada ciclo de 1 dia, uma nova ocorrência é gerada automaticamente",
                "SEMANAL":   "semanal — a cada 7 dias, uma nova ocorrência é gerada automaticamente",
                "QUINZENAL": "quinzenal — a cada 15 dias, uma nova ocorrência é gerada automaticamente",
                "MENSAL":    "mensal — a cada 30 dias, uma nova ocorrência é gerada automaticamente",
                "ISOLADA":   None,
            }
            _freq_upper  = d["frequencia"].upper()
            _nota_recorr = _mapa_freq_txt.get(_freq_upper)
            _horas_mes   = float(d["tempo"]) * {"DIÁRIA":20,"SEMANAL":4,"QUINZENAL":2,"MENSAL":1,"ISOLADA":0}.get(_freq_upper,0)

            _vai_atrasar = prazo_obj and _data_concl_est > _prazo_ts
            _cor_concl   = "#c94f4f" if _vai_atrasar else "#3ecf8e"
            _vai_atr_str = "#c94f4f" if _vai_atrasar else "#3ecf8e"
            _veredicto   = "&#9888; Risco de atraso" if _vai_atrasar else "&#10003; Dentro do prazo"
            _margem_dias = int((_prazo_ts - _data_concl_est).days)
            _margem_html = (f'<div style="font-size:0.82rem;font-weight:600;color:#c94f4f;">&#8722;{abs(_margem_dias)} dia(s) de atraso</div>'
                            if _vai_atrasar else
                            f'<div style="font-size:0.82rem;font-weight:600;color:#3ecf8e;">+{_margem_dias} dia(s) de folga</div>')

            _html = (
                '<div style="background:#111520;border:1px solid #1f2535;border-radius:10px;padding:20px 24px;">'
                '<div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:16px;">'
                '<div>'
                '<div style="font-size:0.7rem;color:#5e6484;text-transform:uppercase;letter-spacing:.1em;">An&aacute;lise de Impacto &middot; Nova Atividade</div>'
                f'<div style="font-size:1rem;font-weight:700;color:#dde1f0;margin-top:2px;">{d["micro"] or "—"}</div>'
                f'<div style="font-size:0.75rem;color:#5e6484;">{d["macro"] or "—"} &middot; {d["tipo"]} &middot; {d["frequencia"]}</div>'
                '</div>'
                '<div style="text-align:right;">'
                '<div style="font-size:0.65rem;color:#5e6484;">Respons&aacute;vel</div>'
                f'<div style="font-size:0.85rem;font-weight:700;color:#dde1f0;text-transform:uppercase;">{responsavel_final}</div>'
                f'<div style="font-size:0.65rem;color:#5e6484;margin-top:2px;">{_agora}</div>'
                '</div></div>'
                '<div style="display:flex;gap:0.6rem;margin-bottom:14px;flex-wrap:wrap;">'
                f'<div style="flex:1;min-width:110px;background:#161b27;border:1px solid #1f2535;border-left:3px solid {_ca};border-radius:6px;padding:8px 12px;">'
                '<div style="font-size:0.58rem;color:#5e6484;text-transform:uppercase;margin-bottom:2px;">&Iacute;ndice atual</div>'
                f'<div style="font-size:1.3rem;font-weight:700;color:{_ca};">{_idx_at:.0f}%</div>'
                f'<div style="font-size:0.6rem;color:#5e6484;">{_la}</div></div>'
                f'<div style="flex:1;min-width:110px;background:#161b27;border:1px solid #1f2535;border-left:3px solid {_cd};border-radius:6px;padding:8px 12px;">'
                '<div style="font-size:0.58rem;color:#5e6484;text-transform:uppercase;margin-bottom:2px;">&Iacute;ndice ap&oacute;s</div>'
                f'<div style="font-size:1.3rem;font-weight:700;color:{_cd};">{_idx_dp:.0f}%</div>'
                f'<div style="font-size:0.6rem;color:#5e6484;">{_ld}</div></div>'
                f'<div style="flex:1;min-width:110px;background:#161b27;border:1px solid #1f2535;border-left:3px solid {_cd_d};border-radius:6px;padding:8px 12px;">'
                '<div style="font-size:0.58rem;color:#5e6484;text-transform:uppercase;margin-bottom:2px;">Varia&ccedil;&atilde;o</div>'
                f'<div style="font-size:1.3rem;font-weight:700;color:{_cd_d};">{_sinal}</div>'
                f'<div style="font-size:0.6rem;color:#5e6484;">+{int(_h_nv)}h ao backlog</div></div>'
                f'<div style="flex:1;min-width:110px;background:#161b27;border:1px solid #1f2535;border-left:3px solid #dde1f0;border-radius:6px;padding:8px 12px;">'
                '<div style="font-size:0.58rem;color:#5e6484;text-transform:uppercase;margin-bottom:2px;">Prazo informado</div>'
                f'<div style="font-size:1.3rem;font-weight:700;color:#dde1f0;">{_data_fmt}</div>'
                f'<div style="font-size:0.6rem;color:#5e6484;">{_dias_imp}d at&eacute; o vencimento</div></div>'
                '</div>'
                '<div style="display:flex;gap:1rem;flex-wrap:wrap;padding-top:10px;border-top:1px solid #1f2535;">'
                f'<span style="font-size:0.7rem;color:#5e6484;">&#9632; Criticidade: <b style="color:#dde1f0;">{d["criticidade"]}</b></span>'
                f'<span style="font-size:0.7rem;color:#5e6484;">&#9632; Tempo: <b style="color:#dde1f0;">{int(_h_nv)}h</b></span>'
                f'<span style="font-size:0.7rem;color:#5e6484;">&#9632; Atividades em aberto: <b style="color:#dde1f0;">{len(_fila_imp)}</b></span>'
                f'<span style="font-size:0.7rem;color:#5e6484;">&#9632; Horas: <b style="color:#dde1f0;">{int(_h_at)}h &rarr; {int(_h_dp)}h</b></span>'
                f'{_prazo_tag}'
                '</div>'
                '<div style="margin-top:14px;padding:14px 16px;background:#0d1220;border-radius:8px;border:1px solid #1f2535;">'
                f'<div style="font-size:0.65rem;font-weight:700;color:#5e6484;text-transform:uppercase;letter-spacing:.1em;margin-bottom:12px;display:flex;justify-content:space-between;">'
                f'<span>Previs&atilde;o de Execu&ccedil;&atilde;o</span>'
                f'<span style="color:{_vai_atr_str};font-weight:700;">{_veredicto}</span></div>'
                '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:12px;">'
                f'<div style="background:#161b27;border-radius:6px;padding:10px 12px;border-left:3px solid #4c7cf3;">'
                f'<div style="font-size:0.58rem;color:#5e6484;text-transform:uppercase;margin-bottom:3px;">Posi&ccedil;&atilde;o na fila</div>'
                f'<div style="font-size:1.1rem;font-weight:700;color:#4c7cf3;">{_pos_nv}&ordm; de {_total_nv}</div>'
                f'<div style="font-size:0.6rem;color:#5e6484;">{_atividades_antes} atividade{"s" if _atividades_antes!=1 else ""} antes</div>'
                f'<div style="font-size:0.6rem;color:#5e6484;margin-top:2px;">{_periodo_fila}</div></div>'
                f'<div style="background:#161b27;border-radius:6px;padding:10px 12px;border-left:3px solid #5e6484;">'
                f'<div style="font-size:0.58rem;color:#5e6484;text-transform:uppercase;margin-bottom:3px;">In&iacute;cio estimado</div>'
                f'<div style="font-size:1.1rem;font-weight:700;color:#dde1f0;">{_data_inicio_str}</div>'
                f'<div style="font-size:0.6rem;color:#5e6484;">{_horas_antes:.0f}h de backlog antes desta</div></div>'
                f'<div style="background:#161b27;border-radius:6px;padding:10px 12px;border-left:3px solid {_cor_concl};">'
                f'<div style="font-size:0.58rem;color:#5e6484;text-transform:uppercase;margin-bottom:3px;">Entrega prevista pelo score</div>'
                f'<div style="font-size:1.1rem;font-weight:700;color:{_cor_concl};">{_data_concl_str}</div>'
                f'<div style="font-size:0.6rem;color:#5e6484;">estimativa baseada na fila atual</div></div>'
                f'<div style="background:#161b27;border-radius:6px;padding:10px 12px;border-left:3px solid {"#c94f4f" if _vai_atrasar else "#dde1f0"};">'
                f'<div style="font-size:0.58rem;color:#5e6484;text-transform:uppercase;margin-bottom:3px;">Prazo informado</div>'
                f'<div style="font-size:1.1rem;font-weight:700;color:{"#c94f4f" if _vai_atrasar else "#dde1f0"};">{_data_fmt}</div>'
                f'<div style="font-size:0.6rem;color:{"#c94f4f" if _vai_atrasar else "#5e6484"};">{"&#9888; entrega prevista ap&oacute;s o prazo" if _vai_atrasar else "data limite definida"}</div></div>'
                '</div>'
                '<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;padding-top:10px;border-top:1px solid #1f2535;">'
                f'<div><div style="font-size:0.58rem;color:#5e6484;text-transform:uppercase;margin-bottom:2px;">Tempo para execu&ccedil;&atilde;o</div>'
                f'<div style="font-size:0.82rem;font-weight:600;color:#dde1f0;">{int(_h_nv)}h &asymp; {max(1,round(_h_nv/8))} dia{"s" if round(_h_nv/8)!=1 else ""} &uacute;til{"" if round(_h_nv/8)!=1 else ""}</div></div>'
                f'<div><div style="font-size:0.58rem;color:#5e6484;text-transform:uppercase;margin-bottom:2px;">Margem at&eacute; o prazo</div>'
                + _margem_html +
                '</div>'
                f'<div><div style="font-size:0.58rem;color:#5e6484;text-transform:uppercase;margin-bottom:2px;">Score desta atividade</div>'
                f'<div style="font-size:0.82rem;font-weight:600;color:#4c7cf3;">{_score_nv:.0f} pontos &middot; {d["criticidade"]} &middot; {d["frequencia"]}</div></div>'
                '</div>'
                + (
                    f'<div style="margin-top:10px;padding:8px 12px;background:#161b27;border-radius:6px;border-left:3px solid #e0a040;">'
                    f'<div style="font-size:0.6rem;color:#e0a040;font-weight:700;text-transform:uppercase;margin-bottom:4px;">&#8635; Atividade recorrente</div>'
                    f'<div style="font-size:0.7rem;color:#dde1f0;">Esta &eacute; uma atividade <b>{_freq_upper.lower()}</b> — {_nota_recorr}.</div>'
                    f'<div style="font-size:0.7rem;color:#5e6484;margin-top:3px;">Carga recorrente estimada: <b style="color:#dde1f0;">~{_horas_mes:.0f}h/m&ecirc;s</b> para este respons&aacute;vel. '
                    f'A an&aacute;lise acima refere-se apenas ao <b>primeiro ciclo</b>; nas pr&oacute;ximas recorr&ecirc;ncias, a posi&ccedil;&atilde;o na fila ser&aacute; recalculada.</div></div>'
                    if _nota_recorr else ""
                ) +
                '</div></div>'
                '<div style="font-size:0.6rem;color:#2a3040;text-align:right;margin-top:10px;">'
                'Sistema de Gest&atilde;o de Atividades &middot; An&aacute;lise gerada automaticamente</div></div>'
            )
            st.markdown(_html, unsafe_allow_html=True)
            st.markdown("")
        else:
            st.info(f"Resumo — **{d['micro'] or '—'}** · {responsavel_final or '(sem responsável)'} · Prazo: {prazo_str} · {d['criticidade']} · {int(d['tempo'])}h")

        st.markdown("**Deseja confirmar e salvar esta atividade?**")
        _c1, _c2 = st.columns(2)
        with _c1:
            if st.button("✅ Confirmar e salvar", type="primary", use_container_width=True):
                status = "NÃO INICIADA" if d["criado_por"] == "GESTOR" else "PENDENTE APROVAÇÃO"
                inserir_linha((
                    d["tipo"],d["macro"],d["micro"],d["criticidade"],d["frequencia"],
                    str(d["data_ref"]) if d["data_ref"] else "",
                    float(d["tempo"]), prazo_str if prazo_str != "—" else "",
                    status, responsavel_final, "MANUAL", d["criado_por"],
                    "APROVADA" if d["criado_por"] == "GESTOR" else "PENDENTE APROVAÇÃO",
                    "NÃO SE APLICA", 0,
                ))
                st.session_state.nova_pendente = False
                st.session_state.pop("nova_dados", None)
                st.session_state["nova_sucesso"] = f"✅ Atividade salva com sucesso — {responsavel_final or '(sem responsável)'} · Prazo: {prazo_str} · Status: {status}"
                st.rerun()
        with _c2:
            if st.button("✖ Voltar e editar", use_container_width=True):
                st.session_state.nova_pendente = False
                st.rerun()


# =========================================================
# IMPORTAR EXCEL
# =========================================================
elif st.session_state.pagina == "Importar":
    arquivo = st.file_uploader("Selecione o arquivo Excel", type=["xlsx"])
    if arquivo is not None:
        st.success("Arquivo carregado com sucesso.")
        chave_limpar = "confirmar_limpar_base"
        if not st.session_state.get(chave_limpar, False):
            if st.button("Limpar base atual"):
                st.session_state[chave_limpar] = True
                st.rerun()
        else:
            st.warning("Tem certeza? Todos os dados serão apagados.")
            cc1, cc2 = st.columns(2)
            with cc1:
                if st.button("Sim, limpar"):
                    limpar_tabela()
                    st.session_state.pop(chave_limpar, None)
                    st.warning("Base apagada com sucesso.")
            with cc2:
                if st.button("Cancelar"):
                    st.session_state.pop(chave_limpar, None)
                    st.rerun()
        if st.button("Importar agora"):
            qtd = importar_excel(arquivo)
            st.success(f"{qtd} atividades importadas com sucesso.")


# =========================================================
# SIMULADOR DE DEMANDA
# =========================================================
elif st.session_state.pagina == "Simulador":
    st.markdown("### Simular Atividade")
    st.markdown('<div style="font-size:0.82rem;color:#5e6484;margin-bottom:1rem;">Preencha os dados da demanda hipotética e veja instantaneamente o impacto na equipe.</div>', unsafe_allow_html=True)

    df_sim = preparar_dashboard(listar())
    if df_sim.empty:
        st.info("Nenhuma atividade cadastrada para simular.")
    else:
        CAPACIDADE_REF_SIM = 40.0
        _hoje_sim = pd.Timestamp.today().normalize()
        responsaveis_sim = sorted([r for r in df_sim["responsavel"].dropna().unique() if str(r).strip()], key=lambda x: x.lower())

        s1, s2, s3, s4 = st.columns(4)
        with s1:
            sim_resp  = st.selectbox("Responsável",  responsaveis_sim,   key="sim_resp")
            sim_crit  = st.selectbox("Criticidade",  OPCOES_CRITICIDADE, key="sim_crit")
        with s2:
            sim_freq  = st.selectbox("Frequência",   OPCOES_FREQUENCIA,  key="sim_freq")
            sim_tipo  = st.selectbox("Tipo",         OPCOES_TIPO,        key="sim_tipo")
        with s3:
            sim_tempo = st.number_input("Tempo estimado (h)", min_value=1, step=1, value=4, format="%d", key="sim_tempo")
            sim_prazo = st.date_input("Prazo", value=(_hoje_sim+pd.Timedelta(days=7)).date(), format="DD/MM/YYYY", key="sim_prazo")
        with s4:
            sim_macro = st.text_input("Macro ação (opcional)", key="sim_macro")
            sim_micro = st.text_input("Micro ação (opcional)", key="sim_micro")

        st.button("▶ Simular", type="primary", key="sim_btn", use_container_width=False)

        if st.session_state.get("sim_btn"):
            _prazo_sim      = pd.Timestamp(sim_prazo)
            _dias_restantes = (_prazo_sim - _hoje_sim).days

            _grp_resp    = df_sim[df_sim["responsavel"].fillna("").astype(str).str.strip() == sim_resp]
            _st_up_sim   = _grp_resp["status"].fillna("").astype(str).str.upper()
            _aberto_resp = _grp_resp[~_st_up_sim.isin(STATUS_CONCLUIDOS)]

            _h_atual  = float(_aberto_resp["horas_em_aberto"].sum())
            _at_atual = int(_aberto_resp["atrasada"].sum())
            _cr_atual = int(_aberto_resp["criticidade"].fillna("").astype(str).str.upper().isin(["ALTA","CRÍTICA"]).sum())
            _idx_atual = min((_h_atual/CAPACIDADE_REF_SIM)*100 + _at_atual*15 + _cr_atual*8, 500)
            _sem_atual = _h_atual/CAPACIDADE_REF_SIM

            _h_nova   = float(sim_tempo)
            _cr_nova  = 1 if sim_crit in ["ALTA","CRÍTICA"] else 0
            _at_nova  = 1 if _dias_restantes < 0 else 0
            _h_novo   = _h_atual + _h_nova
            _idx_novo = min((_h_novo/CAPACIDADE_REF_SIM)*100 + (_at_atual+_at_nova)*15 + (_cr_atual+_cr_nova)*8, 500)
            _sem_novo = _h_novo/CAPACIDADE_REF_SIM

            _peso_prazo_sim = 5 if _dias_restantes<0 else 4 if _dias_restantes<=1 else 3 if _dias_restantes<=3 else 2 if _dias_restantes<=7 else 1
            _peso_freq_sim  = PESO_FREQUENCIA.get(sim_freq.upper(), 1)
            _nova_crit_peso = PESO_CRITICIDADE.get(sim_crit, 1)
            _score_nova     = round(_nova_crit_peso*3 + _peso_freq_sim*2 + 3*2 + _peso_prazo_sim*3 + max(_h_nova/8,1), 2)

            _fila_resp_ord   = _aberto_resp.sort_values("score", ascending=False)
            _ativ_antes_sim  = int((_fila_resp_ord["score"] > _score_nova).sum())
            _horas_antes_sim = float(_fila_resp_ord[_fila_resp_ord["score"] > _score_nova]["horas_em_aberto"].sum())
            _inicio_est_sim  = _hoje_sim + pd.Timedelta(days=_horas_antes_sim/8.0)
            _concl_est_sim   = _inicio_est_sim + pd.Timedelta(days=_h_nova/8.0)
            _vai_atr_sim     = _concl_est_sim > _prazo_sim if pd.notna(_prazo_sim) else False
            _cor_concl_sim   = "#c94f4f" if _vai_atr_sim else "#3ecf8e"
            _margem_sim      = int((_prazo_sim - _concl_est_sim).days) if pd.notna(_prazo_sim) else 0

            _prazos_sim     = _fila_resp_ord["prazo_dt"].dropna()
            _fim_fila_sim   = _prazos_sim.max() if not _prazos_sim.empty else _hoje_sim
            _fim_fila_sim_s = _fim_fila_sim.strftime("%d/%m/%Y") if not _prazos_sim.empty else "sem prazo"
            _dur_fila_sim   = int((_fim_fila_sim - _hoje_sim).days) if not _prazos_sim.empty else 0
            _periodo_sim    = f"{_hoje_sim.strftime('%d/%m/%Y')} a {_fim_fila_sim_s} ({_dur_fila_sim} dias)"
            _pos_nova_sim   = _ativ_antes_sim + 1
            _total_sim      = len(_fila_resp_ord) + 1

            _mapa_freq_sim = {"DIÁRIA":("diária",20),"SEMANAL":("semanal",4),"QUINZENAL":("quinzenal",2),"MENSAL":("mensal",1)}
            _freq_info_sim = _mapa_freq_sim.get(sim_freq.upper())
            _horas_mes_sim = _h_nova * _freq_info_sim[1] if _freq_info_sim else 0

            def _cor_idx_s(v):
                if v<=70:  return "#3ecf8e","Disponível"
                if v<=90:  return "#e0a040","Moderado"
                if v<=120: return "#e07040","Alto"
                return "#c94f4f","Sobrecarregado"

            _cor_a,_lbl_a = _cor_idx_s(_idx_atual)
            _cor_n,_lbl_n = _cor_idx_s(_idx_novo)
            _delta  = _idx_novo - _idx_atual
            _cor_d  = "#c94f4f" if _delta>20 else "#e0a040" if _delta>0 else "#3ecf8e"
            _sinal  = f"+{_delta:.0f}%" if _delta>=0 else f"{_delta:.0f}%"

            st.divider()

            _prazo_cor_sim = "#e0a040" if 0<=_dias_restantes<=7 else "#dde1f0"
            _prazo_tag_sim = (
                '<span style="font-size:0.7rem;color:#c94f4f;font-weight:600;">&#9888; Prazo já vencido!</span>'
                if _dias_restantes < 0 else
                f'<span style="font-size:0.7rem;color:#5e6484;">&#9632; Dias até o prazo: <b style="color:{_prazo_cor_sim};">{_dias_restantes}d</b></span>'
            )
            _vai_atr_s   = "#c94f4f" if _vai_atr_sim else "#3ecf8e"
            _veredicto_s = "&#9888; Risco de atraso" if _vai_atr_sim else "&#10003; Dentro do prazo"
            _margem_sim_html = (f'<div style="font-size:0.82rem;font-weight:600;color:#c94f4f;">&#8722;{abs(_margem_sim)} dia(s) de atraso</div>'
                                if _vai_atr_sim else
                                f'<div style="font-size:0.82rem;font-weight:600;color:#3ecf8e;">+{_margem_sim} dia(s) de folga</div>')

            _html_sim = (
                '<div style="background:#111520;border:1px solid #1f2535;border-radius:10px;padding:20px 24px;">'
                '<div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:16px;">'
                '<div>'
                '<div style="font-size:0.7rem;color:#5e6484;text-transform:uppercase;letter-spacing:.1em;">An&aacute;lise de Impacto &middot; Simula&ccedil;&atilde;o</div>'
                f'<div style="font-size:1rem;font-weight:700;color:#dde1f0;margin-top:2px;">{sim_micro if sim_micro.strip() else "(nova demanda)"}</div>'
                f'<div style="font-size:0.75rem;color:#5e6484;">{sim_macro if sim_macro.strip() else "—"} &middot; {sim_tipo} &middot; {sim_freq}</div>'
                '</div>'
                '<div style="text-align:right;">'
                '<div style="font-size:0.65rem;color:#5e6484;">Respons&aacute;vel</div>'
                f'<div style="font-size:0.85rem;font-weight:700;color:#dde1f0;text-transform:uppercase;">{sim_resp}</div>'
                f'<div style="font-size:0.65rem;color:#5e6484;margin-top:2px;">{pd.Timestamp.today().strftime("%d/%m/%Y %H:%M")}</div>'
                '</div></div>'
                '<div style="display:flex;gap:0.6rem;margin-bottom:14px;flex-wrap:wrap;">'
                f'<div style="flex:1;min-width:110px;background:#161b27;border:1px solid #1f2535;border-left:3px solid {_cor_a};border-radius:6px;padding:8px 12px;">'
                '<div style="font-size:0.58rem;color:#5e6484;text-transform:uppercase;margin-bottom:2px;">&Iacute;ndice atual</div>'
                f'<div style="font-size:1.3rem;font-weight:700;color:{_cor_a};">{_idx_atual:.0f}%</div>'
                f'<div style="font-size:0.6rem;color:#5e6484;">{_lbl_a} &middot; {_sem_atual:.1f} sem.</div></div>'
                f'<div style="flex:1;min-width:110px;background:#161b27;border:1px solid #1f2535;border-left:3px solid {_cor_n};border-radius:6px;padding:8px 12px;">'
                '<div style="font-size:0.58rem;color:#5e6484;text-transform:uppercase;margin-bottom:2px;">&Iacute;ndice ap&oacute;s</div>'
                f'<div style="font-size:1.3rem;font-weight:700;color:{_cor_n};">{_idx_novo:.0f}%</div>'
                f'<div style="font-size:0.6rem;color:#5e6484;">{_lbl_n} &middot; {_sem_novo:.1f} sem.</div></div>'
                f'<div style="flex:1;min-width:110px;background:#161b27;border:1px solid #1f2535;border-left:3px solid {_cor_d};border-radius:6px;padding:8px 12px;">'
                '<div style="font-size:0.58rem;color:#5e6484;text-transform:uppercase;margin-bottom:2px;">Varia&ccedil;&atilde;o</div>'
                f'<div style="font-size:1.3rem;font-weight:700;color:{_cor_d};">{_sinal}</div>'
                f'<div style="font-size:0.6rem;color:#5e6484;">+{int(_h_nova)}h ao backlog</div></div>'
                '<div style="flex:1;min-width:110px;background:#161b27;border:1px solid #1f2535;border-left:3px solid #4c7cf3;border-radius:6px;padding:8px 12px;">'
                '<div style="font-size:0.58rem;color:#5e6484;text-transform:uppercase;margin-bottom:2px;">Posi&ccedil;&atilde;o na fila</div>'
                f'<div style="font-size:1.3rem;font-weight:700;color:#4c7cf3;">{_pos_nova_sim}&ordm; / {_total_sim}</div>'
                f'<div style="font-size:0.6rem;color:#5e6484;">score {_score_nova:.0f} &middot; prazo {_prazo_sim.strftime("%d/%m/%Y")}</div></div>'
                '</div>'
                '<div style="display:flex;gap:1rem;flex-wrap:wrap;padding-top:10px;border-top:1px solid #1f2535;">'
                f'<span style="font-size:0.7rem;color:#5e6484;">&#9632; Criticidade: <b style="color:#dde1f0;">{sim_crit}</b></span>'
                f'<span style="font-size:0.7rem;color:#5e6484;">&#9632; Tempo: <b style="color:#dde1f0;">{int(_h_nova)}h</b></span>'
                f'<span style="font-size:0.7rem;color:#5e6484;">&#9632; Atividades em aberto: <b style="color:#dde1f0;">{len(_fila_resp_ord)}</b></span>'
                f'<span style="font-size:0.7rem;color:#5e6484;">&#9632; Horas: <b style="color:#dde1f0;">{int(_h_atual)}h &rarr; {int(_h_atual+_h_nova)}h</b></span>'
                f'{_prazo_tag_sim}'
                '</div>'
                '<div style="margin-top:14px;padding:14px 16px;background:#0d1220;border-radius:8px;border:1px solid #1f2535;">'
                f'<div style="font-size:0.65rem;font-weight:700;color:#5e6484;text-transform:uppercase;letter-spacing:.1em;margin-bottom:12px;display:flex;justify-content:space-between;">'
                f'<span>Previs&atilde;o de Execu&ccedil;&atilde;o</span><span style="color:{_vai_atr_s};font-weight:700;">{_veredicto_s}</span></div>'
                '<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:12px;">'
                f'<div style="background:#161b27;border-radius:6px;padding:10px 12px;border-left:3px solid #4c7cf3;">'
                f'<div style="font-size:0.58rem;color:#5e6484;text-transform:uppercase;margin-bottom:3px;">Posi&ccedil;&atilde;o na fila</div>'
                f'<div style="font-size:1.1rem;font-weight:700;color:#4c7cf3;">{_pos_nova_sim}&ordm; de {_total_sim}</div>'
                f'<div style="font-size:0.6rem;color:#5e6484;">{_ativ_antes_sim} atividade{"s" if _ativ_antes_sim!=1 else ""} ser&atilde;o conclu&iacute;das antes</div>'
                f'<div style="font-size:0.6rem;color:#5e6484;margin-top:2px;">Per&iacute;odo da fila: {_periodo_sim}</div></div>'
                f'<div style="background:#161b27;border-radius:6px;padding:10px 12px;border-left:3px solid #5e6484;">'
                f'<div style="font-size:0.58rem;color:#5e6484;text-transform:uppercase;margin-bottom:3px;">In&iacute;cio estimado</div>'
                f'<div style="font-size:1.1rem;font-weight:700;color:#dde1f0;">{_inicio_est_sim.strftime("%d/%m/%Y")}</div>'
                f'<div style="font-size:0.6rem;color:#5e6484;">{_horas_antes_sim:.0f}h de backlog antes desta</div></div>'
                f'<div style="background:#161b27;border-radius:6px;padding:10px 12px;border-left:3px solid {_cor_concl_sim};">'
                f'<div style="font-size:0.58rem;color:#5e6484;text-transform:uppercase;margin-bottom:3px;">Conclus&atilde;o estimada</div>'
                f'<div style="font-size:1.1rem;font-weight:700;color:{_cor_concl_sim};">{_concl_est_sim.strftime("%d/%m/%Y")}</div>'
                f'<div style="font-size:0.6rem;color:#5e6484;">Prazo definido: <b style="color:#dde1f0;">{_prazo_sim.strftime("%d/%m/%Y")}</b></div></div>'
                '</div>'
                '<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;padding-top:10px;border-top:1px solid #1f2535;">'
                f'<div><div style="font-size:0.58rem;color:#5e6484;text-transform:uppercase;margin-bottom:2px;">Tempo para execu&ccedil;&atilde;o</div>'
                f'<div style="font-size:0.82rem;font-weight:600;color:#dde1f0;">{int(_h_nova)}h &asymp; {max(1,round(_h_nova/8))} dia{"s" if round(_h_nova/8)!=1 else ""} &uacute;til{"" if round(_h_nova/8)<=1 else "is"}</div></div>'
                f'<div><div style="font-size:0.58rem;color:#5e6484;text-transform:uppercase;margin-bottom:2px;">Margem at&eacute; o prazo</div>'
                + _margem_sim_html +
                '</div>'
                f'<div><div style="font-size:0.58rem;color:#5e6484;text-transform:uppercase;margin-bottom:2px;">Score desta atividade</div>'
                f'<div style="font-size:0.82rem;font-weight:600;color:#4c7cf3;">{_score_nova:.0f} pontos &middot; {sim_crit} &middot; {sim_freq}</div></div>'
                '</div>'
                + (
                    f'<div style="margin-top:10px;padding:8px 12px;background:#161b27;border-radius:6px;border-left:3px solid #e0a040;">'
                    f'<div style="font-size:0.6rem;color:#e0a040;font-weight:700;text-transform:uppercase;margin-bottom:4px;">&#8635; Atividade recorrente</div>'
                    f'<div style="font-size:0.7rem;color:#dde1f0;">Esta &eacute; uma atividade <b>{sim_freq.lower()}</b> — a cada ciclo, uma nova ocorr&ecirc;ncia &eacute; gerada automaticamente.</div>'
                    f'<div style="font-size:0.7rem;color:#5e6484;margin-top:3px;">Carga recorrente estimada: <b style="color:#dde1f0;">~{_horas_mes_sim:.0f}h/m&ecirc;s</b>. '
                    f'A an&aacute;lise refere-se apenas ao <b>primeiro ciclo</b>; nas pr&oacute;ximas recorr&ecirc;ncias a posi&ccedil;&atilde;o ser&aacute; recalculada.</div></div>'
                    if _freq_info_sim else ""
                ) +
                '<div style="font-size:0.6rem;color:#2a3040;text-align:right;margin-top:10px;">Sistema de Gest&atilde;o de Atividades &middot; An&aacute;lise gerada automaticamente</div>'
                '</div></div>'
            )
            st.markdown(_html_sim, unsafe_allow_html=True)

            st.markdown("")
            if _dias_restantes < 0:
                st.error(f"❌ O prazo informado já está vencido ({sim_prazo.strftime('%d/%m/%Y')}). Revise a data antes de criar a demanda.")
            elif _idx_novo <= 70:
                st.success(f"✅ **{sim_resp}** tem capacidade. A nova demanda pode ser atribuída sem impacto relevante.")
            elif _idx_novo <= 90:
                st.warning(f"⚠️ **{sim_resp}** ficará em carga moderada. Avalie as prioridades antes de atribuir.")
            elif _idx_novo <= 120:
                st.warning(f"🟠 **{sim_resp}** ficará com carga alta. Considere redistribuir ou postergar outras atividades.")
            else:
                st.error(f"🔴 **{sim_resp}** ficará sobrecarregada. Não é recomendável atribuir sem remover ou redistribuir outras demandas.")

            st.divider()
            st.markdown(
                '<div style="font-size:0.72rem;font-weight:600;color:#5e6484;text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px;">Fila de prioridade</div>'
                '<div style="font-size:0.78rem;color:#5e6484;margin-bottom:0.6rem;">'
                f'Todas as atividades abertas de <b style="color:#dde1f0;">{sim_resp}</b> com a nova demanda inserida. '
                'Seta ▼ indica atividades deslocadas pela nova entrada.</div>',
                unsafe_allow_html=True,
            )

            _fila_orig = _aberto_resp.sort_values("score", ascending=False).reset_index(drop=True)
            _pos_orig  = {int(float(r["id"])): i+1 for i, (_, r) in enumerate(_fila_orig.iterrows())}

            _fila = _aberto_resp[["id","micro_acao","macro_acao","criticidade","prazo","status","score","atrasada"]].copy()
            _fila["_nova"] = False
            _nova_row = {
                "id":"—","micro_acao": sim_micro if sim_micro.strip() else "(nova demanda)",
                "macro_acao": sim_macro if sim_macro.strip() else "—",
                "criticidade":sim_crit,"prazo":_prazo_sim.strftime("%d/%m/%Y"),
                "status":"NÃO INICIADA","score":_score_nova,"atrasada":False,"_nova":True
            }
            _fila = pd.concat([_fila, pd.DataFrame([_nova_row])], ignore_index=True)
            _fila = _fila.sort_values("score", ascending=False).reset_index(drop=True)
            _pos_nova = next(i+1 for i, (_, r) in enumerate(_fila.iterrows()) if r["_nova"])

            COR_C2 = {"BAIXA":"#3ecf8e","MÉDIA":"#e0a040","ALTA":"#e07040","CRÍTICA":"#c94f4f"}
            COR_S2 = {"NÃO INICIADA":"#5e6484","EM ANDAMENTO":"#4c7cf3","CONCLUÍDA":"#3ecf8e","PENDENTE APROVAÇÃO":"#e0a040"}

            _ths2 = "".join(
                f'<th style="padding:6px 10px;text-align:center;border-bottom:1px solid #1f2535;color:#5e6484;font-size:0.65rem;font-weight:500;text-transform:uppercase;white-space:nowrap;">{h}</th>'
                for h in ["","#","Micro Ação","Criticidade","Prazo","Status","Score"]
            )
            _rows2 = ""
            for _pos, (_, _r) in enumerate(_fila.iterrows(), 1):
                _nova_flag = bool(_r["_nova"])
                _c = str(_r["criticidade"]).upper()
                _s = str(_r["status"])
                if _nova_flag:
                    _seta_html = '<span style="font-size:1rem;color:#4c7cf3;font-weight:700;">★</span>'
                    _bg = "background:#111e35;"
                    _borda_l = "border-left:3px solid #4c7cf3;"
                    _pos_cor = _score_cor = "#4c7cf3"
                    _score_fw = "700"
                else:
                    _id_int  = int(float(_r["id"]))
                    _pos_ant = _pos_orig.get(_id_int, _pos)
                    _deslocou = _pos_ant < _pos
                    _seta_html = '<span style="font-size:0.85rem;color:#c94f4f;">▼</span>' if _deslocou else '<span style="font-size:0.85rem;color:#3ecf8e;">—</span>'
                    _bg       = "background:#1f1117;" if _r.get("atrasada") else ""
                    _borda_l  = "border-left:3px solid #c94f4f;" if _r.get("atrasada") else "border-left:3px solid transparent;"
                    _pos_cor  = "#5e6484"
                    _score_cor = "#dde1f0"
                    _score_fw  = "400"
                _prazo_cor = "#c94f4f" if (not _nova_flag and _r.get("atrasada")) else ("#e0a040" if _nova_flag and _dias_restantes<=3 else "#dde1f0")
                _rows2 += (
                    f'<tr style="{_bg}{_borda_l}">'
                    f'<td style="padding:5px 8px;border-bottom:1px solid #1a1f2e;text-align:center;width:28px;">{_seta_html}</td>'
                    f'<td style="padding:5px 8px;border-bottom:1px solid #1a1f2e;text-align:center;font-size:0.72rem;color:{_pos_cor};font-weight:600;white-space:nowrap;">{_pos}º</td>'
                    f'<td style="padding:5px 10px;border-bottom:1px solid #1a1f2e;text-align:left;font-size:0.75rem;color:#dde1f0;">{_r["micro_acao"]}</td>'
                    f'<td style="padding:5px 8px;border-bottom:1px solid #1a1f2e;text-align:center;"><span style="background:{COR_C2.get(_c,"#5e6484")};color:#fff;font-size:0.58rem;padding:1px 6px;border-radius:3px;font-weight:600;">{_c}</span></td>'
                    f'<td style="padding:5px 8px;border-bottom:1px solid #1a1f2e;text-align:center;font-size:0.72rem;color:{_prazo_cor};white-space:nowrap;">{_r["prazo"]}</td>'
                    f'<td style="padding:5px 8px;border-bottom:1px solid #1a1f2e;text-align:center;"><span style="background:{COR_S2.get(_s,"#5e6484")};color:#fff;font-size:0.58rem;padding:1px 6px;border-radius:3px;font-weight:600;">{_s}</span></td>'
                    f'<td style="padding:5px 10px;border-bottom:1px solid #1a1f2e;text-align:center;font-size:0.75rem;color:{_score_cor};font-weight:{_score_fw};">{float(_r["score"]):.1f}</td>'
                    f'</tr>'
                )

            st.markdown(f'<table style="width:100%;border-collapse:collapse;"><thead><tr style="background:#111520;">{_ths2}</tr></thead><tbody>{_rows2}</tbody></table>', unsafe_allow_html=True)
            st.markdown(
                f'<div style="display:flex;gap:1.5rem;margin-top:0.5rem;flex-wrap:wrap;">'
                f'<span style="font-size:0.7rem;color:#4c7cf3;font-weight:600;">★ Nova demanda — posição {_pos_nova}º de {len(_fila)}</span>'
                f'<span style="font-size:0.7rem;color:#5e6484;">▼ deslocada para baixo &nbsp;·&nbsp; — mantém posição</span>'
                f'</div>',
                unsafe_allow_html=True,
            )


# =========================================================
# DEFINIÇÕES
# =========================================================
elif st.session_state.pagina == "Definições":

    def bloco(titulo, conteudo, cor_borda="#4c7cf3"):
        st.markdown(
            f'<div style="background:#161b27;border:1px solid #1f2535;border-left:3px solid {cor_borda};'
            f'border-radius:6px;padding:14px 18px;margin-bottom:0.8rem;">'
            f'<div style="font-size:0.8rem;font-weight:700;color:{cor_borda};text-transform:uppercase;'
            f'letter-spacing:.07em;margin-bottom:10px;">{titulo}</div>'
            f'<div style="font-size:0.85rem;color:#dde1f0;line-height:1.9;">{conteudo}</div></div>',
            unsafe_allow_html=True,
        )

    def var(nome, descricao):
        return (f'<div style="display:flex;gap:0.5rem;margin-top:4px;">'
                f'<span style="color:#4c7cf3;font-weight:600;white-space:nowrap;min-width:220px;">{nome}</span>'
                f'<span style="color:#5e6484;">→</span>'
                f'<span style="color:#dde1f0;">{descricao}</span></div>')

    def formula(expr):
        return (f'<div style="background:#111520;border:1px solid #1f2535;border-radius:4px;'
                f'padding:8px 14px;margin:8px 0;font-family:monospace;font-size:0.82rem;color:#3ecf8e;">{expr}</div>')

    def subtitulo(texto):
        st.markdown(f'<div style="font-size:0.72rem;font-weight:600;color:#5e6484;text-transform:uppercase;'
                    f'letter-spacing:.1em;margin-bottom:0.6rem;margin-top:1.2rem;">{texto}</div>', unsafe_allow_html=True)

    st.markdown("### Definições e Memória de Cálculo")
    st.markdown('<div style="font-size:0.82rem;color:#5e6484;margin-bottom:1.5rem;">Referência completa de todos os conceitos, classificações e fórmulas utilizados no sistema. Cada cálculo é explicado com suas variáveis para total transparência.</div>', unsafe_allow_html=True)

    subtitulo("1. Campos e Classificações")

    bloco("Tipo de Atividade", """
        Define a natureza estratégica da atividade:<br><br>
        <b>Estratégia</b> — atividades de planejamento, análise e tomada de decisão.<br>
        <b>Operacional</b> — atividades rotineiras ou de execução direta de processos.<br>
        <b>Externo</b> — demandas originadas fora da equipe: outras áreas, clientes ou fornecedores.<br><br>
        O tipo não influencia o score, mas permite filtrar e segmentar a carga de trabalho.
    """)
    bloco("Criticidade", """
        Grau de importância e impacto caso a atividade não seja realizada no prazo:<br><br>
        <b>Baixa</b> — impacto mínimo; pode ser postergada. <b>Peso: 1</b><br>
        <b>Média</b> — impacto moderado em processos ou entregas. <b>Peso: 2</b><br>
        <b>Alta</b> — risco significativo de comprometer resultados. <b>Peso: 3</b><br>
        <b>Crítica</b> — impacto severo e imediato; deve ser priorizada acima de qualquer outra. <b>Peso: 4</b><br><br>
        O peso é multiplicado por 3 no cálculo do score — um dos maiores fatores de influência.
    """)
    bloco("Frequência", """
        Com que regularidade a atividade precisa ser executada:<br><br>
        <b>Isolada</b> — ocorre uma única vez. Prazo definido manualmente. <b>Peso: 1</b><br>
        <b>Mensal</b> — repete-se todo mês. Prazo = data de referência + 30 dias. <b>Peso: 2</b><br>
        <b>Quinzenal</b> — repete-se a cada 15 dias. Prazo = data de referência + 15 dias. <b>Peso: 3</b><br>
        <b>Semanal</b> — repete-se toda semana. Prazo = data de referência + 7 dias. <b>Peso: 4</b><br>
        <b>Diária</b> — ocorre todos os dias úteis. Prazo = data de referência + 1 dia. <b>Peso: 5</b><br><br>
        O peso é multiplicado por 2 no cálculo do score.
    """)
    bloco("Status", """
        Estado atual da atividade no ciclo de execução:<br><br>
        <b>Não Iniciada</b> — aprovada pelo gestor, aguardando início. <b>Peso de urgência: 3</b><br>
        <b>Em Andamento</b> — em execução pelo responsável. <b>Peso: 2</b><br>
        <b>Pendente Aprovação</b> — criada por colaborador, aguardando liberação do gestor. <b>Peso: 4</b><br>
        <b>Concluída</b> — validada pelo gestor como finalizada. <b>Peso: 1</b> — não contribui para carga.<br><br>
        O peso é multiplicado por 2 no cálculo do score.
    """)
    bloco("Origem", """
        Indica como a atividade entrou no sistema:<br><br>
        <b>Manual</b> — criada diretamente pela interface pelo gestor ou colaborador.<br>
        <b>Importação Excel</b> — inserida via planilha; o nome da aba é registrado como origem.<br><br>
        A origem não influencia cálculos, mas permite rastrear de onde cada demanda veio.
    """)
    bloco("Fluxo de Aprovação", """
        O sistema possui dois pontos de controle pelo gestor:<br><br>
        <b>1. Aprovação de Início</b> — quando um colaborador cria uma atividade, ela fica com status
        "Pendente Aprovação" até o gestor aprovar. Após aprovação, passa para "Não Iniciada".<br><br>
        <b>2. Aprovação de Conclusão</b> — quando o colaborador marca como concluída, o gestor valida.
        Após aprovação, o status é confirmado como "Concluída" e o % vai a 100.<br><br>
        Em ambos os casos, o gestor pode <b>reprovar</b>, devolvendo ao estado anterior.
        Atividades criadas pelo próprio gestor são aprovadas automaticamente.
    """)

    subtitulo("2. Memória de Cálculo")

    bloco("Horas em Aberto", f"""
        Representa o trabalho restante estimado para cada atividade.
        {formula("Horas em Aberto = Tempo Estimado × (100 − % Conclusão) ÷ 100")}
        <b>Variáveis:</b>
        {var("Tempo Estimado", "Horas totais previstas para concluir a atividade, informadas no cadastro.")}
        {var("% Conclusão", "Percentual de avanço informado pelo responsável, de 0 a 100%.")}
        <br><b>Exemplos:</b><br>
        Atividade de 10h com 40% concluída → 10 × (100 − 40) ÷ 100 = <b>6h em aberto</b><br>
        Atividade de 8h com 0% → <b>8h em aberto</b> · Atividade concluída → sempre <b>0h em aberto</b>.
    """)
    bloco("Score de Prioridade", f"""
        Índice composto que ordena as atividades do mais urgente ao menos urgente. Quanto maior, maior a prioridade.
        {formula("Score = (Peso Criticidade × 3) + (Peso Frequência × 2) + (Peso Status × 2) + (Peso Prazo × 3) + Peso Tempo")}
        <b>Variáveis:</b>
        {var("Peso Criticidade", "1 a 4 conforme tabela. Multiplicado por 3 por ser fator determinante de risco.")}
        {var("Peso Frequência", "1 a 5 conforme tabela. Multiplicado por 2.")}
        {var("Peso Status", "1 a 4 conforme tabela. Multiplicado por 2.")}
        {var("Peso Prazo", "Vencida→5 | Hoje/amanhã→4 | Até 3 dias→3 | Até 7 dias→2 | Mais de 7 dias→1. Multiplicado por 3.")}
        {var("Peso Tempo", "Tempo estimado ÷ 8, mínimo 1. Representa o volume de trabalho envolvido.")}
        <br><b>Score máximo fixo:</b> (4×3)+(5×2)+(4×2)+(5×3)=45, mais o peso de tempo. Scores acima de 30 indicam urgência crítica.
    """)
    bloco("Índice de Comprometimento — Heatmap das 4 Semanas", f"""
        Mede a carga prevista de cada pessoa por semana, distribuindo as horas proporcionalmente até o prazo.
        <br><br><b>Passo 1 — Distribuição de horas por semana:</b>
        {formula("Horas na semana W = Horas em Aberto ÷ Nº de semanas entre hoje e o prazo")}
        {var("Sem prazo ou prazo além de 4 semanas", "Horas distribuídas igualmente pelas 4 semanas.")}
        {var("Atividade vencida", "Todas as horas vão para a semana 1 (urgente).")}
        <br><b>Passo 2 — Cálculo do Índice:</b>
        {formula("Índice (%) = (Horas comprometidas na semana ÷ 40) × 100 + (Atrasos × 15) + (Críticas × 8)")}
        {var("40", "Capacidade de referência semanal por pessoa (40 horas úteis).")}
        {var("Atrasos × 15", "Cada atividade vencida soma 15 pontos percentuais de pressão.")}
        {var("Críticas × 8", "Cada atividade Alta em aberto soma 8 pontos percentuais.")}
        <br>🟢 Disponível → até 70% &nbsp;|&nbsp; 🟡 Moderado → 71–90% &nbsp;|&nbsp; 🟠 Alto → 91–120% &nbsp;|&nbsp; 🔴 Sobrecarregado → acima de 120%
    """)
    bloco("Diagnóstico da Equipe", f"""
        <b>Déficit de Capacidade:</b>
        {formula("Déficit = Horas em Aberto − (Nº Pessoas × 40h × 4 semanas)")}
        {var("Resultado positivo", "Há trabalho além da capacidade — essas horas só ocorrem após as 4 semanas.")}
        <b>Semanas para Zerar Backlog:</b>
        {formula("Semanas = Horas em Aberto ÷ (Nº Pessoas × 40h)")}
        <b>Índice Individual:</b>
        {formula("Índice individual = (Horas em Aberto da pessoa ÷ 40h) × 100 + Atrasos × 15 + Críticas × 8")}
        {var("Sobrecarregada", "Pessoa com índice individual acima de 100%.")}
        {var("Mais sobrecarregada", "Pessoa com maior número de semanas de trabalho em aberto (Horas ÷ 40h).")}
        <b>Taxa de Atraso:</b>
        {formula("Taxa (%) = (Atividades vencidas ÷ Total de atividades visíveis) × 100")}
    """)
    bloco("Prazo Automático para Atividades Recorrentes", f"""
        Para frequências não-isoladas, o prazo é gerado automaticamente:
        {formula("Prazo = Data de Referência + Incremento conforme frequência")}
        {var("Diária", "Data de Referência + 1 dia")}
        {var("Semanal", "Data de Referência + 7 dias")}
        {var("Quinzenal", "Data de Referência + 15 dias")}
        {var("Mensal", "Data de Referência + 30 dias")}
        {var("Data de Referência", "Data de início do ciclo, informada no cadastro da atividade.")}
    """)
    bloco("Regra de Atividade Atrasada", f"""
        {formula("Atrasada = (Data do Prazo < Data de Hoje) E (Status ≠ Concluída)")}
        Atividades atrasadas são destacadas em vermelho e penalizam o índice de comprometimento em +15% por ocorrência.
    """)

    subtitulo("3. Funcionamento do Sistema")

    bloco("Histórico de Alterações", """
        Registra automaticamente toda ação relevante: atualização de status e %, aprovações, reprovações e edições de campos.
        Cada registro contém <b>data e hora</b>, <b>tipo de ação</b>, <b>campo alterado</b>, <b>valor anterior</b> e <b>valor novo</b>.
        Visível ao expandir qualquer atividade em "Atualizar Atividades".
    """)
    bloco("Backup Automático", """
        A cada inicialização, uma cópia do banco é salva em <code style="background:#1f2535;padding:1px 6px;border-radius:3px;">backups/</code>
        com nome <code style="background:#1f2535;padding:1px 6px;border-radius:3px;">gestao_atividades_AAAAMMDD.db</code>.
        Máximo de <b>7 backups</b> mantidos. Para restaurar, copie o arquivo e renomeie para <code style="background:#1f2535;padding:1px 6px;border-radius:3px;">gestao_atividades.db</code>.
    """)
    bloco("Importação via Excel", """
        Colunas reconhecidas: <code style="background:#1f2535;padding:2px 8px;border-radius:3px;font-size:0.78rem;">
        TIPO | MACRO AÇÃO | MICRO AÇÃO | CRITICIDADE | FREQUÊNCIA | TEMPO | PRAZO | STATUS | RESPONSÁVEL | % CONCLUSÃO</code><br><br>
        Variações com/sem acento são aceitas. Linhas sem micro ação são ignoradas.
        O nome da aba fica registrado como origem. Recomenda-se limpar a base antes de reimportar.
    """)
    bloco("Simulação de Demanda", """
        Permite simular o impacto de uma nova atividade antes de criá-la:<br><br>
        &bull; Verifica capacidade do responsável com base no índice de comprometimento<br>
        &bull; Mostra a posição que a nova demanda ocuparia na fila de prioridade<br>
        &bull; Estima início e conclusão considerando o backlog atual<br>
        &bull; Identifica atividades deslocadas e risco de atraso<br><br>
        A simulação não altera dados — é apenas uma prévia do impacto.
    """)


# =========================================================
# RODAPÉ
# =========================================================
st.markdown(
    '<div style="margin-top:3rem;text-align:center;color:#1f2535;font-size:0.82rem;letter-spacing:0.08em;text-transform:uppercase;">'
    'Software Livre e de Uso Aberto — Sinta-se à Vontade para Usar, Adaptar e Evoluir.</div>',
    unsafe_allow_html=True,
)
