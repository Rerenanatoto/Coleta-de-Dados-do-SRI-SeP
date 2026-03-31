from __future__ import annotations

import os
import tempfile
from io import BytesIO
from pathlib import Path

import pandas as pd
import streamlit as st

import replica_indicadores_publicos_br as core

st.set_page_config(
    page_title="Réplica de Indicadores Públicos do Brasil",
    page_icon="📊",
    layout="wide",
)


def _excel_bytes_from_tables(tabelas: dict[str, pd.DataFrame]) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(
        output,
        engine="openpyxl",
        datetime_format="YYYY-MM-DD",
        date_format="YYYY-MM-DD",
    ) as writer:
        for nome_aba, df in tabelas.items():
            sheet = nome_aba[:31]
            df.to_excel(writer, sheet_name=sheet, index=False)
            ws = writer.sheets[sheet]
            ws.freeze_panes = "B2"
            for idx, col in enumerate(df.columns, start=1):
                values = df[col].head(1000).fillna("").astype(str).tolist()
                max_len = max([len(str(col))] + [len(v) for v in values])
                ws.column_dimensions[ws.cell(row=1, column=idx).column_letter].width = min(max_len + 2, 28)
    output.seek(0)
    return output.getvalue()


@st.cache_data(show_spinner=False)
def gerar_tabelas(start_year: int, end_year: int, rmd_bytes: bytes | None, rmd_name: str | None):
    temp_path = None
    old_start = core.START_YEAR
    old_end = core.END_YEAR
    old_rmd = core.RMD_FILE

    try:
        if rmd_bytes:
            suffix = Path(rmd_name or "rmd.xlsx").suffix or ".xlsx"
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(rmd_bytes)
                temp_path = tmp.name

        core.START_YEAR = int(start_year)
        core.END_YEAR = int(end_year)
        core.RMD_FILE = temp_path

        economic = core.build_economic_data()
        monetary = core.build_monetary_data()
        fiscal = core.build_general_government_data()
        bop = core.build_balance_of_payments_data()
        ebs = core.build_external_balance_sheet()
        debt = core.build_central_government_debt_and_borrowing_data()

        tabelas = {
            "Economic Data": economic,
            "Monetary Data": monetary,
            "General Government Data": fiscal,
            "Balance-Of-Payments Data": bop,
            "External Balance Sheet": ebs,
            "Central Gov Debt and Borrowing": debt,
        }
        excel_bytes = _excel_bytes_from_tables(tabelas)
        return tabelas, excel_bytes
    finally:
        core.START_YEAR = old_start
        core.END_YEAR = old_end
        core.RMD_FILE = old_rmd
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)


st.title("📊 Réplica de Indicadores Públicos do Brasil")
st.caption(
    "App Streamlit para gerar a planilha Excel a partir das séries do SGS/BCB, SIDRA/IBGE "
    "e, opcionalmente, do arquivo RMD do Tesouro."
)

with st.sidebar:
    st.header("Parâmetros")
    ano_atual = int(core.END_YEAR)
    ano_inicial_default = int(core.START_YEAR)

    with st.form("form_parametros"):
        start_year = st.number_input(
            "Ano inicial",
            min_value=1990,
            max_value=ano_atual,
            value=ano_inicial_default,
            step=1,
        )
        end_year = st.number_input(
            "Ano final",
            min_value=int(start_year),
            max_value=ano_atual,
            value=ano_atual,
            step=1,
        )
        rmd_file = st.file_uploader(
            "Arquivo RMD (opcional)",
            type=["xlsx", "xlsm", "xls"],
            help="Se enviado, preenche a aba 'Central Gov Debt and Borrowing'.",
        )
        submit = st.form_submit_button("Gerar base")

    st.markdown("---")
    st.markdown(
        "**Dependências esperadas**"
        "- APIs do BCB (SGS)"
        "- SIDRA/IBGE via `sidrapy`"
        "- Arquivo RMD opcional"
    )

if "resultado" not in st.session_state:
    st.session_state.resultado = None

if submit:
    try:
        rmd_bytes = rmd_file.getvalue() if rmd_file else None
        rmd_name = rmd_file.name if rmd_file else None
        with st.spinner("Consultando bases e montando as tabelas..."):
            tabelas, excel_bytes = gerar_tabelas(
                int(start_year),
                int(end_year),
                rmd_bytes,
                rmd_name,
            )
        st.session_state.resultado = {
            "tabelas": tabelas,
            "excel_bytes": excel_bytes,
            "arquivo_saida": f"replica_indicadores_publicos_brasil_{int(start_year)}_{int(end_year)}.xlsx",
            "intervalo": (int(start_year), int(end_year)),
            "tem_rmd": bool(rmd_file),
        }
        st.success("Base gerada com sucesso.")
    except Exception as e:
        st.session_state.resultado = None
        st.error(f"Erro ao gerar a base: {e}")

resultado = st.session_state.resultado

if resultado is None:
    st.info("Defina os parâmetros na barra lateral e clique em **Gerar base**.")
    st.markdown(
        "### O que este app faz"
        "- Gera 6 tabelas compatíveis com a sua réplica de indicadores."
        "- Permite visualizar cada aba antes do download."
        "- Exporta tudo em um único arquivo Excel formatado."
    )
else:
    tabelas = resultado["tabelas"]
    excel_bytes = resultado["excel_bytes"]
    start_year, end_year = resultado["intervalo"]

    col1, col2, col3 = st.columns(3)
    col1.metric("Abas geradas", len(tabelas))
    col2.metric("Ano inicial", start_year)
    col3.metric("Ano final", end_year)

    st.download_button(
        label="⬇️ Baixar arquivo Excel",
        data=excel_bytes,
        file_name=resultado["arquivo_saida"],
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

    if not resultado["tem_rmd"]:
        st.warning(
            "Nenhum arquivo RMD foi enviado. Nesse caso, a aba **Central Gov Debt and Borrowing** "
            "será gerada com colunas vazias (estrutura preservada)."
        )

    st.markdown("### Pré-visualização das abas")
    abas = st.tabs(list(tabelas.keys()))
    for aba, (nome, df) in zip(abas, tabelas.items()):
        with aba:
            c1, c2 = st.columns([1, 1])
            c1.metric("Linhas", len(df))
            c2.metric("Colunas", len(df.columns))
            st.dataframe(df, use_container_width=True, height=500)
