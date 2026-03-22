import io
import re
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="S&P SRI Sovereign", layout="wide")

META_COLS = ["country_name", "country_code", "lt_fc_rating"]
APP_DIR = Path(__file__).resolve().parent
DATA_DIR = APP_DIR / "data"


def normalize_label(text: str) -> str:
    text = str(text).strip()
    text = re.sub(r"\s+", " ", text)
    return text


def slugify(text: str) -> str:
    text = normalize_label(text).lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def coerce_numeric(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    s = s.replace(
        {
            "N/A": np.nan,
            "N.M.": np.nan,
            "NM": np.nan,
            "None": np.nan,
            "nan": np.nan,
            "": np.nan,
        }
    )
    return pd.to_numeric(s, errors="coerce")


def find_local_xlsx() -> Path | None:
    preferred_names = [
        DATA_DIR / "base.xlsx",
        DATA_DIR / "report.xlsx",
        APP_DIR / "base.xlsx",
        APP_DIR / "report.xlsx",
    ]
    for candidate in preferred_names:
        if candidate.exists():
            return candidate

    for search_dir in [DATA_DIR, APP_DIR]:
        if search_dir.exists():
            files = sorted([p for p in search_dir.glob("*.xlsx") if not p.name.startswith("~$")])
            if files:
                return files[0]
    return None


def find_data_end(raw: pd.DataFrame) -> int:
    first_col = raw.iloc[:, 0].astype(str).fillna("").str.strip().str.lower()
    end_idx = len(raw)
    for idx, value in enumerate(first_col):
        if (
            value.startswith("lt fc--")
            or value.startswith("copyright")
            or value.startswith("no content")
            or value.startswith("credit-related")
            or value.startswith("to reprint")
            or value.startswith("any passwords/user ids")
        ):
            end_idx = idx
            break
    return end_idx


def parse_sheet(raw: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
    raw = raw.copy().dropna(how="all", axis=1)
    if len(raw) < 6:
        return pd.DataFrame()

    end_idx = find_data_end(raw)
    raw = raw.iloc[:end_idx].reset_index(drop=True)
    if len(raw) < 6:
        return pd.DataFrame()

    indicator_row = raw.iloc[3].tolist()
    year_row = raw.iloc[4].tolist()

    records = []
    current_indicator = None
    for col_idx, cell in enumerate(indicator_row):
        if col_idx < 3:
            continue
        if pd.notna(cell):
            current_indicator = normalize_label(cell)
        year_value = year_row[col_idx] if col_idx < len(year_row) else None
        if current_indicator and pd.notna(year_value):
            records.append(
                {
                    "col_idx": col_idx,
                    "indicator": current_indicator,
                    "indicator_key": slugify(current_indicator),
                    "year": str(year_value).strip(),
                }
            )

    if not records:
        return pd.DataFrame()

    col_map = pd.DataFrame(records)
    data = raw.iloc[5:].copy().dropna(how="all")
    if data.empty:
        return pd.DataFrame()

    rename_map = {}
    if 0 in data.columns:
        rename_map[0] = "country_name"
    if 1 in data.columns:
        rename_map[1] = "country_code"
    if 2 in data.columns:
        rename_map[2] = "lt_fc_rating"
    data = data.rename(columns=rename_map)

    required_cols = ["country_name", "country_code", "lt_fc_rating"]
    if not all(col in data.columns for col in required_cols):
        return pd.DataFrame()

    usable_cols = [c for c in col_map["col_idx"].tolist() if c in data.columns]
    if not usable_cols:
        return pd.DataFrame()

    col_map = col_map[col_map["col_idx"].isin(usable_cols)].copy()
    data = data[required_cols + usable_cols].copy()

    data["country_name"] = data["country_name"].astype(str).str.strip()
    data["country_code"] = data["country_code"].astype(str).str.strip()
    data["lt_fc_rating"] = data["lt_fc_rating"].astype(str).str.strip()

    invalid_starts = ("lt fc--", "copyright", "no content")
    data = data[
        data["country_name"].ne("")
        & ~data["country_name"].str.lower().str.startswith(invalid_starts)
    ].copy()

    if data.empty:
        return pd.DataFrame()

    long_df = data.melt(
        id_vars=required_cols,
        value_vars=usable_cols,
        var_name="col_idx",
        value_name="value_raw",
    )
    long_df = long_df.merge(col_map, on="col_idx", how="left")
    long_df["sheet"] = sheet_name
    long_df["sheet_key"] = slugify(sheet_name)
    long_df["value"] = coerce_numeric(long_df["value_raw"])
    long_df["year"] = long_df["year"].astype(str).str.strip()
    long_df["year_num"] = pd.to_numeric(long_df["year"].str.extract(r"(\d{4})")[0], errors="coerce")
    long_df["is_forecast"] = long_df["year"].str.contains(r"[ef]$", case=False, na=False)

    return long_df[
        [
            "sheet",
            "sheet_key",
            "country_name",
            "country_code",
            "lt_fc_rating",
            "indicator",
            "indicator_key",
            "year",
            "year_num",
            "is_forecast",
            "value",
        ]
    ]


@st.cache_data(show_spinner=False)
def load_workbook(file_bytes=None) -> pd.DataFrame:
    if file_bytes is not None:
        source = io.BytesIO(file_bytes)
    else:
        local_file = find_local_xlsx()
        if local_file is None:
            return pd.DataFrame(
                columns=[
                    "sheet",
                    "sheet_key",
                    "country_name",
                    "country_code",
                    "lt_fc_rating",
                    "indicator",
                    "indicator_key",
                    "year",
                    "year_num",
                    "is_forecast",
                    "value",
                ]
            )
        source = local_file

    xls = pd.ExcelFile(source, engine="openpyxl")
    frames = []
    for sheet in xls.sheet_names:
        raw = pd.read_excel(xls, sheet_name=sheet, header=None, engine="openpyxl")
        try:
            parsed = parse_sheet(raw, sheet)
            if not parsed.empty:
                frames.append(parsed)
        except Exception:
            continue

    if not frames:
        return pd.DataFrame(
            columns=[
                "sheet",
                "sheet_key",
                "country_name",
                "country_code",
                "lt_fc_rating",
                "indicator",
                "indicator_key",
                "year",
                "year_num",
                "is_forecast",
                "value",
            ]
        )

    df = pd.concat(frames, ignore_index=True)
    return df.dropna(subset=["indicator", "year"])


def build_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.header("Filtros")

    # ===== Categoria (antes era "Aba da planilha") =====
    all_categories = sorted(df["sheet"].dropna().unique().tolist())
    selected_categories = st.sidebar.multiselect(
        "Categoria",
        options=all_categories,
        default=all_categories,   # mantém o comportamento "tudo selecionado" como antes
        key="f_categories",
        help="Escolha uma ou mais categorias (abas da planilha)."
    )

    df1 = df[df["sheet"].isin(selected_categories)] if selected_categories else df.copy()

    # ===== Rating =====
    all_ratings = sorted(df1["lt_fc_rating"].dropna().unique().tolist())
    selected_ratings = st.sidebar.multiselect(
        "LT FC rating",
        options=all_ratings,
        default=[],               # vazio = todos (no recorte atual)
        key="f_ratings",
        help="Deixe vazio para considerar todos os ratings."
    )

    df2 = df1[df1["lt_fc_rating"].isin(selected_ratings)] if selected_ratings else df1.copy()

    # ===== País =====
    all_countries = sorted(df2["country_name"].dropna().unique().tolist())
    selected_countries = st.sidebar.multiselect(
        "País",
        options=all_countries,
        default=[],               # ✅ vazio = todos os países compatíveis com Categoria+Rating
        key="f_countries",
        help="Deixe vazio para considerar todos os países do recorte atual."
    )

    # ===== Indicadores =====
    all_indicators = sorted(df2["indicator"].dropna().unique().tolist())
    selected_indicators = st.sidebar.multiselect(
        "Indicadores",
        options=all_indicators,
        default=[],               # vazio = todos os indicadores do recorte atual
        key="f_indicators",
        help="Deixe vazio para considerar todos os indicadores do recorte atual."
    )

    # ===== Anos =====
    valid_years = df2["year_num"].dropna()
    year_min, year_max = (2019, 2028) if valid_years.empty else (int(valid_years.min()), int(valid_years.max()))
    selected_year_range = st.sidebar.slider(
        "Faixa de anos",
        min_value=year_min,
        max_value=year_max,
        value=(year_min, year_max),
        key="f_years"
    )

    # ===== Histórico vs Projeção =====
    forecast_mode = st.sidebar.radio(
        "Período",
        ["Todos", "Somente históricos", "Somente estimativas/projeções"],
        index=0,
        key="f_forecast"
    )

    # ===== Aplicar filtros (somente se o usuário selecionou algo) =====
    filtered = df2.copy()

    if selected_countries:
        filtered = filtered[filtered["country_name"].isin(selected_countries)]

    if selected_indicators:
        filtered = filtered[filtered["indicator"].isin(selected_indicators)]

    filtered = filtered[filtered["year_num"].between(selected_year_range[0], selected_year_range[1], inclusive="both")]

    if forecast_mode == "Somente históricos":
        filtered = filtered[~filtered["is_forecast"]]
    elif forecast_mode == "Somente estimativas/projeções":
        filtered = filtered[filtered["is_forecast"]]

    return filtered

def render_dashboard_tab(df: pd.DataFrame):
    st.subheader("Dashboards")

    if df.empty:
        st.warning("Nenhum dado encontrado com os filtros selecionados.")
        return

    # KPIs
    c1, c2, c3 = st.columns(3)
    c1.metric("Países", df["country_name"].nunique())
    c2.metric("Indicadores", df["indicator"].nunique())
    c3.metric("Observações", f"{len(df):,}".replace(",", "."))

    # === NOVO: escolher uma aba (sheet) para mostrar TODOS os indicadores dela ===
    available_sheets = sorted(df["sheet"].dropna().unique().tolist())
    if not available_sheets:
        st.info("Nenhuma aba disponível para plotagem com os filtros atuais.")
        return

    # se houver mais de 1 aba nos dados filtrados, deixa o usuário escolher qual quer plotar
    if len(available_sheets) == 1:
        sheet_for_charts = available_sheets[0]
        st.caption(f"Mostrando todos os indicadores da aba: **{sheet_for_charts}**")
    else:
        sheet_for_charts = st.selectbox(
            "Aba para gerar gráficos (um gráfico por indicador)",
            available_sheets,
            index=0,
        )

    plot_df = df[df["sheet"] == sheet_for_charts].copy()
    plot_df = plot_df.dropna(subset=["year_num", "value"])

    if plot_df.empty:
        st.info("Sem dados numéricos para gerar gráficos nesta aba com os filtros atuais.")
        return

    # Lista de indicadores desta aba
    indicators = sorted(plot_df["indicator"].dropna().unique().tolist())
    if not indicators:
        st.info("Nenhum indicador encontrado para esta aba.")
        return

    # Opcional: ordenar por nome e mostrar tudo (geralmente ~8 a ~15 por aba, ok)
    st.markdown(f"### {sheet_for_charts} — gráficos para **{len(indicators)}** indicadores")

    # Layout em grade (2 colunas). Ajuste para 3 se quiser mais compacto.
    cols_per_row = 2

    # Para evitar legendas gigantes, você pode esconder a legenda quando tiver muitos países
    show_legend = plot_df["country_name"].nunique() <= 12

    for i in range(0, len(indicators), cols_per_row):
        row_inds = indicators[i : i + cols_per_row]
        row_cols = st.columns(cols_per_row)

        for col, ind in zip(row_cols, row_inds):
            with col:
                ind_df = plot_df[plot_df["indicator"] == ind].sort_values(["country_name", "year_num"])

                if ind_df.empty:
                    st.caption(f"Sem dados para: {ind}")
                    continue

                fig = px.line(
                    ind_df,
                    x="year_num",
                    y="value",
                    color="country_name",
                    markers=True,
                    hover_data=["lt_fc_rating", "year", "country_code"],
                    title=ind,
                )
                fig.update_layout(
                    height=340,
                    margin=dict(l=10, r=10, t=50, b=10),
                    legend_title_text="País",
                    showlegend=show_legend,
                )
                fig.update_xaxes(title="Ano")
                fig.update_yaxes(title="Valor")

                st.plotly_chart(fig, use_container_width=True)

    # Resumo por rating (mantido)
    st.markdown("#### Média por LT FC rating (no recorte atual)")
    rating_summary = (
        df.groupby(["lt_fc_rating", "indicator"], as_index=False)["value"]
          .mean()
          .rename(columns={"value": "media_valor"})
          .sort_values(["indicator", "lt_fc_rating"])
    )
    st.dataframe(rating_summary, use_container_width=True, hide_index=True)

def render_table_tab(df: pd.DataFrame):
    st.subheader("Dados em tabela")
    if df.empty:
        st.warning("Nenhum dado encontrado com os filtros selecionados.")
        return

    view_mode = st.radio("Visualização", ["Longa (recomendada)", "Pivotada"], horizontal=True, index=0)
    if view_mode == "Longa (recomendada)":
        display_df = df.sort_values(["sheet", "country_name", "indicator", "year_num"]).copy()
    else:
        display_df = (
            df.pivot_table(
                index=["sheet", "country_name", "country_code", "lt_fc_rating", "indicator"],
                columns="year",
                values="value",
                aggfunc="first",
            )
            .reset_index()
        )

    st.dataframe(display_df, use_container_width=True, hide_index=True)
    csv_data = display_df.to_csv(index=False).encode("utf-8-sig")
    st.download_button("Baixar CSV da visualização atual", data=csv_data, file_name="bda_filtrado.csv", mime="text/csv")


def main():
    st.title("S&P SRI Sovereign")
    st.caption("Dados públicos do site Sovereign Risk Indicators - S&P")

    local_file = find_local_xlsx()
    with st.expander("Arquivo de entrada", expanded=False):
        uploaded = st.file_uploader("Se quiser, envie um arquivo .xlsx para substituir a base local", type=["xlsx"])
        if uploaded is None and local_file is not None:
            st.success(f"Usando arquivo do repositório: {local_file.relative_to(APP_DIR)}")
        elif uploaded is None:
            st.info("Nenhum arquivo local encontrado. Faça upload de um .xlsx ou adicione um arquivo em ./data.")

    uploaded_bytes = uploaded.getvalue() if uploaded is not None else None
    df = load_workbook(uploaded_bytes)
    if df.empty:
        st.error("Não foi possível interpretar a estrutura do workbook.")
        return

    filtered = build_filters(df)

    tab1, tab2 = st.tabs(["Dashboards", "Dados em tabela"])
    with tab1:
        render_dashboard_tab(filtered)
    with tab2:
        render_table_tab(filtered)

    with st.expander("Dicionário de campos", expanded=False):
        st.markdown(
            """
            - **sheet**: nome da aba original da planilha.
            - **country_name**: nome do país.
            - **country_code**: código do país.
            - **lt_fc_rating**: rating LT FC.
            - **indicator**: nome do indicador.
            - **year**: ano original da base (preserva `e` e `f`).
            - **year_num**: ano numérico para ordenação.
            - **is_forecast**: identifica estimativa/projeção.
            - **value**: valor numérico convertido para análise.
            """
        )


if __name__ == "__main__":
    main()
