from __future__ import annotations

import re
import unicodedata
from io import StringIO
from pathlib import Path
from datetime import datetime
from typing import Optional, List

import numpy as np
import pandas as pd
import requests
from sidrapy import get_table

# =========================================================
# CONFIGURAÇÃO GERAL
# =========================================================
START_YEAR = 2019
END_YEAR = datetime.today().year
OUTPUT_NAME = "replica_indicadores_publicos_brasil.xlsx"

# Arquivo opcional do Tesouro para a aba de dívida.
RMD_FILE: Optional[str] = None
RMD_SHEET = "2.1"

PT_MESES = {
    "Jan": 1, "Fev": 2, "Mar": 3, "Abr": 4, "Mai": 5, "Jun": 6,
    "Jul": 7, "Ago": 8, "Set": 9, "Out": 10, "Nov": 11, "Dez": 12,
}
PT_MESES_MIN = {
    "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
    "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12,
}
PAT_MES_ANO = re.compile(r"^(Jan|Fev|Mar|Abr|Mai|Jun|Jul|Ago|Set|Out|Nov|Dez)/\d{2}$")

# =========================================================
# MAPEAMENTO DE SÉRIES SGS
# =========================================================
SGS = {
    # PIB / anual
    "pib_nominal_brl": 1207,
    "pib_nominal_usd": 7324,
    "pib_real_growth": 7326,
    "pib_per_capita_usd": 21776,
    "cambio_fim": 3692,
    "cambio_medio_anual": 3694,
    "export_usd_bi": 23468,
    "import_usd_bi": 23469,
    "transacoes_correntes_usd_bi": 23461,
    "conta_capital_usd_bi": 23611,
    "conta_financeira_usd_bi": 23623,
    "ativo_reservas_usd_bi": 23803,
    "reservas_internacionais_usd_bi": 3545,
    "deflator_implicito": 1211,

    # Mensal / monetário
    "ipca_12m": 13522,
    "igp_di_mensal": 190,
    "selic_acum_mes": 4390,
    "cambio_medio_mensal": 3698,
    "cambio_diario": 1,
    "tjlp": 256,
    "tlp": 27572,
    "reservas_estoque": 3546,
    "inadimplencia_total": 21082,
    "inadimplencia_pf": 21112,
    "inadimplencia_pj": 21086,
    "inadimplencia_recursos_livres": 21085,

    # Crédito
    "credito_total_brl": 20539,
    "credito_total_pct_pib": 20622,

    # Setor externo mensal
    "transacoes_correntes_mensal": 22701,
    "transacoes_correntes_pct_pib": 23079,
    "conta_capital_mensal": 22851,
    "conta_financeira_mensal": 22863,
    "idp_mensal": 22885,
    "idp_pct_pib": 23080,

    # Fiscal mensal
    "resultado_primario_governo_central": 5497,
    "resultado_primario_consolidado": 5793,
    "resultado_nominal_gc_corrente": 4573,
    "resultado_primario_gc_corrente": 4639,
    "resultado_nominal_gc_12m": 5002,
    "resultado_primario_gc_12m": 5068,
    "resultado_nominal_spc_corrente": 4583,
    "resultado_primario_spc_corrente": 4649,
    "resultado_nominal_spc_12m": 5012,
    "resultado_primario_spc_12m": 5078,
    "dbgg_valor": 13761,
    "dbgg_pct_pib": 13762,
    "dlsp_valor": 4478,
    "dlsp_pct_pib": 4513,
}

# =========================================================
# HELPERS GERAIS
# =========================================================
def normalize_text(s: str) -> str:
    """
    Normaliza texto para busca robusta:
    - remove acentos
    - baixa caixa
    - remove espaços duplicados
    """
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s)
    return s


def fetch_sgs(code: int, start_year: str, end_year: str | None = None) -> pd.Series:
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados?formato=json&dataInicial=01/01/{start_year}"
    if end_year:
        url += f"&dataFinal=31/12/{end_year}"

    r = requests.get(url, timeout=60)
    r.raise_for_status()
    df = pd.read_json(StringIO(r.text))

    if df.empty:
        return pd.Series(dtype="float64")

    df["data"] = pd.to_datetime(df["data"], dayfirst=True, errors="coerce")
    df["valor"] = pd.to_numeric(
        df["valor"].astype(str).str.replace(",", ".", regex=False),
        errors="coerce",
    )
    return df.set_index("data")["valor"].sort_index()


def _find_text_cols(raw: pd.DataFrame) -> List[str]:
    return [c for c in raw.columns if c.startswith("D") and (c.endswith("C") or c.endswith("N"))]


def _find_period_col(raw: pd.DataFrame) -> str:
    text_cols = _find_text_cols(raw)
    if not text_cols:
        raise ValueError(f"Não encontrei colunas D*C/D*N. Colunas: {list(raw.columns)}")
    return text_cols[0]


def _build_normalized_mask(
    raw: pd.DataFrame,
    search_terms: List[str],
    text_cols: Optional[List[str]] = None,
) -> pd.Series:
    """
    Procura os termos em todas as colunas textuais do raw,
    com normalização de acentos/caixa/espaços.
    """
    if text_cols is None:
        text_cols = _find_text_cols(raw)

    search_terms_norm = [normalize_text(x) for x in search_terms if x]
    mask = pd.Series(False, index=raw.index)

    for c in text_cols:
        col_norm = raw[c].astype(str).map(normalize_text)
        for term in search_terms_norm:
            mask = mask | col_norm.str.contains(term, regex=False, na=False)
    return mask


def _sidra_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype(str).str.replace(",", ".", regex=False),
        errors="coerce",
    )


def _detect_year_col(raw: pd.DataFrame) -> Optional[str]:
    text_cols = _find_text_cols(raw)
    for c in text_cols:
        vals = raw[c].astype(str).str.strip()
        if vals.str.fullmatch(r"\d{4}", na=False).any():
            return c
    return None


def _detect_quarter_period_col(raw: pd.DataFrame) -> str:
    text_cols = _find_text_cols(raw)

    quarter_patterns = [
        r"^\d{6}$",                      # ex.: 202401
        r"^\d{4}\.\d$",                  # ex.: 2024.1
        r"^\d{4}/\d$",                   # ex.: 2024/1
        r"^\d{4}\s+\d$",                 # ex.: 2024 1
        r"^[1-4].*trimestre.*\d{4}$",    # ex.: 1º trimestre 2024
        r"^\d{4}.*[1-4]$",               # fallback amplo
    ]

    for c in text_cols:
        vals = raw[c].astype(str).map(normalize_text)
        for pat in quarter_patterns:
            if vals.str.contains(pat, regex=True, na=False).any():
                return c

    return _find_period_col(raw)


def _parse_quarter_to_timestamp(s: str) -> pd.Timestamp:
    txt = normalize_text(s)

    # 202401 / 202402 / ...
    m = re.fullmatch(r"(\d{4})(0[1-4]|[1-4])", txt)
    if m:
        ano = int(m.group(1))
        tri = int(m.group(2))
        mes = tri * 3 - 2
        return pd.Timestamp(year=ano, month=mes, day=1)

    # 2024.1 / 2024/1 / 2024 1
    m = re.fullmatch(r"(\d{4})[./\s]+([1-4])", txt)
    if m:
        ano = int(m.group(1))
        tri = int(m.group(2))
        mes = tri * 3 - 2
        return pd.Timestamp(year=ano, month=mes, day=1)

    # 1 trimestre 2024 / 1o trimestre 2024 / 1º trimestre 2024
    m = re.search(r"([1-4]).*trimestre.*?(\d{4})", txt)
    if m:
        tri = int(m.group(1))
        ano = int(m.group(2))
        mes = tri * 3 - 2
        return pd.Timestamp(year=ano, month=mes, day=1)

    # 2024T1 / 2024 t1
    m = re.search(r"(\d{4}).*?t\s*([1-4])", txt)
    if m:
        ano = int(m.group(1))
        tri = int(m.group(2))
        mes = tri * 3 - 2
        return pd.Timestamp(year=ano, month=mes, day=1)

    # Fallback genérico
    m1 = re.search(r"(\d{4}).*?([1-4])", txt)
    m2 = re.search(r"([1-4]).*?(\d{4})", txt)
    if m1:
        ano = int(m1.group(1))
        tri = int(m1.group(2))
        mes = tri * 3 - 2
        return pd.Timestamp(year=ano, month=mes, day=1)
    if m2:
        tri = int(m2.group(1))
        ano = int(m2.group(2))
        mes = tri * 3 - 2
        return pd.Timestamp(year=ano, month=mes, day=1)

    raise ValueError(f"Período trimestral inesperado: {s}")


# =========================================================
# HELPERS SIDRA
# =========================================================
def sidra_annual_named_series(
    table_code: str,
    target_text: str,
    value_name: str,
    period: str = "all",
    aliases: Optional[List[str]] = None,
) -> pd.DataFrame:
    raw = get_table(
        table_code=table_code,
        territorial_level="1",
        ibge_territorial_code="1",
        period=period,
    )
    raw = pd.DataFrame(raw).iloc[1:].copy()

    period_col = _detect_year_col(raw)
    if period_col is None:
        period_col = _find_period_col(raw)

    text_cols = [c for c in _find_text_cols(raw) if c != period_col]
    search_terms = [target_text] + (aliases or [])
    mask = _build_normalized_mask(raw, search_terms, text_cols=text_cols)
    filtered = raw[mask].copy()

    if filtered.empty:
        raise ValueError(f"Não encontrei '{target_text}' na tabela {table_code}")

    df = filtered[[period_col, "V"]].copy()
    df.columns = ["ano", value_name]
    df["ano"] = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")
    df[value_name] = _sidra_numeric(df[value_name])

    return (
        df.dropna(subset=["ano"])
        .groupby("ano", as_index=False)[value_name]
        .first()
        .sort_values("ano")
        .reset_index(drop=True)
    )


def sidra_annual_series_fallback(
    table_code: str,
    target_text: str,
    value_name: str,
    period: str = "all",
    aliases: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Versão mais tolerante para séries anuais em SIDRA.
    Tenta diferentes rótulos/aliases até encontrar a série.
    """
    candidates = [target_text] + (aliases or [])
    last_exc: Optional[Exception] = None

    for candidate in candidates:
        try:
            extra_aliases = [x for x in candidates if x != candidate]
            return sidra_annual_named_series(
                table_code=table_code,
                target_text=candidate,
                value_name=value_name,
                period=period,
                aliases=extra_aliases,
            )
        except Exception as exc:
            last_exc = exc

    raise ValueError(
        f"Não encontrei a série anual '{target_text}' na tabela {table_code}. Último erro: {last_exc}"
    )


def sidra_quarterly_named_series(
    table_code: str,
    target_text: str,
    value_name: str,
    period: str = "all",
    aliases: Optional[List[str]] = None,
) -> pd.DataFrame:
    raw = get_table(
        table_code=table_code,
        territorial_level="1",
        ibge_territorial_code="1",
        period=period,
    )
    raw = pd.DataFrame(raw).iloc[1:].copy()

    period_col = _detect_quarter_period_col(raw)
    text_cols = [c for c in _find_text_cols(raw) if c != period_col]

    search_terms = [target_text] + (aliases or [])
    mask = _build_normalized_mask(raw, search_terms, text_cols=text_cols)
    filtered = raw[mask].copy()

    if filtered.empty:
        raise ValueError(f"Não encontrei '{target_text}' na tabela trimestral {table_code}")

    df = filtered[[period_col, "V"]].copy()
    df.columns = ["periodo", value_name]
    df["periodo"] = df["periodo"].astype(str)
    df["data"] = df["periodo"].apply(_parse_quarter_to_timestamp)
    df[value_name] = _sidra_numeric(df[value_name])

    return (
        df[["data", value_name]]
        .dropna(subset=["data"])
        .sort_values("data")
        .groupby("data", as_index=False)[value_name]
        .first()
        .reset_index(drop=True)
    )


def sidra_quarterly_named_series_sum_by_year(
    table_code: str,
    target_text: str,
    value_name: str,
    period: str = "all",
    aliases: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Soma os 4 trimestres do ano.
    """
    df = sidra_quarterly_named_series(
        table_code=table_code,
        target_text=target_text,
        value_name=value_name,
        period=period,
        aliases=aliases,
    ).copy()

    df["ano"] = df["data"].dt.year
    return (
        df.groupby("ano", as_index=False)[value_name]
        .sum()
        .sort_values("ano")
        .reset_index(drop=True)
    )


def sidra_quarterly_named_series_mean_by_year(
    table_code: str,
    target_text: str,
    value_name: str,
    period: str = "all",
    aliases: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Média simples dos 4 trimestres do ano.
    Útil para taxas já expressas em % (ex.: taxa de poupança).
    """
    df = sidra_quarterly_named_series(
        table_code=table_code,
        target_text=target_text,
        value_name=value_name,
        period=period,
        aliases=aliases,
    ).copy()

    df["ano"] = df["data"].dt.year
    return (
        df.groupby("ano", as_index=False)[value_name]
        .mean()
        .sort_values("ano")
        .reset_index(drop=True)
    )


def sidra_quarterly_single_series_mean_by_year(
    table_code: str,
    value_name: str,
    period: str = "all",
) -> pd.DataFrame:
    """
    Lê uma tabela trimestral SIDRA que tenha essencialmente
    uma única série/variável e retorna a média anual.

    Útil para:
      - tabela 6727: Taxa de investimento
      - tabela 6726: Taxa de poupança
    """
    raw = get_table(
        table_code=table_code,
        territorial_level="1",
        ibge_territorial_code="1",
        period=period,
    )
    raw = pd.DataFrame(raw).iloc[1:].copy()

    if raw.empty:
        raise ValueError(f"Tabela SIDRA {table_code} retornou vazia.")

    period_col = _detect_quarter_period_col(raw)

    if "V" not in raw.columns:
        raise ValueError(f"Tabela SIDRA {table_code} não possui coluna 'V'. Colunas: {list(raw.columns)}")

    df = raw[[period_col, "V"]].copy()
    df.columns = ["periodo", value_name]

    df["periodo"] = df["periodo"].astype(str)
    df["data"] = df["periodo"].apply(_parse_quarter_to_timestamp)
    df[value_name] = _sidra_numeric(df[value_name])

    # Se houver repetição por trimestre, pega o primeiro valor válido
    df = (
        df[["data", value_name]]
        .dropna(subset=["data"])
        .sort_values("data")
        .groupby("data", as_index=False)[value_name]
        .first()
    )

    df["ano"] = df["data"].dt.year

    return (
        df.groupby("ano", as_index=False)[value_name]
        .mean()
        .sort_values("ano")
        .reset_index(drop=True)
    )


def sidra_unemployment_4099_mean_by_year(period: str = "all") -> pd.DataFrame:
    """
    Tabela 4099 (trimestral):
    Taxa de desocupação, na semana de referência, das pessoas de 14 anos ou mais de idade.
    Retorna média anual.
    """
    raw = get_table(
        table_code="4099",
        territorial_level="1",
        ibge_territorial_code="1",
        period=period,
    )
    raw = pd.DataFrame(raw).iloc[1:].copy()

    if raw.empty:
        raise ValueError("Tabela SIDRA 4099 retornou vazia.")

    period_col = _detect_quarter_period_col(raw)
    text_cols = [c for c in _find_text_cols(raw) if c != period_col]

    # filtra especificamente a variável de taxa de desocupação
    search_terms = [
        "Taxa de desocupação, na semana de referência, das pessoas de 14 anos ou mais de idade",
        "Taxa de desocupação",
        "taxa de desocupacao",
    ]
    mask = _build_normalized_mask(raw, search_terms, text_cols=text_cols)
    filtered = raw[mask].copy()

    if filtered.empty:
        raise ValueError("Não encontrei a variável de taxa de desocupação na tabela 4099.")

    # exclui linhas de outras métricas, se vierem junto
    excl = pd.Series(False, index=filtered.index)
    for c in text_cols:
        col_norm = filtered[c].astype(str).map(normalize_text)
        excl = excl | col_norm.str.contains("coeficiente", regex=False, na=False)
        excl = excl | col_norm.str.contains("subutilizacao", regex=False, na=False)
        excl = excl | col_norm.str.contains("forca de trabalho potencial", regex=False, na=False)
        excl = excl | col_norm.str.contains("insuficiencia de horas", regex=False, na=False)

    filtered = filtered[~excl].copy()

    df = filtered[[period_col, "V"]].copy()
    df.columns = ["periodo", "unemployment_rate_pct_workforce"]

    df["periodo"] = df["periodo"].astype(str)
    df["data"] = df["periodo"].apply(_parse_quarter_to_timestamp)
    df["unemployment_rate_pct_workforce"] = _sidra_numeric(df["unemployment_rate_pct_workforce"])

    df = (
        df[["data", "unemployment_rate_pct_workforce"]]
        .dropna(subset=["data"])
        .sort_values("data")
        .groupby("data", as_index=False)["unemployment_rate_pct_workforce"]
        .first()
    )

    df["ano"] = df["data"].dt.year
    df = (
        df.groupby("ano", as_index=False)["unemployment_rate_pct_workforce"]
        .mean()
        .sort_values("ano")
        .reset_index(drop=True)
    )

    return df


# =========================================================
# RMD / TESOURO
# =========================================================
def extract_rmd_generic_series(
    arquivo: str,
    aba: str,
    prefixes: List[str],
) -> pd.Series:
    xl = pd.ExcelFile(arquivo, engine="openpyxl")
    df_raw = pd.read_excel(xl, sheet_name=aba, header=None)

    header_row_idx = None
    for i in range(len(df_raw)):
        row = df_raw.iloc[i]
        matches = sum(isinstance(v, str) and bool(PAT_MES_ANO.match(v.strip())) for v in row)
        if matches >= 10:
            header_row_idx = i
            break
    if header_row_idx is None:
        raise RuntimeError("Não localizei cabeçalho com meses no RMD.")

    header = df_raw.iloc[header_row_idx].tolist()

    first_month_col = None
    for j in range(1, len(header)):
        v = header[j]
        if isinstance(v, str) and PAT_MES_ANO.match(v.strip()):
            first_month_col = j
            break
    if first_month_col is None:
        raise RuntimeError("Não encontrei a primeira coluna de mês no RMD.")

    periods = []
    for v in header[first_month_col:]:
        if isinstance(v, str) and PAT_MES_ANO.match(v.strip()):
            mon_abbr, yy = v.split("/")
            mm = PT_MESES[mon_abbr]
            year = 2000 + int(yy)
            periods.append(pd.Timestamp(year=year, month=mm, day=1))
        else:
            periods.append(pd.NaT)

    row_idx = None
    for i in range(len(df_raw)):
        c0 = df_raw.iloc[i, 0]
        if isinstance(c0, str):
            c0s = c0.strip()
            if any(c0s.startswith(p) for p in prefixes):
                row_idx = i
                break
    if row_idx is None:
        raise RuntimeError(f"Não encontrei linha no RMD para prefixes={prefixes}")

    values = df_raw.iloc[row_idx, first_month_col:first_month_col + len(periods)].tolist()
    s = pd.Series(values, index=periods)
    s = pd.to_numeric(s, errors="coerce")
    s = s[~s.index.isna()]
    return s.sort_index()


def build_rmd_debt_block(rmd_file: Optional[str]) -> pd.DataFrame:
    cols = [
        "ano",
        "gross_lt_commercial_borrowing_usd_bi",
        "commercial_debt_stock_year_end_usd_bi",
        "st_debt_usd_bi",
        "bi_multilateral_debt_pct_total",
        "st_debt_pct_total",
        "fc_debt_pct_total",
        "lt_fixed_rate_debt_pct_total",
        "roll_over_ratio_pct_debt",
        "roll_over_ratio_pct_gdp",
    ]

    if not rmd_file:
        return pd.DataFrame(columns=cols)

    try:
        s_dpf = extract_rmd_generic_series(rmd_file, RMD_SHEET, ["DPF EM PODER DO PÚBLICO"])
        _ = extract_rmd_generic_series(rmd_file, RMD_SHEET, ["DPMFi"])
        _ = extract_rmd_generic_series(rmd_file, RMD_SHEET, ["DPFe", "DPFe "])
    except Exception:
        return pd.DataFrame(columns=cols)

    out = pd.DataFrame({
        "ano": s_dpf.index.year,
        "gross_lt_commercial_borrowing_usd_bi": np.nan,
        "commercial_debt_stock_year_end_usd_bi": np.nan,
        "st_debt_usd_bi": np.nan,
        "bi_multilateral_debt_pct_total": np.nan,
        "st_debt_pct_total": np.nan,
        "fc_debt_pct_total": np.nan,
        "lt_fixed_rate_debt_pct_total": np.nan,
        "roll_over_ratio_pct_debt": np.nan,
        "roll_over_ratio_pct_gdp": np.nan,
    }).drop_duplicates(subset=["ano"]).sort_values("ano").reset_index(drop=True)

    return out[cols]


# =========================================================
# ABA 1 - ECONOMIC DATA
# =========================================================
def build_economic_data() -> pd.DataFrame:
    pib_brl = fetch_sgs(SGS["pib_nominal_brl"], str(START_YEAR), str(END_YEAR))
    pib_usd = fetch_sgs(SGS["pib_nominal_usd"], str(START_YEAR), str(END_YEAR))
    pib_real = fetch_sgs(SGS["pib_real_growth"], str(START_YEAR), str(END_YEAR))
    exp_usd = fetch_sgs(SGS["export_usd_bi"], str(START_YEAR), str(END_YEAR))
    import_usd = fetch_sgs(SGS["import_usd_bi"], str(START_YEAR), str(END_YEAR))

    df = pd.DataFrame({
        "ano": pib_brl.index.year,
        "nominal_gdp_bil_lc": pd.to_numeric(pib_brl.values, errors="coerce") / 1e9,
        "nominal_gdp_bil_usd": pd.to_numeric(pib_usd.values, errors="coerce") / 1000.0,
        "real_gdp_growth_pct": pd.to_numeric(pib_real.values, errors="coerce"),
    }).drop_duplicates("ano").sort_values("ano").reset_index(drop=True)

    # =====================================================
    # PIB per capita
    # Fonte fixa: SGS 21776
    # A coluna final é em milhares de US$, então divide por 1000
    # =====================================================
    pib_pc_usd = fetch_sgs(SGS["pib_per_capita_usd"], str(START_YEAR), str(END_YEAR))
    pib_pc_df = pd.DataFrame({
        "ano": pib_pc_usd.index.year,
        "gdp_per_capita_000s_usd": pd.to_numeric(pib_pc_usd.values, errors="coerce") / 1000.0,
    }).drop_duplicates("ano")
    df = df.merge(pib_pc_df, on="ano", how="left")

    # Crescimento real do PIB per capita
    try:
        real_pc = sidra_annual_series_fallback(
            table_code="6601",
            target_text="Taxa de crescimento real do PIB per capita",
            value_name="real_gdp_per_capita_growth_pct",
            aliases=[
                "crescimento real do pib per capita",
                "taxa de crescimento real do produto interno bruto per capita",
                "pib per capita",
            ],
        )
        df = df.merge(real_pc, on="ano", how="left")
    except Exception:
        df["real_gdp_per_capita_growth_pct"] = np.nan

    # Exportações / PIB
    exp_df = pd.DataFrame({
        "ano": exp_usd.index.year,
        "exports_usd_bi": exp_usd.values,
    }).drop_duplicates("ano")

    imp_df = pd.DataFrame({
        "ano": import_usd.index.year,
        "imports_usd_bi": import_usd.values,
    }).drop_duplicates("ano")

    df = df.merge(exp_df, on="ano", how="left").merge(imp_df, on="ano", how="left")

    # ajuste solicitado: andar 3 casas decimais para a esquerda
    df["exports_gdp_pct"] = ((df["exports_usd_bi"] / df["nominal_gdp_bil_usd"]) * 100) / 1000.0

    # -----------------------------------------------------
    # investment_gdp_pct
    # Usa a tabela direta 6727 (Taxa de investimento)
    # -----------------------------------------------------
    try:
        inv = sidra_quarterly_single_series_mean_by_year(
            table_code="6727",
            value_name="investment_gdp_pct",
            period="all",
        )
        df = df.merge(inv, on="ano", how="left")
    except Exception as e:
        print(f"[aviso] Falha ao usar SIDRA 6727 para investment_gdp_pct: {e}")
        df["investment_gdp_pct"] = np.nan

    # Crescimento real do investimento - proxy oficial
    try:
        fbc_real_tri = sidra_quarterly_named_series(
            table_code="5932",
            target_text="Formação bruta de capital",
            value_name="fbc_var_volume_tri",
            aliases=["formacao bruta de capital", "fbcf", "formação bruta de capital fixo"],
        )
        fbc_real_tri["ano"] = fbc_real_tri["data"].dt.year
        real_inv = (
            fbc_real_tri.groupby("ano", as_index=False)["fbc_var_volume_tri"]
            .mean()
            .rename(columns={"fbc_var_volume_tri": "real_investment_growth_pct"})
        )
        df = df.merge(real_inv, on="ano", how="left")
    except Exception:
        df["real_investment_growth_pct"] = np.nan

    # -----------------------------------------------------
    # savings_gdp_pct
    # Usa a tabela direta 6726 (Taxa de poupança)
    # -----------------------------------------------------
    try:
        savings = sidra_quarterly_single_series_mean_by_year(
            table_code="6726",
            value_name="savings_gdp_pct",
            period="all",
        )
        df = df.merge(savings, on="ano", how="left")
    except Exception as e:
        print(f"[aviso] Falha ao usar SIDRA 6726 para taxa de poupança: {e}")
        df["savings_gdp_pct"] = np.nan

    # -----------------------------------------------------
    # unemployment_rate_pct_workforce
    # Fonte: SIDRA 4099 (trimestral -> média anual)
    # -----------------------------------------------------
    try:
        desemp_4099 = sidra_unemployment_4099_mean_by_year("all")
        df = df.merge(desemp_4099, on="ano", how="left")
    except Exception as e:
        print(f"[aviso] Falha ao usar SIDRA 4099 para unemployment_rate_pct_workforce: {e}")
        df["unemployment_rate_pct_workforce"] = np.nan

    # Garante existência das colunas finais
    for col in [
        "gdp_per_capita_000s_usd",
        "real_gdp_per_capita_growth_pct",
        "real_investment_growth_pct",
        "investment_gdp_pct",
        "savings_gdp_pct",
        "exports_gdp_pct",
        "unemployment_rate_pct_workforce",
    ]:
        if col not in df.columns:
            df[col] = np.nan

    cols = [
        "ano",
        "nominal_gdp_bil_lc",
        "nominal_gdp_bil_usd",
        "gdp_per_capita_000s_usd",
        "real_gdp_growth_pct",
        "real_gdp_per_capita_growth_pct",
        "real_investment_growth_pct",
        "investment_gdp_pct",
        "savings_gdp_pct",
        "exports_gdp_pct",
        "unemployment_rate_pct_workforce",
    ]
    return df[cols]


# =========================================================
# ABA 2 - MONETARY DATA
# =========================================================
def build_monetary_data() -> pd.DataFrame:
    ipca = fetch_sgs(SGS["ipca_12m"], str(START_YEAR), str(END_YEAR))
    cambio_fim = fetch_sgs(SGS["cambio_fim"], str(START_YEAR), str(END_YEAR))
    credito_total = fetch_sgs(SGS["credito_total_brl"], str(START_YEAR), str(END_YEAR))
    credito_pct_pib = fetch_sgs(SGS["credito_total_pct_pib"], str(START_YEAR), str(END_YEAR))

    # nova fonte para gdp_deflator_growth_pct
    deflator_1211 = fetch_sgs(SGS["deflator_implicito"], str(START_YEAR), str(END_YEAR))

    anos = sorted(
        set(ipca.index.year)
        | set(cambio_fim.index.year)
        | set(credito_total.index.year)
        | set(credito_pct_pib.index.year)
        | set(deflator_1211.index.year)
    )
    df = pd.DataFrame({"ano": anos})

    df = df.merge(
        pd.DataFrame({
            "ano": ipca.index.year,
            "cpi_growth_pct": ipca.values
        }).groupby("ano", as_index=False).last(),
        on="ano",
        how="left",
    )

    # usa diretamente a SGS 1211 no indicador gdp_deflator_growth_pct
    df = df.merge(
        pd.DataFrame({
            "ano": deflator_1211.index.year,
            "gdp_deflator_growth_pct": pd.to_numeric(deflator_1211.values, errors="coerce"),
        }).drop_duplicates("ano"),
        on="ano",
        how="left",
    )

    df = df.merge(
        pd.DataFrame({
            "ano": cambio_fim.index.year,
            "exchange_rate_year_end_lc_per_usd": cambio_fim.values
        }).groupby("ano", as_index=False).last(),
        on="ano",
        how="left",
    )

    credito_df = (
        pd.DataFrame({
            "ano": credito_total.index.year,
            "credito_total_brl": credito_total.values
        })
        .groupby("ano", as_index=False)
        .last()
        .sort_values("ano")
    )
    credito_df["banks_claims_growth_pct"] = credito_df["credito_total_brl"].pct_change() * 100

    credito_pib_df = (
        pd.DataFrame({
            "ano": credito_pct_pib.index.year,
            "banks_claims_gdp_pct": credito_pct_pib.values
        })
        .groupby("ano", as_index=False)
        .last()
    )

    df = df.merge(credito_df[["ano", "banks_claims_growth_pct"]], on="ano", how="left")
    df = df.merge(credito_pib_df, on="ano", how="left")

    df["fx_share_claims_pct"] = np.nan
    df["fx_share_deposits_pct"] = np.nan
    df["reer_growth_pct"] = np.nan

    cols = [
        "ano",
        "cpi_growth_pct",
        "gdp_deflator_growth_pct",
        "exchange_rate_year_end_lc_per_usd",
        "banks_claims_growth_pct",
        "banks_claims_gdp_pct",
        "fx_share_claims_pct",
        "fx_share_deposits_pct",
        "reer_growth_pct",
    ]
    return df[cols]


# =========================================================
# ABA 3 - GENERAL GOVERNMENT DATA
# =========================================================
def build_general_government_data() -> pd.DataFrame:
    dbgg = fetch_sgs(SGS["dbgg_pct_pib"], str(START_YEAR), str(END_YEAR))
    dlsp = fetch_sgs(SGS["dlsp_pct_pib"], str(START_YEAR), str(END_YEAR))
    primario = fetch_sgs(SGS["resultado_primario_consolidado"], str(START_YEAR), str(END_YEAR))
    nominal_12m = fetch_sgs(SGS["resultado_nominal_spc_12m"], str(START_YEAR), str(END_YEAR))

    df = pd.DataFrame({"ano": sorted(set(dbgg.index.year) | set(dlsp.index.year))})

    df = df.merge(
        pd.DataFrame({"ano": dbgg.index.year, "gross_gg_debt_gdp_pct": dbgg.values}).groupby("ano", as_index=False).last(),
        on="ano", how="left"
    )
    df = df.merge(
        pd.DataFrame({"ano": dlsp.index.year, "net_gg_debt_gdp_pct": dlsp.values}).groupby("ano", as_index=False).last(),
        on="ano", how="left"
    )
    df = df.merge(
        pd.DataFrame({"ano": primario.index.year, "primary_gg_balance_gdp_pct": primario.values}).groupby("ano", as_index=False).last(),
        on="ano", how="left"
    )
    df = df.merge(
        pd.DataFrame({"ano": nominal_12m.index.year, "gg_balance_gdp_pct": nominal_12m.values}).groupby("ano", as_index=False).last(),
        on="ano", how="left"
    )

    df = df.sort_values("ano").reset_index(drop=True)
    df["change_in_net_gg_debt_gdp_pct"] = df["net_gg_debt_gdp_pct"].diff()
    df["liquid_assets_gdp_pct"] = df["gross_gg_debt_gdp_pct"] - df["net_gg_debt_gdp_pct"]
    df["gg_revenues_gdp_pct"] = np.nan
    df["gg_expenditures_gdp_pct"] = np.nan
    df["gg_interest_expenditure_revenues_pct"] = np.nan
    df["debt_revenues_pct"] = np.nan

    cols = [
        "ano",
        "gg_balance_gdp_pct",
        "change_in_net_gg_debt_gdp_pct",
        "primary_gg_balance_gdp_pct",
        "gg_revenues_gdp_pct",
        "gg_expenditures_gdp_pct",
        "gg_interest_expenditure_revenues_pct",
        "gross_gg_debt_gdp_pct",
        "debt_revenues_pct",
        "net_gg_debt_gdp_pct",
        "liquid_assets_gdp_pct",
    ]
    return df[cols]


# =========================================================
# ABA 4 - BALANCE OF PAYMENTS DATA
# =========================================================
def build_balance_of_payments_data() -> pd.DataFrame:
    tc_pct = fetch_sgs(SGS["transacoes_correntes_pct_pib"], str(START_YEAR), str(END_YEAR))
    idp_pct = fetch_sgs(SGS["idp_pct_pib"], str(START_YEAR), str(END_YEAR))
    exp = fetch_sgs(SGS["export_usd_bi"], str(START_YEAR), str(END_YEAR))
    imp = fetch_sgs(SGS["import_usd_bi"], str(START_YEAR), str(END_YEAR))
    pib_usd = fetch_sgs(SGS["pib_nominal_usd"], str(START_YEAR), str(END_YEAR))

    df = pd.DataFrame({"ano": sorted(set(tc_pct.index.year) | set(idp_pct.index.year))})

    df = df.merge(
        pd.DataFrame({"ano": tc_pct.index.year, "current_account_balance_gdp_pct": tc_pct.values}).groupby("ano", as_index=False).last(),
        on="ano", how="left"
    )
    df = df.merge(
        pd.DataFrame({"ano": idp_pct.index.year, "net_fdi_gdp_pct": idp_pct.values}).groupby("ano", as_index=False).last(),
        on="ano", how="left"
    )

    exp_df = pd.DataFrame({"ano": exp.index.year, "exports_usd_bi": exp.values}).drop_duplicates("ano")
    imp_df = pd.DataFrame({"ano": imp.index.year, "imports_usd_bi": imp.values}).drop_duplicates("ano")
    pib_df = pd.DataFrame({"ano": pib_usd.index.year, "nominal_gdp_bil_usd": pib_usd.values / 1000.0}).drop_duplicates("ano")

    df = df.merge(exp_df, on="ano", how="left").merge(imp_df, on="ano", how="left").merge(pib_df, on="ano", how="left")
    df["trade_balance_gdp_pct"] = ((df["exports_usd_bi"] - df["imports_usd_bi"]) / df["nominal_gdp_bil_usd"]) * 100
    df["real_exports_growth_pct"] = df["exports_usd_bi"].pct_change() * 100
    df["cars_gdp_pct"] = np.nan
    df["current_account_balance_cars_pct"] = np.nan
    df["usable_reserves_caps_months"] = np.nan
    df["gross_ext_fin_needs_over_car_plus_res_pct"] = np.nan
    df["net_portfolio_equity_inflow_gdp_pct"] = np.nan

    cols = [
        "ano",
        "cars_gdp_pct",
        "real_exports_growth_pct",
        "current_account_balance_gdp_pct",
        "current_account_balance_cars_pct",
        "usable_reserves_caps_months",
        "gross_ext_fin_needs_over_car_plus_res_pct",
        "net_fdi_gdp_pct",
        "trade_balance_gdp_pct",
        "net_portfolio_equity_inflow_gdp_pct",
    ]
    return df[cols]


# =========================================================
# ABA 5 - EXTERNAL BALANCE SHEET
# =========================================================
def build_external_balance_sheet() -> pd.DataFrame:
    reservas = fetch_sgs(SGS["reservas_internacionais_usd_bi"], str(START_YEAR), str(END_YEAR))
    df = pd.DataFrame({"ano": sorted(set(reservas.index.year))})

    res_df = pd.DataFrame({
        "ano": reservas.index.year,
        "usable_reserves_usd_bi": reservas.values,
    }).drop_duplicates("ano")
    df = df.merge(res_df, on="ano", how="left")

    df["usable_reserves_usd_mil"] = df["usable_reserves_usd_bi"] * 1000
    df["narrow_net_ext_debt_cars_pct"] = np.nan
    df["narrow_net_ext_debt_caps_pct"] = np.nan
    df["net_ext_liabilities_cars_pct"] = np.nan
    df["st_external_debt_remaining_maturity_cars_pct"] = np.nan

    cols = [
        "ano",
        "narrow_net_ext_debt_cars_pct",
        "narrow_net_ext_debt_caps_pct",
        "net_ext_liabilities_cars_pct",
        "st_external_debt_remaining_maturity_cars_pct",
        "usable_reserves_usd_mil",
    ]
    return df[cols]


# =========================================================
# ABA 6 - CENTRAL GOV DEBT AND BORROWING DATA
# =========================================================
def build_central_government_debt_and_borrowing_data() -> pd.DataFrame:
    return build_rmd_debt_block(RMD_FILE)


# =========================================================
# EXPORTAÇÃO
# =========================================================
def export_to_excel(tabelas: dict[str, pd.DataFrame], output_name: str):
    output = Path(output_name)
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
            ws.freeze_panes = "B2"  # congela primeira linha e primeira coluna

            for idx, col in enumerate(df.columns, start=1):
                values = df[col].head(1000).fillna("").astype(str).tolist()
                max_len = max([len(str(col))] + [len(v) for v in values])
                ws.column_dimensions[ws.cell(row=1, column=idx).column_letter].width = min(max_len + 2, 28)

    print(f"Arquivo exportado com sucesso: {output.resolve()}")


# =========================================================
# MAIN
# =========================================================
def main():
    economic = build_economic_data()
    monetary = build_monetary_data()
    fiscal = build_general_government_data()
    bop = build_balance_of_payments_data()
    ebs = build_external_balance_sheet()
    debt = build_central_government_debt_and_borrowing_data()

    tabelas = {
        "Economic Data": economic,
        "Monetary Data": monetary,
        "General Government Data": fiscal,
        "Balance-Of-Payments Data": bop,
        "External Balance Sheet": ebs,
        "Central Gov Debt and Borrowing": debt,
    }

    export_to_excel(tabelas, OUTPUT_NAME)


if __name__ == "__main__":
    main()