# BDA Dashboard (Streamlit Cloud ready)

Aplicação Streamlit para ler a base Excel, transformar os dados em formato analítico e exibir:

- aba **Dashboards**
- aba **Dados em tabela**
- filtros por **País**, **LT FC rating**, **Indicadores** e **Aba da planilha**

## Estrutura recomendada do repositório

```text
seu-repo/
├─ app.py
├─ requirements.txt
├─ .gitignore
├─ .streamlit/
│  └─ config.toml
└─ data/
   └─ base.xlsx   # opcional, mas recomendado
```

## Opção 1 — Deploy sem upload manual (recomendado)

1. Coloque seu arquivo Excel em `data/base.xlsx`
2. Faça commit do repositório no GitHub
3. No Streamlit Cloud, aponte para:
   - **Repository**: seu repositório
   - **Branch**: a branch desejada
   - **Main file path**: `app.py`

O app vai procurar automaticamente por:

- `data/base.xlsx`
- `data/report.xlsx`
- qualquer outro `.xlsx` em `data/`
- qualquer `.xlsx` na raiz do projeto

## Opção 2 — Sem subir a base para o GitHub

Se você não quiser versionar o Excel no GitHub, o app também permite **upload manual** no próprio Streamlit.

## Como rodar localmente

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Dica para GitHub

Se a base for grande ou sensível, considere:

- deixar o app sem a base no repositório e usar upload manual; ou
- usar um repositório privado; ou
- substituir a planilha por uma versão sanitizada.
