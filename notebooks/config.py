FILE_NAMES_ORIGINAL = [
    "base_atendentes.csv",
    "base_motivos.csv",
    "canais.csv",
    "Chamados_Hora.CSV",
    "clientes.csv",
    "custos.csv",
    "chamados.csv",# faltava no commit anterior
    "pesquisa_satisfacao.csv"
    ]
# Padronização de nomes: <catalog>.<camada>.<tabela>, tudo em snake_case
CATALOG = "v_credit_medallion"
BRONZE_SCHEMA_NAME = "bronze"
SILVER_SCHEMA_NAME = "silver"
GOLD_SCHEMA_NAME = "gold"

BRONZE_SCHEMA = F"{CATALOG}.{BRONZE_SCHEMA_NAME}"
SILVER_SCHEMA = F"{CATALOG}.{SILVER_SCHEMA_NAME}"
GOLD_SCHEMA = F"{CATALOG}.{GOLD_SCHEMA_NAME}"

LANDING_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA_NAME}/Landing"

print(f"catalog: {CATALOG}")
print(f"bronze_schema: {BRONZE_SCHEMA}")
print(f"silver_schema: {SILVER_SCHEMA}")
print(f"gold_schema: {GOLD_SCHEMA}")
print(f"landing_path: {LANDING_PATH}")