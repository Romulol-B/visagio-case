#funções disponibilizadas nos notebooks das aula.
#adaptadas para retornar um bool.
def dq_check(table_name: str, check_name: str, df,condition )-> tuple[list, bool]:
    """checa uma condição no dataframe.
    EX:
    dq_check("silver.info","titulo nulo", df_silver_info,F.col("titulo").isNotNull()) 
    >>> [PASS] silver.info | titulo nulo |0/97879 linhas falharam

    dq_check("silver.info","data lançamento", df_silver_info,F.col("data_lancamento").isNotNull()

    >>> [FAIL] silver.info | data lançamento |64/97879 linhas falharam
    """
    from pyspark.sql import functions as F ,Row
    from datetime import datetime 
    
    total = df.count()
    failed = df.filter(~F.coalesce(condition, F.lit(False))).count()
    passed = failed==0
    dq_results = []
    dq_results.append(Row(table_name = table_name, check_name=check_name,
                           total_rows = total, failed_rows = failed,
                            passed=passed,checked_at = datetime.now()))
    status= "PASS" if passed else "FAIL"
    print(f"[{status}] {table_name} | {check_name} |{failed}/{total} linhas falharam")
    return dq_results, passed

def dq_check_unique(table_name: str, check_name: str, df, key_cols: list)-> tuple[list, bool]:
    """Checagem de qualidade específica para unicidade de chave.
    EX:
    dq_check_unique("bronze.info", "único na mesma ingestão", df_silver_info, ["id","ingestion_datetime"])
    >>> [PASS] bronze.info | id único na mesma ingestão | 0 chaves duplicadas de 97879 linhas
    """
    from pyspark.sql import functions as F ,Row
    from datetime import datetime
    total = df.count()
    dupes = df.groupBy(*key_cols).count().filter("count > 1").count()
    passed = dupes == 0
    dq_results = []
    dq_results.append(
        Row(table_name=table_name, check_name=check_name, total_rows=total,
            failed_rows=dupes, passed=passed, checked_at=datetime.now())
    )
    status = "PASS" if passed else "FAIL"
    print(f"[{status}] {table_name} | {check_name} | {dupes} chaves duplicadas de {total} linhas")
    return dq_results, passed