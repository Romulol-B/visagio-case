"""Normalização 1:1 dos tempos dos chamados, sem gravação de tabelas."""

from pyspark.sql import functions as F


DATA_ATENDIMENTO_INICIO = '2025-01-01'
DATA_ATENDIMENTO_FIM_EXCLUSIVO = '2026-08-01'
FLAGS_PERIODO = {
    'hora_abertura_chamado': 'abertura_fora_periodo',
    'hora_inicio_atendimento': 'inicio_fora_periodo',
    'hora_finalizacao_atendimento': 'finalizacao_fora_periodo',
}


def timestamp_no_periodo(coluna):
    """Jan/2025 a jul/2026 completos, no fuso America/Sao_Paulo da sessão."""
    return ((F.col(coluna) >= F.lit(DATA_ATENDIMENTO_INICIO).cast('timestamp')) &
            (F.col(coluna) < F.lit(DATA_ATENDIMENTO_FIM_EXCLUSIVO).cast('timestamp')))


def aplicar_periodo_tempos(df):
    """Anula timestamps fora do domínio e preserva flags em reexecuções."""
    for coluna, flag in FLAGS_PERIODO.items():
        if coluna not in df.columns:
            continue
        anterior = F.coalesce(F.col(flag), F.lit(False)) if flag in df.columns else F.lit(False)
        df = df.withColumn(flag, anterior | F.coalesce(~timestamp_no_periodo(coluna), F.lit(False)))
        df = df.withColumn(coluna, F.when(timestamp_no_periodo(coluna), F.col(coluna)))
    return df


def validar_periodo_tempos(df):
    for coluna in FLAGS_PERIODO:
        if coluna in df.columns and df.filter(~timestamp_no_periodo(coluna)).limit(1).count():
            raise ValueError(f'{coluna}: data fora do período de atendimento')


def _validar_chave(df, nome):
    if df.filter(F.col('id_chamado').isNull()).limit(1).count():
        raise ValueError(f'{nome}: id_chamado nulo')
    if df.groupBy('id_chamado').count().filter('count > 1').limit(1).count():
        raise ValueError(f'{nome}: id_chamado duplicado/conflitante')


def validar_relacao_tempos(chamados, tempos):
    """Exige exatamente uma linha temporal para cada chamado, sem órfãos."""
    _validar_chave(chamados, 'chamados')
    _validar_chave(tempos, 'chamados_hora')
    ids = chamados.select('id_chamado')
    if ids.join(tempos, 'id_chamado', 'left_anti').limit(1).count():
        raise ValueError('Chamados sem linha em chamados_hora')
    if tempos.join(ids, 'id_chamado', 'left_anti').limit(1).count():
        raise ValueError('chamados_hora contém chamados órfãos')


def _preenchido(coluna):
    return F.col(coluna).isNotNull() & ~F.lower(F.trim(F.col(coluna))).isin('', 'null')


def _contar(condicao):
    return F.coalesce(F.sum(F.when(condicao, 1).otherwise(0)), F.lit(0))


def construir_tempos(chamados, bronze_chamados, bronze_horarios):
    """Retorna (tempos, auditoria) usando timestamps locais e segundos.

    Horários vêm de bronze.chamados_hora; durações informadas vêm de
    bronze.chamados. A expressão textual de espera zero não altera timestamps.
    As fontes Bronze continuam disponíveis para investigar os contadores.
    O domínio temporal é [2025-01-01, 2026-08-01); não se infere um ano substituto.
    """
    if chamados.sparkSession.conf.get('spark.sql.session.timeZone') != 'America/Sao_Paulo':
        raise ValueError('Use America/Sao_Paulo antes de construir os tempos')

    origem = bronze_chamados.drop('ingestion_datetime').dropDuplicates()
    horas = bronze_horarios.drop('ingestion_datetime').dropDuplicates()
    horas = horas.toDF(*[c.lower() for c in horas.columns])
    for df, nome in [(chamados, 'chamados'), (origem, 'bronze.chamados'),
                     (horas, 'bronze.chamados_hora')]:
        _validar_chave(df, nome)

    ids = chamados.select('id_chamado')
    auditoria = {
        'horarios_sem_chamado': horas.join(ids, 'id_chamado', 'left_anti').count(),
        'duracoes_sem_chamado': origem.join(ids, 'id_chamado', 'left_anti').count(),
        'chamados_sem_registro_horarios': ids.join(horas, 'id_chamado', 'left_anti').count(),
        'chamados_sem_registro_duracoes': ids.join(origem, 'id_chamado', 'left_anti').count(),
    }
    clientes = chamados.select('id_chamado', 'id_cliente').join(
        horas.select('id_chamado', F.col('id_cliente').alias('id_cliente_origem')),
        'id_chamado', 'left',
    )
    auditoria.update(clientes.agg(
        _contar(F.col('id_cliente') != F.col('id_cliente_origem')).alias('clientes_divergentes'),
        _contar(F.col('id_cliente').isNull() | F.col('id_cliente_origem').isNull())
        .alias('clientes_nao_comparaveis'),
    ).first().asDict())

    nomes = ['hora_abertura_chamado', 'hora_inicio_atendimento', 'hora_finalizacao_atendimento']
    for nome in nomes:
        horas = horas.withColumn(f'_{nome}_original', F.col(nome)).withColumn(
            nome, F.try_to_timestamp(
                F.regexp_replace(F.trim(F.col(nome)), r'\s+às\s+', ' '),
                F.lit('dd/MM/yyyy HH:mm:ss'),
            ),
        )
    auditoria.update(horas.agg(*[
        _contar(_preenchido(f'_{nome}_original') & F.col(nome).isNull())
        .alias(f'{nome}_invalida') for nome in nomes
    ]).first().asDict())
    horas = aplicar_periodo_tempos(horas)
    auditoria.update(horas.agg(*[
        _contar(F.col(flag)).alias(flag) for flag in FLAGS_PERIODO.values()
    ]).first().asDict())
    # Conferir a ordem somente depois de retirar datas fora do domínio.
    # Uma abertura inválida não invalida um par início/fim válido e independente.
    inicio_invalido = (F.col('hora_inicio_atendimento') < F.col('hora_abertura_chamado'))
    fim_invalido = (F.col('hora_finalizacao_atendimento') <
                   F.greatest('hora_abertura_chamado', 'hora_inicio_atendimento'))
    auditoria.update(horas.agg(
        _contar(inicio_invalido).alias('inicio_antes_abertura'),
        _contar(fim_invalido).alias('fim_antes_abertura_ou_inicio'),
    ).first().asDict())
    horas = horas.withColumn('hora_inicio_atendimento', F.when(
        F.col('hora_abertura_chamado').isNull() |
        (F.col('hora_inicio_atendimento') >= F.col('hora_abertura_chamado')),
        F.col('hora_inicio_atendimento'),
    )).withColumn('hora_finalizacao_atendimento', F.when(
        (F.col('hora_abertura_chamado').isNull() |
         (F.col('hora_finalizacao_atendimento') >= F.col('hora_abertura_chamado'))) &
        (F.col('hora_inicio_atendimento').isNull() |
         (F.col('hora_finalizacao_atendimento') >= F.col('hora_inicio_atendimento'))),
        F.col('hora_finalizacao_atendimento'),
    ))

    for nome in ['tempo_espera', 'tempo_atendimento']:
        numero = F.expr(f'try_cast({nome} AS DOUBLE)')
        valido = (numero >= 0) & ~F.isnan(numero) & (numero < F.lit(float('inf')))
        origem = origem.withColumn(f'{nome}_informado', F.when(valido, numero))
    origem = origem.withColumn('tempo_espera_informado', F.when(
        F.lower(F.trim(F.col('hora_inicio_atendimento'))) == 'igual a hora de abertura',
        F.lit(0.0),
    ).otherwise(F.col('tempo_espera_informado')))
    auditoria.update(origem.agg(*[
        _contar(_preenchido(nome) & F.col(f'{nome}_informado').isNull())
        .alias(f'{nome}_informado_invalido') for nome in ['tempo_espera', 'tempo_atendimento']
    ]).first().asDict())

    tempos = ids.join(horas.select('id_chamado', *nomes, *FLAGS_PERIODO.values()),
                     'id_chamado', 'left').join(
        origem.select('id_chamado', 'tempo_espera_informado', 'tempo_atendimento_informado'),
        'id_chamado', 'left',
    ).fillna({flag: False for flag in FLAGS_PERIODO.values()})
    for nome, inicio, fim in [
        ('tempo_espera_calculado', 'hora_abertura_chamado', 'hora_inicio_atendimento'),
        ('tempo_atendimento_calculado', 'hora_inicio_atendimento', 'hora_finalizacao_atendimento'),
    ]:
        tempos = tempos.withColumn(nome, F.col(fim).cast('long') - F.col(inicio).cast('long'))
    validar_relacao_tempos(chamados, tempos)
    validar_periodo_tempos(tempos)
    auditoria.update(tempos.agg(
        F.count('*').alias('total_chamados'),
        *[F.count(c).alias(f'registros_com_{c}') for c in tempos.columns
          if c != 'id_chamado' and c not in FLAGS_PERIODO.values()],
        *[_contar(F.col(f'{c}_informado') != F.col(f'{c}_calculado'))
          .alias(f'{c}_divergente') for c in ['tempo_espera', 'tempo_atendimento']],
    ).first().asDict())
    return tempos, auditoria
