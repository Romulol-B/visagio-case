"""Gold de jornadas inferidas. Funções sem gravação, reutilizadas pelo notebook."""

from pyspark.sql import Window, functions as F


def validar_chave(df, chave, tabela):
    if df.filter(F.col(chave).isNull()).limit(1).count():
        raise ValueError(f'{tabela}: {chave} nulo')
    if df.groupBy(chave).count().filter('count > 1').limit(1).count():
        raise ValueError(f'{tabela}: {chave} duplicado')


def _contar(condicao):
    return F.sum(F.when(condicao, 1).otherwise(0))


def _dividir(numerador, denominador):
    return F.when(F.col(denominador) > 0, F.col(numerador) / F.col(denominador))


def construir_jornadas(chamados, horarios, motivos, custos, pesquisas, clientes, canais):
    """Retorna vínculo, jornadas e dois agregados; preserva todos os chamados.

    A sessão deve usar America/Sao_Paulo antes de ler os timestamps Silver.
    Os quatro DataFrames precisam ser validados/materializados antes de publicar.
    """
    for df, chave, nome in [
        (chamados, 'id_chamado', 'chamados'),
        (horarios, 'id_chamado', 'chamados_hora'),
        (motivos, 'id_motivo', 'base_motivos'),
        (custos, 'id_custo', 'custos'),
        (pesquisas, 'id_pesquisa', 'pesquisa_satisfacao'),
        (clientes, 'id_cliente', 'clientes'),
        (canais, 'id_canal', 'canais'),
    ]:
        validar_chave(df, chave, nome)

    h = horarios.select(
        'id_chamado', F.col('id_cliente').alias('id_cliente_horario'),
        'hora_abertura_chamado', 'hora_finalizacao_atendimento',
    )
    # Recupera o nome da etapa a partir da chave normalizada na Silver.
    # LEFT preserva chamados com canal ausente ou fora do cadastro.
    cadastro_canais = canais.select('id_canal', F.col('nome_canal').alias('canal'))
    base = chamados.select(
        'id_chamado', 'id_cliente', 'id_motivo', 'id_canal', 'resolvido',
    ).join(cadastro_canais, 'id_canal', 'left').join(h, 'id_chamado', 'left')
    # Chamados é a referência, desde que o cliente esteja no cadastro oficial.
    # Não substituir por clientes da tabela de horários, mesmo se cadastrados.
    cadastro_clientes = clientes.select('id_cliente').withColumn('cliente_cadastrado', F.lit(True))
    base = base.join(cadastro_clientes, 'id_cliente', 'left').fillna(
        {'cliente_cadastrado': False}
    ).withColumn(
        'cliente_divergente', F.col('id_cliente') != F.col('id_cliente_horario')
    ).withColumn(
        'data_jornada', F.to_date('hora_abertura_chamado')
    ).withColumn(
        'motivo_exclusao', F.concat_ws(';',
            F.when(F.col('id_cliente').isNull(), 'cliente_ausente'),
            F.when(F.col('id_cliente').isNotNull() & ~F.col('cliente_cadastrado'),
                   'cliente_nao_cadastrado'),
            F.when(F.col('id_motivo').isNull(), 'motivo_ausente'),
            F.when(F.col('hora_abertura_chamado').isNull(), 'abertura_ausente'),
        )
    ).withColumn(
        'motivo_exclusao', F.when(F.length('motivo_exclusao') > 0, F.col('motivo_exclusao'))
    ).withColumn(
        'id_jornada', F.when(F.col('motivo_exclusao').isNull(), F.sha2(
            F.to_json(F.struct('id_cliente', 'id_motivo', 'data_jornada')), 256
        ))
    )

    # Agregar antes de unir: múltiplos custos e pesquisas nunca multiplicam contatos.
    custo_chamado = custos.groupBy('id_chamado').agg(
        F.count('*').alias('registros_custo'),
        F.count('custo').alias('valores_custo_validos'),
        F.sum('custo').alias('custo_registrado'),
    ).withColumn('custo_completo', F.col('registros_custo') == F.col('valores_custo_validos'))
    notas_validas = pesquisas.filter(F.col('nota_atendimento').between(1, 5))
    pesquisa_chamado = notas_validas.groupBy('id_chamado').agg(
        F.count('*').alias('avaliacoes_validas'),
        F.sum('nota_atendimento').alias('soma_notas'),
        _contar(F.col('nota_atendimento') >= 4).alias('avaliacoes_positivas'),
    )
    base = base.join(custo_chamado, 'id_chamado', 'left').join(
        pesquisa_chamado, 'id_chamado', 'left'
    ).fillna({'registros_custo': 0, 'valores_custo_validos': 0,
              'custo_completo': False, 'avaliacoes_validas': 0,
              'soma_notas': 0, 'avaliacoes_positivas': 0})
    base = base.withColumn('custo_contato_completo',
                           F.when(F.col('custo_completo'), F.col('custo_registrado')))
    elegiveis = base.filter('id_jornada IS NOT NULL')

    jornada = Window.partitionBy('id_jornada')
    ordem = jornada.orderBy('hora_abertura_chamado', 'id_chamado')
    # RANGE inclui todos os contatos empatados, sem usar o ID como precedência.
    acumulado = jornada.orderBy('hora_abertura_chamado').rangeBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    bloco = Window.partitionBy('id_jornada', 'hora_abertura_chamado')
    elegiveis = (elegiveis
        .withColumn('sequencia', F.row_number().over(ordem))
        .withColumn('abertura_empatada', F.count('*').over(bloco) > 1)
        .withColumn('_primeira_abertura', F.min('hora_abertura_chamado').over(jornada))
        .withColumn('_ultima_abertura', F.max('hora_abertura_chamado').over(jornada))
        .withColumn('_custo_acumulado', F.sum('custo_registrado').over(acumulado))
        .withColumn('custo_acumulado_completo',
                    F.min(F.col('custo_completo').cast('int')).over(acumulado) == 1)
        .withColumn('custo_acumulado', F.when(F.col('custo_acumulado_completo'),
                                             F.col('_custo_acumulado')))
        .withColumn('_canal_anterior', F.lag('canal').over(ordem))
        .withColumn('_transicao', F.when(F.col('sequencia') == 1, 0)
                    .when(~F.col('canal').eqNullSafe(F.col('_canal_anterior')), 1).otherwise(0))
    )
    primeiro = F.col('hora_abertura_chamado') == F.col('_primeira_abertura')
    ultimo = F.col('hora_abertura_chamado') == F.col('_ultima_abertura')
    status = F.when(F.col('resolvido').isin(0, 1), F.col('resolvido')).otherwise(-1)
    canal = F.coalesce(F.col('canal'), F.lit('desconhecido'))
    fim_valido = (F.col('hora_finalizacao_atendimento').isNotNull()
                  & (F.col('hora_finalizacao_atendimento') >= F.col('hora_abertura_chamado')))

    # Um empate entre canais diferentes torna a contagem temporal ambígua.
    ambiguidades = elegiveis.groupBy('id_jornada', 'hora_abertura_chamado').agg(
        F.countDistinct(canal).alias('_canais_bloco')
    ).groupBy('id_jornada').agg(F.max('_canais_bloco').alias('_max_canais_bloco'))
    jornadas = elegiveis.groupBy('id_jornada', 'id_cliente', 'id_motivo', 'data_jornada').agg(
        F.min('hora_abertura_chamado').alias('primeira_abertura'),
        F.max('hora_abertura_chamado').alias('ultima_abertura'),
        F.countDistinct(F.when(primeiro, canal)).alias('_canais_primeiro'),
        F.min(F.when(primeiro, canal)).alias('canal_primeiro_contato'),
        F.countDistinct(F.when(ultimo, status)).alias('_status_ultimo'),
        F.max(F.when(ultimo, status)).alias('_resolvido_ultimo'),
        F.min(F.when(ultimo, fim_valido.cast('int'))).alias('_fim_valido'),
        F.max(F.when(ultimo, F.col('hora_finalizacao_atendimento'))).alias('_fim_ultimo'),
        F.count('*').alias('quantidade_contatos'),
        F.sum('_transicao').alias('_transicoes'),
        _contar(F.col('canal').isNull()).alias('_canais_ausentes'),
        F.sum('custo_registrado').alias('custo_total_registrado'),
        _contar(F.col('custo_completo')).alias('contatos_com_custo_completo'),
        F.sum('avaliacoes_validas').alias('avaliacoes_validas'),
        F.sum('soma_notas').alias('soma_notas'),
        F.sum('avaliacoes_positivas').alias('avaliacoes_positivas'),
        F.max(F.col('abertura_empatada').cast('int')).cast('boolean').alias('tem_empate_abertura'),
    ).join(ambiguidades, 'id_jornada').withColumn(
        'origem', F.when((F.col('_canais_primeiro') == 1) &
                         (F.col('canal_primeiro_contato') == 'ura'), 'telefone')
                   .when((F.col('_canais_primeiro') == 1) &
                         (F.col('canal_primeiro_contato') == 'chatbot'), 'digital')
                   .otherwise('indeterminada')
    ).withColumn(
        'resolvido_final', F.when((F.col('_status_ultimo') == 1) &
                                  F.col('_resolvido_ultimo').isin(0, 1), F.col('_resolvido_ultimo'))
    ).withColumn(
        'finalizacao_ultimo_contato', F.when(F.col('_fim_valido') == 1, F.col('_fim_ultimo'))
    ).withColumn(
        'duracao_observada_segundos',
        F.col('finalizacao_ultimo_contato').cast('long') - F.col('primeira_abertura').cast('long')
    ).withColumn(
        'transicoes_observadas', F.when((F.col('_max_canais_bloco') == 1) &
                                        (F.col('_canais_ausentes') == 0), F.col('_transicoes'))
    ).withColumn(
        'custo_completo', F.col('contatos_com_custo_completo') == F.col('quantidade_contatos')
    ).withColumn(
        'custo_total_completo', F.when(F.col('custo_completo'), F.col('custo_total_registrado'))
    ).withColumn('nota_media', _dividir('soma_notas', 'avaliacoes_validas'))
    jornadas = jornadas.drop(*[c for c in jornadas.columns if c.startswith('_')]).join(
        motivos.select('id_motivo', 'nome_motivo', 'categoria'), 'id_motivo', 'left'
    )

    detalhe = elegiveis.select(
        'id_chamado', 'sequencia', 'abertura_empatada',
        'custo_acumulado_completo', 'custo_acumulado',
    )
    vinculo = base.join(detalhe, 'id_chamado', 'left').join(
        jornadas.select('id_jornada', 'origem'), 'id_jornada', 'left'
    ).withColumn('etapa', F.when(F.col('canal').isin(
        'ura', 'chatbot', 'atendimento inicial', 'atendimento especializado'
    ), F.col('canal')).otherwise('canal desconhecido'))

    dimensoes = ['data_jornada', 'id_motivo', 'origem']
    etapas = vinculo.filter('id_jornada IS NOT NULL').groupBy(*dimensoes, 'etapa').agg(
        F.count('*').alias('total_contatos'),
        F.countDistinct('id_jornada').alias('total_jornadas'),
        _contar(F.col('resolvido') == 1).alias('contatos_resolvidos'),
        _contar(F.col('resolvido').isin(0, 1)).alias('contatos_status_conhecido'),
        _contar(~F.coalesce(F.col('resolvido').isin(0, 1), F.lit(False))).alias('contatos_status_desconhecido'),
        F.sum('custo_registrado').alias('custo_total_registrado'),
        F.sum('custo_contato_completo').alias('soma_custo_contatos_completos'),
        _contar(F.col('custo_completo')).alias('contatos_custo_completo'),
        F.sum('custo_acumulado').alias('soma_custo_acumulado_completo'),
        _contar(F.col('custo_acumulado_completo')).alias('contatos_acumulado_completo'),
    ).withColumn('taxa_sucesso', _dividir('contatos_resolvidos', 'contatos_status_conhecido'))
    etapas = etapas.withColumn('custo_medio_contato',
        _dividir('soma_custo_contatos_completos', 'contatos_custo_completo')
    ).withColumn('custo_medio_acumulado',
        _dividir('soma_custo_acumulado_completo', 'contatos_acumulado_completo'))

    indicadores = jornadas.groupBy(*dimensoes).agg(
        F.count('*').alias('total_jornadas'),
        F.sum('quantidade_contatos').alias('total_contatos'),
        _contar(F.col('quantidade_contatos') > 1).alias('jornadas_multiplos_contatos'),
        _contar(F.col('resolvido_final') == 1).alias('jornadas_resolvidas'),
        F.count('resolvido_final').alias('jornadas_status_conhecido'),
        _contar(F.col('resolvido_final').isNull()).alias('jornadas_status_desconhecido'),
        F.sum('duracao_observada_segundos').alias('soma_duracao_observada_segundos'),
        F.count('duracao_observada_segundos').alias('jornadas_duracao_conhecida'),
        F.sum('transicoes_observadas').alias('total_transicoes_observadas'),
        F.count('transicoes_observadas').alias('jornadas_transicoes_conhecidas'),
        F.sum('custo_total_registrado').alias('custo_total_registrado'),
        F.sum('custo_total_completo').alias('soma_custo_jornadas_completas'),
        _contar(F.col('custo_completo')).alias('jornadas_custo_completo'),
        F.sum('avaliacoes_validas').alias('avaliacoes_validas'),
        F.sum('soma_notas').alias('soma_notas'),
        F.sum('avaliacoes_positivas').alias('avaliacoes_positivas'),
        _contar(F.col('avaliacoes_validas') > 0).alias('jornadas_avaliadas'),
    )
    for nome, numerador, denominador in [
        ('contatos_por_jornada', 'total_contatos', 'total_jornadas'),
        ('proporcao_multiplos_contatos', 'jornadas_multiplos_contatos', 'total_jornadas'),
        ('taxa_resolucao', 'jornadas_resolvidas', 'jornadas_status_conhecido'),
        ('duracao_media_observada_segundos', 'soma_duracao_observada_segundos', 'jornadas_duracao_conhecida'),
        ('custo_medio_jornada_completa', 'soma_custo_jornadas_completas', 'jornadas_custo_completo'),
        ('nota_media', 'soma_notas', 'avaliacoes_validas'),
        ('proporcao_avaliacoes_positivas', 'avaliacoes_positivas', 'avaliacoes_validas'),
    ]:
        indicadores = indicadores.withColumn(nome, _dividir(numerador, denominador))
    cadastro = motivos.select('id_motivo', 'nome_motivo', 'categoria')
    return {
        'jornada_chamados': vinculo,
        'jornadas': jornadas,
        'indicadores_jornadas': indicadores.join(cadastro, 'id_motivo', 'left'),
        'indicadores_etapas_jornada': etapas.join(cadastro, 'id_motivo', 'left'),
    }


def validar_resultados(chamados, tabelas):
    vinculo, jornadas = tabelas['jornada_chamados'], tabelas['jornadas']
    validar_chave(vinculo, 'id_chamado', 'gold.jornada_chamados')
    validar_chave(jornadas, 'id_jornada', 'gold.jornadas')
    if vinculo.count() != chamados.count():
        raise ValueError('Quantidade de chamados mudou na construção das jornadas')
    elegiveis = vinculo.filter('id_jornada IS NOT NULL').count()
    total = jornadas.agg(F.sum('quantidade_contatos')).first()[0] or 0
    if elegiveis != total:
        raise ValueError('Contatos elegíveis não reconciliam com as jornadas')
    if vinculo.filter('id_jornada IS NOT NULL').join(
        jornadas.select('id_jornada'), 'id_jornada', 'left_anti'
    ).limit(1).count():
        raise ValueError('Vínculo aponta para jornada inexistente')
