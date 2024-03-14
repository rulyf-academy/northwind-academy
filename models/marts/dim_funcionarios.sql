with
    stg_funcionarios as (
        select
            pk_funcionario
            , fk_gerente
            , nm_funcionario
            , cargo_funcionario
            , dt_nascimento_funcionario
            , dt_contratacao
        from {{ ref('stg_sap__funcionarios') }}
    )

    /* Fazemos um self join com a tabela para pegar o nome dos funcionarios que são gerentes. */
    , self_join_funcionarios as (
        select
            funcionario.pk_funcionario
            , funcionario.nm_funcionario
            , gerente.nm_funcionario as nm_gerente
            , funcionario.cargo_funcionario
            , funcionario.dt_nascimento_funcionario
            , funcionario.dt_contratacao
        from stg_funcionarios as funcionario
        left join stg_funcionarios as gerente on
            funcionario.fk_gerente = gerente.pk_funcionario
    )

select *
from self_join_funcionarios
