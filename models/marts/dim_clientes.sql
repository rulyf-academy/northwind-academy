with
    stg_clientes as (
        select
            pk_cliente
            , nm_cliente
            , empresa_cliente
            , cidade_cliente
            , regiao_cliente
            , pais_cliente
        from {{ ref('stg_sap__clientes') }}
    )

select *
from stg_clientes
