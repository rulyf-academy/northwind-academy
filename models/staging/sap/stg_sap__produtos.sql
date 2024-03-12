with
    fonte_produtos as (
        select *
        from {{ source('sap', 'product') }}
    )

    , renomear as (
        select
            cast(ID as int) as id_produto
            , cast(PRODUCTNAME as string) as nm_produto
            , cast(SUPPLIERID as int) as fk_fornecedor 
            , cast(CATEGORYID as int) as fk_categoria
            , cast(QUANTITYPERUNIT as string) as quantidade_por_unidade
            , cast(UNITPRICE as numeric(18,2)) as preco_por_unidade
            , cast(UNITSINSTOCK as int) as unidade_em_estoque
            , cast(UNITSONORDER as int) as unidade_por_pedido
            , cast(REORDERLEVEL as int) as nivel_de_pedido
            , case
                when DISCONTINUED = 0 then true
                when DISCONTINUED = 1 then false
            end as is_discontinuado
        from fonte_produtos
    )

select *
from renomear
