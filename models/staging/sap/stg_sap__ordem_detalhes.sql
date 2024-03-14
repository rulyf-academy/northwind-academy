with
    fonte_ordem_detalhes as (
        select
            cast(order_id as int) as fk_pedido
            , cast(product_id as int) as fk_produto
            , cast(discount as numeric(18,2)) as desconto_perc
            , cast(unit_price as numeric(18,2)) as preco_da_unidade
            , cast(quantity as int) as quantidade
        from {{ source('sap', 'orders_details') }}
    )

select *
from fonte_ordem_detalhes
