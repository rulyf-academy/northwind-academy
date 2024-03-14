with
    fonte_ordens as (
        select
            cast(order_id as int) pk_pedido
            , cast(employee_id as int) as fk_funcionario
            , cast(customer_id as string) as fk_cliente
            , cast(ship_via as int) as fk_transportadora
            , cast(order_date as date) as data_do_pedido
            , cast(freight as numeric) as frete
            , cast(ship_name as string) as nm_destinatario
            --, cast(ship_address as string) as endereco_destinatario
            --, cast(ship_postal_code as string) as cep_destinatario
            , cast(ship_city as string) as cidade_destinatario
            , cast(ship_region as string) as regiao_destinatario
            , cast(ship_country as string) as pais_destinatario
            , cast(shipped_date as date) as data_do_envio
            , cast(required_date as date) as data_requerida_entrega
        from {{ source('sap', 'orders') }}
    )

select *
from fonte_ordens
