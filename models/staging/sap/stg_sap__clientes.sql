with
    fonte_clientes as (
        select
            cast(id as string) pk_cliente
            , cast(contactname as string) as nm_cliente
            , cast(companyname as string) as empresa_cliente
            , cast(city as string) as cidade_cliente
            , cast(region as string) as regiao_cliente
            , cast(country as string) as pais_cliente
        from {{ source('sap', 'customer') }}
    )

select *
from fonte_clientes
