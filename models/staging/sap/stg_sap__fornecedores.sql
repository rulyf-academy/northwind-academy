with
    fonte_fornecedores as (
        select
            cast(id as int) as pk_fornecedor
            , cast(companyname as string) as nm_fornecedor
            , cast(city as string) as cidade_fornecedor
            , cast(region as string) as regiao_fornecedor
            , cast(country as string) as pais_fornecedor
            --, contacttitle
            --, address
            --, postalcode
            --, contactname
        from {{ source('sap', 'supplier') }}
    )

select *
from fonte_fornecedores
