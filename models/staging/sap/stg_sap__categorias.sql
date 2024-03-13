with
    fonte_categorias as (
        select
            cast(id as int) as pk_categoria
            , cast(categoryname as varchar(128)) as nm_categoria
            , cast(description as string) as descricao_categoria
        from {{ source('sap', 'category') }}
    )

select *
from fonte_categorias
