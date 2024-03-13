with
    categorias as (
        select *
        from {{ ref('stg_sap__categorias') }}
    )

    , fornecedores as (
        select *
        from {{ ref('stg_sap__fornecedores') }}
    )

    , produtos as (
        select 
            pk_produto
            , fk_fornecedor
            , fk_categoria
            , nm_produto
            , preco_por_unidade
            , is_discontinuado
        from {{ ref('stg_sap__produtos') }}
    )

    , joined_tabelas as (
        select
            produtos.pk_produto
            , produtos.nm_produto
            , produtos.preco_por_unidade
            , produtos.is_discontinuado
            , fornecedores.nm_fornecedor
            , fornecedores.pais_fornecedor
            , categorias.nm_categoria
        from produtos
        left join fornecedores on produtos.fk_fornecedor = fornecedores.pk_fornecedor
        left join categorias on produtos.fk_categoria = categorias.pk_categoria
    )

select *
from joined_tabelas
