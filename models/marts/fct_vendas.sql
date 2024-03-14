with
    dim_produtos as (
        select *
        from {{ ref('dim_produtos') }}
    )

    , dim_clientes as (
        select *
        from {{ ref('dim_clientes') }}
    )

    , dim_funcionarios as (
        select *
        from {{ ref('dim_funcionarios') }}
    )

    , ordem_detalhes as (
        select *
        from {{ ref('stg_sap__ordem_detalhes') }}
    )

    , ordens as (
        select *
        from {{ ref('stg_sap__ordens') }}
    )

    /* Vamos fazer primeiro a relação das ordens e dos itens do pedido. */
    , joined_ordens_itens as (
        select
            ordem_detalhes.fk_pedido
            , ordem_detalhes.fk_produto
            , ordens.fk_funcionario
            , ordens.fk_cliente
            , ordens.fk_transportadora
            , ordem_detalhes.desconto_perc::numeric(18,2) as desconto_perc
            , ordem_detalhes.preco_da_unidade::numeric(18,2) as preco_da_unidade
            , ordem_detalhes.quantidade
            , ordens.data_do_pedido
            , ordens.frete
            , ordens.nm_destinatario
            , ordens.cidade_destinatario
            , ordens.regiao_destinatario
            , ordens.pais_destinatario
            , ordens.data_do_envio
            , ordens.data_requerida_entrega
        from ordem_detalhes
        left join ordens on ordem_detalhes.fk_pedido = ordens.pk_pedido
    )

    /* Aqui fazemos todos os joins com dimensões. */
    , joined_dimensoes as (
        select
            fatos.*
            , dim_produtos.nm_produto
            , dim_produtos.nm_fornecedor
            , dim_produtos.nm_categoria
            , dim_funcionarios.nm_funcionario
            , dim_funcionarios.nm_gerente
            , dim_clientes.nm_cliente
            , dim_clientes.empresa_cliente
            , dim_clientes.cidade_cliente
            , dim_clientes.pais_cliente
        from joined_ordens_itens as fatos
        left join dim_produtos on fatos.fk_produto = dim_produtos.pk_produto
        left join dim_funcionarios on fatos.fk_funcionario = dim_funcionarios.pk_funcionario
        left join dim_clientes on fatos.fk_cliente = dim_clientes.pk_cliente
    )

    /* Criação de métricas e chave primária da tabela. */
    , metricas as (
        select
            {{ dbt_utils.generate_surrogate_key(['fk_pedido', 'fk_produto']) }} as sk_fatos_vendas
            , *
            , preco_da_unidade::numeric(18,2) * quantidade::numeric(18,2) as faturamento_bruto
            , preco_da_unidade * quantidade * (1 - desconto_perc) as faturamento_liquido
            , case
                when desconto_perc > 0 then true
                else false
            end as teve_desconto
            , cast((frete / count(fk_pedido) over(partition by fk_pedido)) as numeric(18,2)) as frete_rateado
        from joined_dimensoes
    )

select *
from metricas