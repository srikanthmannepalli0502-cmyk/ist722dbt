with stg_orders as (
    select
        orderid,
        {{ dbt_utils.generate_surrogate_key(['customerid']) }} as customerkey,
        {{ dbt_utils.generate_surrogate_key(['employeeid']) }} as employeekey,
        replace(to_date(orderdate)::varchar,'-','')::int as orderdatekey,
        replace(to_date(requireddate)::varchar,'-','')::int as requireddatekey,
        replace(to_date(shippeddate)::varchar,'-','')::int as shippeddatekey
    from {{ source('northwind','Orders') }}
),

stg_details as (
    select
        orderid,
        productid,
        quantity,
        unitprice,
        discount,
        (quantity * unitprice * (1 - discount)) as extendedamount
    from {{ source('northwind','Order_Details') }}
),

stg_products as (
    select
        productid,
        {{ dbt_utils.generate_surrogate_key(['productid']) }} as productkey,
        {{ dbt_utils.generate_surrogate_key(['supplierid']) }} as supplierkey,
        {{ dbt_utils.generate_surrogate_key(['categoryid']) }} as categorykey
    from {{ source('northwind','Products') }}
)

select
    d.orderid,
    p.productkey,
    p.supplierkey,
    p.categorykey,
    o.customerkey,
    o.employeekey,
    o.orderdatekey,
    o.requireddatekey,
    o.shippeddatekey,

    d.quantity,
    d.unitprice,
    d.discount,
    d.extendedamount

from stg_details d
join stg_orders o
    on d.orderid = o.orderid
join stg_products p
    on d.productid = p.productid