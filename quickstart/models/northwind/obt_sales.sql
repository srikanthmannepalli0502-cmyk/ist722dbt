with f_sales as (
    select *
    from {{ ref('fact_sales') }}
),

d_product as (
    select *
    from {{ ref('dim_product') }}
),

d_supplier as (
    select *
    from {{ ref('dim_supplier') }}
),

d_category as (
    select *
    from {{ ref('dim_category') }}
),

d_customer as (
    select *
    from {{ ref('dim_customer') }}
),

d_employee as (
    select *
    from {{ ref('dim_employee') }}
),

d_date as (
    select *
    from {{ ref('dim_date') }}
)

select
    -- FACT ROW
    f_sales.*,

    -- DIMENSIONS (exclude surrogate keys to avoid duplicates)
    d_product.productname,
    d_product.quantityperunit,
    d_product.unitprice as product_unitprice,
    d_product.unitsinstock,
    d_product.unitsonorder,
    d_product.reorderlevel,
    d_product.discontinued,

    d_supplier.companyname as supplier_companyname,
    d_supplier.contactname as supplier_contactname,
    d_supplier.contacttitle as supplier_contacttitle,
    d_supplier.address as supplier_address,
    d_supplier.city as supplier_city,
    d_supplier.region as supplier_region,
    d_supplier.postalcode as supplier_postalcode,
    d_supplier.country as supplier_country,
    d_supplier.phone as supplier_phone,
    d_supplier.fax as supplier_fax,

    d_category.categoryname,
    d_category.description as category_description,

    -- Customer attributes
    d_customer.companyname as customer_companyname,
    d_customer.contactname as customer_contactname,
    d_customer.contacttitle as customer_contacttitle,
    d_customer.address as customer_address,
    d_customer.city as customer_city,
    d_customer.region as customer_region,
    d_customer.postalcode as customer_postalcode,
    d_customer.country as customer_country,
    d_customer.phone as customer_phone,
    d_customer.fax as customer_fax,

    -- Employee attributes
    d_employee.employeenamelastfirst,
    d_employee.employeenamefirstlast,
    d_employee.employeetitle,
    d_employee.supervisornamelastfirst,
    d_employee.supervisornamefirstlast,

    -- Date attributes
    d_date.date,
    d_date.year,
    d_date.month,
    d_date.quarter,
    d_date.day,
    d_date.dayofweek,
    d_date.weekofyear,
    d_date.dayofyear,
    d_date.quartername,
    d_date.monthname,
    d_date.dayname,
    d_date.weekday

from f_sales
    left join d_product  on f_sales.productkey  = d_product.productkey
    left join d_supplier on f_sales.supplierkey = d_supplier.supplierkey
    left join d_category on f_sales.categorykey = d_category.categorykey
    left join d_customer on f_sales.customerkey = d_customer.customerkey
    left join d_employee on f_sales.employeekey = d_employee.employeekey
    left join d_date     on f_sales.orderdatekey = d_date.datekey