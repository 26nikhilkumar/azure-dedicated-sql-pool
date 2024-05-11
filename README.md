# azure-dedicated-sql-pool

1. upload the data in adls gen2 - orders
2. we want to create a order table in dedicated sql pool

In orders table we want to load the data from datalake
there are 2 options to load the data from datalake 
1. Polybase
2. Copy command

----------------------------

1) Polybase

Step 1 - 
First we should create an external table in dedicated sql pool 
- Data Source
- External File format
- External Table

IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name =
    'SynapseDelimitedTextFormat')
        CREATE EXTERNAL FILE FORMAT [SynapseDelimitedTextFormat]
        WITH ( FORMAT_TYPE = DELIMITEDTEXT ,
        FORMAT_OPTIONS (
        FIELD_TERMINATOR = ',',
        FIRST_ROW = 2,
        USE_TYPE_DEFAULT = FALSE
))
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name =
  'raw_ttsynapsesa_dfs_core_windows_net')
    CREATE EXTERNAL DATA SOURCE [raw_ttsynapsesa_dfs_core_windows_net]
    WITH (
    LOCATION = 'abfss://raw@ttsynapsesa.dfs.core.windows.net',
    TYPE = HADOOP
  )
GO

CREATE EXTERNAL TABLE orders_ext (
  [order_id] bigint,
  [order_date] VARCHAR(4000),
  [customer_id] bigint,
  [order_status] nvarchar(4000)
)
WITH (
  LOCATION = 'orders.csv',
  DATA_SOURCE = [raw_ttsynapsesa_dfs_core_windows_net],
  FILE_FORMAT = [SynapseDelimitedTextFormat]
)
GO

Step 2-
CTAS

Create table internal_table as 
select * from external table

CREATE table orders_internal_roundrobin
WITH
(
DISTRIBUTION = ROUND_ROBIN
)
AS
select * from orders_ext

select customer_id, count(*) as total_orders from orders_internal_roundrobin group by
customer_id order by total_orders DESC ;

DBCC PDW_SHOWSPACEUSED('orders_internal_roundrobin')

-----------------------

2) Copy command

CREATE table orders_internal_HASH
  WITH
    (
    DISTRIBUTION = HASH(CUSTOMER_ID)
    )
AS
  select * from orders_ext

DBCC PDW_SHOWSPACEUSED('orders_internal_HASH')

select customer_id, count(*) as total_orders from orders_internal_HASH group by
customer_id order by total_orders DESC ;
