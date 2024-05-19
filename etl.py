import pandas as pd
import pyodbc
from sqlalchemy import create_engine, text
from pandas import Int64Dtype
from dotenv import load_dotenv
import os
from datetime import datetime
from prefect import flow, task

# Load environment variables
server = 'HuyThai\SQLEXPRESS'
database = 'Thai1'
dwh = 'ThaiDWH2'
# username = os.environ["user"]
# password = os.environ["pass"]
username = 'Thai'
password = '2'
driver = 'ODBC Driver 17 for SQL Server' 

# Tạo URL kết nối SQLAlchemy
url = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}'
url_dwh = f'mssql+pyodbc://{username}:{password}@{server}/{dwh}?driver={driver}'

def create_engine_and_connect(url):
    engine = create_engine(url)
    connection = engine.connect()
    return engine, connection
# # Tạo đối tượng Engine
# engine = create_engine(url)
# engine_dwh = create_engine(url_dwh)
# # Tạo connection
# conn_source = engine.connect()
# conn_dwh = engine_dwh.connect()

@task
# Load data from source
def extract(conn_source):
    customers = pd.read_sql(text('SELECT * FROM Customers'), conn_source)
    products = pd.read_sql(text('SELECT * FROM Product'), conn_source, dtype={'ProductSubcategoryID': Int64Dtype()})
    order_header = pd.read_sql(text('SELECT * FROM OrderHeader'), conn_source)
    order_detail = pd.read_sql(text('SELECT * FROM OrderDetail'), conn_source)
    product_subcategory = pd.read_sql(text('SELECT * FROM ProductSubCategory'), conn_source)
    product_category = pd.read_sql(text('SELECT * FROM ProductCategory'), conn_source)
    return customers, products, order_header, order_detail, product_subcategory, product_category

@task
# Transform data
def transform(customers, products, order_header, order_detail, product_subcategory, product_category):
    dim_customer = customers[['CustomerID', 'AccountNumber', 'FirstName', 'MiddleName', 'LastName']].copy()
    dim_product = products[['ProductID', 'Name', 'Color', 'ListPrice', 'Size', 'ProductSubcategoryID']].copy()
    dim_product_subcategory = product_subcategory[['ProductSubcategoryID', 'Name', 'ProductCategoryID']].copy()
    dim_product_category = product_category[['ProductCategoryID', 'Name']].copy()
    # dim_date
    order_header['OrderDate'] = pd.to_datetime(order_header['OrderDate'])
    dim_date = order_header[['OrderDate']].drop_duplicates().copy()
    dim_date['DateID'] = dim_date['OrderDate'].dt.strftime('%Y%m%d').astype(int)
    dim_date['Day'] = dim_date['OrderDate'].dt.day
    dim_date['Month'] = dim_date['OrderDate'].dt.month
    dim_date['Quarter'] = dim_date['OrderDate'].dt.quarter
    dim_date['Year'] = dim_date['OrderDate'].dt.year
    dim_date['Weekday'] = dim_date['OrderDate'].dt.weekday
    #factsales
    fact_sales = order_detail.merge(order_header, on='SalesOrderID')
    fact_sales['TotalAmount'] = fact_sales['OrderQty'] * (fact_sales['UnitPrice'] - fact_sales['UnitPriceDiscount'])
    fact_sales = fact_sales[['SalesOrderDetailID', 'SalesOrderID', 'CustomerID', 'ProductID', 'OrderDate', 'ShipDate', 'SalesOrderNumber', 'SubTotal', 'OrderQty', 'UnitPrice', 'UnitPriceDiscount', 'TotalAmount']]
    fact_sales = fact_sales.merge(dim_date, left_on='OrderDate', right_on='OrderDate')
    fact_sales = fact_sales[['SalesOrderDetailID', 'SalesOrderID', 'CustomerID', 'ProductID', 'DateID','ShipDate', 'SalesOrderNumber', 'SubTotal', 'OrderQty', 'UnitPrice', 'UnitPriceDiscount', 'TotalAmount']]
    fact_sales = fact_sales.merge(products, on='ProductID')
    fact_sales = fact_sales.merge(product_subcategory, on='ProductSubcategoryID')
    fact_sales = fact_sales.merge(product_category, on='ProductCategoryID')
    fact_sales = fact_sales[['SalesOrderDetailID', 'SalesOrderID', 'CustomerID', 'ProductID', 'DateID', 'ProductSubcategoryID', 'ProductCategoryID', 'ShipDate', 'SalesOrderNumber', 'SubTotal', 'OrderQty', 'UnitPrice', 'UnitPriceDiscount', 'TotalAmount']]
    return dim_customer, dim_product, dim_product_subcategory, dim_product_category, dim_date, fact_sales

@task
def load(conn_dwh,dim_customer, dim_product, dim_product_subcategory, dim_product_category, dim_date, fact_sales):
    dim_customer.to_sql('DimCustomer', conn_dwh, if_exists='replace', index=False, method='multi', chunksize=50)
    dim_product.to_sql('DimProduct', conn_dwh, if_exists='replace', index=False, method='multi', chunksize=50)
    dim_product_subcategory.to_sql('DimProductSubcategory', conn_dwh, if_exists='replace', index=False, method='multi', chunksize=50)
    dim_product_category.to_sql('DimProductCategory', conn_dwh, if_exists='replace', index=False, method='multi', chunksize=50)
    dim_date.to_sql('DimDate', conn_dwh, if_exists='replace', index=False, method='multi', chunksize=50)
    fact_sales.to_sql('FactSales', conn_dwh, if_exists='replace', index=False, method='multi', chunksize=50)

@task
def log_pipeline_run(connection, start_time, end_time, status):
    log_df = pd.DataFrame({
        'start_time': [start_time],
        'end_time': [end_time],
        'status': [status]
    })
    log_df.to_sql('pipeline_log', connection, if_exists='append', index=False)

# def main():
#     customers, products, order_header, order_detail, product_subcategory, product_category = extract()
#     dim_customer, dim_product, dim_product_subcategory, dim_product_category, dim_date, fact_sales = transform(customers, products, order_header, order_detail, product_subcategory, product_category)
#     load(dim_customer, dim_product, dim_product_subcategory, dim_product_category, dim_date, fact_sales)

# if __name__ == '__main__':
#     main()
#     conn_source.close()
#     conn_dwh.close()
#     engine.dispose()
#     engine_dwh.dispose()

@flow
def run_pipeline(start_date=None, end_date=None):
    start_time = datetime.now()
    
    # source_engine, conn_source = create_engine_and_connect(url)
    # dwh_engine, conn_dwh = create_engine_and_connect(url_dwh)
    # # Tạo đối tượng Engine
    source_engine = create_engine(url)
    dwh_engine = create_engine(url_dwh)
    # Tạo connection
    conn_source = source_engine.connect()
    conn_dwh = dwh_engine.connect()
    try:
        customers, products, order_header, order_detail, product_subcategory, product_category = extract(conn_source)
        dim_customer, dim_product, dim_product_subcategory, dim_product_category, dim_date, fact_sales = transform(customers, products, order_header, order_detail, product_subcategory, product_category)
        load(conn_dwh, dim_customer, dim_product, dim_product_subcategory, dim_product_category, dim_date, fact_sales)
        
        end_time = datetime.now()
        log_pipeline_run(conn_dwh, start_time, end_time, 'success')
    except Exception as e:
        end_time = datetime.now()
        log_pipeline_run(conn_dwh, start_time, end_time, 'failure')
        raise e
    finally:
        conn_source.close()
        conn_dwh.close()
        source_engine.dispose()
        dwh_engine.dispose()

if __name__ == '__main__':
    run_pipeline(start_date='2024-01-01', end_date='2024-12-31')