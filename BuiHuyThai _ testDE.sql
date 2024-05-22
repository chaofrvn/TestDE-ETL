use ThaiDWH;
-- Cau hoi 1: Thong ke so luong don hang & doanh thu cua moi khach hang da mua

select c.CustomerID, 
c.AccountNumber, 
CONCAT(c.firstname, ' ', c.middlename, ' ', c.lastname) as CustomerFullname, 
count(distinct f.SalesOrderID) as TotalOrder, 
sum(f.TotalAmount) as TotalRevenue
from 
FactSales f join DimCustomer c on f.CustomerID = c.CustomerID
group by c.CustomerID, c.AccountNumber, c.FirstName, c.MiddleName, c.LastName
order by c.AccountNumber;

-- Cau hoi 2: Thong ke doanh thu va so luong san pham cua tung ProductCategory da ban

select 
pc.ProductCategoryID as CategoryID,
pc.Name as ProductCategory, 
sum(f.orderqty) as total_product_sales, 
sum(f.TotalAmount) as total_revenue 
from
FactSales f join DimProductCategory pc on f.ProductCategoryID = pc.ProductCategoryID
group by pc.Name, pc.ProductCategoryID
order by pc.ProductCategoryID;

-- Cau hoi 3: 
with table1 as 
(
select 
d.Year, 
d.Month,
f.CustomerID,
concat(c.FirstName, ' ', c.MiddleName, ' ', c.LastName) as CustomerFullName,
sum(f.TotalAmount) as EmployeeMonthAmount
from 
FactSales f join DimDate d on f.DateID = d.DateID 
join DimCustomer c on f.CustomerID = c.CustomerID
group by d.Month, d.Year, f.CustomerID , concat(c.FirstName, ' ', c.MiddleName, ' ', c.LastName)
),
table2 as 
(
select
Year,
Month,
CustomerID,
CustomerFullName,
EmployeeMonthAmount,
row_number() over(partition by Year, Month order by EmployeeMonthAmount desc) as top_customer 
from table1)

select Year,
Month,
CustomerID,
CustomerFullName,
EmployeeMonthAmount from table2 where top_customer <= 5
order by Year, Month;

-- Cau 4: 
with table1 as 
(
select 
d.Year, 
d.Month,
f.CustomerID,
concat(c.FirstName, ' ', c.MiddleName, ' ', c.LastName) as CustomerFullName,
sum(f.TotalAmount) as EmployeeMonthAmount
from 
FactSales f join DimDate d on f.DateID = d.DateID 
join DimCustomer c on f.CustomerID = c.CustomerID
group by d.Month, d.Year, f.CustomerID , concat(c.FirstName, ' ', c.MiddleName, ' ', c.LastName)
),
table3 as
(
select Year, Month, CustomerID, CustomerFullName, EmployeeMonthAmount,
lag(EmployeeMonthAmount) over (partition by customerID, month order by year) CustomerMonthAmount_LastYear,
row_number() over(partition by Year, Month order by EmployeeMonthAmount desc) as top_customer 
from table1)

select Year,
Month,
CustomerID,
CustomerFullName,
EmployeeMonthAmount,
CustomerMonthAmount_LastYear
from table3 where top_customer <= 5 order by Year, Month;


select top(100) * from FactSales order by CustomerID;













