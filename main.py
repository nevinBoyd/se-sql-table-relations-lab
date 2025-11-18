# STEP 0

# SQL Library and Pandas Library
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

pd.read_sql("""SELECT * FROM sqlite_master""", conn)

# STEP 1
# Replace None with your code
# Boston employees
df_boston = pd.read_sql("""
SELECT e.firstName, e.lastName
FROM employees e
JOIN offices o ON e.officeCode = o.officeCode
WHERE o.city = 'Boston'
ORDER BY e.firstName;
""", conn)

# STEP 2
# Replace None with your code
# Offices with zero employees
df_zero_emp = pd.read_sql("""
SELECT o.officeCode
FROM offices o
LEFT JOIN employees e ON o.officeCode = e.officeCode
WHERE e.employeeNumber IS NULL;
""", conn)

# STEP 3
# Replace None with your code
# All employees + their office city/state
df_employee = pd.read_sql("""
SELECT e.firstName, e.lastName, o.city, o.state
FROM employees e
LEFT JOIN offices o ON e.officeCode = o.officeCode
ORDER BY e.firstName, e.lastName;
""", conn)

# STEP 4
# Replace None with your code
# Customers who have never placed an order
df_contacts = pd.read_sql("""
SELECT c.contactFirstName, c.contactLastName, c.phone, c.salesRepEmployeeNumber
FROM customers c
LEFT JOIN orders o ON c.customerNumber = o.customerNumber
WHERE o.orderNumber IS NULL
ORDER BY c.contactLastName;
""", conn)

# STEP 5
# Replace None with your code
# Customer payment history sorted by amount DESC
df_payment = pd.read_sql("""
SELECT c.contactFirstName,
       c.contactLastName,
       CAST(p.amount AS REAL) AS amount,
       p.paymentDate
FROM payments p
JOIN customers c ON p.customerNumber = c.customerNumber
ORDER BY amount DESC;
""", conn)

# STEP 6
# Replace None with your code
# Employees with customers average creditLimit > 90000
df_credit = pd.read_sql("""
SELECT e.employeeNumber,
       e.firstName,
       e.lastName,
       COUNT(c.customerNumber) AS num_customers
FROM employees e
JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
GROUP BY e.employeeNumber
HAVING AVG(c.creditLimit) > 90000
ORDER BY num_customers DESC;
""", conn)

# STEP 7
# Replace None with your code
# Product sales volume and order count
df_product_sold = pd.read_sql("""
SELECT p.productName,
       COUNT(od.orderNumber) AS numorders,
       SUM(od.quantityOrdered) AS totalunits
FROM orderdetails od
JOIN products p ON od.productCode = p.productCode
GROUP BY p.productCode
ORDER BY totalunits DESC;
""", conn)

# STEP 8
# Replace None with your code
# Distinct number of customers per product
df_total_customers = pd.read_sql("""
SELECT p.productName,
       p.productCode,
       COUNT(DISTINCT o.customerNumber) AS numpurchasers
FROM orderdetails od
JOIN orders o ON od.orderNumber = o.orderNumber
JOIN products p ON od.productCode = p.productCode
GROUP BY p.productCode
ORDER BY numpurchasers DESC;
""", conn)

# STEP 9
# Replace None with your code
# Customers per office
df_customers = pd.read_sql("""
SELECT
    COUNT(DISTINCT c.customerNumber) AS n_customers,
    o.officeCode,
    o.city
FROM offices o
LEFT JOIN employees e ON o.officeCode = e.officeCode
LEFT JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
GROUP BY o.officeCode
ORDER BY o.officeCode ASC;
""", conn)

# STEP 10
# Replace None with your code
# Employees who sold products ordered <20 distinct customers
df_under_20 = pd.read_sql("""
SELECT DISTINCT
    e.employeeNumber,
    e.firstName,
    e.lastName,
    o.city,
    o.officeCode
FROM employees e
JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
JOIN orders ord ON c.customerNumber = ord.customerNumber
JOIN orderdetails od ON ord.orderNumber = od.orderNumber
JOIN products p ON od.productCode = p.productCode
JOIN offices o ON e.officeCode = o.officeCode
WHERE p.productCode IN (
    SELECT productCode
    FROM orderdetails od
    JOIN orders ord ON od.orderNumber = ord.orderNumber
    GROUP BY productCode
    HAVING COUNT(DISTINCT ord.customerNumber) < 20
)
ORDER BY e.lastName, e.firstName;
""", conn)

conn.close()