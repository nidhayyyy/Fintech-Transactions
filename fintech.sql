Create Database Fintech;
Use Fintech;

Select * from customers;
Select * From transactions;
Select * From accounts;
Select * From digital_wallet_usage;

-- DATA CLEANING
-- 1. Handle Missing Values
Update digital_wallet_usage
SET cashback = 0 
WHERE cashback IS NULL;

UPDATE transactions
SET merchant = 'UNKNOWN'
WHERE txn_type = 'Payment' AND merchant IS NULL;

-- 2 Deleting Rows with Null Values
DELETE FROM customers
WHERE customer_id IS NULL;

DELETE FROM accounts
WHERE customer_id IS NULL;

DELETE FROM transactions
WHERE account_id IS NULL;

DELETE FROM digital_wallet_usage
WHERE customer_id IS NULL;

-- REMOVE DUPLICATES
-- Customer Table
WITH cust_duplicates AS (
   Select customer_id,
       ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY join_date) AS rn
   From customers
) 
Delete From customers 
  WHERE customer_id IN (
      Select customer_id From cust_duplicates 
      where rn > 1
);     
-- Accounts Table
WITH acc_duplicates AS (
   Select account_id,
       ROW_NUMBER() OVER(PARTITION BY account_id ORDER BY last_updated) AS rn
   From accounts
) 
Delete From accounts 
  WHERE account_id IN (
      Select account_id From acc_duplicates 
      where rn > 1
);
-- Transactions Table     
WITH trans_duplicates AS (
   Select txn_id,
       ROW_NUMBER() OVER(PARTITION BY txn_id ORDER BY txn_date) AS rn
   From transactions
)
Delete From transactions
 WHERE txn_id IN (
    Select txn_id from trans_duplicates 
    where rn > 1
);
-- Digital_Wallet_Usage Table
WITH dwu_duplicates AS (
   Select wallet_id,
       ROW_NUMBER() OVER(PARTITION BY wallet_id ORDER BY txn_date) AS rn
   From digital_wallet_usage
)
Delete From digital_wallet_usage
  WHERE wallet_id IN (
      Select wallet_id from dwu_duplicates
      Where rn > 1
);

-- STANDARDIZE DATA FORMATS
-- 1. Convert dates to DATE type
UPDATE customers
SET join_date = STR_TO_DATE(join_date, '%d-%m-%Y');
ALTER TABLE customers MODIFY join_date DATE;

UPDATE accounts
SET last_updated = STR_TO_DATE(last_updated, '%d-%m-%Y');
ALTER TABLE accounts MODIFY last_updated DATE;

UPDATE transactions
SET txn_date = STR_TO_DATE(txn_date, '%d-%m-%Y');
ALTER TABLE transactions MODIFY txn_date DATE;

UPDATE digital_wallet_usage
SET txn_date = STR_TO_DATE(txn_date, '%d-%m-%Y');
ALTER TABLE digital_wallet_usage MODIFY txn_date DATE;

-- HANDLE OUTLIERS
DELETE From accounts Where account_balance < 0;
DELETE From transactions Where amount <= 0;
DELETE From digital_wallet_usage Where amount_spent <= 0;

-- ASSIGNING PRIMARY AND FOREIGN KEY
-- Customers Table
ALTER TABLE customers
MODIFY customer_id VARCHAR(50) NOT NULL;
ALTER TABLE customers
ADD PRIMARY KEY (customer_id);
-- Accounts Table
ALTER TABLE accounts
MODIFY account_id VARCHAR(50) NOT NULL;
ALTER TABLE accounts
MODIFY customer_id VARCHAR(50) NOT NULL;
ALTER TABLE accounts
ADD PRIMARY KEY (account_id);
ALTER TABLE accounts
ADD CONSTRAINT fk_accounts_customers
FOREIGN KEY (customer_id) REFERENCES customers(customer_id);

-- Transactions Table
ALTER TABLE transactions
MODIFY txn_id VARCHAR(50) NOT NULL;
ALTER TABLE transactions
MODIFY account_id VARCHAR(50) NOT NULL;
ALTER TABLE transactions
ADD PRIMARY KEY (txn_id);
ALTER TABLE transactions
ADD CONSTRAINT fk_transaction_accounts
FOREIGN KEY (account_id) REFERENCES accounts(account_id);

-- Digital_Wallet_Usage Table
ALTER TABLE digital_wallet_usage
MODIFY wallet_id VARCHAR(50) NOT NULL;
ALTER TABLE digital_wallet_usage
MODIFY customer_id VARCHAR(50) NOT NULL;
ALTER TABLE digital_wallet_usage
ADD PRIMARY KEY (wallet_id);
ALTER TABLE digital_wallet_usage
ADD CONSTRAINT fk_wallet_customers
FOREIGN KEY (customer_id) REFERENCES customers(customer_id);

-- END OF DATA CLEANING 

-- Basic Queries
-- 1. CUSTOMERS FROM MUMBAI WITH SAVINGS ACCOUNT
SELECT customer_id, `name`, city, account_type
FROM customers
WHERE city = 'Mumbai' AND account_type = 'Savings';

-- 2. Total ACTIVE CUSTOMERS
SELECT COUNT(*) AS active_counts
FROM accounts
WHERE account_status = 'Active';

-- 3. TOP 5 HIGHEST ACCOUNT BALANCES
SELECT customer_id, account_balance
FROM accounts
ORDER BY account_balance desc
LIMIT 5;

-- Joins
-- 4. CUSTOMERS WITH THEIR LATEST ACCOUNT BALANCE
SELECT c.customer_id, c.`name`, a.account_balance
FROM customers c
LEFT JOIN accounts a ON c.customer_id = a.customer_id;

-- 5. TOTAL TRANSACTIONS PER CUSTOMER
SELECT c.customer_id, c.`name`, COUNT(t.txn_id) AS total_trans, ROUND(SUM(t.amount),2) AS total_amount
FROM customers c
LEFT JOIN accounts a ON c.customer_id = a.customer_id
LEFT JOIN transactions t ON a.account_id = t.account_id
GROUP BY c.customer_id,c.`name`
ORDER BY total_amount desc;

-- 6. WALLET USAGE WITH CUSTOMER NAMES
SELECT c.customer_id, c.`name`, w.wallet_provider, w.amount_spent, w.cashback
FROM digital_wallet_usage w
join customers c ON w.customer_id = c.customer_id;

-- Sub Queries
-- 7. CUSTOMERS WITH BALANCES GREATER THAN AVERAGE
SELECT c.customer_id, c.`name`, a.account_balance
FROM accounts a
JOIN customers c ON a.customer_id = c.customer_id
WHERE account_balance > (SELECT AVG(account_balance) FROM accounts);

-- 8.CUSTOMERS WHO MADE AT LEAST ONE TRANSACTION > 10000
SELECT customer_id, name
FROM customers
WHERE customer_id IN (
    SELECT a.customer_id
    FROM accounts a
    WHERE a.account_id IN (
        SELECT t.account_id
        FROM transactions t
        WHERE t.amount > 10000
    )
);

-- 9. MERCHANTS WITH PAYMENTS EXCEEDING 20000
SELECT merchant, total_payment
FROM (
    SELECT merchant, ROUND(SUM(amount),2) AS total_payment
    FROM transactions
		WHERE txn_type = 'Payment'
        GROUP BY merchant
) AS merchant_totals
WHERE  total_payment > 20000;

-- CTEs
-- 10 CALCULATE MONY TRANSACTION VOLUME AND AVERAGE TRANSACTION SIZE
WITH monthly_trans AS (
    SELECT date_format(txn_date,'%Y-%m') AS `month`, COUNT(*) AS txn_count, ROUND(AVG(amount),2) AS avg_amount
    FROM transactions
    GROUP BY `month`
)
SELECT * FROM monthly_trans;

-- 11. IDENTIFY CUSTOMERS WITH DECLINING BALANCES IN LAST 3 MONTHS
WITH last_3_months AS (
SELECT customer_id, account_balance, last_updated,
ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY last_updated DESC) AS rn
FROM accounts
WHERE last_updated >= DATE_SUB(CURDATE(), INTERVAL 3 MONTH)
)
SELECT customer_id, account_balance
FROM last_3_months
WHERE rn = 1;

-- 12. GENERATE CUMULATIVE DEPOSITS PER CUSTOMER
WITH deposit_cte AS (
    SELECT a.customer_id, t.txn_id, t.txn_date, t.amount
    FROM transactions t
    JOIN accounts a ON t.account_id = a.account_id
    WHERE t.txn_type = 'Deposit'
)
SELECT customer_id, txn_id, txn_date, amount,
    SUM(amount) OVER (PARTITION BY customer_id ORDER BY txn_date, txn_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM deposit_cte
ORDER BY customer_id, txn_date, txn_id;

-- Windows Functions
-- 13. RANK CUSTOMERS BY TOTAL TRANSACTION AMOUNT
SELECT c.customer_id, c.name, ROUND(SUM(t.amount),2) AS total_amount,
RANK() OVER (ORDER BY SUM(t.amount) DESC) AS `rank`
FROM customers c
JOIN accounts a ON c.customer_id = a.customer_id
JOIN transactions t ON a.account_id = t.account_id
GROUP BY c.customer_id, c.name
ORDER BY `rank`;

-- 14 CALCULATE RUNNING BALANCE PER AMOUNT
SELECT a.account_id, t.txn_date, t.amount,
SUM(t.amount) OVER (PARTITION BY a.account_id ORDER BY t.txn_date) AS running_balance
FROM transactions t
JOIN accounts a ON t.account_id = a.account_id
ORDER BY a.account_id, t.txn_date;

-- 15. RETRIVE TOP TRANSACTIONS PER CUSTOMER
SELECT customer_id, txn_id, amount, txn_date
FROM (
SELECT c.customer_id, t.txn_id, t.amount, t.txn_date,
ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY t.amount DESC) AS rn
FROM customers c
JOIN accounts a ON c.customer_id = a.customer_id
JOIN transactions t ON a.account_id = t.account_id
) AS sub
WHERE rn = 1
ORDER BY amount DESC;

-- 16. 3 MONTH MOVING AVERAGE OF WALLET SPENDS
SELECT customer_id, txn_date,
AVG(amount_spent) OVER (PARTITION BY customer_id ORDER BY txn_date 
ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg_3months
FROM digital_wallet_usage
ORDER BY customer_id, txn_date;

-- End of Script

























      
   
       


