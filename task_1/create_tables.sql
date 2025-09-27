-- Categories
CREATE TABLE categories (
    category_id INTEGER PRIMARY KEY,
    category_name VARCHAR,
    parent_category_id INTEGER
);

-- Products
CREATE TABLE products (
    product_id INTEGER PRIMARY KEY,
    name VARCHAR,
    price DECIMAL,
    description VARCHAR,
    availability BOOLEAN,
    category_id INTEGER REFERENCES categories(category_id)
);

-- Customers
CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    address VARCHAR,
    region VARCHAR,
    registration_date DATE
);

-- Orders
CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    order_date DATE,
    order_status VARCHAR
);

-- Order Items
CREATE TABLE order_items (
    order_item_id INTEGER PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    product_id INTEGER REFERENCES products(product_id),
    quantity INTEGER,
    unit_price DECIMAL
);

-- Transactions
CREATE TABLE transactions (
    transaction_id INTEGER PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    transaction_date DATE,
    payment_method VARCHAR,
    amount DECIMAL
);


-- -- 1.4 
-- Sposob: Slowly Changing Dimensions (SCD)
-- For price changes:
-- sqlCREATE TABLE product_price_history (
--     price_history_id INT PRIMARY KEY,
--     product_id INT,
--     price DECIMAL(10,2),
--     effective_from TIMESTAMP,
--     effective_to TIMESTAMP,
--     is_current BOOLEAN,
--     FOREIGN KEY (product_id) REFERENCES products(product_id)
-- );
