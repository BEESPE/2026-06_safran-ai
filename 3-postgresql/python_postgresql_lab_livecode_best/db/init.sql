CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    price NUMERIC(10,2),
    stock INTEGER NOT NULL CHECK(stock>=0)
);