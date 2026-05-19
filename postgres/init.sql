

CREATE TABLE IF NOT EXISTS orders (
    id            SERIAL PRIMARY KEY,
    customer_name VARCHAR(255)  NOT NULL,
    product_name  VARCHAR(255)  NOT NULL,
    status        VARCHAR(20)   NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'shipped', 'delivered')),
    updated_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);


CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER orders_set_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at();

CREATE PUBLICATION orders_publication FOR TABLE orders
    WITH (publish = 'insert,update,delete');


INSERT INTO orders (customer_name, product_name, status) VALUES
    ('Alice Johnson',  'Wireless Headphones', 'pending'),
    ('Bob Martinez',   'Mechanical Keyboard',  'shipped'),
    ('Carol Williams', 'USB-C Hub',            'delivered');
