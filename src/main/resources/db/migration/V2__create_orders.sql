CREATE TABLE orders
(
    id            BIGINT         NOT NULL AUTO_INCREMENT,
    customer_name VARCHAR(100)   NOT NULL,
    product       VARCHAR(100)   NOT NULL,
    quantity      INT            NOT NULL,
    price         DECIMAL(19, 2) NOT NULL,
    status        VARCHAR(20)    NOT NULL,
    created_at    DATETIME(6)    NOT NULL,
    updated_at    DATETIME(6)    NOT NULL,
    version       BIGINT         NOT NULL DEFAULT 0,
    CONSTRAINT pk_orders PRIMARY KEY (id)
);
