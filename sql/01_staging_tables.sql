DROP TABLE IF EXISTS staging.online_retail CASCADE;

CREATE TABLE staging.online_retail (
  InvoiceNo      TEXT,
  StockCode      TEXT,
  Description    TEXT,
  Quantity       INT,
  InvoiceDate    TIMESTAMP,
  UnitPrice      NUMERIC(12,4),
  CustomerID     TEXT,
  Country        TEXT
);
