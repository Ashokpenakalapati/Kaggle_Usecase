CREATE TABLE IF NOT EXISTS DimCustomer (
    Customer_ID INTEGER PRIMARY KEY,
    Customer_Type TEXT,
    Gender TEXT,
    City TEXT
);

CREATE TABLE IF NOT EXISTS DimProduct (
    Product_ID INTEGER PRIMARY KEY,
    Product_Line TEXT,
    Unit_Price REAL
);

CREATE TABLE IF NOT EXISTS FactSales (
    Invoice_ID TEXT PRIMARY KEY,
    Customer_ID INTEGER,
    Product_ID INTEGER,
    Branch TEXT,
    Quantity INTEGER,
    Tax_5_percent REAL,
    Total REAL,
    Date TEXT,
    Time TEXT,
    Payment TEXT,
    COGS REAL,
    Gross_Margin_Percentage REAL,
    Gross_Income REAL,
    Rating REAL,
    FOREIGN KEY (Customer_ID) REFERENCES DimCustomer(Customer_ID),
    FOREIGN KEY (Product_ID) REFERENCES DimProduct(Product_ID)
);
