WITH sales_aggregated AS (
            SELECT
                p.Product_Line,
                c.City,
                SUM(f.Total) AS Total_Sales,
                AVG(f.Rating) AS Avg_Rating,
                RANK() OVER (PARTITION BY c.City ORDER BY SUM(f.Total) DESC) AS Sales_Rank
            FROM
                FactSales f
            JOIN
                DimCustomer c ON f.Customer_Id = c.Customer_Id
            JOIN
                DimProduct p ON f.Product_ID = p.Product_Id
            GROUP BY
                p.Product_Line, c.City
        )
        SELECT
            Product_Line,
            City,
            Total_Sales,
            Avg_Rating,
            Sales_Rank
        FROM
            sales_aggregated
        ORDER BY
            city, Sales_Rank;