import os
import sqlite3
import logging
import pandas as pd
import subprocess
from datetime import datetime


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger=logging.getLogger(__name__)

MOUNTED_VOLUME_PATH = "/app/input-file"


def extract_data_from_kaggle(dataset_name):
    """Download data from Kaggle and move it to the mounted volume."""
    try:
        kaggle_config_dir = os.environ.get("KAGGLE_CONFIG_DIR", "/root/.kaggle")
        os.makedirs(kaggle_config_dir, exist_ok=True)

        kaggle_credentials = os.environ.get("kaggle_api_key")
        if not kaggle_credentials:
            logger.error("Kaggle credentials not found in environment variables.")
            raise EnvironmentError("Kaggle credentials not set.")

        kaggle_json_path = os.path.join(kaggle_config_dir, "kaggle.json")
        with open(kaggle_json_path, "w") as f:
            f.write(kaggle_credentials)

        subprocess.run(["chmod", "600", kaggle_json_path])

        import kaggle  # Import only after credentials are set

        current_date = datetime.now().strftime("%Y-%m-%d")
        output_path = f"{MOUNTED_VOLUME_PATH}/kaggle_data/{current_date}/"
        os.makedirs(output_path, exist_ok=True)

        logger.info(f"Downloading dataset: {dataset_name} to {output_path}")
        kaggle.api.dataset_download_files(dataset_name, path=output_path, unzip=True)

        logger.info(f"Dataset {dataset_name} successfully downloaded.")
        return output_path

    except Exception as e:
        logger.error(f"Data extraction failed: {str(e)}")
        return None


def read_sql_file(file_path):
    """Reads and returns the content of an SQL file."""
    with open(file_path, "r") as file:
        return file.read()


def transform(csv_file, db_path):
    """Transforms the CSV data and loads it into an SQLite database."""
    try:
        if not os.path.exists(csv_file):
            logger.error(f"CSV file not found: {csv_file}")
            raise FileNotFoundError(f"CSV file not found: {csv_file}")

        data = pd.read_csv(csv_file)
        data.columns = data.columns.str.replace(" ", "_").str.replace("%", "percent")

        # Create Dimensions
        dim_customer = data[["Customer_type", "Gender", "City"]].drop_duplicates().reset_index(drop=True)
        dim_customer["Customer_ID"] = dim_customer.index + 1

        dim_product = data[["Product_line", "Unit_price"]].drop_duplicates().reset_index(drop=True)
        dim_product["Product_ID"] = dim_product.index + 1

        # Create Fact Table
        fact_sales = (
            data.merge(dim_customer, on=["Customer_type", "Gender", "City"], how="left")
            .merge(dim_product, on=["Product_line", "Unit_price"], how="left")
            .rename(columns={"Tax_5percent": "Tax_5_percent"})
        )
        fact_sales = fact_sales[
            [
                "Invoice_ID",
                "Customer_ID",
                "Product_ID",
                "Branch",
                "Quantity",
                "Tax_5_percent",
                "Total",
                "Date",
                "Time",
                "Payment",
                "cogs",
                "gross_margin_percentage",
                "gross_income",
                "Rating",
            ]
        ]

        # Database Connection
        os.makedirs(db_path, exist_ok=True)
        db_file = os.path.join(db_path, "sales_data.db")
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Create Tables from External SQL Files
        cursor.executescript(read_sql_file("sql/Create_Tables.sql"))

        logger.info("Created the tables DimCustomer, DimProduct and Fact Sales")

        # Load Data
        dim_customer.to_sql("DimCustomer", conn, if_exists="replace", index=False)
        dim_product.to_sql("DimProduct", conn, if_exists="replace", index=False)
        fact_sales.to_sql("FactSales", conn, if_exists="replace", index=False)

        logger.info("Loaded data into Tables")

        conn.commit()
        conn.close()

        logger.info(f"Data successfully transformed and stored in {db_file}")

    except Exception as e:
        logger.error(f"Data transformation failed: {str(e)}")


def generate_report(report_path, db_path):
    """Generates an aggregated sales report."""
    try:
        conn = sqlite3.connect(db_path)
        query = read_sql_file("sql/Sales_Report.sql")
        df = pd.read_sql(query, conn)

        os.makedirs(report_path, exist_ok=True)
        report_file = os.path.join(report_path, "aggregated_sales_report.csv")
        df.to_csv(report_file, index=False)

        logger.info(f"Report generated successfully: {report_file}")
        conn.close()

    except Exception as e:
        logger.error(f"Report generation failed: {str(e)}")


def main():
    """Main ETL pipeline controller."""
    dataset_name = "aungpyaeap/supermarket-sales"
    task_name = os.getenv("task_name")
    current_date = datetime.now().strftime("%Y-%m-%d")

    if task_name == "Extract":
        logger.info("Starting data extraction from Kaggle API.")
        csv_path = extract_data_from_kaggle(dataset_name)
        logger.info(f"Data extraction complete. Path: {csv_path}")

    elif task_name == "Transform":
        logger.info("Transformation step is getting executed")
        csv_file = f"{MOUNTED_VOLUME_PATH}/kaggle_data/{current_date}/supermarket_sales - Sheet1.csv"
        db_path = f"{MOUNTED_VOLUME_PATH}/SQLite_DB/"
        transform(csv_file, db_path)
        logger.info(f"Data transformation completed. Database path: {db_path}/sales_data.db")

    elif task_name == "Generate_Report":
        logger.info("Generating the report")
        db_path = f"{MOUNTED_VOLUME_PATH}/SQLite_DB/sales_data.db"
        report_path = f"{MOUNTED_VOLUME_PATH}/report/{current_date}/"
        generate_report(report_path, db_path)
        logger.info(f"Report successfully generated at {report_path}")

    else:
        logger.error(f"Invalid task name provided: {task_name}")


if __name__ == "__main__":
    main()
