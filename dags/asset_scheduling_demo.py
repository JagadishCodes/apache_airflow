import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset


# ============================================================
# 1. TIME ZONE
# ============================================================

india_tz = pendulum.timezone("Asia/Kolkata")


# ============================================================
# 2. MULTIPLE DATASETS
# ============================================================

sales_dataset = Dataset(
    "file:///data/sales.csv"
)

customer_dataset = Dataset(
    "file:///data/customer.csv"
)


# ============================================================
# 3. PRODUCER DAG
# ============================================================

with DAG(
    dag_id="data_update_dag",

    # Daily at 9:00 AM IST
    schedule="0 9 * * *",

    start_date=pendulum.datetime(
        2026,
        9,
        1,
        tz=india_tz
    ),

    catchup=False,
) as producer_dag:

    @task(
        outlets=[
            sales_dataset,
            customer_dataset
        ]
    )
    def update_data_files():

        print("Updating sales.csv...")
        print("Updating customer.csv...")

        print("Both files updated successfully!")

    update_data_files()


# ============================================================
# 4. CONSUMER DAG
# ============================================================

with DAG(
    dag_id="sales_report_dag",

    # Run when BOTH datasets are updated
    schedule=[
        sales_dataset,
        customer_dataset
    ],

    start_date=pendulum.datetime(
        2026,
        9,
        1,
        tz=india_tz
    ),

    catchup=False,
) as consumer_dag:

    @task
    def generate_report():

        print("Sales data is ready!")
        print("Customer data is ready!")

        print("Generating sales report...")

    generate_report()