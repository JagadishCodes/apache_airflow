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
    def generate_report(triggering_dataset_events=None):

        print("===================================")
        print("TRIGGERING DATASET EVENT INFORMATION")
        print("===================================")

        # Local output file
        output_path = "/opt/airflow/output/asset_event_report.txt"

        with open(output_path, "w") as f:

            f.write(
                "TRIGGERING DATASET EVENT INFORMATION\n"
            )
            f.write(
                "===================================\n\n"
            )

            # Check whether event information exists
            if not triggering_dataset_events:

                print("No triggering dataset events found.")

                f.write(
                    "No triggering dataset events found.\n"
                )

            else:

                # Loop through datasets
                for dataset, dataset_events in (
                    triggering_dataset_events.items()
                ):

                    print(
                        "Dataset URI:",
                        dataset.uri
                    )

                    f.write(
                        f"Dataset URI: {dataset.uri}\n"
                    )

                    # Loop through events
                    for event in dataset_events:

                        print(
                            "Event ID:",
                            event.id
                        )

                        print(
                            "Event Timestamp:",
                            event.timestamp
                        )

                        f.write(
                            f"Event ID: {event.id}\n"
                        )

                        f.write(
                            f"Event Timestamp: "
                            f"{event.timestamp}\n"
                        )

                        # Source DAG information
                        if event.source_dag_run:

                            print(
                                "Source DAG:",
                                event.source_dag_run.dag_id
                            )

                            print(
                                "Source Run:",
                                event.source_dag_run.run_id
                            )

                            f.write(
                                f"Source DAG: "
                                f"{event.source_dag_run.dag_id}\n"
                            )

                            f.write(
                                f"Source Run: "
                                f"{event.source_dag_run.run_id}\n"
                            )

                        f.write("\n")

            # Report information
            f.write(
                "Sales data is ready!\n"
            )

            f.write(
                "Customer data is ready!\n"
            )

            f.write(
                "Generating sales report...\n"
            )

        print("Sales data is ready!")
        print("Customer data is ready!")
        print("Generating sales report...")

        print(
            "Output file created:",
            output_path
        )


    generate_report()