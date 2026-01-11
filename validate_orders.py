import os
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(BASE_DIR, "order_data.csv")

df = pd.read_csv(csv_path, encoding="latin1", on_bad_lines="skip")

errors = []
clean_rows = []

for index, row in df.iterrows():
    row_errors = []

    if pd.isna(row["order_id"]) or pd.isna(row["customer_id"]):
        row_errors.append("Missing ID")

    if pd.to_datetime(row["delivery_date"]) < pd.to_datetime(row["order_date"]):
        row_errors.append("Delivery date before order date")

    if row["quantity"] <= 0:
        row_errors.append("Invalid quantity")

    if row_errors:
        errors.append({
            "row_number": index + 1,
            "order_id": row["order_id"],
            "error_details": ", ".join(row_errors)
        })
    else:
        clean_rows.append(row)

pd.DataFrame(clean_rows).to_csv("clean_data.csv", index=False)
pd.DataFrame(errors).to_csv("error_report.csv", index=False)

print("Validation completed.")
print("Clean data saved to clean_data.csv")
print("Error report saved to error_report.csv")