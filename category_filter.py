import os
import pandas as pd

def export_category_data(input_csv: str, category_name: str, output_folder: str = "filtered_exports"):
    """
    Extracts rows matching a specific category from a CSV and saves them
    in a new CSV file inside the specified folder.

    Parameters:
    - input_csv (str): Path to the input CSV file
    - category_name (str): The exact category value to filter
    - output_folder (str): Folder to save the filtered output
    """

    # Ensure the output folder exists
    os.makedirs(output_folder, exist_ok=True)

    try:
        # Load the CSV data
        df = pd.read_csv(input_csv)

        # Filter by category
        filtered_df = df[df["Category"] == category_name]

        # Define output path
        filename_safe = category_name.replace(" ", "_") + ".csv"
        output_path = os.path.join(output_folder, filename_safe)

        # Save filtered data
        filtered_df.to_csv(output_path, index=False)
        print(f"[SUCCESS] Exported {len(filtered_df)} rows to '{output_path}'")

    except Exception as e:
        print(f"[ERROR] Failed to export category data: {e}")
if __name__ == "__main__":
    # Example usage
    input_csv_file = "insights_output.csv"

    # List of categories to process
    categories = [
        "Total Sales Performance",
        "Channel-wise Performance",
        "Promotions Impact",
        "Customer Retention",
        "Market Share & ASP",
        "Innovation & Features",
        "Demand & Inventory",
        "Cost Optimization",
        "Dealer Stock",
        "Brand-wise Sales"
    ]

    # Loop through all categories and export their data
    for category in categories:
        export_category_data(input_csv_file, category)