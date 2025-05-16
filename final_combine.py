import pandas as pd
import glob
import os

def combine_and_deduplicate_csv(folder_path, output_file):
    """
    Combines all CSV files in a folder and removes duplicate rows
    based on Country, Year, Brand, Metric, and Value.

    Args:
        folder_path (str): Path to the folder containing CSV files.
        output_file (str): Path to save the combined cleaned CSV.
    """
    try:
        # Get all CSV files in the folder
        all_files = glob.glob(os.path.join(folder_path, "*.csv"))

        # List to store dataframes
        df_list = []


        for file in all_files:
            df = pd.read_csv(file)
            df_list.append(df)

        # Combine all dataframes
        combined_df = pd.concat(df_list, ignore_index=True)

        # Drop duplicate rows based on the specified columns
        deduplicated_df = combined_df.drop_duplicates(subset=['Country', 'Year', 'Brand', 'Metric', 'Value'])

        # Save to output file
        deduplicated_df.to_csv(output_file, index=False)
        print(f"Combined and cleaned file saved to: {output_file}")

    except Exception as e:
        print(f"Error occurred: {e}")

