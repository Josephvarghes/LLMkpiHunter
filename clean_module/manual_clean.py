import pandas as pd

def clean_csv(input_file):
    """
    Cleans the CSV file by removing rows where:
    - The 'Brand' column is empty or NaN.
    - The 'Value' column contains null, empty, or text values.
    - If 'Country' is empty, replace it with 'India'.
    Saves the cleaned DataFrame back to the same file.

    Args:
        input_file (str): The path to the CSV file to clean.
    """
    try:
        # Load the CSV file into a DataFrame
        df = pd.read_csv(input_file)

        # Replace empty 'Country' values with 'India'
        df['Country'] = df['Country'].fillna('India')

        # Remove rows where the 'Brand' column is empty or NaN
        df_cleaned = df[df['Brand'].notna() & (df['Brand'] != '')]

        # Remove rows where the 'Value' column is null, empty, or contains text
        df_cleaned = df_cleaned[df_cleaned['Value'].notna() & (df_cleaned['Value'] != '')]
        df_cleaned = df_cleaned[pd.to_numeric(df_cleaned['Value'], errors='coerce').notna()]

        # Save the cleaned DataFrame back to the same CSV file
        df_cleaned.to_csv(input_file, index=False)
        print(f"File '{input_file}' cleaned successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage
# clean_csv(r"C:\Users\user\OneDrive\Desktop\Crawl4AI\cleaned_category\Customer_Retention.csv")
