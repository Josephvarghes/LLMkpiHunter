# json_link_extractor.py

import json

def extract_links_range(
    input_json_path: str,
    output_json_path: str = "India_links_150_250.json",
    start: int = 0,
    end: int = 100
) -> None:
    """
    Extracts links from the 'Germany' key in a JSON file, from index `start` to `end`,
    and saves them into a new JSON file.

    Parameters:
        input_json_path (str): Path to the original JSON file.
        output_json_path (str): Path to save the sliced JSON file.
        start (int): Starting index (inclusive).
        end (int): Ending index (exclusive).
    """
    try:
        with open(input_json_path, "r") as infile:
            data = json.load(infile)

        if "Germany" not in data or not isinstance(data["Germany"], list):
            raise ValueError("The 'Germany' key is missing or is not a list.")

        sliced_links = data["Germany"][start:end]
        output_data = {"Germany": sliced_links}

        with open(output_json_path, "w") as outfile:
            json.dump(output_data, outfile, indent=4)

        print(f"✅ Links from index {start} to {end} saved to '{output_json_path}'")

    except Exception as e:
        print(f"❌ Error: {e}")

extract_links_range("i4.json")