import pandas as pd
import zipfile
import os
import argparse

def merge_csv_from_zips(folder_path, file_name_to_extract, output_path):
    """
    Cycles through zip files in a folder, extracts a specified CSV file from each,
    and merges them together based on ID and date instead of appending.

    Args:
        folder_path (str): Path to the folder containing the zip files.
        file_name_to_extract (str): Name of the CSV file to extract and merge.
        output_path (str): Path where the combined CSV will be saved.

    Returns:
        pd.DataFrame: Merged DataFrame with data from all zip files.
    """
    all_dfs = []
    
    print(f"Scanning folder: {folder_path}")
    print(f"Looking for file: {file_name_to_extract} in zip files")
    
    # List all zip files in the folder
    zip_files = [f for f in os.listdir(folder_path) if f.endswith('.zip')]
    print(f"Found {len(zip_files)} zip files")
    
    for zip_file in zip_files:
        zip_path = os.path.join(folder_path, zip_file)
        try:
            with zipfile.ZipFile(zip_path, 'r') as z:
                if file_name_to_extract in z.namelist():
                    print(f"Extracting from {zip_file}")
                    with z.open(file_name_to_extract) as f:
                        df = pd.read_csv(f)
                        # Not adding ZipFileName column anymore
                        all_dfs.append(df)
                else:
                    print(f"File {file_name_to_extract} not found in {zip_file}")
        except Exception as e:
            print(f"Error processing {zip_file}: {e}")
    
    if not all_dfs:
        print("No data was found")
        return pd.DataFrame()
    
    # Handle merging instead of appending
    # Identify key columns for merging (assuming 'Date' and 'Content' are the keys)
    # You may need to adjust these based on your actual ID columns
    merged_df = None
    
    for df in all_dfs:
        if merged_df is None:
            merged_df = df
            continue
        
        # Extract key columns that identify unique rows
        # Adjust these as needed for your data structure
        key_columns = ['Date', 'Content', 'Video title', 'Video publish time', 'Duration']
        
        # Use an outer merge to ensure we keep all rows
        # and update with new metrics when available
        merged_df = pd.merge(
            merged_df, 
            df, 
            on=key_columns,
            how='outer', 
            suffixes=('', '_new')
        )
        
        # For columns that exist in both datasets (except key columns),
        # fill NaN values in the original with values from the new dataset
        for col in df.columns:
            if col not in key_columns:
                new_col = f"{col}_new" if f"{col}_new" in merged_df.columns else None
                if new_col:
                    # Fill NaN values in the original column with values from the new column
                    merged_df[col] = merged_df[col].fillna(merged_df[new_col])
                    # Drop the temporary column
                    merged_df.drop(new_col, axis=1, inplace=True)
    
    # Drop any duplicate rows that might have been created
    merged_df = merged_df.drop_duplicates()
    
    # Save the output to a CSV
    if not merged_df.empty:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        merged_df.to_csv(output_path, index=False)
        print(f"Successfully saved merged data to: {output_path}")
        print(f"Combined {len(merged_df)} rows from {len(all_dfs)} CSV files")
    else:
        print("No data was found or merged")
    
    return merged_df

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Merge Chart Data from multiple zip files')
    parser.add_argument('--input_directory', type=str, required=True, help='Directory containing the zip files')
    parser.add_argument('--csv_filename', type=str, default='Chart data.csv', help='Name of the CSV file to extract (default: "Chart data.csv")')
    parser.add_argument('--output_path', type=str, required=True, help='Path where the combined CSV will be saved')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Call the function with provided arguments
    merge_csv_from_zips(
        args.input_directory,
        args.csv_filename,
        args.output_path
    )

if __name__ == "__main__":
    main()