import os
import pandas as pd
import zipfile
import re
import datetime
from pathlib import Path
import argparse

def process_directory(directory, output_dir=None, output_filename=None):
    """
    Processes all zip files in the specified directory.
    This is the core processing function extracted from the original code.
    
    Args:
    directory (str): The directory containing the zip files.
    output_dir (str): Directory to save the output CSV file
    output_filename (str): Name of the output CSV file
    
    Returns:
    merged_df (pandas.DataFrame): The enhanced merged dataframe.
    """
    # Create an empty dataframe to store the merged data
    merged_df = pd.DataFrame()
    
    # Set default output directory and filename if not provided
    if not output_dir:
        output_dir = str(Path.home() / "Downloads")
    if not output_filename:
        output_filename = f"retention_analysis_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    print(f"Processing files in directory: {directory}")
    
    # Loop through the zip files in the directory
    for filename in os.listdir(directory):
        if filename.endswith(".zip"):
            zip_path = os.path.join(directory, filename)
            print(f"Processing zip file: {filename}")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                try:
                    # Extract relevant CSV files from the zip file
                    zip_ref.extract("Organic.csv", path=directory)
                    zip_ref.extract("Detailed activity.csv", path=directory)
                    zip_ref.extract("Subscribers and non-subscribers.csv", path=directory)
                    zip_ref.extract("New and returning viewers.csv", path=directory)

                    # Read the extracted CSV files into dataframes
                    organic_df = pd.read_csv(os.path.join(directory, "Organic.csv"))
                    detailed_df = pd.read_csv(os.path.join(directory, "Detailed activity.csv"))
                    sub_nonsub_df = pd.read_csv(os.path.join(directory, "Subscribers and non-subscribers.csv"))
                    new_return_df = pd.read_csv(os.path.join(directory, "New and returning viewers.csv"))
                except KeyError as e:
                    print(f"Error extracting CSV files from {filename}: {e}")
                    continue
                except pd.errors.EmptyDataError as e:
                    print(f"Error reading CSV files from {filename}: {e}")
                    continue

            try:
                # Pivot the subscribers data to have separate columns for Subscribed and Not subscribed audience retention
                sub_nonsub_pivot = sub_nonsub_df.pivot(index='Video position (%)', columns='Subscription status', values='Absolute audience retention (%)').reset_index()
                # Ensure the pivot table has the expected columns, and fill missing ones with NaN
                if 'Subscribed' not in sub_nonsub_pivot.columns:
                    sub_nonsub_pivot['Subscribed'] = pd.NA
                if 'Not subscribed' not in sub_nonsub_pivot.columns:
                    sub_nonsub_pivot['Not subscribed'] = pd.NA
                sub_nonsub_pivot.columns = ['Video position (%)', 'Not subscribed Retention', 'Subscribed Retention']
            except ValueError as e:
                sub_nonsub_pivot = None
                print(f"Error pivoting subscribers data for {filename}: {e}")

            # Replace the existing "New and returning viewers" handling code block with this improved version

            try:
                # First, check if we have the expected columns
                expected_columns = ['Video position (%)', 'New and Returning Viewers', 'Absolute audience retention (%)']
                columns_exist = all(col in new_return_df.columns for col in expected_columns)
                
                if columns_exist:
                    # Clean column names to ensure consistent formatting
                    new_return_df.columns = [col.strip() for col in new_return_df.columns]
                    
                    # Create a simple pivot using pandas pivot_table instead
                    new_return_pivot = pd.pivot_table(
                        new_return_df,
                        index='Video position (%)',
                        columns='New and Returning Viewers',
                        values='Absolute audience retention (%)',
                        aggfunc='first'  # In case of duplicates, take the first value
                    ).reset_index()
                    
                    # Ensure we have the expected column names after pivoting
                    # Get the actual column names after pivot (excluding the index column)
                    pivot_columns = new_return_pivot.columns.tolist()[1:]
                    
                    # Map whatever column names we got to our expected output columns
                    column_mapping = {}
                    for col in pivot_columns:
                        if 'new' in col.lower():
                            column_mapping[col] = 'New Viewer Retention'
                        elif 'return' in col.lower():
                            column_mapping[col] = 'Return Viewer Retention'
                    
                    # Rename columns
                    new_return_pivot.rename(columns=column_mapping, inplace=True)
                    
                    # Make sure we have both expected columns, add them with NaN if missing
                    if 'New Viewer Retention' not in new_return_pivot.columns:
                        new_return_pivot['New Viewer Retention'] = pd.NA
                    if 'Return Viewer Retention' not in new_return_pivot.columns:
                        new_return_pivot['Return Viewer Retention'] = pd.NA
                    
                    # Keep only the columns we need
                    new_return_pivot = new_return_pivot[['Video position (%)', 'New Viewer Retention', 'Return Viewer Retention']]
                    
                else:
                    # Fallback to direct extraction approach
                    # Create empty dictionaries for new and returning viewers data
                    new_viewers_data = {}
                    returning_viewers_data = {}
                    
                    # Iterate through the dataframe to extract values
                    for _, row in new_return_df.iterrows():
                        position = row['Video position (%)']
                        viewer_type = row['New and Returning Viewers']
                        retention = row['Absolute audience retention (%)']
                        
                        if 'New' in viewer_type:
                            new_viewers_data[position] = retention
                        elif 'Returning' in viewer_type:
                            returning_viewers_data[position] = retention
                    
                    # Create a new dataframe from the dictionaries
                    positions = sorted(set(list(new_viewers_data.keys()) + list(returning_viewers_data.keys())))
                    new_return_pivot_data = []
                    
                    for pos in positions:
                        new_return_pivot_data.append({
                            'Video position (%)': pos,
                            'New Viewer Retention': new_viewers_data.get(pos, pd.NA),
                            'Return Viewer Retention': returning_viewers_data.get(pos, pd.NA)
                        })
                    
                    new_return_pivot = pd.DataFrame(new_return_pivot_data)
                            
            except Exception as e:
                new_return_pivot = None
                print(f"Error processing new and returning viewers data for {filename}: {str(e)}")
                # Add detailed debugging info but without traceback for cleaner output
                print(f"  - DataFrame shape: {new_return_df.shape}")
                print(f"  - DataFrame columns: {new_return_df.columns.tolist()}")
                print(f"  - First few rows: {new_return_df.head(2).to_dict('records')}")
           

            try:
                # Merge the two dataframes based on a common column
                merged_temp_df = pd.merge(organic_df, detailed_df, on="Video position (%)")
                if sub_nonsub_pivot is not None:
                    merged_temp_df = pd.merge(merged_temp_df, sub_nonsub_pivot, on="Video position (%)", how='left')
                if new_return_pivot is not None:
                    merged_temp_df = pd.merge(merged_temp_df, new_return_pivot, on="Video position (%)", how='left')
            except pd.errors.MergeError as e:
                print(f"Error merging dataframes for {filename}: {e}")
                continue

            # Add a new column 'zipfilename' and fill it with the name of the zip file
            merged_temp_df['zipfilename'] = filename

            # Append the merged_temp_df to the main dataframe
            merged_df = pd.concat([merged_df, merged_temp_df], ignore_index=True)

            # Clean up extracted CSV files
            for csv_file in ["Organic.csv", "Detailed activity.csv", "Subscribers and non-subscribers.csv", "New and returning viewers.csv"]:
                try:
                    os.remove(os.path.join(directory, csv_file))
                except FileNotFoundError:
                    pass

    # Calculate 'People Remaining' and 'Stopped/Remaining %'
    try:
        if not merged_df.empty:
            merged_df.sort_values(by=['zipfilename', 'Video position (%)'], inplace=True)
            merged_df['People Remaining'] = merged_df.groupby('zipfilename')['Stopped watching'].transform(lambda x: x[::-1].cumsum()[::-1])
            merged_df['Stopped/Remaining %'] = (merged_df['Stopped watching'] / merged_df['People Remaining'])
    except KeyError as e:
        print(f"Error calculating additional metrics: {e}")

    # Parse 'zipfilename' to extract "Video Title", "Start Date", and "End Date"
    try:
        if not merged_df.empty:
            pattern = r"Audience retention (\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2}) (.+?)\.zip"
            merged_df['Start Date'], merged_df['End Date'], merged_df['Video Title'] = zip(*merged_df['zipfilename'].apply(lambda x: re.match(pattern, x).groups() if re.match(pattern, x) else ('Unknown', 'Unknown', 'Unknown')))
    except AttributeError as e:
        print(f"Error parsing 'zipfilename': {e}")

    # Reorder columns to the desired order
    reorder_columns = [
        'Video position (%)', 'Absolute audience retention (%)', 'Compared to other videos (%)',
        'Started watching', 'Stopped watching', 'Number of times each moment was seen',
        'Not subscribed Retention', 'Subscribed Retention', 'New Viewer Retention',
        'Return Viewer Retention', 'zipfilename', 'People Remaining', 'Stopped/Remaining %',
        'Start Date', 'End Date', 'Video Title'
    ]

    # Reorder the DataFrame columns, ignoring missing columns
    if not merged_df.empty:
        merged_df = merged_df.reindex(columns=[col for col in reorder_columns if col in merged_df.columns], fill_value=pd.NA)

    # Save to CSV if we have data
    if not merged_df.empty:
        output_path = os.path.join(output_dir, output_filename)
        try:
            # Ensure output directory exists
            os.makedirs(output_dir, exist_ok=True)
            merged_df.to_csv(output_path, index=False)
            print(f"Successfully saved data to: {output_path}")
        except Exception as e:
            print(f"Error saving CSV file: {str(e)}")
    else:
        print("No data was processed or an error occurred.")

    # Return the enhanced merged dataframe
    return merged_df

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Merge audience retention data from YouTube Studio zip files')
    parser.add_argument('--input_directory', type=str, help='Directory containing the zip files')
    parser.add_argument('--output_directory', type=str, help='Directory to save the output CSV file')
    parser.add_argument('--output_filename', type=str, help='Name of the output CSV file')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Process the directory with the provided arguments
    return process_directory(
        args.input_directory, 
        args.output_directory, 
        args.output_filename
    )

if __name__ == "__main__":
    main()