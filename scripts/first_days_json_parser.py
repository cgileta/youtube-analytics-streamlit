import json
import csv
import argparse
import os
import pandas as pd
from datetime import datetime
import glob

def extract_value_from_path(data, path):
    """
    Extract a value from a nested dictionary based on a dot-notation path.
    
    Args:
        data (dict): The nested dictionary to extract from
        path (str): Path to the value using dot notation (e.g., 'results[0].value.getCards')
    
    Returns:
        The value at the specified path or None if the path doesn't exist
    """
    try:
        # Split the path into components
        parts = path.split('.')
        current = data
        
        for part in parts:
            # Handle array indexing
            if '[' in part and ']' in part:
                array_name, index_part = part.split('[', 1)
                index = int(index_part.split(']')[0])
                current = current[array_name][index]
            else:
                current = current[part]
        
        return current
    except (KeyError, IndexError, TypeError):
        return None

def convert_timestamp_to_datetime(timestamp):
    """
    Convert a Unix timestamp to a readable datetime format (for Excel)
    
    Args:
        timestamp (str or int): Unix timestamp in seconds
        
    Returns:
        str: Formatted datetime string (YYYY-MM-DD HH:MM:SS)
    """
    try:
        timestamp = int(timestamp)
        dt = datetime.fromtimestamp(timestamp)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError):
        return None

def process_json_file(json_path):
    """
    Process a single JSON file and return a DataFrame of extracted metrics.
    
    Args:
        json_path (str): Path to the input JSON file
        
    Returns:
        tuple: (success, message, DataFrame)
    """
    try:
        # Read the JSON file
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Get the filename for logging
        filename = os.path.basename(json_path)
        
        print(f"Processing {filename}...")
        
        # Define the base paths
        base_path = "results[0].value.getCards.cards[0].scatterplotData.resultTable"
        videos_path = f"{base_path}.dimensionColumns[0].strings.values"
        
        # Get the list of videos
        video_ids = extract_value_from_path(data, videos_path)
        
        if not video_ids:
            print(f"Error: Could not extract videos from path {videos_path} in {filename}")
            return False, f"Could not extract videos from {filename}", pd.DataFrame()
        
        # Get video metadata (titles and publish dates)
        video_metadata = {}
        video_entities_path = "results[0].value.getCards.cards[0].sideEntities.videos"
        video_entities = extract_value_from_path(data, video_entities_path)
        
        if video_entities:
            for entity in video_entities:
                try:
                    video_id = entity['entityData']['videoId']
                    title = entity['entityData']['title']
                    published_seconds = entity['entityData']['timePublishedSeconds']
                    published_date = convert_timestamp_to_datetime(published_seconds)
                    
                    video_metadata[video_id] = {
                        'title': title,
                        'published_date': published_date,
                        'published_seconds': published_seconds
                    }
                except (KeyError, TypeError) as e:
                    print(f"Warning: Could not extract metadata for a video in {filename}: {e}")
        else:
            print(f"Warning: Could not find video metadata in {filename}")
        
        # Define all the metric paths and their names
        metric_paths = [
            (f"{base_path}.metricColumns[0].counts.values", "VIEWS"),
            (f"{base_path}.metricColumns[1].counts.values", "VIDEO_THUMBNAIL_IMPRESSIONS"),
            (f"{base_path}.metricColumns[2].percentages.values", "VIDEO_THUMBNAIL_IMPRESSIONS_VTR"),
            (f"{base_path}.metricColumns[3].percentages.values", "AVERAGE_WATCH_PERCENTAGE"),
            (f"{base_path}.metricColumns[4].milliseconds.values", "AVERAGE_WATCH_TIME"),
            (f"{base_path}.metricColumns[5].milliseconds.values", "WATCH_TIME"),
            (f"{base_path}.metricColumns[6].counts.values", "RATINGS_LIKES"),
            (f"{base_path}.metricColumns[7].counts.values", "RATINGS_DISLIKES"),
            (f"{base_path}.metricColumns[8].counts.values", "NEW_VIEWERS"),
            (f"{base_path}.metricColumns[9].counts.values", "RETURNING_NEW_VIEWERS")
        ]
        
        # Extract metric names from the JSON file itself for verification
        metric_names = []
        for i in range(10):
            metric_type_path = f"{base_path}.metricColumns[{i}].metric.type"
            metric_type = extract_value_from_path(data, metric_type_path)
            if metric_type:
                metric_names.append(metric_type)
            else:
                # If we can't find the metric name for this index, try to get the next one
                continue
        
        # Get all metrics values
        metric_values = []
        for path, _ in metric_paths:
            values = extract_value_from_path(data, path)
            if values:
                metric_values.append(values)
            else:
                print(f"Warning: Could not extract values from path {path} in {filename}")
                metric_values.append([None] * len(video_ids))
        
        # Check if we have metrics data
        if not metric_values or len(metric_values) == 0:
            print(f"No metric values found in {filename}")
            return False, f"No metric values found in {filename}", pd.DataFrame()
        
        # Extract time period information from the configuration
        config_path = "results[0].value.getCards.cards[0].config.scatterplotDataConfig.timePeriod"
        time_period_config = extract_value_from_path(data, config_path)
        
        time_period = "24h"  # Default value
        if time_period_config:
            count = time_period_config.get('count', 1)
            if count == 1:
                time_period = "24h"
            elif count == 7:
                time_period = "7d"
            elif count == 28:
                time_period = "28d"
            else:
                time_period = f"{count}d"
        
        # Prepare CSV data
        csv_data = []
        headers = ["VIDEO_ID", "TITLE", "PUBLISHED_DATE", "TIME_PERIOD", "JSON_SOURCE"]
        metric_headers = [name for _, name in metric_paths]
        
        # Use metric names from the JSON if available and not empty
        if len(metric_names) == len(metric_paths) and all(metric_names):
            metric_headers = metric_names
        
        # Complete headers list
        headers.extend(metric_headers)
        
        # For each video, create one row with its metrics
        for i, video_id in enumerate(video_ids):
            # Get video metadata
            title = video_metadata.get(video_id, {}).get('title', '')
            published_date = video_metadata.get(video_id, {}).get('published_date', '')
            
            row = [video_id, title, published_date, time_period, os.path.basename(json_path)]
            
            for metric_idx, metric in enumerate(metric_values):
                if i < len(metric):
                    row.append(metric[i])
                else:
                    row.append(None)
            
            csv_data.append(row)
        
        # Create DataFrame for easier manipulation
        df = pd.DataFrame(csv_data, columns=headers)
        
        # Remove rows with all None metrics
        metric_columns = df.columns[5:]  # All columns except VIDEO_ID, TITLE, PUBLISHED_DATE, TIME_PERIOD, and JSON_SOURCE
        df = df.dropna(subset=metric_columns, how='all')
        
        # Convert metric values to appropriate types
        for col in metric_columns:
            if "PERCENTAGE" in col or "VTR" in col:
                # Convert percentage values to float with 2 decimal places
                df[col] = pd.to_numeric(df[col], errors='coerce').round(2)
            elif col == "WATCH_TIME":
                # Convert milliseconds to hours for WATCH_TIME
                df[col] = pd.to_numeric(df[col], errors='coerce') / 3600000
                df[col] = df[col].round(2)
            elif col == "AVERAGE_WATCH_TIME":
                # Convert milliseconds to minutes for AVERAGE_WATCH_TIME
                df[col] = pd.to_numeric(df[col], errors='coerce') / 60000
                df[col] = df[col].round(2)
            elif "TIME" in col and "MILLI" in col:
                # Convert other millisecond times to seconds
                df[col] = pd.to_numeric(df[col], errors='coerce') / 1000
                df[col] = df[col].round(1)
            else:
                # Convert count values to integers
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        
        print(f"Successfully extracted data from {filename} - {len(df)} rows")
        return True, f"Successfully extracted data from {filename}", df
        
    except Exception as e:
        print(f"Error processing {json_path}: {str(e)}")
        return False, f"Error processing {json_path}: {str(e)}", pd.DataFrame()

def process_multiple_jsons(json_paths, output_csv):
    """
    Process multiple JSON files and output a combined CSV with extracted metrics.
    
    Args:
        json_paths (list): List of paths to the input JSON files
        output_csv (str): Path to the output CSV file
    """
    all_dfs = []
    successful_files = 0
    
    for json_path in json_paths:
        success, message, df = process_json_file(json_path)
        if success and not df.empty:
            all_dfs.append(df)
            successful_files += 1
    
    if not all_dfs:
        print("No data extracted from any of the JSON files.")
        return False, "No data extracted from any of the JSON files"
    
    # Combine all dataframes
    combined_df = pd.concat(all_dfs, ignore_index=True)
    
    # Write to CSV
    combined_df.to_csv(output_csv, index=False)
    
    print(f"Successfully extracted data to {output_csv}")
    print(f"Processed {successful_files} out of {len(json_paths)} JSON files")
    print(f"Total rows in output: {len(combined_df)}")
    
    return True, f"Successfully extracted data from {successful_files} files with {len(combined_df)} total rows"

def main():
    parser = argparse.ArgumentParser(description='Extract metrics from YouTube Studio JSON files to CSV')
    parser.add_argument('json_paths', nargs='+', help='Paths to the input JSON files (supports wildcards)')
    parser.add_argument('--output', '-o', help='Path to the output CSV file', default=None)
    
    args = parser.parse_args()
    
    # Expand any wildcards in the json_paths arguments
    expanded_paths = []
    for path in args.json_paths:
        if '*' in path:
            expanded_paths.extend(glob.glob(path))
        else:
            expanded_paths.append(path)
    
    if not expanded_paths:
        print("No JSON files found.")
        return 1
    
    # If output path is not specified, create one with timestamp
    if not args.output:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        args.output = f"youtube_metrics_{timestamp}.csv"
    
    success, message = process_multiple_jsons(expanded_paths, args.output)
    if success:
        print(message)
        return 0
    else:
        print(f"Error: {message}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)