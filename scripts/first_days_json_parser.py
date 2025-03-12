import json
import csv
import argparse
import os
import pandas as pd
from datetime import datetime

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

def process_json_to_csv(json_path, output_csv):
    """
    Process the JSON file and output a CSV with extracted metrics.
    
    Args:
        json_path (str): Path to the input JSON file
        output_csv (str): Path to the output CSV file
    """
    # Read the JSON file
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Define the base paths
    base_path = "results[0].value.getCards.cards[0].scatterplotData.resultTable"
    videos_path = f"{base_path}.dimensionColumns[0].strings.values"
    
    # Get the list of videos
    video_ids = extract_value_from_path(data, videos_path)
    
    if not video_ids:
        print(f"Error: Could not extract videos from path {videos_path}")
        return False, "Could not extract videos from JSON"
    
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
                print(f"Warning: Could not extract metadata for a video: {e}")
    else:
        print("Warning: Could not find video metadata in the JSON")
    
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
            print(f"Warning: Could not extract values from path {path}")
            metric_values.append([None] * len(video_ids))
    
    # Check if we have metrics data
    if not metric_values or len(metric_values) == 0:
        print("No metric values found in JSON")
        return False, "No metric values found in JSON"
    
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
    
    # Prepare CSV data
    csv_data = []
    headers = ["VIDEO_ID", "TITLE", "PUBLISHED_DATE", "TIME_PERIOD"]
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
        
        row = [video_id, title, published_date, time_period]
        
        for metric_idx, metric in enumerate(metric_values):
            if i < len(metric):
                row.append(metric[i])
            else:
                row.append(None)
        
        csv_data.append(row)
    
    # Create DataFrame for easier manipulation
    df = pd.DataFrame(csv_data, columns=headers)
    
    # Remove rows with all None metrics
    metric_columns = df.columns[4:]  # All columns except VIDEO_ID, TITLE, PUBLISHED_DATE, and TIME_PERIOD
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
    
    # Write to CSV
    df.to_csv(output_csv, index=False)
    
    print(f"Successfully extracted data to {output_csv}")
    print(f"Processed {len(video_ids)} videos with {len(metric_paths)} metrics each")
    
    return True, f"Successfully extracted data for {len(df)} rows of metrics"

def main():
    parser = argparse.ArgumentParser(description='Extract metrics from YouTube Studio JSON to CSV')
    parser.add_argument('json_path', help='Path to the input JSON file')
    parser.add_argument('--output', '-o', help='Path to the output CSV file', default=None)
    
    args = parser.parse_args()
    
    # If output path is not specified, create one based on the input file
    if not args.output:
        base_name = os.path.splitext(os.path.basename(args.json_path))[0]
        args.output = f"{base_name}_metrics.csv"
    
    success, message = process_json_to_csv(args.json_path, args.output)
    if success:
        print(message)
        return 0
    else:
        print(f"Error: {message}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)