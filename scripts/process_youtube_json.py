import json
import pandas as pd
import os
import argparse
from datetime import datetime

def find_path(data, target_key):
    """Find the results path in the JSON data."""
    for result in data.get('results', []):
        if result.get('key') == target_key:
            return result.get('value', {}).get('resultTable')
    return None

def extract_video_metadata(data):
    """Extract video metadata from YouTube API JSON response."""
    metadata_df = pd.DataFrame()
    
    try:
        # Look for the metadata in the expected path
        creator_videos = None
        for result in data.get('results', []):
            if 'getCreatorVideos' in str(result):
                creator_videos = result.get('value', {}).get('getCreatorVideos', {}).get('videos', [])
                break
        
        if not creator_videos:
            return metadata_df
        
        # Extract relevant fields from each video
        video_data = []
        for video in creator_videos:
            if 'videoId' in video:
                # Convert timePublishedSeconds to datetime
                published_date = None
                if 'timePublishedSeconds' in video:
                    try:
                        timestamp = int(video['timePublishedSeconds'])
                        published_date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                    except (ValueError, TypeError):
                        published_date = None
                
                video_data.append({
                    'Video IDs': video.get('videoId'),
                    'Title': video.get('title'),
                    'Published Date': published_date,
                    'Length (seconds)': video.get('lengthSeconds')
                })
        
        if video_data:
            metadata_df = pd.DataFrame(video_data)
            print(f"Extracted metadata for {len(metadata_df)} videos")
        
    except Exception as e:
        print(f"Error extracting video metadata: {e}")
    
    return metadata_df

def extract_and_match_data(data):
    """Extract video metrics from the YouTube API JSON response."""
    target_key = "2__TOP_ENTITIES_CHARTS_QUERY_KEY"
    result_table = find_path(data, target_key)
    
    if result_table is None:
        return pd.DataFrame()
    
    try:
        video_ids = result_table['dimensionColumns'][1]['strings']['values']
        dates = result_table['dimensionColumns'][0]['dateIds']['values']
        metrics = {}
        
        for metric_col in result_table['metricColumns']:
            metric_name = metric_col['metric']['type']
            for key in metric_col.keys():
                if 'values' in metric_col[key]:
                    metrics[metric_name] = metric_col[key]['values']
                    break
        
        data_dict = {
            'Video IDs': video_ids,
            'Dates': dates,
        }
        data_dict.update(metrics)
        
        df = pd.DataFrame(data_dict)
        df['Dates'] = pd.to_datetime(df['Dates'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
        
        return df
    except (KeyError, IndexError) as e:
        print(f"Error extracting data: {e}")
        return pd.DataFrame()

def process_json_files(directory_path):
    """Process all JSON files in the directory."""
    combined_df = pd.DataFrame()
    combined_metadata_df = pd.DataFrame()
    processed_files = 0
    skipped_files = 0

    print(f"Scanning directory: {directory_path}")
    json_files = [f for f in os.listdir(directory_path) if f.endswith('.json')]
    print(f"Found {len(json_files)} JSON files")
    
    for filename in json_files:
        file_path = os.path.join(directory_path, filename)
        try:
            print(f"Processing file: {filename}")
            with open(file_path, 'r', encoding='utf-8') as file:
                file_content = file.read().strip()
                if not file_content:  # Skip empty files
                    print(f"Skipping empty file: {filename}")
                    skipped_files += 1
                    continue
                data = json.loads(file_content)
                
                # Extract metrics data
                df = extract_and_match_data(data)
                
                # Extract video metadata
                metadata_df = extract_video_metadata(data)
                
                # Process and merge metrics data
                if not df.empty:  # Only merge non-empty DataFrames
                    if combined_df.empty:
                        combined_df = df
                    else:
                        combined_df = pd.merge(combined_df, df, on=["Video IDs", "Dates"], how="outer")
                    processed_files += 1
                else:
                    print(f"No metrics data extracted from: {filename}")
                
                # Combine metadata
                if not metadata_df.empty:
                    if combined_metadata_df.empty:
                        combined_metadata_df = metadata_df
                    else:
                        # Append new metadata, but avoid duplicates based on Video IDs
                        combined_metadata_df = pd.concat([
                            combined_metadata_df, 
                            metadata_df[~metadata_df['Video IDs'].isin(combined_metadata_df['Video IDs'])]
                        ]).reset_index(drop=True)
                
                if df.empty and metadata_df.empty:
                    print(f"No data extracted from: {filename}")
                    skipped_files += 1
                    
        except UnicodeDecodeError:
            print(f"Unicode decode error, trying with latin1 encoding: {filename}")
            try:
                with open(file_path, 'r', encoding='latin1') as file:
                    file_content = file.read().strip()
                    if not file_content:  # Skip empty files
                        print(f"Skipping empty file: {filename}")
                        skipped_files += 1
                        continue
                    data = json.loads(file_content)
                    
                    # Extract metrics data
                    df = extract_and_match_data(data)
                    
                    # Extract video metadata
                    metadata_df = extract_video_metadata(data)
                    
                    # Process and merge metrics data
                    if not df.empty:  # Only merge non-empty DataFrames
                        if combined_df.empty:
                            combined_df = df
                        else:
                            combined_df = pd.merge(combined_df, df, on=["Video IDs", "Dates"], how="outer")
                        processed_files += 1
                    
                    # Combine metadata
                    if not metadata_df.empty:
                        if combined_metadata_df.empty:
                            combined_metadata_df = metadata_df
                        else:
                            # Append new metadata, but avoid duplicates based on Video IDs
                            combined_metadata_df = pd.concat([
                                combined_metadata_df, 
                                metadata_df[~metadata_df['Video IDs'].isin(combined_metadata_df['Video IDs'])]
                            ]).reset_index(drop=True)
                    
                    if df.empty and metadata_df.empty:
                        print(f"No data extracted from: {filename}")
                        skipped_files += 1
                        
            except Exception as e:
                print(f"Error processing file with latin1 encoding {filename}: {e}")
                skipped_files += 1
        except Exception as e:
            print(f"Error processing file {filename}: {e}")
            skipped_files += 1

    print(f"Successfully processed {processed_files} files, skipped {skipped_files} files")
    print(f"Collected metadata for {len(combined_metadata_df)} videos")
    
    if combined_df.empty:
        print("No metrics data was extracted from any files")
        metrics_df = pd.DataFrame(columns=["Video IDs", "Dates"])
    else:
        combined_df.fillna(0, inplace=True)
        metrics_df = combined_df
    
    return metrics_df, combined_metadata_df

def add_running_totals(df):
    """Calculate running totals for all metrics."""
    # Convert WATCH_TIME from milliseconds to hours
    if 'WATCH_TIME' in df.columns:
        df['WATCH_TIME'] = df['WATCH_TIME'] / 3_600_000
        print("Converted WATCH_TIME from milliseconds to hours")
    
    # Find all metrics in the dataframe (excluding Video IDs and Dates)
    all_metrics = [col for col in df.columns if col not in ['Video IDs', 'Dates'] and not col.endswith('_RUNNING_TOTAL')]
    print(f"Calculating running totals for {len(all_metrics)} metrics")
    
    # Calculate running totals for all metrics
    for metric in all_metrics:
        running_total_col = f"{metric}_RUNNING_TOTAL"
        df[running_total_col] = df.groupby("Video IDs")[metric].cumsum()
    
    return df

def organize_columns(df):
    """Organize columns in a specific order."""
    # Define the known metrics and their running totals in the desired order
    known_metrics = [
        "SHORTS_FEED_IMPRESSIONS", "SHORTS_FEED_IMPRESSIONS_VTR", 
        "RATINGS_LIKES", "SUBSCRIBERS_NET_CHANGE", "VIEWS", "WATCH_TIME", 
        "VIDEO_THUMBNAIL_IMPRESSIONS", "VIDEO_THUMBNAIL_IMPRESSIONS_VTR", 
        "COMMENTS", "SHARINGS"
    ]
    
    # Start with the basic columns
    ordered_columns = ["Video IDs", "Dates"]
    
    # Add known metrics that exist in the dataframe
    for metric in known_metrics:
        if metric in df.columns:
            ordered_columns.append(metric)
    
    # Find additional metrics (excluding running totals)
    additional_metrics = [col for col in df.columns 
                         if col not in ordered_columns 
                         and not col.endswith('_RUNNING_TOTAL')
                         and col not in ['Video IDs', 'Dates']]
    
    # Add additional metrics
    ordered_columns.extend(additional_metrics)
    
    # Add running totals for known metrics
    for metric in known_metrics:
        running_total = f"{metric}_RUNNING_TOTAL"
        if running_total in df.columns:
            ordered_columns.append(running_total)
    
    # Add running totals for additional metrics
    for metric in additional_metrics:
        running_total = f"{metric}_RUNNING_TOTAL"
        if running_total in df.columns:
            ordered_columns.append(running_total)
    
    # Only include columns that actually exist in the dataframe
    ordered_columns = [col for col in ordered_columns if col in df.columns]
    
    return ordered_columns

def process_youtube_json(input_directory, output_directory, output_filename=None):
    """Process YouTube JSON files and generate a CSV with metrics."""
    print(f"Processing YouTube JSON files from: {input_directory}")
    
    # Create the output file path with the current date and time if not provided
    if not output_filename:
        current_datetime = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        output_filename = f"youtube_metrics_{current_datetime}.csv"
    
    output_path = os.path.join(output_directory, output_filename)
    
    # Create output directory if it doesn't exist
    os.makedirs(output_directory, exist_ok=True)
    
    # Process JSON files
    metrics_df, metadata_df = process_json_files(input_directory)
    
    has_metrics = not metrics_df.empty and len(metrics_df.columns) > 2
    has_metadata = not metadata_df.empty
    
    if not has_metrics and not has_metadata:
        print("No data was found in the JSON files")
        # Save an empty file to indicate processing was done
        pd.DataFrame(columns=["Video IDs"]).to_csv(output_path, index=False)
        return None
    
    # Final dataframe that will be saved to CSV
    final_df = None
    
    # Process metrics data if available
    if has_metrics:
        # Add running totals and organize columns
        metrics_df_with_totals = add_running_totals(metrics_df)
        columns_order = organize_columns(metrics_df_with_totals)
        metrics_df_with_totals = metrics_df_with_totals[columns_order]
        
        # If we have both metrics and metadata, join them
        if has_metadata:
            print("Merging metrics data with video metadata...")
            final_df = pd.merge(
                metrics_df_with_totals, 
                metadata_df,
                on="Video IDs", 
                how="left"
            )
            
            # Move metadata columns right after Video IDs and Dates
            metadata_cols = [col for col in metadata_df.columns if col != 'Video IDs']
            reordered_cols = ["Video IDs", "Dates"] + metadata_cols + [
                col for col in final_df.columns 
                if col not in ["Video IDs", "Dates"] + metadata_cols
            ]
            final_df = final_df[reordered_cols]
        else:
            final_df = metrics_df_with_totals
    else:
        # If we only have metadata, use that
        final_df = metadata_df
    
    # Save the processed data
    final_df.to_csv(output_path, index=False)
    
    metrics_count = len(final_df.columns) - (2 + len(metadata_df.columns) if has_metadata else 2)
    if has_metrics:
        print(f"Found and processed {metrics_count} metrics across {len(final_df)} rows.")
    if has_metadata:
        print(f"Included metadata for {len(metadata_df)} videos in the output.")
    
    print(f"Processing complete. Output saved to: {output_path}")
    
    return final_df

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Process YouTube Analytics JSON files')
    parser.add_argument('--input_directory', type=str, required=True, help='Directory containing JSON files')
    parser.add_argument('--output_directory', type=str, required=True, help='Directory to save the output CSV file')
    parser.add_argument('--output_filename', type=str, help='Name of the output CSV file (default: auto-generated)')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Process the directory with the provided arguments
    process_youtube_json(
        args.input_directory, 
        args.output_directory, 
        args.output_filename
    )

if __name__ == "__main__":
    main()