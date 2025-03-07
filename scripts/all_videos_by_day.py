import pandas as pd
import numpy as np
import datetime
from dateutil import parser
import pyodbc
import argparse
import os
import sys

def calculate_video_metrics(filter_date, output_path):
    """
    Calculate video metrics from Azure SQL database with running totals
    
    Args:
        filter_date (str): Only include videos published after this date (YYYY-MM-DD)
        output_path (str): Path where the CSV file will be saved
    
    Returns:
        pandas.DataFrame: Processed metrics data with running totals
    """
    # Database connection details
    db_user = 'karan'
    db_password = 'SuperSecret!'
    db_host = 'mrbeastyoutube-dev.database.windows.net'
    db_name = 'MrBeastYoutube_YT'
    
    print(f"Connecting to database {db_name} on {db_host}...")
    print(f"Filtering videos published after: {filter_date}")
    print(f"Output will be saved to: {output_path}")
    
    try:
        # Create a direct pyodbc connection string for Azure SQL
        conn_str = (
            f"DRIVER={{SQL Server}};"
            f"SERVER={db_host};"
            f"DATABASE={db_name};"
            f"UID={db_user};"
            f"PWD={db_password};"
            f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        )
        
        # Create connection
        connection = pyodbc.connect(conn_str)
        cursor = connection.cursor()
        
        # SQL query - updated with correct column names
        sql_query = """
        SELECT 
            vd.ID as ytVideoID,
            vd.ytChannelID,
            vd.ytVideoTitle,
            vd.ytVideoPublishedDate,
            vd.ytVideoPublishedTime,
            vbs.Date,
            vbs.views,
            vbs.estimatedMinutesWatched,
            vbs.comments,
            vbs.likes,
            vbs.dislikes,
            vbs.shares,
            vbs.subscribersGained,
            vbs.subscribersLost
        FROM 
            dbo.VideoDimension vd
        JOIN 
            dbo.VideoBasicStats vbs ON vd.ID = vbs.ytVideoID
        """
        
        # Add filter for publish date
        try:
            # Parse the input date to ensure it's valid
            parsed_date = parser.parse(filter_date).strftime('%Y-%m-%d')
            sql_query += f" WHERE vd.ytVideoPublishedDate >= '{parsed_date}'"
        except Exception as e:
            print(f"Error parsing filter date: {e}")
            print("Please provide date in YYYY-MM-DD format.")
            return pd.DataFrame()
        
        # Execute the query manually to avoid pandas SQL read issues
        cursor.execute(sql_query)
        columns = [column[0] for column in cursor.description]
        
        # Fetch all rows
        rows = cursor.fetchall()
        print(f"Retrieved {len(rows)} records.")
        
        # Convert to DataFrame
        df = pd.DataFrame.from_records(rows, columns=columns)
        
        if df.empty:
            print("No data found. Check your filter date or database connection.")
            return pd.DataFrame()
        
        # Convert date strings to datetime objects for calculations
        df['ytVideoPublishedDate'] = pd.to_datetime(df['ytVideoPublishedDate'])
        df['Date'] = pd.to_datetime(df['Date'])
        
        # Calculate days since publish
        df['DaysSincePublish'] = (df['Date'] - df['ytVideoPublishedDate']).dt.days
        
        # Sort data by video ID and date for running totals
        df = df.sort_values(['ytVideoID', 'Date'])
        
        # Calculate running totals for each video
        print("Calculating running totals...")
        metrics_to_total = [
            'views', 'subscribersGained', 'subscribersLost', 
            'estimatedMinutesWatched', 'comments', 'likes', 
            'dislikes', 'shares'
        ]
        
        # Group by video ID and calculate cumulative sums
        for metric in metrics_to_total:
            df[f'RunningTotal_{metric}'] = df.groupby('ytVideoID')[metric].cumsum()
        
        # Add some additional useful metrics
        df['ViewsPerDay'] = df['views'] / df['DaysSincePublish'].replace(0, 1)  # Avoid division by zero
        df['EngagementRate'] = ((df['comments'] + df['likes'] + df['shares']) / df['views'] * 100).fillna(0)
        df['RetentionRate'] = (df['estimatedMinutesWatched'] / df['views']).fillna(0)
        
        # Save to CSV file
        try:
            # Create output directory if it doesn't exist
            output_dir = os.path.dirname(output_path)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
                
            # Ensure the output path has a .csv extension
            if not output_path.lower().endswith('.csv'):
                output_path += '.csv'
                
            df.to_csv(output_path, index=False)
            file_size = os.path.getsize(output_path)
            print(f"Data saved to {output_path} (Size: {file_size} bytes)")
            
            # Check if file size is reasonable
            if file_size < 100:
                print("Warning: The saved file is very small. It might be empty or contain minimal data.")
        except Exception as e:
            print(f"Error saving CSV file: {e}")
            return df
        
        return df
        
    except Exception as e:
        print(f"Error connecting to database or processing data: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

def validate_date(date_text):
    """Validate date format is YYYY-MM-DD"""
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Calculate video metrics after a specific date')
    parser.add_argument('--filter_date', type=str, required=True, 
                        help='Filter date (YYYY-MM-DD format)')
    parser.add_argument('--output_path', type=str, required=True,
                        help='Output file path for the CSV (with or without .csv extension)')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Validate date format
    if not validate_date(args.filter_date):
        print(f"Error: Date '{args.filter_date}' is not in the correct format. Please use YYYY-MM-DD.")
        sys.exit(1)
    
    # Run the calculation
    result_df = calculate_video_metrics(args.filter_date, args.output_path)
    
    if result_df.empty:
        print("No data was processed or an error occurred.")
        sys.exit(1)
    else:
        print(f"Successfully processed {len(result_df)} records.")
        sys.exit(0)

if __name__ == "__main__":
    main()