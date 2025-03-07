import streamlit as st
import subprocess
import os
import sys
import tempfile
import time
from datetime import datetime
import pandas as pd

# Set page configuration
st.set_page_config(page_title="Script Launcher", layout="wide")

# Define scripts with their required inputs (same as in your Flask app)
scripts = {
    "YouTube Retention Analysis": {
        "path": "scripts/merge_retention.py",
        "description": "Merge audience retention data from multiple YouTube Studio export zip files into a single CSV.",
        "inputs": [
            {"name": "input_directory", "type": "folder", "label": "Input Directory (contains zip files)"},
            {"name": "output_directory", "type": "folder", "label": "Output Directory (for CSV file)"},
            {"name": "output_filename", "type": "text", "label": "Output Filename", 
             "default": f"retention_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"}
        ]
    },
    "Merge All Chart Data From Zip": {
        "path": "scripts/merge_chart_data.py",
        "description": "Merge Chart Data from multiple zip files into a single CSV.",
        "inputs": [
            {"name": "input_directory", "type": "folder", "label": "Input Directory (contains zip files)"},
            {"name": "csv_filename", "type": "text", "label": "CSV Filename to Extract", "default": "Chart data.csv"},
            {"name": "output_path", "type": "text", "label": "Output File Path (including filename)", 
             "default": f"chart_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"}
        ]
    },
    "YouTube JSON Processor": {
        "path": "scripts/process_youtube_json.py",
        "description": "Process YouTube Analytics JSON files and extract metrics like views, watch time, and impressions with running totals.",
        "inputs": [
            {"name": "input_directory", "type": "folder", "label": "Input Directory (contains JSON files)"},
            {"name": "output_directory", "type": "folder", "label": "Output Directory (for CSV file)"},
            {"name": "output_filename", "type": "text", "label": "Output Filename", 
             "default": f"youtube_metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"}
        ]
    },
    "All Videos By Day": {
        "path": "scripts/all_videos_by_day.py",
        "description": "Calculate daily metrics for all videos published after a specific date.",
        "inputs": [
            {"name": "filter_date", "type": "text", "label": "Filter Date (YYYY-MM-DD format)", "default": "2024-01-01"},
            {"name": "output_path", "type": "text", "label": "Output File Path (.csv extension will be added if missing)", 
             "default": f"video_metrics_by_day_{datetime.now().strftime('%Y%m%d_%H%M%S')}"}
        ]
    }
}

# Create main UI
st.title("Script Launcher")

# Create a sidebar for script selection
with st.sidebar:
    st.header("Select a Script")
    script_name = st.selectbox(
        "Choose a script to run:",
        options=list(scripts.keys()),
        index=0
    )
    
    if script_name:
        st.write(f"**Description:** {scripts[script_name]['description']}")

# Main content area
if script_name:
    script_info = scripts[script_name]
    
    # Create form for script inputs
    with st.form("script_form"):
        st.header(f"Configure {script_name}")
        
        # Store input values
        input_values = {}
        
        # Create input fields based on script requirements
        for input_def in script_info['inputs']:
            input_name = input_def['name']
            input_label = input_def['label']
            input_type = input_def.get('type', 'text')
            default_value = input_def.get('default', '')
            
            # Different input types
            if input_type == 'folder':
                input_values[input_name] = st.text_input(input_label, value=default_value)
            elif input_type == 'file':
                input_values[input_name] = st.text_input(input_label, value=default_value)
            elif input_type == 'dropdown':
                options = input_def.get('options', [])
                input_values[input_name] = st.selectbox(input_label, options=options)
            else:  # Default to text input
                input_values[input_name] = st.text_input(input_label, value=default_value)
        
        # Submit button
        submitted = st.form_submit_button("Run Script")
    
    # Handle script execution
    if submitted:
        # Show a spinner while the script runs
        with st.spinner(f"Running {script_name}..."):
            # Build command with arguments
            cmd = [sys.executable, script_info['path']]
            
            # Special handling for Merge All Chart Data From Zip script
            if script_name == "Merge All Chart Data From Zip":
                input_directory = input_values.get('input_directory', '').strip()
                csv_filename = input_values.get('csv_filename', '').strip()
                output_path = input_values.get('output_path', '').strip()
                
                if not output_path.lower().endswith('.csv'):
                    output_path += '.csv'
                
                if input_directory:
                    cmd.append(f"--input_directory={input_directory}")
                if csv_filename:
                    cmd.append(f"--csv_filename={csv_filename}")
                if output_path:
                    cmd.append(f"--output_path={output_path}")
            elif script_name == "All Videos By Day":
                # Ensure output_path ends with .csv for this script as well
                filter_date = input_values.get('filter_date', '').strip()
                output_path = input_values.get('output_path', '').strip()
                
                if not output_path.lower().endswith('.csv'):
                    output_path += '.csv'
                
                if filter_date:
                    cmd.append(f"--filter_date={filter_date}")
                if output_path:
                    cmd.append(f"--output_path={output_path}")
            else:
                # Handle other scripts with standard approach
                for input_def in script_info['inputs']:
                    input_name = input_def['name']
                    input_value = input_values.get(input_name, '').strip()
                    
                    # Ensure output filename ends with .csv
                    if input_name == 'output_filename' and input_value:
                        if not input_value.lower().endswith('.csv'):
                            input_value += '.csv'
                    
                    if input_value:
                        cmd.append(f"--{input_name}={input_value}")
            
            # Run the script and capture output
            try:
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    universal_newlines=True
                )
                stdout, stderr = process.communicate()
                
                # Display the results
                st.subheader("Script Output")
                
                if process.returncode == 0:
                    st.success("Script completed successfully!")
                else:
                    st.error(f"Script failed with exit code {process.returncode}")
                
                # Create expandable sections for stdout and stderr
                if stdout:
                    with st.expander("Standard Output", expanded=True):
                        st.code(stdout)
                
                if stderr:
                    with st.expander("Error Output", expanded=process.returncode != 0):
                        st.code(stderr)
                
                # Try to find the output file and offer preview/download if possible
                for input_def in script_info['inputs']:
                    if input_def['name'] in ['output_path', 'output_filename']:
                        output_file = input_values.get(input_def['name'], '').strip()
                        output_dir = input_values.get('output_directory', '').strip()
                        
                        # Construct full path based on available info
                        if 'output_directory' in input_values and output_file:
                            full_path = os.path.join(output_dir, output_file)
                        else:
                            full_path = output_file
                        
                        # Ensure .csv extension
                        if full_path and not full_path.lower().endswith('.csv'):
                            full_path += '.csv'
                        
                        # If file exists, offer preview
                        if full_path and os.path.exists(full_path):
                            st.subheader("Output File Preview")
                            try:
                                df = pd.read_csv(full_path)
                                st.dataframe(df.head(10))
                                
                                # Create download button
                                with open(full_path, 'rb') as f:
                                    st.download_button(
                                        label="Download Output File",
                                        data=f,
                                        file_name=os.path.basename(full_path),
                                        mime="text/csv"
                                    )
                            except Exception as e:
                                st.error(f"Error previewing file: {e}")
                        break
                        
            except Exception as e:
                st.error(f"Error running script: {e}")

# Add useful information in the sidebar
with st.sidebar:
    st.markdown("---")
    st.markdown("### Instructions")
    st.markdown("""
    1. Select a script from the dropdown menu
    2. Fill in the required parameters
    3. Click 'Run Script' to execute
    4. View the results and download output files
    """)
    
    st.markdown("---")
    st.markdown(f"**Current Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")