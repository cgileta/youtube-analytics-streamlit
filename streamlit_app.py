import streamlit as st
import subprocess
import os
import sys
import tempfile
import time
import zipfile
import json
from datetime import datetime
import pandas as pd
import io
import shutil

# Set page configuration
st.set_page_config(page_title="YouTube Analytics Tools", layout="wide")

# Define scripts with their required inputs and updated for file uploaders
# Add the new script to the scripts dictionary in your streamlit_app.py file
scripts = {
    "YouTube Retention Analysis": {
        "path": "scripts/merge_retention.py",
        "description": "Merge audience retention data from multiple YouTube Studio export zip files into a single CSV.",
        "file_type": "zip",
        "multiple_files": True,
        "inputs": [
            {"name": "output_filename", "type": "text", "label": "Output Filename", 
             "default": f"retention_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"}
        ]
    },
    "Merge All Chart Data From Zip": {
        "path": "scripts/merge_chart_data.py",
        "description": "Merge Chart Data from multiple zip files into a single CSV.",
        "file_type": "zip",
        "multiple_files": True,
        "inputs": [
            {"name": "csv_filename", "type": "text", "label": "CSV Filename to Extract", "default": "Chart data.csv"},
            {"name": "output_path", "type": "text", "label": "Output Filename", 
             "default": f"merged_chart_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"}
        ]
    },
    "YouTube JSON Processor": {
        "path": "scripts/process_youtube_json.py",
        "description": "Process YouTube Analytics JSON files and extract metrics like views, watch time, and impressions with running totals.",
        "file_type": "json",
        "multiple_files": True,
        "inputs": [
            {"name": "output_filename", "type": "text", "label": "Output Filename", 
             "default": f"youtube_metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"}
        ]
    },
    "First 24, 7, 28 Days JSON Parser": {
        "path": "scripts/first_days_json_parser.py",
        "description": "Extract metrics for the first 24 hours, 7 days, and 28 days from YouTube Analytics JSON files.",
        "file_type": "json",
        "multiple_files": True,
        "inputs": [
            {"name": "output", "type": "text", "label": "Output Filename", 
             "default": f"first_days_metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"}
        ]
    },
    "All Videos By Day": {
        "path": "scripts/all_videos_by_day.py",
        "description": "Calculate daily metrics for all videos published after a specific date.",
        "file_type": None,  # No file upload needed
        "multiple_files": False,
        "inputs": [
            {"name": "filter_date", "type": "text", "label": "Filter Date (YYYY-MM-DD format)", "default": "2024-01-01"},
            {"name": "output_path", "type": "text", "label": "Output Filename", 
             "default": f"video_metrics_by_day_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"}
        ]
    }
}

# Function to create a temporary directory and save uploaded files
def save_uploaded_files(uploaded_files, file_type):
    temp_dir = tempfile.mkdtemp()
    file_paths = []
    
    for uploaded_file in uploaded_files:
        file_path = os.path.join(temp_dir, uploaded_file.name)
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        file_paths.append(file_path)
        
    return temp_dir, file_paths

# Function to run merge_retention.py script with uploaded zip files
def run_retention_analysis(zip_files, output_filename):
    if not zip_files:
        return False, "No files uploaded", ""
    
    # Create temporary directories
    temp_input_dir = tempfile.mkdtemp()
    temp_output_dir = tempfile.mkdtemp()
    
    try:
        # Save uploaded zip files to temp directory
        for zip_file in zip_files:
            file_path = os.path.join(temp_input_dir, zip_file.name)
            with open(file_path, "wb") as f:
                f.write(zip_file.getbuffer())
        
        # Build command
        cmd = [
            sys.executable, 
            "scripts/merge_retention.py",
            f"--input_directory={temp_input_dir}",
            f"--output_directory={temp_output_dir}",
            f"--output_filename={output_filename}"
        ]
        
        # Run the script
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            universal_newlines=True
        )
        stdout, stderr = process.communicate()
        
        # Check if output file was created
        output_file_path = os.path.join(temp_output_dir, output_filename)
        if os.path.exists(output_file_path):
            with open(output_file_path, 'rb') as f:
                output_data = f.read()
            return True, stdout, output_data
        else:
            return False, f"Script ran but no output file was created.\nStdout: {stdout}\nStderr: {stderr}", ""
            
    except Exception as e:
        return False, f"Error running script: {str(e)}", ""
    finally:
        # Clean up temp directories
        shutil.rmtree(temp_input_dir, ignore_errors=True)
        shutil.rmtree(temp_output_dir, ignore_errors=True)

# Function to run merge_chart_data.py script with uploaded zip files
def run_chart_data_merge(zip_files, csv_filename, output_path):
    if not zip_files:
        return False, "No files uploaded", ""
    
    # Create temporary directory
    temp_input_dir = tempfile.mkdtemp()
    
    try:
        # Save uploaded zip files to temp directory
        for zip_file in zip_files:
            file_path = os.path.join(temp_input_dir, zip_file.name)
            with open(file_path, "wb") as f:
                f.write(zip_file.getbuffer())
        
        # Make sure output path has .csv extension
        if not output_path.lower().endswith('.csv'):
            output_path += '.csv'
        
        # Create temp output path
        temp_output_path = os.path.join(tempfile.mkdtemp(), output_path)
        
        # Build command
        cmd = [
            sys.executable, 
            "scripts/merge_chart_data.py",
            f"--input_directory={temp_input_dir}",
            f"--csv_filename={csv_filename}",
            f"--output_path={temp_output_path}"
        ]
        
        # Run the script
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            universal_newlines=True
        )
        stdout, stderr = process.communicate()
        
        # Check if output file was created
        if os.path.exists(temp_output_path):
            with open(temp_output_path, 'rb') as f:
                output_data = f.read()
            return True, stdout, output_data
        else:
            return False, f"Script ran but no output file was created.\nStdout: {stdout}\nStderr: {stderr}", ""
            
    except Exception as e:
        return False, f"Error running script: {str(e)}", ""
    finally:
        # Clean up temp directories
        shutil.rmtree(temp_input_dir, ignore_errors=True)

# Function to run process_youtube_json.py script with uploaded JSON files
def run_youtube_json_processor(json_files, output_filename):
    if not json_files:
        return False, "No files uploaded", ""
    
    # Create temporary directories
    temp_input_dir = tempfile.mkdtemp()
    temp_output_dir = tempfile.mkdtemp()
    
    try:
        # Save uploaded JSON files to temp directory
        for json_file in json_files:
            file_path = os.path.join(temp_input_dir, json_file.name)
            with open(file_path, "wb") as f:
                f.write(json_file.getbuffer())
        
        # Build command
        cmd = [
            sys.executable, 
            "scripts/process_youtube_json.py",
            f"--input_directory={temp_input_dir}",
            f"--output_directory={temp_output_dir}",
            f"--output_filename={output_filename}"
        ]
        
        # Run the script
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            universal_newlines=True
        )
        stdout, stderr = process.communicate()
        
        # Check if output file was created
        output_file_path = os.path.join(temp_output_dir, output_filename)
        if os.path.exists(output_file_path):
            with open(output_file_path, 'rb') as f:
                output_data = f.read()
            return True, stdout, output_data
        else:
            return False, f"Script ran but no output file was created.\nStdout: {stdout}\nStderr: {stderr}", ""
            
    except Exception as e:
        return False, f"Error running script: {str(e)}", ""
    finally:
        # Clean up temp directories
        shutil.rmtree(temp_input_dir, ignore_errors=True)
        shutil.rmtree(temp_output_dir, ignore_errors=True)

# Function to run first_days_json_parser.py script with uploaded JSON files
def run_first_days_parser(json_files, output_filename):
    if not json_files:
        return False, "No files uploaded", ""
    
    # Create temporary directories
    temp_input_dir = tempfile.mkdtemp()
    temp_output_dir = tempfile.mkdtemp()
    
    try:
        # Handle both single file and multiple files
        if not isinstance(json_files, list):
            json_files = [json_files]
        
        # Save uploaded JSON files to temp directory
        file_paths = []
        for json_file in json_files:
            file_path = os.path.join(temp_input_dir, json_file.name)
            with open(file_path, "wb") as f:
                f.write(json_file.getbuffer())
            file_paths.append(file_path)
        
        # Make sure output filename has .csv extension
        if not output_filename.lower().endswith('.csv'):
            output_filename += '.csv'
        
        output_file_path = os.path.join(temp_output_dir, output_filename)
        
        # Build command with all file paths
        cmd = [
            sys.executable, 
            "scripts/first_days_json_parser.py"
        ]
        # Add all JSON file paths
        cmd.extend(file_paths)
        # Add the output path
        cmd.append(f"--output={output_file_path}")
        
        # Run the script
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            universal_newlines=True
        )
        stdout, stderr = process.communicate()
        
        # Check if output file was created
        if os.path.exists(output_file_path):
            with open(output_file_path, 'rb') as f:
                output_data = f.read()
            return True, stdout, output_data
        else:
            return False, f"Script ran but no output file was created.\nStdout: {stdout}\nStderr: {stderr}", ""
            
    except Exception as e:
        return False, f"Error running script: {str(e)}", ""
    finally:
        # Clean up temp directories
        shutil.rmtree(temp_input_dir, ignore_errors=True)
        shutil.rmtree(temp_output_dir, ignore_errors=True)

# Function to run all_videos_by_day.py script
def run_videos_by_day(filter_date, output_path):
    # Ensure output path has .csv extension
    if not output_path.lower().endswith('.csv'):
        output_path += '.csv'
    
    # Create temp output directory
    temp_output_dir = tempfile.mkdtemp()
    temp_output_path = os.path.join(temp_output_dir, output_path)
    
    try:
        # Build command
        cmd = [
            sys.executable, 
            "scripts/all_videos_by_day.py",
            f"--filter_date={filter_date}",
            f"--output_path={temp_output_path}"
        ]
        
        # Run the script
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            universal_newlines=True
        )
        stdout, stderr = process.communicate()
        
        # Check if output file was created
        if os.path.exists(temp_output_path):
            with open(temp_output_path, 'rb') as f:
                output_data = f.read()
            return True, stdout, output_data
        else:
            return False, f"Script ran but no output file was created.\nStdout: {stdout}\nStderr: {stderr}", ""
            
    except Exception as e:
        return False, f"Error running script: {str(e)}", ""
    finally:
        # Clean up temp directory
        shutil.rmtree(temp_output_dir, ignore_errors=True)

# Create main UI
st.title("YouTube Analytics Tools")

# Create a sidebar for script selection
with st.sidebar:
    st.header("Select a Tool")
    script_name = st.selectbox(
        "Choose a tool to run:",
        options=list(scripts.keys()),
        index=0
    )
    
    if script_name:
        st.markdown(f"**Description:** {scripts[script_name]['description']}")

# Main content area
if script_name:
    script_info = scripts[script_name]
    
    st.header(script_name)
    
    # File uploader section
    if script_info['file_type']:
        # Create a row with the upload header and a clear button
        col1, col2 = st.columns([3, 1])
        with col1:
            st.subheader(f"Upload {script_info['file_type'].upper()} Files")
        
        # Initialize session state for file uploader key if it doesn't exist
        if 'file_uploader_key' not in st.session_state:
            st.session_state.file_uploader_key = 0
            
        # Function to clear uploaded files by changing the key
        def clear_uploaded_files():
            st.session_state.file_uploader_key += 1
            # Use the current method instead of experimental_rerun
            st.rerun()
        
        with col2:
            # Only show clear button if files are uploaded
            if 'uploaded_files' in st.session_state and st.session_state.get('uploaded_files'):
                st.button("Clear Files", on_click=clear_uploaded_files, type="secondary")
        
        help_text = "Select one or more files" if script_info['multiple_files'] else "Select a file"
        
        # Use a dynamic key for the file uploader to force reset when needed
        uploader_key = f"file_uploader_{script_info['file_type']}_{st.session_state.file_uploader_key}"
        
        if script_info['file_type'] == 'zip':
            uploaded_files = st.file_uploader(
                f"Upload Zip Files", 
                type=["zip"], 
                accept_multiple_files=script_info['multiple_files'],
                help=help_text,
                key=uploader_key
            )
        elif script_info['file_type'] == 'json':
            uploaded_files = st.file_uploader(
                f"Upload JSON Files", 
                type=["json"], 
                accept_multiple_files=script_info['multiple_files'],
                help=help_text,
                key=uploader_key
            )
        
        # Store uploaded files in session state for the clear button to work
        st.session_state.uploaded_files = uploaded_files
    else:
        uploaded_files = None
    
    # Count uploaded files and display info
    if uploaded_files and isinstance(uploaded_files, list):
        file_count = len(uploaded_files)
        if file_count > 0:
            st.text(f"Found {file_count} files")
            
            # Show file sizes
            for file in uploaded_files:
                file_size_kb = round(len(file.getvalue()) / 1024, 1)
                st.text(f"{file.name} ({file_size_kb} KB)")
    elif uploaded_files and not isinstance(uploaded_files, list):
        file_size_kb = round(len(uploaded_files.getvalue()) / 1024, 1)
        st.text(f"{uploaded_files.name} ({file_size_kb} KB)")
    
    # Input parameters section
    st.subheader("Parameters")
    
    # Create form for script inputs
    input_values = {}
    
    # Create input fields based on script requirements
    for input_def in script_info['inputs']:
        input_name = input_def['name']
        input_label = input_def['label']
        input_type = input_def.get('type', 'text')
        default_value = input_def.get('default', '')
        
        # Different input types
        if input_type == 'dropdown':
            options = input_def.get('options', [])
            input_values[input_name] = st.selectbox(input_label, options=options)
        elif input_type == 'date':
            input_values[input_name] = st.date_input(input_label, value=default_value)
        else:  # Default to text input
            input_values[input_name] = st.text_input(input_label, value=default_value)
    
    # Submit button
    if st.button("Run Tool", type="primary"):
        # Validate inputs
        if script_info['file_type'] and not uploaded_files:
            st.error(f"Please upload at least one {script_info['file_type']} file")
        else:
            # Show a spinner while the script runs
            with st.spinner(f"Running {script_name}..."):
                success = False
                output_text = ""
                output_data = None
                
                # Run the appropriate script based on selection
                if script_name == "YouTube Retention Analysis":
                    success, output_text, output_data = run_retention_analysis(
                        uploaded_files,
                        input_values.get('output_filename', '')
                    )
                
                elif script_name == "Merge All Chart Data From Zip":
                    success, output_text, output_data = run_chart_data_merge(
                        uploaded_files,
                        input_values.get('csv_filename', ''),
                        input_values.get('output_path', '')
                    )
                
                elif script_name == "YouTube JSON Processor":
                    success, output_text, output_data = run_youtube_json_processor(
                        uploaded_files,
                        input_values.get('output_filename', '')
                    )
                
                elif script_name == "First 24, 7, 28 Days JSON Parser":
                    # For this script, all uploaded files should be passed
                    success, output_text, output_data = run_first_days_parser(
                        uploaded_files,  # Pass all uploaded files directly
                        input_values.get('output', '')
                    )
                
                elif script_name == "All Videos By Day":
                    success, output_text, output_data = run_videos_by_day(
                        input_values.get('filter_date', ''),
                        input_values.get('output_path', '')
                    )
                
                # Display the results
                st.subheader("Tool Output")
                
                if success:
                    st.success("Process completed successfully!")
                else:
                    st.error("Process failed")
                
                # Display output text
                with st.expander("Process Log", expanded=not success):
                    st.code(output_text)
                
                # If we have output data, show preview and download button
                if output_data:
                    try:
                        # Get output filename
                        output_filename = None
                        for input_def in script_info['inputs']:
                            if input_def['name'] in ['output_path', 'output_filename', 'output']:
                                output_filename = input_values.get(input_def['name'], '')
                                # Ensure .csv extension
                                if output_filename and not output_filename.lower().endswith('.csv'):
                                    output_filename += '.csv'
                                break
                        
                        if not output_filename:
                            output_filename = f"output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                            
                        # Preview the data
                        st.subheader("Output File Preview")
                        df = pd.read_csv(io.BytesIO(output_data))
                        st.dataframe(df.head(10))
                        
                        # Download button
                        st.download_button(
                            label="Download Output File",
                            data=output_data,
                            file_name=output_filename,
                            mime="text/csv"
                        )
                    except Exception as e:
                        st.error(f"Error previewing output data: {str(e)}")

# Add useful information in the sidebar
with st.sidebar:
    st.markdown("---")
    st.markdown("### Instructions")
    st.markdown("""
    1. Select a tool from the dropdown menu
    2. Upload required files (if needed)
    3. Fill in the parameters
    4. Click 'Run Tool' to execute
    5. View the results and download output files
    """)
    
    st.markdown("---")
    st.markdown(f"**Current Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")