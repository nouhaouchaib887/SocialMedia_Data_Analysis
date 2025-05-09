import os
import time
import random
import pandas as pd
from datetime import datetime
from typing import List, Optional, Callable, Dict, Any


class FacebookCollector:
    """Collects Facebook data from Excel file and simulates real-time data streaming."""
    
    def __init__(self, file_path: str, column_name: str = 'STRUCTUREDTEXT'):
        """Initialize the Facebook collector.
        
        Args:
            file_path: Path to the Excel file containing data
            column_name: Name of the column containing structured text data
        """
        self.file_path = file_path
        self.column_name = column_name
        self.data = None
    
    def load_data(self) -> bool:
        """Load data from the Excel file.
        
        Returns:
            bool: True if data was loaded successfully, False otherwise
        """
        try:
            # Check if file exists
            if not os.path.exists(self.file_path):
                print(f"Error: File '{self.file_path}' not found")
                return False
            
            # Read the Excel file
            df = pd.read_excel(self.file_path)
            
            # Check if specified column exists
            if self.column_name not in df.columns:
                column_names = df.columns.tolist()
                print(f"Error: '{self.column_name}' column not found in the Excel file.")
                print(f"Available columns: {column_names}")
                return False
            
            # Extract the data column
            self.data = df[self.column_name].astype(str).tolist()
            print(f"Loaded {len(self.data)} records from '{self.file_path}'")
            return True
            
        except Exception as e:
            print(f"Error reading Excel file: {e}")
            return False
    
    def simulate_streaming(self, 
                          data_handler: Callable[[str], None],
                          interval_min: float = 0.5, 
                          interval_max: float = 2.0, 
                          jitter: bool = True) -> None:
        """Simulate streaming data in real time.
        
        Args:
            data_handler: Callback function to process each data item
            interval_min: Minimum interval between messages in seconds
            interval_max: Maximum interval between messages in seconds
            jitter: Whether to add random time jitter between messages
        """
        if not self.data:
            if not self.load_data():
                return
        
        print(f"Starting real-time data simulation...")
        print(f"Press Ctrl+C to stop the simulation")
        print("-" * 80)
        
        try:
            for i, text in enumerate(self.data):
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                
                # Display the data with timestamp
                print(f"[{timestamp}] Processing record {i+1}/{len(self.data)}")
                
                # Process the data using the provided handler
                data_handler(text)
                
                # Don't pause after the last item
                if i < len(self.data) - 1:
                    if jitter:
                        # Add random jitter to make it feel more realistic
                        sleep_time = random.uniform(interval_min, interval_max)
                    else:
                        sleep_time = interval_min
                    time.sleep(sleep_time)
                    
            print("-" * 80)
            print(f"Simulation completed. All data has been processed.")
        
        except KeyboardInterrupt:
            print("\n" + "-" * 80)
            print(f"Simulation stopped by user.")