#!/usr/bin/env python3
"""
Main script to execute the Facebook data processing system.
"""
import argparse
import os
from colorama import Fore, Style, init

from collectors.Facebook.facebook_processor import FacebookProcessor


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Process Facebook data from an Excel file and publish to Kafka')
    
    # File-related arguments
    parser.add_argument('--file', default ='data/data_facebook1.xlsx', help='Path to Excel file containing the Facebook data')
    parser.add_argument('--column', default='STRUCTUREDTEXT', 
                        help='Column name containing the XML data (default: structuredtext)')
    
    # Kafka-related arguments
    parser.add_argument('--bootstrap-servers', default='localhost:9092',
                        help='Comma-separated list of Kafka bootstrap servers (default: localhost:9092)')
    parser.add_argument('--topic', default='facebook-data',
                        help='Kafka topic to publish events to (default: facebook-data)')
    
    # Timing-related arguments
    parser.add_argument('--min-interval', type=float, default=0.5,
                        help='Minimum interval between messages in seconds (default: 0.5)')
    parser.add_argument('--max-interval', type=float, default=2.0,
                        help='Maximum interval between messages in seconds (default: 2.0)')
    parser.add_argument('--no-jitter', action='store_true',
                        help='Disable random time jitter between messages')
    
    return parser.parse_args()


def main():
    """Main function to execute the Facebook data processing system."""
    # Initialize colorama for cross-platform colored terminal output
    init()
    
    # Parse command line arguments
    args = parse_args = parse_arguments()
    
    # Check if file exists
    if not os.path.exists(args.file):
        print(f"{Fore.RED}Error: File '{args.file}' not found{Style.RESET_ALL}")
        return 1
    
    try:
        print(f"{Fore.CYAN}=== Facebook Data Processing System ==={Style.RESET_ALL}")
        print(f"File: {args.file}")
        print(f"Column: {args.column}")
        print(f"Kafka bootstrap servers: {args.bootstrap_servers}")
        print(f"Kafka topic: {args.topic}")
        print("-" * 80)
        
        # Create a processor
        processor = FacebookProcessor(
            file_path=args.file,
            column_name=args.column,
            bootstrap_servers=args.bootstrap_servers.split(','),
            topic=args.topic
        )
        
        # Start processing
        processor.start_processing(
            interval_min=args.min_interval,
            interval_max=args.max_interval,
            jitter=not args.no_jitter
        )
        
        return 0
    
    except Exception as e:
        print(f"{Fore.RED}Error: {e}{Style.RESET_ALL}")
        return 1


if __name__ == "__main__":
    exit(main())