import os
import glob
import pandas as pd
import argparse
from pathlib import Path

def evaluate_test_results(artifacts_dir):
    """
    Evaluate test results from parquet files
    Returns: (success, avg_time, message)
    """
    success = False
    avg_time = 0
    message = ""
    
    try:
        test_files = glob.glob(str(Path(artifacts_dir) / '*' / '*.parquet'))
        if not test_files:
            return False, 0, "No test files found"
            
        df = pd.read_parquet(test_files[0])
        records = len(df)
        avg_time = df.processing_time.mean()
        errors = df['error'].notna().sum()
        
        message = (
            f'Test Results:\n'
            f'Records processed: {records}\n'
            f'Average processing time: {avg_time:.2f}s\n'
            f'Records with errors: {errors}'
        )
        
        # Consider test successful if:
        # 1. At least 8 records processed (out of 10)
        # 2. Average processing time < 30 seconds
        # 3. Error rate < 20%
        success = (records >= 8 and 
                  avg_time < 30 and 
                  errors/records < 0.2)
        
        message += f'\nTest evaluation: {"SUCCESS" if success else "FAILED"}'
        
    except Exception as e:
        message = f'Error evaluating test: {str(e)}'
        success = False
        avg_time = 0
        
    return success, avg_time, message

def main():
    parser = argparse.ArgumentParser(description='Evaluate test run results')
    parser.add_argument('--artifacts-dir', required=True, help='Directory containing test artifacts')
    parser.add_argument('--github-output', help='GitHub Actions output file')
    args = parser.parse_args()
    
    success, avg_time, message = evaluate_test_results(args.artifacts_dir)
    
    # Print results
    print(message)
    
    # Write to GitHub Actions output file if specified
    if args.github_output:
        with open(args.github_output, 'a') as f:
            f.write(f'success={str(success).lower()}\n')
            f.write(f'avg_time={avg_time}\n')
            
    # Exit with appropriate status code
    exit(0 if success else 1)

if __name__ == '__main__':
    main() 