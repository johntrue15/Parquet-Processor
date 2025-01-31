import os
import glob
import pandas as pd
import argparse
import logging
from pathlib import Path
from typing import Tuple, List, Optional
import sys
import traceback

def setup_logging(level: int = logging.INFO) -> logging.Logger:
    """Configure logging for the script"""
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def validate_parquet_file(file_path: Path) -> Tuple[bool, str]:
    """Validate parquet file structure"""
    required_columns = {'url', 'processing_time', 'error', 'batch_index'}
    try:
        df = pd.read_parquet(file_path)
        missing_cols = required_columns - set(df.columns)
        if missing_cols:
            return False, f"Missing required columns: {missing_cols}"
        return True, "Valid parquet file"
    except Exception as e:
        return False, f"Error validating parquet file: {str(e)}"

def evaluate_test_results(artifacts_dir: str, logger: logging.Logger) -> Tuple[bool, float, str]:
    """
    Evaluate test results from parquet files
    Returns: (success, avg_time, message)
    """
    success = False
    avg_time = 0
    message: List[str] = []
    
    try:
        artifacts_path = Path(artifacts_dir)
        if not artifacts_path.exists():
            return False, 0, f"Artifacts directory not found: {artifacts_dir}"
            
        logger.info(f"Checking directory: {artifacts_dir}")
        message.append(f"Checking directory: {artifacts_dir}")
        
        # Search recursively for parquet files
        parquet_files = []
        for root, dirs, files in os.walk(artifacts_dir):
            logger.debug(f"Scanning {root}")
            message.append(f"Scanning {root}")
            message.append(f"Found directories: {dirs}")
            message.append(f"Found files: {files}")
            for file in files:
                if file.endswith('.parquet'):
                    full_path = Path(root) / file
                    # Validate file before adding
                    is_valid, validation_msg = validate_parquet_file(full_path)
                    if is_valid:
                        parquet_files.append(full_path)
                        message.append(f"Found valid parquet file: {full_path}")
                    else:
                        message.append(f"Invalid parquet file {full_path}: {validation_msg}")
        
        if not parquet_files:
            message.append("No valid parquet files found")
            return False, 0, "\n".join(message)
            
        # Process all valid parquet files
        dfs = []
        for file in parquet_files:
            try:
                df = pd.read_parquet(file)
                dfs.append(df)
                message.append(f"Successfully read parquet file: {file}")
                message.append(f"DataFrame shape: {df.shape}")
                message.append(f"Columns: {df.columns.tolist()}")
            except Exception as e:
                logger.error(f"Error reading {file}: {e}")
                message.append(f"Error reading parquet file: {str(e)}")
        
        if not dfs:
            return False, 0, "\n".join(message)
            
        # Combine all dataframes
        df = pd.concat(dfs, ignore_index=True)
        records = len(df)
        avg_time = df.processing_time.mean()
        errors = df['error'].notna().sum()
        
        # Add detailed statistics
        message.append(
            f'\nTest Results:'
            f'\nTotal records processed: {records}'
            f'\nAverage processing time: {avg_time:.2f}s'
            f'\nMedian processing time: {df.processing_time.median():.2f}s'
            f'\nMax processing time: {df.processing_time.max():.2f}s'
            f'\nRecords with errors: {errors}'
            f'\nError rate: {(errors/records*100):.1f}%'
        )
        
        # Success criteria
        criteria = {
            'records_sufficient': records >= 8,
            'time_acceptable': avg_time < 30,
            'error_rate_acceptable': errors/records < 0.2
        }
        
        success = all(criteria.values())
        
        message.append(f'\nTest evaluation: {"SUCCESS" if success else "FAILED"}')
        if not success:
            message.append(
                f'\nCriteria Results:'
                f'\n- Records >= 8: {criteria["records_sufficient"]}'
                f'\n- Avg time < 30s: {criteria["time_acceptable"]}'
                f'\n- Error rate < 20%: {criteria["error_rate_acceptable"]}'
            )
        
    except Exception as e:
        logger.error(f"Error evaluating test: {e}")
        logger.error(traceback.format_exc())
        message.append(f'Error evaluating test: {str(e)}')
        if artifacts_dir and Path(artifacts_dir).exists():
            message.append(f'Directory contents: {os.listdir(artifacts_dir)}')
        success = False
        avg_time = 0
        
    return success, avg_time, "\n".join(message)

def main():
    parser = argparse.ArgumentParser(description='Evaluate test run results')
    parser.add_argument('--artifacts-dir', required=True, help='Directory containing test artifacts')
    parser.add_argument('--github-output', help='GitHub Actions output file')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    args = parser.parse_args()
    
    logger = setup_logging(level=logging.DEBUG if args.debug else logging.INFO)
    
    try:
        success, avg_time, message = evaluate_test_results(args.artifacts_dir, logger)
        
        # Print results
        print(message)
        
        # Write to GitHub Actions output file if specified
        if args.github_output:
            with open(args.github_output, 'a') as f:
                f.write(f'success={str(success).lower()}\n')
                f.write(f'avg_time={avg_time}\n')
                
        # Exit with appropriate status code
        sys.exit(0 if success else 1)
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == '__main__':
    main() 