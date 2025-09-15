#!/usr/bin/env python3
"""
Wikipedia Header Validator - Main Script
"""

import os
import argparse
import logging
from wiki_headers_function import process_file, create_summary_report

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("header_validation.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def process_directory(input_dir, text_output_dir, report_output_dir, language='vi', json_dir=None, perfect_match_dir=None):
    """Process all annotated files in a directory"""
    # Create output directories
    os.makedirs(text_output_dir, exist_ok=True)
    os.makedirs(report_output_dir, exist_ok=True)
    
    # Create perfect match directory if specified
    if perfect_match_dir:
        os.makedirs(perfect_match_dir, exist_ok=True)
        logger.info(f"Created directory for perfect matches: {perfect_match_dir}")
    
    # Get all annotated text files
    files = [
        os.path.join(input_dir, f) for f in os.listdir(input_dir) 
        if f.startswith("annotated_") and f.endswith(".txt") 
        and os.path.isfile(os.path.join(input_dir, f))
    ]
    
    if not files:
        logger.error(f"No annotated files found in {input_dir}")
        return {'error': 'No annotated files found'}
    
    logger.info(f"Processing {len(files)} annotated files...")
    
    # Process each file
    results = []
    perfect_matches = []
    for file_path in files:
        logger.info(f"Processing {os.path.basename(file_path)}")
        result = process_file(file_path, text_output_dir, report_output_dir, language, json_dir)
        results.append(result)
        
        # Check if this is a perfect match (100%)
        if 'error' not in result and result.get('match_percentage', 0) == 100:
            perfect_matches.append(result)
            logger.info(f"Found perfect match (100%): {os.path.basename(file_path)}")
            
            # Copy to perfect match directory if specified
            if perfect_match_dir and result.get('updated_file'):
                import shutil
                filename = os.path.basename(result['updated_file'])
                dest_path = os.path.join(perfect_match_dir, filename)
                try:
                    shutil.copy2(result['updated_file'], dest_path)
                    logger.info(f"Copied perfect match to: {dest_path}")
                except Exception as e:
                    logger.error(f"Error copying perfect match file: {str(e)}")
    
    # Create a comprehensive summary report
    summary_path = create_summary_report(results, report_output_dir)
    
    # Create a separate report just for perfect matches if any were found
    perfect_match_report = None
    if perfect_matches and perfect_match_dir:
        perfect_match_report = os.path.join(perfect_match_dir, "perfect_matches.txt")
        try:
            with open(perfect_match_report, 'w', encoding='utf-8') as f:
                f.write("PERFECT MATCH FILES (100% MATCH PERCENTAGE)\n")
                f.write("=" * 60 + "\n\n")
                f.write(f"Total files with perfect matches: {len(perfect_matches)}\n\n")
                
                for i, match in enumerate(perfect_matches, 1):
                    file_info = match['file_info']
                    wiki_results = match['wiki_results']
                    
                    f.write(f"{i}. {file_info['filename']}\n")
                    f.write(f"   Wiki title: {wiki_results.get('title', 'Unknown')}\n")
                    f.write(f"   Wiki URL: {wiki_results.get('url', 'Unknown')}\n")
                    f.write(f"   Headers count: {len(wiki_results['headers'])}\n")
                    f.write(f"   Updated file: {match.get('updated_file', 'Unknown')}\n")
                    f.write("\n")
            
            logger.info(f"Perfect match report saved to: {perfect_match_report}")
        except Exception as e:
            logger.error(f"Error creating perfect match report: {str(e)}")
    
    return {
        'results': results, 
        'summary': summary_path,
        'perfect_matches': perfect_matches,
        'perfect_match_count': len(perfect_matches),
        'perfect_match_report': perfect_match_report
    }

def main():
    """Main entry point for the script"""
    parser = argparse.ArgumentParser(description="Validate and update headers against Wikipedia articles")
    parser.add_argument("--input", type=str, required=True, 
                        help="Input file or directory containing annotated files")
    parser.add_argument("--text-output", type=str, default="updated_texts", 
                        help="Output directory for updated text files (default: updated_texts)")
    parser.add_argument("--report-output", type=str, default="header_reports", 
                        help="Output directory for reports and summary (default: header_reports)")
    parser.add_argument("--json-dir", type=str, 
                        help="Directory containing JSON files with article info (default: same as input)")
    parser.add_argument("--language", type=str, default="vi", 
                        help="Wikipedia language code (default: vi for Vietnamese)")
    parser.add_argument("--single-file", action="store_true", 
                        help="Input is a single file (not a directory)")
    parser.add_argument("--perfect-matches", type=str, 
                        help="Directory to store files with 100% match percentage")
    
    args = parser.parse_args()
    
    if args.single_file:
        if not os.path.isfile(args.input):
            logger.error(f"Input file not found: {args.input}")
            return 1
            
        result = process_file(args.input, args.text_output, args.report_output, args.language, args.json_dir)
        if 'error' in result:
            logger.error(f"Error processing file: {result['error']}")
            return 1
            
        logger.info(f"File processing complete")
        logger.info(f"Updated text saved to: {result.get('updated_file', 'unknown')}")
        logger.info(f"Report saved to: {result.get('report_file', 'unknown')}")
        
        # Handle perfect match for single file
        if args.perfect_matches and result.get('match_percentage', 0) == 100:
            import shutil
            os.makedirs(args.perfect_matches, exist_ok=True)
            
            if result.get('updated_file'):
                filename = os.path.basename(result['updated_file'])
                dest_path = os.path.join(args.perfect_matches, filename)
                try:
                    shutil.copy2(result['updated_file'], dest_path)
                    logger.info(f"Perfect match! File copied to: {dest_path}")
                except Exception as e:
                    logger.error(f"Error copying perfect match file: {str(e)}")
    else:
        if not os.path.isdir(args.input):
            logger.error(f"Input directory not found: {args.input}")
            return 1
            
        result = process_directory(
            args.input, 
            args.text_output, 
            args.report_output, 
            args.language, 
            args.json_dir,
            args.perfect_matches
        )
        
        if 'error' in result:
            logger.error(f"Error processing directory: {result['error']}")
            return 1
            
        logger.info(f"Directory processing complete")
        logger.info(f"Updated text files saved to: {args.text_output}")
        logger.info(f"Reports saved to: {args.report_output}")
        logger.info(f"Summary report: {result.get('summary', 'unknown')}")
        
        if args.perfect_matches:
            logger.info(f"Perfect matches (100%): {result.get('perfect_match_count', 0)}")
            if result.get('perfect_match_count', 0) > 0:
                logger.info(f"Perfect match files saved to: {args.perfect_matches}")
                logger.info(f"Perfect match report: {result.get('perfect_match_report', 'unknown')}")
    
    return 0

if __name__ == "__main__":
    exit(main())