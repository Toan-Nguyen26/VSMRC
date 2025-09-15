#!/usr/bin/env python3
"""
Wiki Segment Verifier

This script processes an XML file containing Wiki segments and verifies their suitability
for educational content. It processes segments in small batches using OpenAI or DeepSeek API.

Usage:
    python wiki_segment_verifier.py --input input.xml --output output.xml --model openai --batch-size 5
"""

import os
import argparse
import logging
import time
from typing import Dict, Any
import json
from datetime import datetime
import xml.etree.ElementTree as ET

# Import helper functions
from verification_helper import (
    load_xml_file,
    extract_segments_from_wiki_xml,
    process_segments_in_batches,
    update_xml_with_results,
    save_updated_xml
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("wiki_segment_verification.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Verify Wiki segments for educational suitability")
    parser.add_argument("--input", required=True, help="Input XML file with Wiki segments")
    parser.add_argument("--output", required=True, help="Output XML file with verification results")
    parser.add_argument("--model", choices=["openai", "deepseek"], default="openai", 
                      help="Model to use for verification (default: openai)")
    parser.add_argument("--model-name", help="Specific model name (default: gpt-3.5-turbo for OpenAI, deepseek-chat for DeepSeek)")
    parser.add_argument("--batch-size", type=int, default=5, 
                      help="Number of segments to process in each API call (default: 5)")
    parser.add_argument("--rate-limit", type=int, default=10, 
                      help="Maximum requests per minute (default: 10 for OpenAI, 20 for DeepSeek)")
    parser.add_argument("--max-segments", type=int, help="Maximum number of segments to process")
    parser.add_argument("--summary", help="Path to save summary JSON file")
    
    args = parser.parse_args()
    
    # Validate input file
    if not os.path.isfile(args.input):
        logger.error(f"Input file not found: {args.input}")
        return 1
    
    # Load XML file
    tree = load_xml_file(args.input)
    if tree is None:
        logger.error(f"Failed to load XML file: {args.input}")
        return 1
    
    # Extract segments
    segments = extract_segments_from_wiki_xml(args.input)
    
    if not segments:
        logger.error("No segments found in input XML")
        return 1
    
    # Apply max_segments limit if specified
    if args.max_segments is not None and args.max_segments < len(segments):
        logger.info(f"Limiting to {args.max_segments} segments")
        segments = segments[:args.max_segments]
    
    # Set rate limit (DeepSeek can handle higher rates)
    rate_limit = args.rate_limit
    if args.model == "deepseek" and args.rate_limit == 10:
        rate_limit = 20  # Default to higher rate for DeepSeek if not specified
    
    # Record start time
    start_time = time.time()
    
    # Set model name if not specified
    model_name = args.model_name
    if not model_name:
        model_name = "gpt-4o-mini" if args.model == "openai" else "deepseek-chat"
    
    logger.info(f"Starting verification with {args.model} ({model_name}) at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Processing {len(segments)} segments in batches of {args.batch_size}")
    
    # Process segments in batches
    results = process_segments_in_batches(
        segments,
        args.batch_size,
        args.model,
        model_name,
        rate_limit
    )
    
    # Calculate processing time
    processing_time = time.time() - start_time
    
    # Calculate statistics
    total_segments = len(segments)
    successful_verifications = sum(1 for r in results if r.get("success", False))
    suitable_segments = sum(1 for r in results if r.get("success", False) and r.get("is_suitable", False))
    unsuitable_segments = sum(1 for r in results if r.get("success", False) and not r.get("is_suitable", False))
    verification_errors = total_segments - successful_verifications
    
    # Update XML with results
    updated_tree = update_xml_with_results(tree, results)
    
    # Save updated XML
    save_updated_xml(updated_tree, args.output)
    
    # Create statistics summary
    statistics = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "model": args.model,
        "model_name": model_name,
        "input_file": os.path.abspath(args.input),
        "output_file": os.path.abspath(args.output),
        "total_segments": total_segments,
        "successful_verifications": successful_verifications,
        "suitable_segments": suitable_segments,
        "unsuitable_segments": unsuitable_segments,
        "verification_errors": verification_errors,
        "processing_time_seconds": processing_time,
        "processing_time": f"{processing_time:.2f} seconds",
        "average_time_per_segment": f"{(processing_time/total_segments):.2f} seconds",
        "average_time_per_batch": f"{(processing_time/(total_segments/args.batch_size)):.2f} seconds"
    }
    
    # Log summary
    logger.info(f"Verification completed in {processing_time:.2f} seconds")
    logger.info(f"Segments processed: {total_segments}")
    logger.info(f"Successful verifications: {successful_verifications}")
    logger.info(f"Suitable segments: {suitable_segments}")
    logger.info(f"Unsuitable segments: {unsuitable_segments}")
    logger.info(f"Verification errors: {verification_errors}")
    logger.info(f"Results saved to: {args.output}")
    
    # Save summary if requested
    if args.summary:
        try:
            with open(args.summary, 'w', encoding='utf-8') as f:
                json.dump(statistics, f, indent=2)
            logger.info(f"Summary saved to: {args.summary}")
        except Exception as e:
            logger.error(f"Error saving summary: {str(e)}")
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())