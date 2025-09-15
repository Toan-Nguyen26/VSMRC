#!/usr/bin/env python3
"""
Wiki QA Main Script (XML Version) - Gemini API

This script processes an XML file containing Wikipedia segments to generate question-answer pairs
using the Gemini API. It extracts valid segments, processes them in small batches,
and integrates results back into the XML structure.

Unlike the OpenAI version, this does not rely on batch processing API but instead
handles segments in smaller groups for direct API calls.
"""

import os
import argparse
import logging
import time
import json
from datetime import datetime

# Import helper module
from question_generator_helper import process_xml_file

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("gemini_wiki_qa.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Generate QA pairs for Wikipedia segments in XML using Gemini API")
    parser.add_argument("--input", required=True, help="Input XML file (e.g., test.xml)")
    parser.add_argument("--output", required=True, help="Output XML file with QA pairs")
    
    # API parameters
    parser.add_argument("--model", default="gemini-2.0-flash-lite",
                        help="Gemini model to use (default: gemini-2.0-flash-lite)")
    parser.add_argument("--max-tokens", type=int, default=1500,
                        help="Maximum tokens for each API response")
    parser.add_argument("--temperature", type=float, default=0.5,
                        help="Temperature for model responses (0.0-1.0)")
    
    # Processing parameters
    parser.add_argument("--batch-size", type=int, default=3,
                        help="Number of segments to process in each API call (default: 3)")
    parser.add_argument("--no-intermediate", action="store_true",
                        help="Don't save intermediate results")
    
    # Output options
    parser.add_argument("--stats-file", 
                        help="Save processing statistics to this JSON file")
    
    args = parser.parse_args()
    
    # Validate input file
    if not os.path.isfile(args.input):
        logger.error(f"Input file not found: {args.input}")
        return 1
    
    # Create output directory if needed
    output_dir = os.path.dirname(args.output)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info(f"Created output directory: {output_dir}")
    
    # Record start time
    start_time = time.time()
    logger.info(f"Starting QA generation with Gemini API at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Using model: {args.model}, batch size: {args.batch_size}")
    
    # Process the XML file
    stats = process_xml_file(
        input_xml=args.input,
        output_xml=args.output,
        model=args.model,
        temperature=args.temperature,
        max_tokens=args.max_tokens,
        batch_size=args.batch_size,
        save_intermediate=not args.no_intermediate
    )
    
    # Calculate total processing time
    processing_time = time.time() - start_time
    stats["processing_time_seconds"] = processing_time
    stats["processing_time"] = f"{processing_time:.2f} seconds"
    
    # Calculate token usage rates and costs per minute
    if "token_usage" in stats and processing_time > 0:
        tokens_per_minute = {
            "prompt_tokens_per_minute": stats["token_usage"]["prompt_tokens"] / (processing_time / 60),
            "output_tokens_per_minute": stats["token_usage"]["output_tokens"] / (processing_time / 60),
            "total_tokens_per_minute": stats["token_usage"]["total_tokens"] / (processing_time / 60)
        }
        stats["tokens_per_minute"] = tokens_per_minute
        
        costs_per_minute = {
            "input_cost_per_minute": stats["cost"]["input_cost"] / (processing_time / 60),
            "output_cost_per_minute": stats["cost"]["output_cost"] / (processing_time / 60),
            "total_cost_per_minute": stats["cost"]["total_cost"] / (processing_time / 60)
        }
        stats["costs_per_minute"] = costs_per_minute
    
    # Log summary
    if "error" in stats:
        logger.error(f"Processing failed: {stats['error']}")
        return 1
    
    logger.info(f"Processing completed in {processing_time:.2f} seconds")
    logger.info(f"Segments processed: {stats['segments_processed']}")
    logger.info(f"Segments updated with QA: {stats['segments_updated']}")
    logger.info(f"Total questions created: {stats['questions_created']}")
    
    # Log token usage and costs
    if "token_usage" in stats:
        logger.info(f"Token usage - Input: {stats['token_usage']['prompt_tokens']}, "
                  f"Output: {stats['token_usage']['output_tokens']}, "
                  f"Total: {stats['token_usage']['total_tokens']}")
        
        # Show costs with more precision for small values
        input_cost = stats["cost"]["input_cost"]
        output_cost = stats["cost"]["output_cost"]
        total_cost = stats["cost"]["total_cost"]
        
        logger.info(f"API costs - Input: ${input_cost:.6f}, "
                  f"Output: ${output_cost:.6f}, "
                  f"Total: ${total_cost:.6f}")
        
        # Show per-million token rates
        input_per_m = (stats['token_usage']['prompt_tokens'] / 1_000_000) * 0.075
        output_per_m = (stats['token_usage']['output_tokens'] / 1_000_000) * 0.3
        
        logger.info(f"Cost per million tokens - Input (@$0.075/M): ${input_per_m:.6f}, "
                  f"Output (@$0.3/M): ${output_per_m:.6f}")
        
        # Show token/cost per minute rates if processing time is significant
        if processing_time >= 10:  # Only show rates if processing took at least 10 seconds
            logger.info(f"Tokens per minute - Input: {tokens_per_minute['prompt_tokens_per_minute']:.1f}, "
                      f"Output: {tokens_per_minute['output_tokens_per_minute']:.1f}, "
                      f"Total: {tokens_per_minute['total_tokens_per_minute']:.1f}")
            
            logger.info(f"Cost per minute - Input: ${costs_per_minute['input_cost_per_minute']:.6f}, "
                      f"Output: ${costs_per_minute['output_cost_per_minute']:.6f}, "
                      f"Total: ${costs_per_minute['total_cost_per_minute']:.6f}")
    
    # Save statistics if requested
    if args.stats_file:
        try:
            with open(args.stats_file, 'w', encoding='utf-8') as f:
                # Add timestamp to statistics
                stats["timestamp"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                stats["input_file"] = args.input
                stats["output_file"] = args.output
                stats["model"] = args.model
                
                # Add pricing info for reference
                stats["pricing"] = {
                    "input_per_million_tokens": 0.075,  # $0.075 per million input tokens
                    "output_per_million_tokens": 0.3,   # $0.3 per million output tokens
                }
                
                json.dump(stats, f, indent=2)
            logger.info(f"Saved processing statistics to {args.stats_file}")
        except Exception as e:
            logger.error(f"Error saving statistics file: {str(e)}")
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())