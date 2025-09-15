#!/usr/bin/env python3
"""
Document Validator Script (Simplified)

This script processes Vietnamese Wikipedia JSON files to validate entire documents for
appropriateness in educational question generation using OpenAI API batch processing.
It identifies and marks documents containing sensitive topics, political issues,
or content unsuitable for generating educational questions.
"""

import os
import argparse
import logging
import time
import json
import subprocess
import re

# Import helper module
from segment_validator_helper import process_directory_for_validation, process_validation_results

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("document_validation.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def run_openai_client(command: str, *args) -> dict:
    """
    Run the openai_client.py script with the given command and arguments.
    
    Args:
        command: The command to run (upload, start, check, or result)
        *args: Additional arguments for the command
        
    Returns:
        Dictionary with output or error information
    """
    try:
        cmd = ["python", "openai_client.py", command] + list(args)
        logger.info(f"Running command: {' '.join(cmd)}")
        
        process = subprocess.run(cmd, capture_output=True, text=True)
        
        if process.returncode != 0:
            logger.error(f"Command failed with return code {process.returncode}")
            logger.error(f"Error output: {process.stderr}")
            return {"error": process.stderr}
        
        output = process.stdout
        result = {"output": output}
        
        # Try to extract useful information based on the command
        if command == "upload":
            file_id_match = re.search(r"Uploaded file ID: (.*)", output)
            if file_id_match:
                result["file_id"] = file_id_match.group(1).strip()
        
        elif command == "start":
            batch_id_match = re.search(r"Batch process started - ID: (.*)", output)
            if batch_id_match:
                result["batch_id"] = batch_id_match.group(1).strip()
        
        elif command == "check":
            status_match = re.search(r"Status: (.*)", output)
            if status_match:
                result["status"] = status_match.group(1).strip()
                
            output_file_match = re.search(r"Output file ID: (.*?)(\s|$)", output)
            if output_file_match:
                output_file_id = output_file_match.group(1).strip()
                if output_file_id != "Not available yet":
                    result["output_file_id"] = output_file_id
        
        return result
        
    except Exception as e:
        logger.error(f"Error running openai_client: {str(e)}")
        return {"error": str(e)}

def wait_for_batch_completion(batch_id: str) -> dict:
    """
    Wait for a batch process to complete, checking every 60 seconds.
    """
    logger.info(f"Waiting for batch {batch_id} to complete...")
    
    while True:
        # Check batch status
        result = run_openai_client("check", batch_id)
        
        if "error" in result:
            logger.error(f"Error checking batch status: {result['error']}")
            return result
        
        # Check if we have a status
        status = result.get("status")
        if not status:
            status_match = re.search(r"Status: (.*)", result["output"])
            if status_match:
                status = status_match.group(1).strip()
            else:
                logger.error("Could not extract status from response")
                return {"error": "Could not extract status"}
        
        logger.info(f"Batch status: {status}")
        
        # Check if we're done or failed
        if status == "completed":
            logger.info("Batch processing complete!")
            
            # Get output file ID if available
            if "output_file_id" in result:
                return result
                
            # Try to extract it from output
            output_file_match = re.search(r"Output file ID: (.*?)(\s|$)", result["output"])
            if output_file_match:
                output_file_id = output_file_match.group(1).strip()
                if output_file_id != "Not available yet":
                    result["output_file_id"] = output_file_id
                    return result
            
            logger.error("Batch completed but output file ID not found")
            return {"error": "Output file ID not found"}
            
        elif status in ["failed", "cancelled"]:
            logger.error(f"Batch processing {status}")
            return {"error": f"Batch processing {status}", "status": status}
        
        # Wait before checking again
        logger.info("Waiting 60 seconds before checking again...")
        time.sleep(60)

def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Validate Vietnamese Wikipedia documents for question generation suitability")
    parser.add_argument("--input", required=True, help="Directory containing JSON files")
    parser.add_argument("--output", required=True, help="Output directory for working files")
    parser.add_argument("--max-files", type=int, help="Maximum number of files to process")
    
    # Add batch configuration arguments
    parser.add_argument("--model", default="gpt-3.5-turbo",
                        help="OpenAI model to use for batch processing (default: gpt-3.5-turbo)")
    parser.add_argument("--max-tokens", type=int, default=1000,
                        help="Maximum tokens for each API response")
    parser.add_argument("--temperature", type=float, default=0.0,
                        help="Temperature for model responses (should be low for consistent validation)")
    
    args = parser.parse_args()
    
    # Create output directory
    os.makedirs(args.output, exist_ok=True)
    
    # File paths
    batch_jsonl = os.path.join(args.output, "validation_requests.jsonl")
    mapping_file = os.path.join(args.output, "validation_id_to_doc_mapping.json")
    results_file = os.path.join(args.output, "document_validation_results.json")
    
    # Process directory to prepare batch file
    logger.info("Processing input directory and preparing validation batch file...")
    id_to_doc_map, documents = process_directory_for_validation(
        args.input, 
        batch_jsonl, 
        args.model,
        args.max_tokens,
        args.temperature,
        args.max_files
    )
    
    # Save mapping to a file for later use
    with open(mapping_file, 'w', encoding='utf-8') as f:
        json.dump(id_to_doc_map, f, ensure_ascii=False, indent=2)
    
    logger.info(f"Created batch file with {len(documents)} documents to validate")
    logger.info(f"ID to document mapping saved to {mapping_file}")
    
    # Start batch processing for validation
    logger.info("Starting batch validation...")
    
    # Upload the file
    upload_result = run_openai_client("upload", batch_jsonl)
    
    if "error" in upload_result:
        logger.error(f"Error uploading batch file: {upload_result['error']}")
        return
    
    file_id = upload_result.get("file_id")
    if not file_id:
        file_id_match = re.search(r"Uploaded file ID: (.*)", upload_result["output"])
        if file_id_match:
            file_id = file_id_match.group(1).strip()
        else:
            logger.error("Failed to get file ID from upload result")
            return
    
    logger.info(f"Validation batch file uploaded with ID: {file_id}")
    
    # Start the batch
    start_result = run_openai_client("start", file_id)
    
    if "error" in start_result:
        logger.error(f"Error starting batch validation: {start_result['error']}")
        return
    
    batch_id = start_result.get("batch_id")
    if not batch_id:
        batch_id_match = re.search(r"Batch process started - ID: (.*)", start_result["output"])
        if batch_id_match:
            batch_id = batch_id_match.group(1).strip()
        else:
            logger.error("Failed to get batch ID from start result")
            return
    
    logger.info(f"Batch validation started with ID: {batch_id}")
    
    # Save the batch ID to a file for future reference
    with open(os.path.join(args.output, "validation_batch_id.txt"), 'w') as f:
        f.write(batch_id)
    
    # Wait for completion
    completion_result = wait_for_batch_completion(batch_id)
    
    if "error" in completion_result:
        logger.error(f"Error in batch completion: {completion_result['error']}")
        return
    
    output_file_id = completion_result.get("output_file_id")
    if not output_file_id:
        logger.error("No output file ID found after batch completion")
        return
    
    logger.info(f"Batch completed with output file ID: {output_file_id}")
    
    # Save output file ID for future reference
    with open(os.path.join(args.output, "validation_output_file_id.txt"), 'w') as f:
        f.write(output_file_id)
    
    # Get and process results
    logger.info("Retrieving validation results...")
    result_output = run_openai_client("result", output_file_id)
    
    if "error" in result_output:
        logger.error(f"Error retrieving validation results: {result_output['error']}")
        return
    
    # Process and save validation results
    results = process_validation_results(
        result_output["output"], 
        id_to_doc_map,
        results_file
    )
    
    logger.info(f"Validation results saved to: {results_file}")
    logger.info(f"Validated {results['stats']['documents_validated']} documents")
    logger.info(f"Rejected {results['stats']['documents_rejected']} documents")
    logger.info(f"Affected {results['stats']['segments_affected']} segments")

if __name__ == "__main__":
    main()