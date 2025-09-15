#!/usr/bin/env python3
"""
JSON Handler for Wikipedia Segmented Documents

This script processes TXT files containing segmented Wikipedia articles and updates
corresponding JSON files with structured segment information for QA tasks.

Usage:
    python json_handler.py --txt_file path/to/file.txt --json_file path/to/file.json --output_file path/to/output.json
    python json_handler.py --txt_dir path/to/txt/dir --json_dir path/to/json/dir --output_dir path/to/output/dir
    python json_handler.py --txt_file path/to/file.txt --json_file path/to/file.json --log_only
"""

import os
import re
import json
import argparse
import logging
from datetime import datetime
from underthesea import sent_tokenize

def setup_logger(log_file=None):
    """Set up logging to both console and file if specified"""
    handlers = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file, 'w', encoding='utf-8'))
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=handlers
    )
    return logging.getLogger(__name__)

def extract_segments(text):
    """
    Extract segments from text that follows the format:
    ========,level,title.
    content
    
    Args:
        text (str): Text with segments marked
        
    Returns:
        list: List of dictionaries with segment information
    """
    # Split text by segment markers
    pattern = r'(========,\d+,[^.]+\.)'
    parts = re.split(pattern, text)
    
    # If the text doesn't start with a marker, the first part is empty or preface text
    segments = []
    current_marker = None
    
    for i, part in enumerate(parts):
        if re.match(pattern, part):
            # This is a marker
            current_marker = part
        elif i > 0 and current_marker:
            # This is content following a marker
            marker_parts = current_marker.replace("========,", "").split(",", 1)
            
            if len(marker_parts) == 2:
                title = marker_parts[1].strip().rstrip(".")
            else:
                # Handle irregular markers
                title = current_marker.replace("========,", "").strip()
            
            content = part.strip()
            # Count sentences using underthesea
            sentences = sent_tokenize(content)
            
            # Add the segment info
            segments.append({
                "title": title,
                "content": content,
                "char_count": len(content),
                "sentence_count": len(sentences)
            })
    
    # Handle case where text doesn't have markers but might still be content
    if not segments and text.strip():
        # Treat the entire text as a single segment (preface)
        content = text.strip()
        sentences = sent_tokenize(content)
        segments.append({
            "title": "preface",
            "content": content,
            "char_count": len(content),
            "sentence_count": len(sentences)
        })
    
    return segments

def count_segments(segments):
    """
    Count segments
    
    Args:
        segments (list): List of segment dictionaries
        
    Returns:
        int: Number of segments
    """
    return len(segments)

def get_matching_json_file(txt_file, json_dir):
    """
    Find a matching JSON file for a given TXT file
    
    Args:
        txt_file (str): Path to the TXT file
        json_dir (str): Directory containing JSON files
        
    Returns:
        str: Path to the matching JSON file, or None if not found
    """
    base_name = os.path.splitext(os.path.basename(txt_file))[0]
    json_file = os.path.join(json_dir, f"{base_name}.json")
    
    if os.path.exists(json_file):
        return json_file
    
    # Try case-insensitive matching
    for file in os.listdir(json_dir):
        if file.lower() == f"{base_name.lower()}.json":
            return os.path.join(json_dir, file)
    
    # Try fuzzy matching - if the base name is contained in any JSON file name
    for file in os.listdir(json_dir):
        if file.endswith('.json') and base_name.lower() in file.lower():
            return os.path.join(json_dir, file)
    
    return None

def update_json_with_segments(txt_file, json_file, output_file=None, log_only=False, min_char=500, max_char=10000, ignore_preface=False):
    """
    Process a TXT file and update the corresponding JSON file
    
    Args:
        txt_file (str): Path to the segmented TXT file
        json_file (str): Path to the JSON file to update
        output_file (str): Path to save the updated JSON (default: overwrite json_file)
        log_only (bool): If True, only log information without writing output files
        min_char (int): Minimum character count for segments to not be rejected
        max_char (int): Maximum character count for segments to not be rejected
        ignore_preface (bool): If True, automatically reject all preface segments for QA
        
    Returns:
        bool: Success status
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Read the TXT file
        with open(txt_file, 'r', encoding='utf-8') as f:
            text = f.read()
        
        # Read the JSON file
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Extract segments from TXT
        segments = extract_segments(text)
        
        # Create the segment field
        segment_field = {
            "text": text,
            "count": count_segments(segments),
            "segments": segments
        }
        
        # Create the multi field
        multi_field = {}
        file_name = os.path.basename(txt_file)
        
        if "." in file_name:
            file_name = file_name.split(".")[0]  # Remove extension
        
        # Track rejection counts by reason
        min_char_rejected = 0
        max_char_rejected = 0
        preface_segments = 0
        preface_auto_rejected = 0
        total_segments = len(segments)
        
        for i, segment in enumerate(segments):
            segment_id = f"{file_name}_{i+1:02d}"  # Zero-padded ID starting from 01
            
            # Check if this is a preface segment
            is_preface = segment["title"].lower() == "preface"
            if is_preface:
                preface_segments += 1
            
            # Check if segment should be rejected
            is_rejected = False
            rejection_reason = None
            
            # Case 1: Preface segments when ignore_preface is True - always reject
            if is_preface and ignore_preface:
                is_rejected = True
                rejection_reason = "Preface segment (automatically rejected)"
                preface_auto_rejected += 1
            # Case 2: Apply normal character limit checks for other segments or prefaces when not ignoring
            else:
                if segment["char_count"] < min_char:
                    is_rejected = True
                    rejection_reason = f"Less than {min_char} characters"
                    min_char_rejected += 1
                elif segment["char_count"] > max_char:
                    is_rejected = True
                    rejection_reason = f"More than {max_char} characters"
                    max_char_rejected += 1
            
            multi_field[segment_id] = {
                "segment_title": segment["title"],
                "segment_text": segment["content"],
                "char_count": segment["char_count"],
                "sentence_count": segment["sentence_count"],
                "rejected_for_qa": is_rejected,
                "reason_for_rejected": rejection_reason,
                "qa": []  # Initialize empty QA list
            }
        
        # Add/update the fields in the JSON data
        data["segment"] = segment_field
        data["multi"] = multi_field
        
        # Calculate total rejected and accepted segments
        total_rejected = min_char_rejected + max_char_rejected + preface_auto_rejected
        accepted_segments = total_segments - total_rejected
        rejection_rate = (total_rejected / total_segments * 100) if total_segments > 0 else 0
        
        # Log information
        logger.info(f"Processed: {txt_file}")
        logger.info(f"Character limits: min={min_char}, max={max_char}")
        logger.info(f"Ignore preface segments: {'Yes' if ignore_preface else 'No'}")
        logger.info(f"Total segments: {total_segments}")
        logger.info(f"  - Regular segments: {total_segments - preface_segments}")
        logger.info(f"  - Preface segments: {preface_segments}")
        logger.info(f"Accepted segments: {accepted_segments} ({100 - rejection_rate:.1f}%)")
        logger.info(f"Rejected segments: {total_rejected} ({rejection_rate:.1f}%)")
        logger.info(f"  - Too short (<{min_char} chars): {min_char_rejected}")
        logger.info(f"  - Too long (>{max_char} chars): {max_char_rejected}")
        if preface_segments > 0:
            logger.info(f"  - Preface segments rejected: {preface_auto_rejected}")
        logger.info(f"Total sentences: {sum(s['sentence_count'] for s in segments)}")
        
        # Write updated JSON if not log_only mode
        if not log_only:
            # Determine output path
            if not output_file:
                output_file = json_file
            
            # Write updated JSON
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Updated JSON file: {output_file}")
        else:
            logger.info(f"Log only mode: No file was written")
        
        return True
    
    except Exception as e:
        logger.error(f"Error processing {txt_file} -> {json_file}: {str(e)}")
        return False
        
        # Write updated JSON if not log_only mode
        if not log_only:
            # Determine output path
            if not output_file:
                output_file = json_file
            
            # Write updated JSON
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Updated JSON file: {output_file}")
        else:
            logger.info(f"Log only mode: No file was written")
        
        return True
    
    except Exception as e:
        logger.error(f"Error processing {txt_file} -> {json_file}: {str(e)}")
        return False

def process_directory(txt_dir, json_dir, output_dir=None, log_only=False, min_char=500, max_char=10000, ignore_preface=False):
    """
    Process all TXT files in a directory and update corresponding JSON files
    
    Args:
        txt_dir (str): Directory containing segmented TXT files
        json_dir (str): Directory containing JSON files to update
        output_dir (str): Directory to save updated JSON files (default: overwrite original)
        log_only (bool): If True, only log information without writing output files
        min_char (int): Minimum character count for segments to not be rejected
        max_char (int): Maximum character count for segments to not be rejected
        ignore_preface (bool): If True, automatically reject all preface segments for QA
        
    Returns:
        tuple: (success_count, failure_count, skipped_count)
    """
    logger = logging.getLogger(__name__)
    
    if not os.path.exists(txt_dir) or not os.path.exists(json_dir):
        logger.error(f"Directory not found: {txt_dir if not os.path.exists(txt_dir) else json_dir}")
        return (0, 0, 0)
    
    # Create output directory if needed and not in log_only mode
    if not log_only and output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Find all TXT files
    txt_files = [f for f in os.listdir(txt_dir) if f.endswith('.txt') and os.path.isfile(os.path.join(txt_dir, f))]
    
    if not txt_files:
        logger.warning(f"No TXT files found in {txt_dir}")
        return (0, 0, 0)
    
    # Log character limit settings
    logger.info(f"Character limits: min={min_char}, max={max_char}")
    logger.info(f"Ignore preface segments: {'Yes' if ignore_preface else 'No'}")
    logger.info(f"Log only mode: {'Yes' if log_only else 'No'}")
    
    # Process each file
    success_count = 0
    failure_count = 0
    skipped_count = 0
    
    # Aggregate statistics
    total_segments = 0
    total_accepted = 0
    total_rejected_short = 0
    total_rejected_long = 0
    total_preface_segments = 0
    total_preface_auto_rejected = 0
    
    for txt_filename in txt_files:
        txt_path = os.path.join(txt_dir, txt_filename)
        
        # Find corresponding JSON file
        json_path = get_matching_json_file(txt_path, json_dir)
        
        if not json_path:
            logger.warning(f"No matching JSON file found for {txt_filename}")
            skipped_count += 1
            continue
        
        # Determine output path (only relevant if not log_only)
        output_path = None
        if not log_only and output_dir:
            output_path = os.path.join(output_dir, os.path.basename(json_path))
        
        # Update JSON with segments from TXT
        if update_json_with_segments(txt_path, json_path, output_path, log_only, min_char, max_char, ignore_preface):
            success_count += 1
            
            # Collect statistics for this file
            with open(txt_path, 'r', encoding='utf-8') as f:
                text = f.read()
            segments = extract_segments(text)
            
            file_total_segments = len(segments)
            file_preface_segments = sum(1 for s in segments if s["title"].lower() == "preface")
            
            # Apply the same rejection logic as in update_json_with_segments
            file_rejected_short = 0
            file_rejected_long = 0
            file_preface_auto_rejected = 0
            
            for segment in segments:
                is_preface = segment["title"].lower() == "preface"
                
                # Case 1: Preface segments when ignore_preface is True - always reject
                if is_preface and ignore_preface:
                    file_preface_auto_rejected += 1
                # Case 2: Apply normal character limit checks
                else:
                    if segment["char_count"] < min_char:
                        file_rejected_short += 1
                    elif segment["char_count"] > max_char:
                        file_rejected_long += 1
            
            file_accepted = file_total_segments - file_rejected_short - file_rejected_long - file_preface_auto_rejected
            
            # Add to aggregate statistics
            total_segments += file_total_segments
            total_accepted += file_accepted
            total_rejected_short += file_rejected_short
            total_rejected_long += file_rejected_long
            total_preface_segments += file_preface_segments
            total_preface_auto_rejected += file_preface_auto_rejected
            
        else:
            failure_count += 1
    
    # Calculate aggregate rates
    total_rejected = total_rejected_short + total_rejected_long + total_preface_auto_rejected
    rejection_rate = (total_rejected / total_segments * 100) if total_segments > 0 else 0
    acceptance_rate = (total_accepted / total_segments * 100) if total_segments > 0 else 0
    
    # Log summary information
    logger.info(f"Processed {len(txt_files)} files: {success_count} succeeded, {failure_count} failed, {skipped_count} skipped")
    logger.info(f"Summary statistics across all files:")
    logger.info(f"  - Total segments: {total_segments}")
    logger.info(f"    * Regular segments: {total_segments - total_preface_segments}")
    logger.info(f"    * Preface segments: {total_preface_segments}")
    logger.info(f"  - Accepted segments: {total_accepted} ({acceptance_rate:.1f}%)")
    logger.info(f"  - Rejected segments: {total_rejected} ({rejection_rate:.1f}%)")
    logger.info(f"    * Too short (<{min_char} chars): {total_rejected_short}")
    logger.info(f"    * Too long (>{max_char} chars): {total_rejected_long}")
    if total_preface_segments > 0:
        preface_rejection_rate = (total_preface_auto_rejected / total_preface_segments * 100) if total_preface_segments > 0 else 0
        logger.info(f"    * Preface segments auto-rejected: {total_preface_auto_rejected} ({preface_rejection_rate:.1f}%)")
    
    return (success_count, failure_count, skipped_count)
    
    return (success_count, failure_count, skipped_count)
    
    return (success_count, failure_count, skipped_count)

if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Process segmented TXT files and update corresponding JSON files')
    
    # Add arguments
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--txt_file', help='Path to segmented TXT file')
    group.add_argument('--txt_dir', help='Path to directory containing segmented TXT files')
    
    parser.add_argument('--json_file', help='Path to JSON file to update (for single file mode)')
    parser.add_argument('--json_dir', help='Path to directory containing JSON files to update')
    parser.add_argument('--output_file', help='Path to save updated JSON file (for single file mode)')
    parser.add_argument('--output_dir', help='Path to directory to save updated JSON files')
    parser.add_argument('--log_file', default='json_handler.log', help='Path to log file')
    parser.add_argument('--log_only', action='store_true', help='Only log information without writing output files')
    parser.add_argument('--min_char', type=int, default=500, help='Minimum character count for segments to not be rejected')
    parser.add_argument('--max_char', type=int, default=10000, help='Maximum character count for segments to not be rejected')
    parser.add_argument('--ignore_preface', action='store_true', help='Automatically reject all preface segments for QA')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Set up logger
    logger = setup_logger(args.log_file)
    
    # Process based on mode
    if args.txt_file:
        # Single file mode
        if not args.json_file:
            logger.error("--json_file is required when using --txt_file")
            exit(1)
        
        if update_json_with_segments(args.txt_file, args.json_file, args.output_file, args.log_only, args.min_char, args.max_char, args.ignore_preface):
            logger.info("Processing completed successfully")
        else:
            logger.error("Processing failed")
    else:
        # Directory mode
        if not args.json_dir:
            logger.error("--json_dir is required when using --txt_dir")
            exit(1)
        
        success, failure, skipped = process_directory(args.txt_dir, args.json_dir, args.output_dir, args.log_only, args.min_char, args.max_char, args.ignore_preface)
        
        if failure == 0 and skipped == 0:
            logger.info("All files processed successfully")
        else:
            logger.warning(f"Processing completed with {failure} failures and {skipped} skipped files")