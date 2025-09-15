#!/usr/bin/env python3
"""
Vietnamese Wikipedia Single Sentence Header Detector

This script identifies potential headers in Vietnamese text files by:
1. Finding lines that contain exactly ONE sentence
2. Checking if they meet criteria for headers (length, punctuation, etc.)
3. Marking potential section transitions in the text

This approach doesn't rely on empty lines or standalone formatting.
"""

import os
import re
import argparse
import logging
import random
from tqdm import tqdm
from underthesea import sent_tokenize

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("header_detection.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def is_potential_header(text, min_chars=3, max_chars=150):
    """
    Check if text is a potential header based on characteristics
    
    Args:
        text (str): Text to check
        min_chars (int): Minimum character length
        max_chars (int): Maximum character length
        
    Returns:
        tuple: (is_header, reason) - Boolean result and reason string
    """
    # Skip empty or very short text
    if not text or len(text.strip()) < min_chars:
        return False, "Too short"
    
    # Check if text is not too long (headers are typically shorter)
    if len(text.strip()) > max_chars:
        return False, "Too long"
    
    # Headers typically end with punctuation
    if not re.search(r'[.!?]$', text.strip()):
        return False, "No end punctuation"
    
    # Check if it has characteristics of a header
    # Could add more specific heuristics here
    
    return True, "Meets criteria"

def detect_headers(filepath, output_file=None, min_chars=3, max_chars=150):
    """
    Detect potential headers in a file by finding single-sentence lines
    
    Args:
        filepath (str): Path to the text file
        output_file (str, optional): Path to save annotated file
        min_chars (int): Minimum character length for headers
        max_chars (int): Maximum character length for headers
        
    Returns:
        dict: Detection results
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        logger.error(f"Error reading file {filepath}: {str(e)}")
        return {'error': str(e)}
    
    # Split content into lines
    lines = content.splitlines()
    
    # Initialize results
    results = {
        'filepath': filepath,
        'filename': os.path.basename(filepath),
        'total_lines': len(lines),
        'single_sentence_lines': 0,
        'potential_headers': [],
        'rejected_headers': []
    }
    
    # Annotated lines for output
    annotated_lines = []
    
    # Process each line
    for i, line in enumerate(lines):
        line_stripped = line.strip()
        
        # Skip empty lines (though analysis suggests there may not be any)
        if not line_stripped:
            annotated_lines.append(line)
            continue
        
        # Count sentences in this line
        sentences = sent_tokenize(line_stripped)
        sent_count = len(sentences)
        
        # Check if this is a single-sentence line
        if sent_count == 1:
            results['single_sentence_lines'] += 1
            
            # Check if it meets header criteria
            is_header, reason = is_potential_header(line_stripped, min_chars, max_chars)
            
            if is_header:
                results['potential_headers'].append({
                    'line_num': i+1,
                    'text': line_stripped,
                    'length': len(line_stripped)
                })
                annotated_lines.append(f"[HEADER] {line}")
            else:
                results['rejected_headers'].append({
                    'line_num': i+1,
                    'text': line_stripped,
                    'reason': reason
                })
                annotated_lines.append(f"[SINGLE-SENT] {line}")
        else:
            # Multi-sentence line
            annotated_lines.append(f"[MULTI-SENT:{sent_count}] {line}")
    
    # Write annotated output if requested
    if output_file:
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write('\n'.join(annotated_lines))
        except Exception as e:
            logger.error(f"Error writing annotated file {output_file}: {str(e)}")
    
    # Calculate statistics
    header_percentage = 0
    if results['single_sentence_lines'] > 0:
        header_percentage = len(results['potential_headers']) / results['single_sentence_lines'] * 100
    
    results['header_percentage'] = header_percentage
    
    return results

def process_files(input_dir, output_dir=None, num_files=5, random_selection=True, 
                 min_chars=3, max_chars=150):
    """
    Process multiple files to detect headers
    
    Args:
        input_dir (str): Directory containing text files
        output_dir (str, optional): Directory to save annotated files
        num_files (int): Number of files to process
        random_selection (bool): Whether to randomly select files
        min_chars (int): Minimum character length for headers
        max_chars (int): Maximum character length for headers
        
    Returns:
        list: Results for each file
    """
    # Create output directory if needed
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info(f"Created directory: {output_dir}")
    
    # Get all text files in the input directory
    files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) 
             if f.endswith('.txt') and os.path.isfile(os.path.join(input_dir, f))]
    
    if not files:
        logger.error(f"No .txt files found in {input_dir}")
        return []
    
    # Select files
    if random_selection:
        selected_files = random.sample(files, min(num_files, len(files)))
    else:
        selected_files = files[:min(num_files, len(files))]
    
    logger.info(f"Processing {len(selected_files)} files for single-sentence header detection...")
    
    results = []
    for file_path in tqdm(selected_files, desc="Processing files"):
        filename = os.path.basename(file_path)
        output_file = os.path.join(output_dir, f"annotated_{filename}") if output_dir else None
        
        result = detect_headers(
            file_path, 
            output_file, 
            min_chars=min_chars, 
            max_chars=max_chars
        )
        
        if 'error' not in result:
            results.append(result)
            logger.info(
                f"File: {filename} - "
                f"Found {len(result['potential_headers'])} potential headers "
                f"out of {result['single_sentence_lines']} single-sentence lines "
                f"({result['header_percentage']:.1f}%)"
            )
    
    # Write summary report
    if output_dir and results:
        summary_path = os.path.join(output_dir, "header_detection_summary.txt")
        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write("SINGLE-SENTENCE HEADER DETECTION SUMMARY\n")
            f.write("=" * 60 + "\n\n")
            
            f.write(f"Files processed: {len(results)}\n\n")
            
            # Overall statistics
            total_lines = sum(r['total_lines'] for r in results)
            total_single_sent = sum(r['single_sentence_lines'] for r in results)
            total_headers = sum(len(r['potential_headers']) for r in results)
            
            f.write("OVERALL STATISTICS:\n")
            f.write(f"Total lines: {total_lines}\n")
            f.write(f"Single-sentence lines: {total_single_sent} ({total_single_sent/total_lines*100:.1f}% of all lines)\n")
            f.write(f"Potential headers: {total_headers} ({total_headers/total_single_sent*100:.1f}% of single-sentence lines)\n\n")
            
            f.write("INDIVIDUAL FILE STATISTICS:\n")
            for r in results:
                f.write(f"\n{r['filename']}:\n")
                f.write(f"  Lines: {r['total_lines']} total\n")
                f.write(f"  Single-sentence lines: {r['single_sentence_lines']} ({r['single_sentence_lines']/r['total_lines']*100:.1f}%)\n")
                f.write(f"  Potential headers: {len(r['potential_headers'])} ({r['header_percentage']:.1f}% of single-sentence lines)\n")
                
                # Header examples
                if r['potential_headers']:
                    f.write("\n  Header examples:\n")
                    for i, header in enumerate(r['potential_headers'][:5]):
                        f.write(f"    {i+1}. Line {header['line_num']}: {header['text'][:100]}\n")
                    
                    if len(r['potential_headers']) > 5:
                        f.write(f"    ... and {len(r['potential_headers']) - 5} more headers\n")
                
                # Rejected headers
                if r['rejected_headers']:
                    rejected_reasons = {}
                    for item in r['rejected_headers']:
                        reason = item['reason']
                        rejected_reasons[reason] = rejected_reasons.get(reason, 0) + 1
                    
                    f.write("\n  Rejected single-sentence lines:\n")
                    for reason, count in rejected_reasons.items():
                        f.write(f"    {reason}: {count} lines\n")
                    
                    # Examples of rejected lines
                    f.write("\n  Rejected examples:\n")
                    for i, item in enumerate(r['rejected_headers'][:3]):
                        f.write(f"    {i+1}. Line {item['line_num']}: {item['text'][:100]}\n")
                        f.write(f"       Reason: {item['reason']}\n")
                    
                    if len(r['rejected_headers']) > 3:
                        f.write(f"    ... and {len(r['rejected_headers']) - 3} more\n")
                
                f.write("\n" + "-" * 40 + "\n")
            
        logger.info(f"Summary written to {summary_path}")
    
    return results

def segment_file(filepath, output_file, min_chars=3, max_chars=150):
    """
    Segment a file by identifying single-sentence headers
    and creating a segmented version
    
    Args:
        filepath (str): Path to input file
        output_file (str): Path to save segmented file
        min_chars (int): Minimum character length for headers
        max_chars (int): Maximum character length for headers
        
    Returns:
        dict: Segmentation results
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        logger.error(f"Error reading file {filepath}: {str(e)}")
        return {'error': str(e)}
    
    # Split content into lines
    lines = content.splitlines()
    
    # Initialize results and segmented content
    segments = []
    current_segment = {'header': None, 'content': []}
    segment_count = 0
    
    for i, line in enumerate(lines):
        line_stripped = line.strip()
        
        # Skip empty lines
        if not line_stripped:
            continue
        
        # Count sentences in this line
        sentences = sent_tokenize(line_stripped)
        sent_count = len(sentences)
        
        # Check if this is a potential header (single sentence line)
        if sent_count == 1:
            is_header, _ = is_potential_header(line_stripped, min_chars, max_chars)
            
            if is_header:
                # Save previous segment if it exists
                if current_segment['header'] or current_segment['content']:
                    segments.append(current_segment)
                    segment_count += 1
                
                # Start a new segment
                current_segment = {
                    'header': line_stripped,
                    'content': []
                }
                continue
        
        # Regular content line - add to current segment
        current_segment['content'].append(line_stripped)
    
    # Add the last segment if it has content
    if current_segment['header'] or current_segment['content']:
        segments.append(current_segment)
        segment_count += 1
    
    # Write segmented file
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            for i, segment in enumerate(segments):
                # Determine segment level (1 for preface, 2 for rest)
                level = 1 if i == 0 else 2
                
                # Write segment header
                if segment['header']:
                    f.write(f"========,{level},{segment['header']}\n")
                else:
                    # Use generic header if none exists
                    f.write(f"========,{level},Section {i+1}.\n")
                
                # Write segment content
                if segment['content']:
                    # Join content without adding extra newlines
                    f.write('\n'.join(segment['content']))
                    # Only add a newline if we're not at the end of the file
                    if i < len(segments) - 1:
                        f.write('\n')
                
                # Don't add an extra newline between segments
            
    except Exception as e:
        logger.error(f"Error writing segmented file {output_file}: {str(e)}")
        return {'error': str(e), 'segments': segment_count}
    
    return {
        'filepath': filepath,
        'filename': os.path.basename(filepath),
        'segments': segment_count
    }

def segment_files(input_dir, output_dir, num_files=5, random_selection=True,
                 min_chars=3, max_chars=150):
    """
    Segment multiple files based on single-sentence header detection
    
    Args:
        input_dir (str): Directory containing input files
        output_dir (str): Directory to save segmented files
        num_files (int): Number of files to process
        random_selection (bool): Whether to randomly select files
        min_chars (int): Minimum character length for headers
        max_chars (int): Maximum character length for headers
        
    Returns:
        list: Results for each file
    """
    # Create output directory if needed
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info(f"Created directory: {output_dir}")
    
    # Get all text files in the input directory
    files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) 
             if f.endswith('.txt') and os.path.isfile(os.path.join(input_dir, f))]
    
    if not files:
        logger.error(f"No .txt files found in {input_dir}")
        return []
    
    # Select files
    if random_selection:
        selected_files = random.sample(files, min(num_files, len(files)))
    else:
        selected_files = files[:min(num_files, len(files))]
    
    logger.info(f"Segmenting {len(selected_files)} files...")
    
    results = []
    for file_path in tqdm(selected_files, desc="Segmenting files"):
        filename = os.path.basename(file_path)
        output_file = os.path.join(output_dir, filename)
        
        result = segment_file(
            file_path, 
            output_file, 
            min_chars=min_chars, 
            max_chars=max_chars
        )
        
        if 'error' not in result:
            results.append(result)
            logger.info(f"File: {filename} - Created {result['segments']} segments")
    
    # Write summary
    summary_path = os.path.join(output_dir, "segmentation_summary.txt")
    with open(summary_path, 'w', encoding='utf-8') as f:
        f.write("FILE SEGMENTATION SUMMARY\n")
        f.write("=" * 40 + "\n\n")
        
        f.write(f"Files processed: {len(results)}\n\n")
        
        total_segments = sum(r['segments'] for r in results)
        avg_segments = total_segments / len(results) if results else 0
        
        f.write(f"Total segments created: {total_segments}\n")
        f.write(f"Average segments per file: {avg_segments:.2f}\n\n")
        
        f.write("INDIVIDUAL FILE STATISTICS:\n")
        for r in results:
            f.write(f"  {r['filename']}: {r['segments']} segments\n")
    
    logger.info(f"Segmentation summary written to {summary_path}")
    
    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Detect and segment based on single-sentence headers")
    parser.add_argument("--input", type=str, required=True, 
                        help="Input directory containing text files")
    parser.add_argument("--output", type=str, required=True, 
                        help="Output directory for annotated and segmented files")
    parser.add_argument("--files", type=int, default=5, 
                        help="Number of files to process (default: 5)")
    parser.add_argument("--sequential", action="store_true", 
                        help="Use sequential file selection instead of random")
    parser.add_argument("--detect", action="store_true", 
                        help="Run header detection only (no segmentation)")
    parser.add_argument("--segment", action="store_true", 
                        help="Run segmentation only (no detection analysis)")
    parser.add_argument("--min-chars", type=int, default=3, 
                        help="Minimum character length for headers (default: 3)")
    parser.add_argument("--max-chars", type=int, default=150, 
                        help="Maximum character length for headers (default: 150)")
    
    args = parser.parse_args()
    
    # Validate args
    if not args.detect and not args.segment:
        # By default, do both
        args.detect = True
        args.segment = True
    
    # Create annotation subdirectory
    annotation_dir = os.path.join(args.output, "annotated") if args.detect else None
    if annotation_dir and not os.path.exists(annotation_dir):
        os.makedirs(annotation_dir)
    
    # Create segmentation subdirectory
    segment_dir = os.path.join(args.output, "segmented") if args.segment else None
    if segment_dir and not os.path.exists(segment_dir):
        os.makedirs(segment_dir)
    
    # Run detection if requested
    if args.detect:
        logger.info("Running header detection...")
        process_files(
            args.input,
            annotation_dir,
            args.files,
            not args.sequential,
            args.min_chars,
            args.max_chars
        )
    
    # Run segmentation if requested
    if args.segment:
        logger.info("Running segmentation...")
        segment_files(
            args.input,
            segment_dir,
            args.files,
            not args.sequential,
            args.min_chars,
            args.max_chars
        )