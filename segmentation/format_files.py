#!/usr/bin/env python3
"""
Enhanced Format Script

Processes files and formats them with proper segment structure.
Features:
- First content block goes under preface (level 1)
- All headers are formatted as level 2
- Content is tokenized into sentences using underthesea
- Detects segments with only one sentence (excluding preface)
- Detects segments with incomplete sentences (not capitalized, missing punctuation, etc.)
- Can remove problematic segments and filter documents

Usage: python enhanced_format.py --input INPUT_DIR --output OUTPUT_DIR [--files NUM] [--random] 
                                [--filtered FILTERED_DIR] [--remove-one-sentence] [--remove-incomplete]
                                [--min-segments 4] [--removal-threshold 0.8]
"""

import os
import re
import argparse
import logging
import random
from underthesea import sent_tokenize

# Set up basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

def is_incomplete_sentence(sentence):
    """Check if a sentence is incomplete based on various criteria"""
    if not sentence:
        return False
    
    # 1. Check if it starts with a symbol
    if re.match(r'^[^\w\s]', sentence):
        return True
    
    # 2. Check if it doesn't start with a capitalized letter
    if re.match(r'^[a-z]', sentence):
        return True
    
    # 3. Check if it doesn't end with proper punctuation
    if not re.search(r'[.!?:"\)…]$', sentence):
        return True
    
    return False

def format_file(input_file, output_file, filtered_dir=None, remove_one_sentence=False, 
               remove_incomplete=False, min_segments=4, removal_threshold=0.8):
    """Format a single file with proper segment structure"""
    try:
        # Read input file
        with open(input_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Initialize statistics
        stats = {
            'segments': 0,
            'sentences': 0,
            'tokens': 0,
            'one_sentence_segments': 0,
            'segments_with_one_sentence': [],
            'has_non_preface_one_sentence': False,
            'has_incomplete_sentences': False,
            'segments_with_incomplete': [],
            'incomplete_count': 0,
            'original_segment_count': 0,
            'segments_after_removal': 0,
            'removal_percentage': 0
        }
        
        # Extract headers and content blocks
        lines = content.splitlines()
        headers = []
        content_blocks = []
        current_content = []
        current_header = None
        
        # Parse the file
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # Check for header
            header_match = re.match(r'^\[HEADER\]\s+(.+)$', line)
            if header_match:
                # Save current content if there is any
                if current_content:
                    content_blocks.append({
                        'header': current_header,
                        'content': current_content
                    })
                    current_content = []
                
                # Process header
                header_text = header_match.group(1).strip()
                headers.append(header_text)
                current_header = header_text
                continue
            
            # Check for content
            content_match = re.match(r'^\[(SINGLE-SENT|MULTI-SENT[:\d]*)\]\s+(.+)$', line)
            if content_match:
                content_text = content_match.group(2).strip()
                current_content.append(content_text)
        
        # Add any remaining content
        if current_content:
            content_blocks.append({
                'header': current_header,
                'content': current_content
            })
        
        # Build output
        output_lines = []
        
        # 1. Start with preface
        output_lines.append("========,1,preface.")
        stats['segments'] += 1
        
        # Track for one-sentence removal
        preface_has_one_sentence = False
        
        # 2. Add first content block under preface
        if content_blocks:
            first_block = content_blocks[0]['content']
            # Track sentences for preface segment
            preface_sentences = []
            
            for text in first_block:
                sentences = sent_tokenize(text)
                for sentence in sentences:
                    sentence = sentence.strip()
                    if sentence:
                        output_lines.append(sentence)
                        stats['sentences'] += 1
                        preface_sentences.append(sentence)
                        # Count tokens (words)
                        tokens = sentence.split()
                        stats['tokens'] += len(tokens)
            
            # Check if preface has exactly one sentence
            if len(preface_sentences) == 1:
                stats['one_sentence_segments'] += 1
                stats['segments_with_one_sentence'].append("preface")
                preface_has_one_sentence = True
        
        # Track segments that should be removed
        segments_to_remove = []
        
        # 3. Process each header and its content
        for header in headers:
            # Track issues with this segment
            has_one_sentence = False
            has_incomplete = False
            segment_sentences = []
            segment_content = []
            
            # Find content for this header
            for block in content_blocks[1:]:
                if block['header'] == header:
                    # Process sentences for this segment
                    for text in block['content']:
                        sentences = sent_tokenize(text)
                        for sentence in sentences:
                            sentence = sentence.strip()
                            if sentence:
                                segment_sentences.append(sentence)
                                segment_content.append(sentence)
                                stats['sentences'] += 1
                                # Count tokens
                                tokens = sentence.split()
                                stats['tokens'] += len(tokens)
                                
                                # Check for incomplete sentences
                                if is_incomplete_sentence(sentence):
                                    has_incomplete = True
                                    stats['incomplete_count'] += 1
                    break
            
            # Check if this segment has exactly one sentence
            if len(segment_sentences) == 1:
                stats['one_sentence_segments'] += 1
                stats['segments_with_one_sentence'].append(header)
                stats['has_non_preface_one_sentence'] = True
                has_one_sentence = True
                
                if remove_one_sentence:
                    segments_to_remove.append(header)
            
            # Check if segment has incomplete sentences
            if has_incomplete:
                stats['has_incomplete_sentences'] = True
                stats['segments_with_incomplete'].append(header)
                
                if remove_incomplete:
                    if header not in segments_to_remove:
                        segments_to_remove.append(header)
            
            # Add header and content only if we're not removing this segment
            if not ((remove_one_sentence and has_one_sentence) or 
                   (remove_incomplete and has_incomplete)):
                # Add header with proper format
                if header.endswith('.') or header.endswith('!') or header.endswith('?') or header.endswith(':'):
                    output_lines.append(f"========,2,{header}")
                else:
                    output_lines.append(f"========,2,{header}.")
                
                # Add content lines
                output_lines.extend(segment_content)
                
                # Count the segment
                stats['segments'] += 1
        
        # Update removal statistics
        if remove_one_sentence or remove_incomplete:
            stats['original_segment_count'] = 1 + len(headers)  # preface + all headers
            stats['segments_after_removal'] = stats['segments']  # Already counted correctly above
            
            if stats['original_segment_count'] > 0:
                stats['removal_percentage'] = (len(segments_to_remove) / stats['original_segment_count'])
        
        # Write output file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(output_lines))
        
        # Handle filtered directory logic
        if filtered_dir:
            should_filter = False
            
            # Check if it meets the filter criteria
            if remove_one_sentence or remove_incomplete:
                # Filter based on segments after removal
                if stats['segments_after_removal'] >= min_segments:
                    # Also check if we didn't remove too many segments
                    if stats['removal_percentage'] <= removal_threshold:
                        should_filter = True
            else:
                # Original filter logic: just check total segment count
                if stats['segments'] >= min_segments:
                    should_filter = True
            
            # Save to filtered directory if criteria are met
            if should_filter:
                filtered_output = os.path.join(filtered_dir, os.path.basename(output_file))
                with open(filtered_output, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(output_lines))
        
        return stats
        
    except Exception as e:
        logger.error(f"Error processing {input_file}: {e}")
        return None

def process_directory(input_dir, output_dir, filtered_dir=None, num_files=None, random_select=False, 
                     remove_one_sentence=False, remove_incomplete=False, min_segments=4, removal_threshold=0.8):
    """Process multiple files in a directory"""
    # Create output directories if they don't exist
    os.makedirs(output_dir, exist_ok=True)
    if filtered_dir:
        os.makedirs(filtered_dir, exist_ok=True)
    
    # Get all relevant files
    all_files = [f for f in os.listdir(input_dir) 
                if f.startswith("updated_") and f.endswith(".txt")]
    
    if not all_files:
        logger.error(f"No updated_*.txt files found in {input_dir}")
        return
    
    # Select files to process
    if num_files and num_files < len(all_files):
        if random_select:
            files_to_process = random.sample(all_files, num_files)
            logger.info(f"Randomly selected {num_files} of {len(all_files)} files")
        else:
            sorted_files = sorted(all_files)
            files_to_process = sorted_files[:num_files]
            logger.info(f"Processing first {num_files} of {len(all_files)} files")
    else:
        files_to_process = sorted(all_files)
        logger.info(f"Processing all {len(all_files)} files")
    
    # Process files and collect statistics
    total_stats = {
        'processed': 0,
        'errors': 0,
        'total_segments': 0,
        'total_sentences': 0,
        'total_tokens': 0,
        'files_with_few_segments': 0,
        'files_with_enough_segments': 0,
        'filtered_files': 0
    }
    
    # Process each file
    for filename in files_to_process:
        # Create input and output paths
        input_path = os.path.join(input_dir, filename)
        
        # Remove "updated_" prefix for output filename
        output_filename = filename
        if output_filename.startswith("updated_"):
            output_filename = output_filename[len("updated_"):]
        output_path = os.path.join(output_dir, output_filename)
        
        # Process file
        stats = format_file(input_path, output_path, filtered_dir, 
                          remove_one_sentence, remove_incomplete, min_segments, removal_threshold)
        
        # Update basic statistics
        if stats:
            total_stats['processed'] += 1
            total_stats['total_segments'] += stats['segments']
            total_stats['total_sentences'] += stats['sentences']
            total_stats['total_tokens'] += stats['tokens']
            
            # Check for files with few segments
            removal_active = remove_one_sentence or remove_incomplete
            if (removal_active and stats['segments_after_removal'] < min_segments) or \
               (not removal_active and stats['segments'] < min_segments):
                total_stats['files_with_few_segments'] += 1
            else:
                total_stats['files_with_enough_segments'] += 1
                if filtered_dir:
                    # If it passed the additional filtering criteria
                    if removal_active:
                        if stats['removal_percentage'] <= removal_threshold:
                            total_stats['filtered_files'] += 1
                    else:
                        total_stats['filtered_files'] += 1
        else:
            total_stats['errors'] += 1
    
    # Write summary
    summary_path = os.path.join(output_dir, "processing_summary.txt")
    try:
        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write("PROCESSING SUMMARY\n")
            f.write("=================\n\n")
            f.write(f"Files processed: {total_stats['processed']}\n")
            f.write(f"Files with errors: {total_stats['errors']}\n")
            f.write(f"Files saved to filtered directory: {total_stats['filtered_files']}\n\n")
            
            f.write("STATISTICS:\n")
            f.write(f"Total segments: {total_stats['total_segments']}\n")
            f.write(f"Total sentences: {total_stats['total_sentences']}\n")
            f.write(f"Total tokens: {total_stats['total_tokens']}\n")
            
            if total_stats['processed'] > 0:
                f.write("\nAVERAGES:\n")
                f.write(f"Average segments per document: {total_stats['total_segments']/total_stats['processed']:.2f}\n")
                f.write(f"Average sentences per document: {total_stats['total_sentences']/total_stats['processed']:.2f}\n")
                if total_stats['total_sentences'] > 0:
                    f.write(f"Average tokens per sentence: {total_stats['total_tokens']/total_stats['total_sentences']:.2f}\n")
    except Exception as e:
        logger.error(f"Error creating summary file: {str(e)}")

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Format files with segment structure")
    parser.add_argument("--input", required=True, help="Input directory with files to process")
    parser.add_argument("--output", required=True, help="Output directory for formatted files")
    parser.add_argument("--filtered", help="Directory for saving only files that meet filtering criteria")
    parser.add_argument("--files", type=int, help="Number of files to process (default: all)")
    parser.add_argument("--random", action="store_true", help="Randomly select files (default: sequential)")
    parser.add_argument("--remove-one-sentence", action="store_true", 
                       help="Remove segments that have only one sentence (preface excluded)")
    parser.add_argument("--remove-incomplete", action="store_true",
                       help="Remove segments containing incomplete sentences")
    parser.add_argument("--min-segments", type=int, default=4, 
                       help="Minimum number of segments required for filtering (default: 4)")
    parser.add_argument("--removal-threshold", type=float, default=0.8, 
                       help="Maximum percentage of segments that can be removed (default: 0.8)")
    
    args = parser.parse_args()
    
    # Check if input directory exists
    if not os.path.isdir(args.input):
        logger.error(f"Input directory not found: {args.input}")
        return
    
    # Process the directory
    process_directory(
        args.input, 
        args.output, 
        args.filtered, 
        args.files, 
        args.random,
        args.remove_one_sentence,
        args.remove_incomplete,
        args.min_segments,
        args.removal_threshold
    )

if __name__ == "__main__":
    main()