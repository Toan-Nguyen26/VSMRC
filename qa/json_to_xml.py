#!/usr/bin/env python3
"""
JSON to XML Converter with Text Normalization

This script converts JSON files containing Wikipedia segments into a single XML file,
with text normalization to reduce unnecessary newlines and improve token efficiency.

Usage:
    python json_to_xml_normalized.py input_directory output_file.xml [options]
"""

import os
import sys
import json
import glob
import xml.dom.minidom
import argparse
import re
from xml.etree.ElementTree import Element, SubElement, ElementTree, tostring

def natural_sort_key(s):
    """
    Sort strings with numbers in a natural way.
    For example: ['file1', 'file10', 'file2'] -> ['file1', 'file2', 'file10']
    """
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r'(\d+)', s)]

def normalize_text(text, preserve_paragraphs=True):
    """
    Normalize text by removing extra whitespace and normalizing newlines.
    
    Args:
        text: The text to normalize
        preserve_paragraphs: If True, maintains paragraph breaks (single newlines)
                            If False, converts all newlines to spaces
    
    Returns:
        Normalized text
    """
    if not text:
        return ""
    
    # Replace multiple newlines with a single newline
    text = re.sub(r'\n+', '\n', text)
    
    # Replace other whitespace characters with a single space
    text = re.sub(r'\s+', ' ', text)
    
    if preserve_paragraphs:
        # For paragraph preservation, we temporarily replace newlines with a marker,
        # normalize whitespace, then restore the newlines
        text = text.replace('\n', '[NEWLINE]')
        text = re.sub(r'\s+', ' ', text)
        text = text.replace('[NEWLINE]', '\n')
    else:
        # Convert all newlines to spaces
        text = text.replace('\n', ' ')
        # Normalize all whitespace to single spaces
        text = re.sub(r'\s+', ' ', text)
    
    return text.strip()

def create_xml_from_json_directory(input_directory, output_file, min_chars=0, max_chars=0, 
                                  include_rejected=False, pretty=True, normalize=True, 
                                  preserve_paragraphs=False):
    """
    Process all JSON files in the input directory and create a single XML file.
    Orders segments sequentially by filename and segment ID.
    Normalizes text to reduce unnecessary newlines and whitespace.
    
    Args:
        input_directory: Directory containing JSON files
        output_file: Path to save the XML output
        min_chars: Minimum character count for segments (0 = no minimum)
        max_chars: Maximum character count for segments (0 = no maximum)
        include_rejected: Whether to include rejected segments
        pretty: Whether to format XML with pretty printing
        normalize: Whether to normalize text by removing extra newlines and whitespace
        preserve_paragraphs: If normalizing, whether to preserve paragraph breaks
    """
    # Find all JSON files in the directory
    json_files = glob.glob(os.path.join(input_directory, "*.json"))
    
    # Sort files naturally (so file10 comes after file2, not after file1)
    json_files.sort(key=lambda x: natural_sort_key(os.path.basename(x)))
    
    print(f"Found {len(json_files)} JSON files in {input_directory}")
    
    # Create root element with metadata
    root = Element("WikiSegments")
    root.set("source", input_directory)
    if min_chars > 0:
        root.set("min_chars", str(min_chars))
    if max_chars > 0:
        root.set("max_chars", str(max_chars))
    root.set("include_rejected", str(include_rejected).lower())
    root.set("normalize_text", str(normalize).lower())
    if normalize:
        root.set("preserve_paragraphs", str(preserve_paragraphs).lower())
    
    # Process each JSON file
    total_segment_count = 0
    valid_segment_count = 0
    rejected_segment_count = 0
    included_segment_count = 0
    segments_with_qa = 0
    segments_without_qa = 0
    files_with_segments = 0
    
    all_segments = []  # We'll collect all segments here for later sorting
    
    for file_idx, file_path in enumerate(json_files, 1):
        try:
            # Load JSON file
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            file_has_segments = False
            
            # Get document info
            filename = os.path.basename(file_path)
            document_title = data.get("title", "Unknown")
            document_url = data.get("url", "")
            
            # Check if the file has segments in the "multi" field
            if "multi" not in data:
                print(f"Warning: No 'multi' field in {filename}, skipping file")
                continue
            
            # Get total segment count for this document
            segment_count = len(data["multi"]) if "multi" in data else 0
            
            # Get all segment IDs and sort them naturally
            segment_ids = list(data["multi"].keys())
            segment_ids.sort(key=natural_sort_key)
            
            # Process each segment in the file
            for segment_id in segment_ids:
                segment_data = data["multi"][segment_id]
                total_segment_count += 1
                
                # Check if segment is rejected
                is_rejected = segment_data.get("rejected_for_qa", False)
                rejection_reason = segment_data.get("reason_for_rejected", "")
                
                if is_rejected:
                    rejected_segment_count += 1
                    # Skip if we don't want rejected segments
                    if not include_rejected:
                        continue
                else:
                    valid_segment_count += 1
                
                # Get character count for filtering
                char_count = segment_data.get("char_count", 0)
                
                # Apply length filters if specified
                if (min_chars > 0 and char_count < min_chars) or (max_chars > 0 and char_count > max_chars):
                    continue
                
                # Normalize the segment text if requested
                segment_text = segment_data.get("segment_text", "")
                if normalize:
                    segment_text = normalize_text(segment_text, preserve_paragraphs)
                
                # Store the segment info for later processing
                all_segments.append({
                    "filename": filename,
                    "document_title": document_title,
                    "document_url": document_url,
                    "segment_id": segment_id,
                    "segment_data": segment_data,
                    "segment_text": segment_text,  # Store normalized text
                    "segment_count": segment_count,
                    "is_rejected": is_rejected,
                    "rejection_reason": rejection_reason,
                })
                
                included_segment_count += 1
                file_has_segments = True
                
                # Count whether this segment has QA data
                has_qa = "qa" in segment_data and segment_data["qa"]
                if has_qa:
                    segments_with_qa += 1
                else:
                    segments_without_qa += 1
            
            if file_has_segments:
                files_with_segments += 1
                
            # Print progress every 10 files or at the end
            if file_idx % 10 == 0 or file_idx == len(json_files):
                print(f"Processed {file_idx}/{len(json_files)} files...")
            
        except Exception as e:
            print(f"Error processing {file_path}: {str(e)}")
    
    # Sort all segments by filename and then by segment_id
    all_segments.sort(key=lambda x: (natural_sort_key(x["filename"]), natural_sort_key(x["segment_id"])))
    
    # Now add all segments to the XML in the sorted order
    for segment_info in all_segments:
        # Create segment element
        segment_elem = SubElement(root, "Segment")
        segment_elem.set("id", segment_info["segment_id"])
        
        # Add filename
        filename_elem = SubElement(segment_elem, "Filename")
        filename_elem.text = segment_info["filename"]
        
        # Add document title
        doc_title_elem = SubElement(segment_elem, "DocumentTitle")
        doc_title_elem.text = segment_info["document_title"]
        
        # Extract segment number from segment_id
        segment_number = segment_info["segment_id"].split('_')[-1] if '_' in segment_info["segment_id"] else segment_info["segment_id"]
        
        # Add segment number
        seg_num_elem = SubElement(segment_elem, "SegmentNumber")
        seg_num_elem.text = segment_number
        
        # Get segment data
        segment_data = segment_info["segment_data"]
        
        # Add segment title
        seg_title_elem = SubElement(segment_elem, "SegmentTitle")
        seg_title_elem.text = segment_data.get("segment_title", "")
        
        # Add segment text (using the normalized version if it was normalized)
        seg_text_elem = SubElement(segment_elem, "SegmentText")
        seg_text_elem.text = segment_info["segment_text"]
        
        # Add segment count (total segments in document)
        seg_count_elem = SubElement(segment_elem, "SegmentCount")
        seg_count_elem.text = str(segment_info["segment_count"])
        
        # Add character count
        char_count_elem = SubElement(segment_elem, "CharCount")
        char_count_elem.text = str(segment_data.get("char_count", 0))
        
        # Add rejected_for_qa status
        rejected_elem = SubElement(segment_elem, "RejectedForQA")
        rejected_elem.text = str(segment_info["is_rejected"]).lower()
        
        # Add rejection reason if available
        reason_elem = SubElement(segment_elem, "ReasonForRejected")
        # Set empty string as text to ensure it's not a self-closing tag
        reason_elem.text = segment_info["rejection_reason"] if segment_info["rejection_reason"] else ""
        
        # Add QA data if available
        qa_elem = SubElement(segment_elem, "QA")
        
        # If QA data exists, add it to the QA element
        if "qa" in segment_data and segment_data["qa"]:
            qa_data = segment_data["qa"]
            
            # If QA is a dictionary of question_id -> question data
            if isinstance(qa_data, dict):
                # Sort question IDs for consistent ordering
                question_ids = sorted(qa_data.keys(), key=natural_sort_key)
                
                for question_id in question_ids:
                    question_data = qa_data[question_id]
                    q_elem = SubElement(qa_elem, "Question")
                    q_elem.set("id", question_id)
                    
                    # Add question type if available
                    if "question_type" in question_data:
                        q_type_elem = SubElement(q_elem, "QuestionType")
                        q_type_elem.text = question_data["question_type"]
                    
                    # Add question text
                    q_text_elem = SubElement(q_elem, "QuestionText")
                    q_text_elem.text = question_data.get("question", "")
                    
                    # Add choices
                    choices_elem = SubElement(q_elem, "Choices")
                    for i, choice in enumerate(question_data.get("choices", [])):
                        choice_elem = SubElement(choices_elem, "Choice")
                        choice_elem.set("index", str(i))
                        choice_elem.text = choice
                    
                    # Add correct choice
                    correct_elem = SubElement(q_elem, "CorrectChoice")
                    correct_elem.text = str(question_data.get("correct_choice", 0))
        else:
            # No QA data, add empty attribute and ensure it's not self-closing
            qa_elem.set("empty", "true")
            # Add a dummy text to prevent self-closing
            qa_elem.text = ""
    
    # Add metadata about the conversion
    metadata = SubElement(root, "Metadata")
    metadata.set("totalFiles", str(len(json_files)))
    metadata.set("filesWithSegments", str(files_with_segments))
    metadata.set("totalSegments", str(total_segment_count))
    metadata.set("validSegments", str(valid_segment_count))
    metadata.set("rejectedSegments", str(rejected_segment_count))
    metadata.set("includedSegments", str(included_segment_count))
    metadata.set("segmentsWithQA", str(segments_with_qa))
    metadata.set("segmentsWithoutQA", str(segments_without_qa))
    
    print(f"\nSummary:")
    print(f"- Total files processed: {len(json_files)}")
    print(f"- Files with segments: {files_with_segments}")
    print(f"- Total segments: {total_segment_count}")
    print(f"- Valid segments: {valid_segment_count}")
    print(f"- Rejected segments: {rejected_segment_count}")
    print(f"- Segments included in XML: {included_segment_count}")
    print(f"- Segments with QA data: {segments_with_qa}")
    print(f"- Segments without QA data: {segments_without_qa}")
    
    # Write to file, with or without pretty formatting
    if pretty:
        # Use tostring() as a function, not as a method of ElementTree
        xml_string = xml.dom.minidom.parseString(
            tostring(root, encoding='utf-8')
        ).toprettyxml(indent="  ")
        
        # Fix ALL empty elements to use explicit opening and closing tags
        # Replace self-closing tags with explicit open/close tags for consistency
        xml_string = re.sub(r'<([^>\s]+)([^>]*)/>',
                         r'<\1\2></\1>',
                         xml_string)
        
        # Write to file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(xml_string)
    else:
        # Write directly for better performance with large files
        tree = ElementTree(root)
        tree.write(output_file, encoding='utf-8', xml_declaration=True)
        
        # Since ElementTree tends to use self-closing tags, we need to
        # post-process the XML file for consistency if not using pretty printing
        with open(output_file, 'r', encoding='utf-8') as f:
            xml_content = f.read()
            
        # Fix ALL empty elements to use explicit opening and closing tags
        xml_content = re.sub(r'<([^>\s]+)([^>]*)/>',
                           r'<\1\2></\1>',
                           xml_content)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(xml_content)
    
    print(f"XML output saved to {output_file}")
    return included_segment_count

def main():
    parser = argparse.ArgumentParser(description="Convert JSON files with Wikipedia segments to a single XML file")
    parser.add_argument("input_directory", help="Directory containing JSON files")
    parser.add_argument("output_file", help="Path to save the XML output")
    parser.add_argument("--min-chars", type=int, default=0, help="Minimum character count (default: 0, no minimum)")
    parser.add_argument("--max-chars", type=int, default=0, help="Maximum character count (default: 0, no maximum)")
    parser.add_argument("--include-rejected", action="store_true", help="Include rejected segments in output")
    parser.add_argument("--no-pretty", action="store_true", help="Don't use pretty formatting (faster for large files)")
    parser.add_argument("--normalize", action="store_true", help="Normalize text by removing extra newlines and whitespace")
    parser.add_argument("--preserve-paragraphs", action="store_true", 
                       help="When normalizing, preserve paragraph breaks (single newlines)")
    
    args = parser.parse_args()
    
    if not os.path.isdir(args.input_directory):
        print(f"Error: {args.input_directory} is not a directory")
        sys.exit(1)
    
    create_xml_from_json_directory(
        args.input_directory, 
        args.output_file,
        min_chars=args.min_chars,
        max_chars=args.max_chars,
        include_rejected=args.include_rejected,
        pretty=not args.no_pretty,
        normalize=args.normalize,
        preserve_paragraphs=args.preserve_paragraphs
    )

if __name__ == "__main__":
    main()