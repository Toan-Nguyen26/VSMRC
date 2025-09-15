#!/usr/bin/env python3
"""
Combine Original XML with Gemini Validation Results (Whitespace-Preserving Version)

This script takes the original XML file with Wikipedia segments and a Gemini validation
results XML file, then updates the original XML to mark segments as rejected based on
Gemini's assessment. This version preserves the existing whitespace in the XML.

Usage:
    python combine_validation_whitespace.py --original original.xml --gemini gemini_results.xml --output updated.xml
"""

import os
import argparse
import logging
import xml.etree.ElementTree as ET
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%H:%M:%S',
    handlers=[logging.StreamHandler(), logging.FileHandler("combine_validation.log")]
)
logger = logging.getLogger(__name__)

def load_xml_file(file_path):
    """Load an XML file and return its ElementTree."""
    try:
        tree = ET.parse(file_path)
        return tree
    except Exception as e:
        logger.error(f"Error loading XML file {file_path}: {str(e)}")
        return None

def extract_gemini_validations(validation_file):
    """Extract Gemini's validation results from XML file."""
    validations = {}
    
    try:
        # Parse the validation XML
        tree = load_xml_file(validation_file)
        if tree is None:
            return validations
            
        root = tree.getroot()
            
        # Extract segment validations
        for segment_elem in root.findall('.//segment'):
            segment_id = segment_elem.get('id')
            
            if segment_id:
                is_appropriate_elem = segment_elem.find('is_appropriate')
                
                # Only extract when is_appropriate is "no"
                if is_appropriate_elem is not None and is_appropriate_elem.text and is_appropriate_elem.text.lower() == 'no':
                    reason_elem = segment_elem.find('reason')
                    
                    reason = "Not appropriate for QA" 
                    reason_type = ""
                    
                    if reason_elem is not None:
                        if reason_elem.text:
                            reason = reason_elem.text
                        reason_type = reason_elem.get('type', '')
                    
                    validations[segment_id] = {
                        'segment_id': segment_id,
                        'is_appropriate': False,
                        'reason': reason,
                        'reason_type': reason_type
                    }
        
        logger.info(f"Extracted {len(validations)} 'not appropriate' validations from Gemini results")
        
    except Exception as e:
        logger.error(f"Error extracting Gemini validations: {str(e)}")
    
    return validations

def update_xml_in_place(content, segment_id, gemini_validation):
    """
    Update the XML content string directly for a specific segment ID.
    This preserves all whitespace and formatting.
    
    Args:
        content: XML content as a string
        segment_id: ID of the segment to update
        gemini_validation: Dictionary with validation info
        
    Returns:
        Updated XML content string
    """
    # Create patterns to find the RejectedForQA and ReasonForRejected elements
    # The pattern matches the entire element, including whitespace and tags
    rejected_pattern = re.compile(
        f'(<Segment\\s+id="{segment_id}".*?<RejectedForQA>)(.*?)(</RejectedForQA>)', 
        re.DOTALL
    )
    
    reason_pattern = re.compile(
        f'(<Segment\\s+id="{segment_id}".*?<ReasonForRejected>)(.*?)(</ReasonForRejected>)', 
        re.DOTALL
    )
    
    # First, update the RejectedForQA element
    rejected_match = rejected_pattern.search(content)
    if rejected_match:
        # Replace the value with "true"
        updated_content = content[:rejected_match.start(2)] + "true" + content[rejected_match.end(2):]
        content = updated_content
    
    # Then, update the ReasonForRejected element
    reason_match = reason_pattern.search(content)
    if reason_match:
        # Format the reason with type if available
        reason_text = gemini_validation['reason']
        if gemini_validation['reason_type']:
            reason_text = f"[{gemini_validation['reason_type']}] {reason_text}"
        
        # Replace the content
        updated_content = content[:reason_match.start(2)] + reason_text + content[reason_match.end(2):]
        content = updated_content
    
    return content

def update_original_xml_preserve_whitespace(original_file, gemini_validations, output_file):
    """
    Update the original XML with Gemini validation results, preserving all whitespace.
    
    Args:
        original_file: Path to original XML file
        gemini_validations: Dictionary of Gemini validations
        output_file: Path to save the updated XML
    
    Returns:
        Number of segments updated
    """
    try:
        # Read the file content as a string
        with open(original_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Track updates
        updated_count = 0
        
        # Update each segment that has a validation
        for segment_id, validation in gemini_validations.items():
            # Check if this segment exists in the content
            if f'id="{segment_id}"' in content:
                # Update the content for this segment
                content = update_xml_in_place(content, segment_id, validation)
                updated_count += 1
                logger.info(f"Updated segment {segment_id}: RejectedForQA=true")
        
        # Save the updated content
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(content)
            
        logger.info(f"Saved updated XML to {output_file}")
        return updated_count
        
    except Exception as e:
        logger.error(f"Error updating original XML: {str(e)}")
        return 0

def main():
    parser = argparse.ArgumentParser(description="Combine original XML with Gemini validation results")
    parser.add_argument("--original", required=True, help="Original XML file with segments")
    parser.add_argument("--gemini", required=True, help="Gemini validation results XML file")
    parser.add_argument("--output", required=True, help="Path to save updated XML")
    
    args = parser.parse_args()
    
    # Validate input files
    if not os.path.isfile(args.original):
        logger.error(f"Original XML file not found: {args.original}")
        return 1
        
    if not os.path.isfile(args.gemini):
        logger.error(f"Gemini validation file not found: {args.gemini}")
        return 1
    
    # Create output directory if needed
    output_dir = os.path.dirname(args.output)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Extract Gemini validations
    gemini_validations = extract_gemini_validations(args.gemini)
    
    # Update original XML with Gemini validations, preserving whitespace
    updated_count = update_original_xml_preserve_whitespace(args.original, gemini_validations, args.output)
    
    logger.info(f"Summary: {updated_count} segments marked as rejected based on Gemini validations")
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())