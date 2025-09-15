#!/usr/bin/env python3
"""
XML False-Only Segment Filter

This script reads an XML file containing Wiki segments and creates a new XML file
containing ONLY segments where RejectedForQA is explicitly "false".

Usage:
    python filter_false_only_segments.py --input input.xml --output output.xml
"""

import os
import argparse
import logging
import xml.etree.ElementTree as ET
from xml.dom import minidom
from typing import Dict, List, Optional, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def load_xml_file(file_path: str) -> Optional[ET.ElementTree]:
    """Load an XML file and return its ElementTree."""
    try:
        tree = ET.parse(file_path)
        logger.info(f"Loaded XML file: {file_path}")
        return tree
    except Exception as e:
        logger.error(f"Error loading XML file {file_path}: {str(e)}")
        return None

def filter_false_only_segments(input_xml: str, output_xml: str) -> Dict[str, int]:
    """
    Filter the XML file to keep ONLY segments with RejectedForQA="false".
    
    Args:
        input_xml: Path to input XML file
        output_xml: Path to save the filtered XML
        
    Returns:
        Statistics about the filtering process
    """
    # Stats to track
    stats = {
        "total_segments": 0,
        "false_segments": 0,
        "true_segments": 0,
        "undecided_segments": 0,
        "missing_segments": 0
    }
    
    # Load the original XML
    tree = load_xml_file(input_xml)
    if tree is None:
        return stats
    
    root = tree.getroot()
    
    # Find all segments in the original XML
    segments = root.findall(".//Segment")
    stats["total_segments"] = len(segments)
    
    # Identify segments to remove (anything not explicitly "false")
    segments_to_remove = []
    
    for segment in segments:
        rejected_elem = segment.find("RejectedForQA")
        
        if rejected_elem is None:
            # No RejectedForQA element - remove it
            stats["missing_segments"] += 1
            segments_to_remove.append(segment)
        elif rejected_elem.text == "false":
            # Explicitly marked as false - keep it
            stats["false_segments"] += 1
        elif rejected_elem.text == "true":
            # Rejected segment - remove it
            stats["true_segments"] += 1
            segments_to_remove.append(segment)
        elif rejected_elem.text == "undecided":
            # Undecided segment - remove it
            stats["undecided_segments"] += 1
            segments_to_remove.append(segment)
        else:
            # Unknown value - remove it
            stats["missing_segments"] += 1
            segments_to_remove.append(segment)
    
    # Remove all segments except those with RejectedForQA="false"
    for segment in segments_to_remove:
        # Try to find the parent
        parent = root
        for child in list(root):
            if child == segment:
                parent.remove(segment)
                break
            # Check if the segment is a child of this element
            for grandchild in list(child):
                if grandchild == segment:
                    child.remove(segment)
                    break
    
    # Save the filtered XML with pretty formatting
    try:
        # Convert to string
        xml_str = ET.tostring(root, encoding="utf-8", xml_declaration=True)
        
        # Pretty print the XML
        pretty_xml = minidom.parseString(xml_str).toprettyxml(indent="  ")
        
        # Save to file
        with open(output_xml, "w", encoding="utf-8") as f:
            f.write(pretty_xml)
        
        logger.info(f"Filtered XML saved to: {output_xml}")
    except Exception as e:
        logger.error(f"Error saving XML: {str(e)}")
        # Fallback to direct writing
        tree.write(output_xml, encoding="utf-8", xml_declaration=True)
        logger.info(f"Filtered XML saved to: {output_xml} (without pretty formatting)")
    
    return stats

def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Filter XML to keep only segments with RejectedForQA='false'")
    parser.add_argument("--input", required=True, help="Input XML file")
    parser.add_argument("--output", required=True, help="Output path for filtered XML")
    
    args = parser.parse_args()
    
    # Filter the XML
    stats = filter_false_only_segments(args.input, args.output)
    
    # Log summary
    logger.info(f"XML filtering complete:")
    logger.info(f"  Total segments: {stats['total_segments']}")
    logger.info(f"  Segments with RejectedForQA='false' kept: {stats['false_segments']}")
    logger.info(f"  Segments with RejectedForQA='true' removed: {stats['true_segments']}")
    logger.info(f"  Segments with RejectedForQA='undecided' removed: {stats['undecided_segments']}")
    logger.info(f"  Segments with missing RejectedForQA removed: {stats['missing_segments']}")

if __name__ == "__main__":
    main()