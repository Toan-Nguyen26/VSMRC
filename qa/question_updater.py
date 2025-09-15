#!/usr/bin/env python3
"""
XML Verification Processor

This script processes three XML files:
1. Original XML with Wiki segments and questions
2. OpenAI verification results XML
3. DeepSeek verification results XML

It applies verification logic:
- If both models agree (valid): Keep segment as is
- If both models agree (invalid): Mark segment as rejected
- If models disagree: Mark segment as undecided
- If missing results from both models: Mark segment as undecided
- If segment has <QA empty="true">: Mark segment as rejected (applied after other checks)

Usage:
    python question_updater.py --original <original.xml> --openai <openai.xml> --deepseek <deepseek.xml> --output <output.xml>
"""

import os
import argparse
import logging
import xml.etree.ElementTree as ET
from typing import Dict, Any, List, Optional

# Configure simple logging
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

def extract_verification_results(xml_file: str, model_name: str) -> Dict[str, Dict[str, Any]]:
    """
    Extract verification results from a model's XML file.
    
    Args:
        xml_file: Path to XML file with verification results
        model_name: Name of the model ("openai" or "deepseek")
        
    Returns:
        Dictionary mapping segment IDs to verification results
    """
    results = {}
    
    # Load the XML file
    tree = load_xml_file(xml_file)
    if tree is None:
        return results
    
    root = tree.getroot()
    
    # First try the format where is_suitable is directly under Segment
    for segment in root.findall(".//Segment"):
        segment_id = segment.get("id")
        if not segment_id:
            continue
        
        # Extract verification data
        is_suitable_elem = segment.find("is_suitable")
        reason_elem = segment.find("reason")
        
        # Skip if no verification data
        if is_suitable_elem is None:
            continue
        
        # Default values
        is_suitable = False
        reason = "No reason provided"
        
        # Extract values if elements exist
        if is_suitable_elem is not None and is_suitable_elem.text:
            is_suitable_text = is_suitable_elem.text.strip().lower()
            is_suitable = is_suitable_text == "true"
        
        if reason_elem is not None and reason_elem.text:
            reason = reason_elem.text.strip()
        
        # Store the results
        results[segment_id] = {
            "is_suitable": is_suitable,
            "reason": reason,
            "model": model_name
        }
    
    # If no results found, try alternative format 
    if not results:
        # Try validation_results format
        for segment in root.findall(".//segment"):
            segment_id = segment.get("id")
            if not segment_id:
                continue
            
            # Extract verification data
            is_suitable_elem = segment.find("is_suitable")
            reason_elem = segment.find("reason")
            
            # Default values
            is_suitable = False
            reason = "No reason provided"
            
            # Extract values if elements exist
            if is_suitable_elem is not None and is_suitable_elem.text:
                is_suitable_text = is_suitable_elem.text.strip().lower()
                is_suitable = is_suitable_text == "true"
            
            if reason_elem is not None and reason_elem.text:
                reason = reason_elem.text.strip()
            
            # Store the results
            results[segment_id] = {
                "is_suitable": is_suitable,
                "reason": reason,
                "model": model_name
            }
    
    logger.info(f"Extracted {len(results)} verification results from {model_name}")
    return results

def combine_verification_results(openai_results: Dict[str, Dict[str, Any]], 
                                deepseek_results: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Combine verification results from both models using our specific decision logic:
    - If both models agree (both say valid OR both say invalid): 
        - If both valid: Keep as is
        - If both invalid: Mark as rejected
    - If models disagree: Mark as undecided
    
    Args:
        openai_results: OpenAI verification results
        deepseek_results: DeepSeek verification results
        
    Returns:
        Combined verification results with decision
    """
    combined_results = {}
    
    # Get all unique segment IDs
    all_segment_ids = set(openai_results.keys()).union(set(deepseek_results.keys()))
    logger.info(f"Processing {len(all_segment_ids)} unique segments across both models")
    
    # Stats for logging
    both_agree_valid = 0
    both_agree_invalid = 0
    models_disagree = 0
    openai_only = 0
    deepseek_only = 0
    
    # Process each segment
    for segment_id in all_segment_ids:
        openai_result = openai_results.get(segment_id)
        deepseek_result = deepseek_results.get(segment_id)
        
        # Initialize combined result
        combined_result = {
            "segment_id": segment_id,
            "rejected_for_qa": None,  # Will be set based on logic
            "reason": None,           # Will be populated based on logic
            "openai_result": openai_result,
            "deepseek_result": deepseek_result
        }
        
        # Apply verification logic
        if openai_result and deepseek_result:
            # Both models assessed this segment
            openai_suitable = openai_result.get("is_suitable", False)
            deepseek_suitable = deepseek_result.get("is_suitable", False)
            
            if openai_suitable == deepseek_suitable:
                # Models agree
                if openai_suitable:
                    # Both say it's valid - don't change anything
                    combined_result["rejected_for_qa"] = "no"
                    combined_result["reason"] = "Both models agree: Valid"
                    both_agree_valid += 1
                else:
                    # Both say it's invalid - mark as rejected
                    combined_result["rejected_for_qa"] = "yes"
                    combined_result["reason"] = f"Both models agree: Invalid. OpenAI: {openai_result.get('reason')}. DeepSeek: {deepseek_result.get('reason')}"
                    both_agree_invalid += 1
            else:
                # Models disagree - mark as undecided per requirements
                combined_result["rejected_for_qa"] = "undecided"
                if openai_suitable:
                    combined_result["reason"] = f"Models disagree: OpenAI (valid), DeepSeek (invalid: {deepseek_result.get('reason')})"
                else:
                    combined_result["reason"] = f"Models disagree: OpenAI (invalid: {openai_result.get('reason')}), DeepSeek (valid)"
                models_disagree += 1
        elif openai_result:
            # Only OpenAI assessed this segment
            openai_suitable = openai_result.get("is_suitable", False)
            if openai_suitable:
                combined_result["rejected_for_qa"] = "no"
                combined_result["reason"] = "Only OpenAI verified: Valid"
            else:
                combined_result["rejected_for_qa"] = "yes"
                combined_result["reason"] = f"Only OpenAI verified: Invalid. Reason: {openai_result.get('reason')}"
            openai_only += 1
        elif deepseek_result:
            # Only DeepSeek assessed this segment
            deepseek_suitable = deepseek_result.get("is_suitable", False)
            if deepseek_suitable:
                combined_result["rejected_for_qa"] = "no"
                combined_result["reason"] = "Only DeepSeek verified: Valid"
            else:
                combined_result["rejected_for_qa"] = "yes"
                combined_result["reason"] = f"Only DeepSeek verified: Invalid. Reason: {deepseek_result.get('reason')}"
            deepseek_only += 1
        
        # Add to combined results
        combined_results[segment_id] = combined_result
    
    # Log statistics
    logger.info(f"Verification decision statistics:")
    logger.info(f"  Both models agree valid: {both_agree_valid}")
    logger.info(f"  Both models agree invalid: {both_agree_invalid}")
    logger.info(f"  Models disagree (marked as undecided): {models_disagree}")
    logger.info(f"  Only OpenAI verified: {openai_only}")
    logger.info(f"  Only DeepSeek verified: {deepseek_only}")
    
    return combined_results

def update_original_xml(original_xml: str, combined_results: Dict[str, Dict[str, Any]], output_xml: str) -> Dict[str, int]:
    """
    Update the original XML with verification results.
    
    Args:
        original_xml: Path to original XML file
        combined_results: Combined verification results
        output_xml: Path to save the updated XML
        
    Returns:
        Statistics about the update process
    """
    # Stats to track
    stats = {
        "total_segments": 0,
        "unchanged": 0,
        "rejected": 0,
        "undecided": 0,
        "missing_results": 0,
        "empty_qa": 0
    }
    
    # Load the original XML
    tree = load_xml_file(original_xml)
    if tree is None:
        return stats
    
    root = tree.getroot()
    
    # Find all segments in the original XML
    segments = root.findall(".//Segment")
    stats["total_segments"] = len(segments)
    
    # First pass: Update each segment based on verification results
    for segment in segments:
        segment_id = segment.get("id")
        
        if not segment_id or segment_id not in combined_results:
            # Mark segments with missing results as "undecided"
            stats["missing_results"] += 1
            
            # Mark as undecided
            rejected_elem = segment.find("RejectedForQA")
            if rejected_elem is None:
                rejected_elem = ET.SubElement(segment, "RejectedForQA")
            rejected_elem.text = "undecided"
            
            # Update the reason
            reason_elem = segment.find("ReasonForRejected")
            if reason_elem is None:
                reason_elem = ET.SubElement(segment, "ReasonForRejected")
            reason_elem.text = "Missing verification results from models"
            
            continue
        
        # Get verification result for this segment
        result = combined_results[segment_id]
        rejected_for_qa = result.get("rejected_for_qa")
        reason = result.get("reason", "")
        
        # Remove existing verification elements if present
        # This ensures we don't end up with duplicate elements
        for elem_to_remove in segment.findall("is_suitable"):
            segment.remove(elem_to_remove)
        for elem_to_remove in segment.findall("reason"):
            segment.remove(elem_to_remove)
        
        # Update the segment based on the decision
        if rejected_for_qa == "yes":
            # Mark as rejected
            rejected_elem = segment.find("RejectedForQA")
            if rejected_elem is None:
                rejected_elem = ET.SubElement(segment, "RejectedForQA")
            rejected_elem.text = "true"
            
            # Update the reason
            reason_elem = segment.find("ReasonForRejected")
            if reason_elem is None:
                reason_elem = ET.SubElement(segment, "ReasonForRejected")
            reason_elem.text = reason
            
            stats["rejected"] += 1
        elif rejected_for_qa == "undecided":
            # Mark as undecided
            rejected_elem = segment.find("RejectedForQA")
            if rejected_elem is None:
                rejected_elem = ET.SubElement(segment, "RejectedForQA")
            rejected_elem.text = "undecided"
            
            # Update the reason
            reason_elem = segment.find("ReasonForRejected")
            if reason_elem is None:
                reason_elem = ET.SubElement(segment, "ReasonForRejected")
            reason_elem.text = reason
            
            stats["undecided"] += 1
        else:
            # Both models agree it's valid - don't change anything
            # But ensure RejectedForQA is properly set to "false"
            rejected_elem = segment.find("RejectedForQA")
            if rejected_elem is not None:
                rejected_elem.text = "false"
            else:
                # Add the element if it doesn't exist
                rejected_elem = ET.SubElement(segment, "RejectedForQA")
                rejected_elem.text = "false"
            
            stats["unchanged"] += 1
    
    # Second pass: Check for empty QA elements and mark as rejected
    # This is done after verification to avoid overwriting validation decisions
    empty_qa_count = 0
    for segment in segments:
        # Check for empty QA element
        qa_elem = segment.find("QA")
        if qa_elem is not None and qa_elem.get("empty") == "true":
            # Mark segments with empty QA as rejected
            empty_qa_count += 1
            
            # Mark as rejected
            rejected_elem = segment.find("RejectedForQA")
            if rejected_elem is None:
                rejected_elem = ET.SubElement(segment, "RejectedForQA")
            rejected_elem.text = "true"
            
            # Update the reason
            reason_elem = segment.find("ReasonForRejected")
            if reason_elem is None:
                reason_elem = ET.SubElement(segment, "ReasonForRejected")
            reason_elem.text = "No QA content available"
    
    # Update stats with empty QA count
    stats["empty_qa"] = empty_qa_count
    
    # Save the updated XML with pretty formatting
    try:
        # Convert to string
        xml_str = ET.tostring(root, encoding="utf-8", xml_declaration=True)
        
        # Pretty print the XML
        from xml.dom import minidom
        pretty_xml = minidom.parseString(xml_str).toprettyxml(indent="  ")
        
        # Save to file
        with open(output_xml, "w", encoding="utf-8") as f:
            f.write(pretty_xml)
        
        logger.info(f"Updated XML saved to: {output_xml}")
    except Exception as e:
        logger.error(f"Error saving XML: {str(e)}")
        # Fallback to direct writing
        tree.write(output_xml, encoding="utf-8", xml_declaration=True)
        logger.info(f"Updated XML saved to: {output_xml} (without pretty formatting)")
    
    return stats

def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Verify and update XML with model verification results")
    parser.add_argument("--original", required=True, help="Original XML file")
    parser.add_argument("--openai", required=True, help="OpenAI verification results XML")
    parser.add_argument("--deepseek", required=True, help="DeepSeek verification results XML")
    parser.add_argument("--output", required=True, help="Output path for updated XML")
    
    args = parser.parse_args()
    
    # Extract verification results from both models
    openai_results = extract_verification_results(args.openai, "openai")
    deepseek_results = extract_verification_results(args.deepseek, "deepseek")
    
    # Combine verification results
    combined_results = combine_verification_results(openai_results, deepseek_results)
    
    # Update the original XML
    stats = update_original_xml(args.original, combined_results, args.output)
    
    # Initialize any missing stats keys with default value of 0
    default_keys = ["total_segments", "unchanged", "rejected", "undecided", "missing_results", "empty_qa"]
    for key in default_keys:
        if key not in stats:
            stats[key] = 0
    
    # Log summary
    logger.info(f"XML verification complete:")
    logger.info(f"  Total segments: {stats['total_segments']}")
    logger.info(f"  Unchanged (valid): {stats['unchanged']}")
    logger.info(f"  Rejected (invalid): {stats['rejected']}")
    logger.info(f"  Undecided (models disagree): {stats['undecided']}")
    logger.info(f"  Missing results (marked as undecided): {stats['missing_results']}")
    logger.info(f"  Empty QA (marked as rejected): {stats['empty_qa']}")

if __name__ == "__main__":
    main()