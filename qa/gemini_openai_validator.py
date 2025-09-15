"""
XML Gemini Validator

Reviews Vietnamese Wikipedia segments for appropriateness and outputs standardized XML.
Processes segments in batches for efficiency.
"""

import os
import argparse
import logging
import json
import time
import re
import xml.etree.ElementTree as ET
from xml.dom import minidom
from dotenv import load_dotenv
from google import genai

# Load environment variables and initialize Gemini
load_dotenv()
api_key = os.getenv("GEMINI_API_KEY")
if not api_key:
    raise ValueError("GEMINI_API_KEY environment variable not set")
client = genai.Client(api_key=api_key)

# Configure logging - concise output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%H:%M:%S',
    handlers=[logging.StreamHandler(), logging.FileHandler("gemini_validation.log")]
)
logger = logging.getLogger(__name__)

# Default Gemini model
DEFAULT_MODEL = "gemini-2.0-flash-lite"

def load_xml_file(file_path):
    """Load an XML file."""
    try:
        tree = ET.parse(file_path)
        return tree.getroot()
    except Exception as e:
        logger.error(f"Error loading XML file {file_path}: {str(e)}")
        return None

def extract_segment_text(xml_file):
    """Extract segment text from XML input file."""
    segments = {}
    
    try:
        # Parse the XML file
        root = load_xml_file(xml_file)
        if root is None:
            return segments
            
        # Extract segments
        for segment_elem in root.findall('./Segment'):
            segment_id = segment_elem.get('id')
            
            if segment_id:
                seg_text = segment_elem.find('SegmentText')
                seg_title = segment_elem.find('SegmentTitle')
                
                if seg_text is not None and seg_text.text:
                    segments[segment_id] = {
                        'segment_id': segment_id,
                        'segment_title': seg_title.text if seg_title is not None else "",
                        'segment_text': seg_text.text
                    }
        
        logger.info(f"Extracted {len(segments)} segments from XML input")
        
    except Exception as e:
        logger.error(f"Error extracting segments: {str(e)}")
    
    return segments

def create_batch_xml(segments_batch):
    """
    Create XML for a batch of segments.
    
    Args:
        segments_batch: List of segment dictionaries
        
    Returns:
        XML string with the batch segments
    """
    root = ET.Element("segments")
    
    for segment in segments_batch:
        segment_elem = ET.SubElement(root, "segment")
        segment_elem.set("id", segment['segment_id'])
        
        title_elem = ET.SubElement(segment_elem, "title")
        title_elem.text = segment.get('segment_title', '')
        
        text_elem = ET.SubElement(segment_elem, "text")
        text_elem.text = segment['segment_text']
    
    # Convert to string
    xml_str = ET.tostring(root, encoding='utf-8').decode('utf-8')
    
    # Format with proper indentation
    xml_pretty = minidom.parseString(xml_str).toprettyxml(indent="  ")
    
    return xml_pretty

def create_batch_prompt(segments_batch):
    """
    Create a prompt for Gemini to review multiple segments.
    
    Args:
        segments_batch: List of segment dictionaries
        
    Returns:
        Prompt string for Gemini
    """
    # Create XML for segments
    batch_xml = create_batch_xml(segments_batch)
    
    # Create prompt
    prompt = f"""You are given segments from one or more Vietnamese Wikipedia articles to review.
    Your task is to evaluate each segment for its suitability for generating educational multiple-choice questions.

    Here are the segments:

    {batch_xml}

    - Evaluate each segment based on:
        1. **Sensitive Information**: No political controversies, religious topics, violence, or mature themes.
        2. **Accuracy**: No misleading, incomplete, or culturally insensitive content.
        3. **Duplicates**: If nearly identical to another segment (>80% overlap), keep only the most detailed (longest, most informative).
        4. **Specificity**: Contains specific entities (e.g., names, dates, events) for unique questions.
        5. **Passage Dependence**: Questions must require the full segment, not general knowledge, a single sentence, or external data.
        6. **Clarity**: Clear, unambiguous, and free of contradictions.
        7. **Distractor Potential**: Supports questions with at least one passage-derived distractor that's plausible but clearly wrong.
        8. **Complexity**: Has 2-5 entities/relationships with clear connections, not a simple list or overly technical.
    - Mark as unsuitable if:
        - It has sensitive content (e.g., political, religious, violent).
        - It's misleading, incomplete, or culturally insensitive.
        - It's a duplicate (not the most detailed version).
        - It's too generic (e.g., "Hà Nội is a city"), allowing obvious questions.
        - Questions could be answered without the segment (e.g., general knowledge).
        - It's vague, contradictory, or overly complex, confusing models.
        - It lacks details for plausible, passage-derived distractors.

    Reply in XML format:
    <validation>
    <segment id="SEGMENT_ID_1">
        <is_appropriate>yes/no</is_appropriate>
        <!-- Only include reason element if is_appropriate is "no" -->
        <reason type="[criterion]">Brief reason (max 30 words)</reason> <!-- Omit this for "yes" responses -->
    </segment>
    <segment id="SEGMENT_ID_2">
        <is_appropriate>yes/no</is_appropriate>
        <!-- Only include reason element if is_appropriate is "no" -->
        <reason type="[criterion]">Brief reason (max 30 words)</reason> <!-- Omit this for "yes" responses -->
    </segment>
    <!-- Include all segments -->
    </validation>

    Use "yes" or "no" for is_appropriate. IMPORTANT: Only include the reason element when is_appropriate is "no". For appropriate segments, omit the reason element entirely. When providing reasons for rejected segments, replace [criterion] with the most relevant criterion type from: sensitive_information, accuracy, duplicates, specificity, passage_dependence, clarity, distractor_potential, complexity.
    
    Make sure to include ALL segments in your response, with the correct segment IDs from the input.
    """
    
    return prompt

def process_batch_with_gemini(segments_batch, model_name=DEFAULT_MODEL):
    """
    Process a batch of segments with Gemini.
    
    Args:
        segments_batch: List of segment dictionaries
        model_name: Gemini model to use
        
    Returns:
        Validation XML string or None on error
    """
    try:
        # Get batch prompt
        prompt = create_batch_prompt(segments_batch)
        
        # Call Gemini API
        response = client.models.generate_content(
            model=model_name, 
            contents=prompt
        )
        
        # Extract response text
        response_text = response.text
        
        # Try to extract validation XML
        validation_match = re.search(r'<validation>(.*?)</validation>', response_text, re.DOTALL)
        if validation_match:
            validation_xml = f"<validation>{validation_match.group(1)}</validation>"
            
            # Verify the XML is well-formed
            try:
                ET.fromstring(validation_xml)
                return validation_xml
            except ET.ParseError:
                logger.error("Gemini returned malformed XML")
                return create_error_validation(segments_batch, "Malformed XML response")
                
        else:
            logger.error("No validation XML found in Gemini response")
            return create_error_validation(segments_batch, "No validation XML in response")
    
    except Exception as e:
        logger.error(f"Error processing batch with Gemini: {str(e)}")
        return create_error_validation(segments_batch, str(e))

def create_error_validation(segments_batch, error_message):
    """
    Create error validation XML for a batch of segments.
    
    Args:
        segments_batch: List of segment dictionaries
        error_message: Error message to include
        
    Returns:
        XML string with error validations
    """
    # Create root element
    root = ET.Element("validation")
    
    # Add error validation for each segment
    for segment in segments_batch:
        segment_elem = ET.SubElement(root, "segment")
        segment_elem.set("id", segment['segment_id'])
        
        is_appropriate = ET.SubElement(segment_elem, "is_appropriate")
        is_appropriate.text = "no"  # Default to no on error
        
        reason = ET.SubElement(segment_elem, "reason")
        reason.set("type", "error")
        reason.text = f"Error: {error_message[:30]}..."
    
    # Convert to string
    xml_str = ET.tostring(root, encoding='utf-8').decode('utf-8')
    
    # Format with proper indentation
    xml_pretty = minidom.parseString(xml_str).toprettyxml(indent="  ")
    
    return xml_pretty

def combine_validation_results(validation_xmls):
    """
    Combine multiple validation XMLs into one.
    
    Args:
        validation_xmls: List of validation XML strings
        
    Returns:
        Combined XML string
    """
    # Create root element
    root = ET.Element("validationResults")
    
    # Process each validation XML
    for xml_str in validation_xmls:
        if not xml_str:
            continue
            
        try:
            # Parse the validation XML
            validation_elem = ET.fromstring(xml_str)
            
            # Add each segment validation to the root
            for segment_elem in validation_elem.findall('./segment'):
                # Create a new segment element in the root
                new_segment = ET.SubElement(root, "segment")
                
                # Copy segment ID attribute
                segment_id = segment_elem.get('id')
                if segment_id:
                    new_segment.set('id', segment_id)
                
                # Copy is_appropriate element
                is_appropriate_elem = segment_elem.find('is_appropriate')
                if is_appropriate_elem is not None:
                    is_appropriate = ET.SubElement(new_segment, "is_appropriate")
                    if is_appropriate_elem.text:
                        is_appropriate.text = is_appropriate_elem.text
                    
                    # Only include reason if is_appropriate is "no"
                    if is_appropriate_elem.text and is_appropriate_elem.text.lower() == "no":
                        reason_elem = segment_elem.find('reason')
                        if reason_elem is not None:
                            reason = ET.SubElement(new_segment, "reason")
                            if reason_elem.text:
                                reason.text = reason_elem.text
                            
                            # Copy reason attributes
                            for attr_name, attr_value in reason_elem.attrib.items():
                                reason.set(attr_name, attr_value)
        
        except ET.ParseError as e:
            logger.error(f"Error parsing validation XML: {str(e)}")
    
    # Convert to string
    xml_str = ET.tostring(root, encoding='utf-8').decode('utf-8')
    
    # Format with proper indentation
    xml_pretty = minidom.parseString(xml_str).toprettyxml(indent="  ")
    
    return xml_pretty

def process_validations(segments_xml, output_file, model_name=DEFAULT_MODEL, 
                       rate_limit=20, batch_size=20):
    """
    Process segments with Gemini in batches.
    
    Args:
        segments_xml: Path to XML file with segments
        output_file: Path to save combined validation results
        model_name: Gemini model to use
        rate_limit: API rate limit (requests per minute)
        batch_size: Maximum segments per batch
        
    Returns:
        Dictionary with processing statistics
    """
    # Load segments
    segments = extract_segment_text(segments_xml)
    
    if not segments:
        logger.error("No segments found in XML")
        return {"error": "No segments found"}
    
    # Prepare segments for validation
    segments_to_validate = list(segments.values())
    
    logger.info(f"Found {len(segments_to_validate)} segments to validate")
    
    # Create batches
    num_batches = (len(segments_to_validate) + batch_size - 1) // batch_size  # Ceiling division
    
    batches = []
    for i in range(num_batches):
        start_idx = i * batch_size
        end_idx = min(start_idx + batch_size, len(segments_to_validate))
        batches.append(segments_to_validate[start_idx:end_idx])
    
    logger.info(f"Processing {len(segments_to_validate)} segments in {len(batches)} batches")
    
    # Calculate delay for rate limiting
    delay_seconds = 60 / rate_limit
    
    # Process each batch
    validation_xmls = []
    
    for i, batch in enumerate(batches):
        logger.info(f"Processing batch {i+1}/{len(batches)} ({len(batch)} segments)")
        
        # Process batch
        batch_xml = process_batch_with_gemini(batch, model_name)
        
        if batch_xml:
            validation_xmls.append(batch_xml)
            
            # Save individual batch results
            batch_file = os.path.join(os.path.dirname(output_file), f"batch_{i+1}_validation.xml")
            with open(batch_file, 'w', encoding='utf-8') as f:
                f.write(batch_xml)
        
        # Apply rate limit delay (except for last batch)
        if i < len(batches) - 1:
            time.sleep(delay_seconds)
    
    # Combine all validation results
    combined_xml = combine_validation_results(validation_xmls)
    
    # Save combined results
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(combined_xml)
    
    logger.info(f"Saved combined validation results to {output_file}")
    
    # Calculate statistics
    try:
        root = ET.fromstring(combined_xml)
        total_segments = len(root.findall('./segment'))
        approved = sum(1 for elem in root.findall('./segment/is_appropriate') 
                     if elem is not None and elem.text and elem.text.lower() == 'yes')
        rejected = total_segments - approved
        
        stats = {
            "segments_processed": len(segments_to_validate),
            "segments_validated": total_segments,
            "approved": approved,
            "rejected": rejected,
            "batches": len(batches)
        }
        
        logger.info(f"Summary: {total_segments} segments validated ({approved} approved, {rejected} rejected)")
        return stats
        
    except Exception as e:
        logger.error(f"Error calculating statistics: {str(e)}")
        return {
            "error": f"Error calculating statistics: {str(e)}",
            "segments_processed": len(segments_to_validate),
            "batches": len(batches)
        }
    
def main():
    parser = argparse.ArgumentParser(description="Gemini validator for Vietnamese Wikipedia segments")
    parser.add_argument("--segments", required=True, help="XML file with segments to validate")
    parser.add_argument("--output", required=True, help="Path to save Gemini validation results")
    parser.add_argument("--model", default=DEFAULT_MODEL, help=f"Gemini model name (default: {DEFAULT_MODEL})")
    parser.add_argument("--rate-limit", type=int, default=20, help="API rate limit (requests/minute)")
    parser.add_argument("--batch-size", type=int, default=20, help="Maximum segments per batch")
    
    args = parser.parse_args()
    
    # Validate input files
    if not os.path.isfile(args.segments):
        logger.error(f"Segments file not found: {args.segments}")
        return 1
    
    # Create output directory if needed
    output_dir = os.path.dirname(args.output)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Process validations
    stats = process_validations(
        args.segments,
        args.output,
        args.model,
        args.rate_limit,
        args.batch_size
    )
    
    if "error" in stats:
        logger.error(stats["error"])
        return 1
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())

    # Can you create a file which is takes in the original xml, like the one I pasted, as well as the result by gemini and check if , well if any viloate in gemini, if it's not is_appropriate, then we ignore, else we edit the rejected qa to true and the reason will be the reason given in the gemini reason