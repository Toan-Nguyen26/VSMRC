#!/usr/bin/env python3
"""
Parallel Vietnamese Wikipedia Document Processor

This script:
1. Loads the tdtunlp/wikipedia_vi dataset
2. Uses underthesea to count tokens in each document in parallel
3. Filters documents with token counts between 600-3000
4. Saves documents with filenames based on revid
5. Additionally saves JSON metadata for each document
6. Calculates and logs average token count for filtered documents

Features parallel processing for significantly improved performance.
"""

import os
import argparse
import logging
import time
import json
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import partial
import numpy as np
from datasets import load_dataset
from underthesea import word_tokenize
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("wiki_processing.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def count_tokens(text):
    """Count tokens in Vietnamese text using underthesea"""
    tokens = word_tokenize(text)
    return len(tokens)

def process_article_batch(batch_data, output_dir, json_dir, min_tokens, max_tokens):
    """
    Process a batch of articles in parallel
    
    Args:
        batch_data: Batch of articles from the dataset
        output_dir: Directory to save filtered articles
        json_dir: Directory to save JSON metadata
        min_tokens: Minimum token threshold
        max_tokens: Maximum token threshold
        
    Returns:
        dict: Processing statistics for this batch
    """
    results = {
        'processed': 0,
        'saved': 0,
        'filtered': 0,
        'all_token_counts': [],     # Token counts for all articles in batch
        'filtered_token_counts': [] # Token counts for articles that pass the filter
    }
    
    for article in batch_data:
        try:
            id = article['id']
            revid = article['revid']
            text = article['text']
            title = article.get('title', '')
            url = article.get('url', '')
            
            # Count tokens
            token_count = count_tokens(text)
            results['all_token_counts'].append(token_count)
            results['processed'] += 1
            
            # Filter by token count
            if min_tokens <= token_count <= max_tokens:
                # Create filename based on revid
                filename = f"{id}_{revid}.txt"
                filepath = os.path.join(output_dir, filename)
                
                # Save text article
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(text)
                
                # Save JSON metadata
                json_filepath = os.path.join(json_dir, f"{id}_{revid}.json")
                
                # Create JSON metadata object
                metadata = {
                    "file_name": filename,
                    "title": title,
                    "url": url,
                    "text": text,
                    "token_count": token_count
                }
                
                # Save JSON metadata
                with open(json_filepath, "w", encoding="utf-8") as f:
                    json.dump(metadata, f, ensure_ascii=False, indent=2)
                
                # Track token count for filtered articles specifically
                results['filtered_token_counts'].append(token_count)
                results['saved'] += 1
            else:
                results['filtered'] += 1
                
        except Exception as e:
            logger.error(f"Error processing article with id {article.get('id', 'unknown')}: {str(e)}")
    
    return results

def process_wiki_articles(output_dir="viwiki_processed", json_dir="viwiki_json", min_tokens=600, max_tokens=3000, 
                         max_articles=None, workers=None, batch_size=100):
    """
    Process Vietnamese Wikipedia articles in parallel and save those with token counts in the specified range
    
    Args:
        output_dir (str): Directory to save processed articles
        json_dir (str): Directory to save JSON metadata
        min_tokens (int): Minimum number of tokens for inclusion
        max_tokens (int): Maximum number of tokens for inclusion
        max_articles (int, optional): Maximum number of articles to process, None for all
        workers (int, optional): Number of worker processes, None for auto-detection
        batch_size (int): Number of articles to process in each batch
    """
    start_time = time.time()
    
    # Create output directories if needed
    for directory in [output_dir, json_dir]:
        if not os.path.exists(directory):
            os.makedirs(directory)
            logger.info(f"Created directory: {directory}")
    
    # Load the dataset
    logger.info(f"Loading Vietnamese Wikipedia dataset (tdtunlp/wikipedia_vi)...")
    dataset = load_dataset("tdtunlp/wikipedia_vi")
    train_dataset = dataset['train']
    total_articles = len(train_dataset)
    logger.info(f"Dataset loaded with {total_articles} articles")
    
    # Limit processing if requested
    if max_articles and max_articles < total_articles:
        logger.info(f"Limiting processing to {max_articles} articles")
        process_count = max_articles
    else:
        process_count = total_articles
    
    # Determine optimal batch size and number of workers
    if not workers:
        import multiprocessing
        workers = max(1, multiprocessing.cpu_count() - 1)  # Leave one CPU free
    
    logger.info(f"Using {workers} worker processes with batch size {batch_size}")
    
    # Calculate number of batches
    num_batches = (process_count + batch_size - 1) // batch_size  # Ceiling division
    
    # Create batches of indices
    all_indices = list(range(process_count))
    batches = [all_indices[i:i + batch_size] for i in range(0, process_count, batch_size)]
    
    # Initialize counters
    total_processed = 0
    total_saved = 0
    total_filtered = 0
    all_token_counts = []       # Token counts for all processed articles
    filtered_token_counts = []  # Token counts only for articles that pass the filter
    
    # Process articles in parallel batches
    with ProcessPoolExecutor(max_workers=workers) as executor:
        # Create a partial function with fixed arguments
        process_func = partial(
            process_batch_with_indices, 
            dataset=train_dataset,
            output_dir=output_dir,
            json_dir=json_dir,
            min_tokens=min_tokens,
            max_tokens=max_tokens
        )
        
        # Submit all batches to the executor
        future_to_batch = {executor.submit(process_func, batch_idx): batch_idx 
                          for batch_idx, batch_idx in enumerate(batches)}
        
        # Process results as they complete
        for future in tqdm(as_completed(future_to_batch), total=len(batches), desc="Processing batches"):
            try:
                result = future.result()
                total_processed += result['processed']
                total_saved += result['saved']
                total_filtered += result['filtered']
                all_token_counts.extend(result['all_token_counts'])
                filtered_token_counts.extend(result['filtered_token_counts'])
            except Exception as e:
                logger.error(f"Error in batch processing: {str(e)}")
    
    # Calculate processing time
    processing_time = time.time() - start_time
    
    # Calculate averages
    all_avg = sum(all_token_counts) / len(all_token_counts) if all_token_counts else 0
    filtered_avg = sum(filtered_token_counts) / len(filtered_token_counts) if filtered_token_counts else 0
    
    # Log the filtered average
    logger.info(f"Average token count for filtered articles: {filtered_avg:.2f}")
    
    # Write summary
    summary_path = os.path.join(output_dir, "summary.txt")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(f"Vietnamese Wikipedia Articles Processing Summary\n")
        f.write(f"===============================================\n\n")
        f.write(f"Total articles processed: {total_processed}\n")
        f.write(f"Articles saved: {total_saved}\n")
        f.write(f"Articles filtered out: {total_filtered}\n")
        f.write(f"Token filter range: {min_tokens}-{max_tokens}\n")
        f.write(f"Processing time: {processing_time:.2f} seconds\n")
        f.write(f"Processing rate: {total_processed/processing_time:.2f} articles/second\n\n")
        
        # Token statistics for all articles
        if all_token_counts:
            min_seen = min(all_token_counts)
            max_seen = max(all_token_counts)
            
            # Calculate percentiles for all articles
            all_percentiles = np.percentile(all_token_counts, [25, 50, 75])
            
            f.write(f"Token statistics for ALL processed articles:\n")
            f.write(f"  Average tokens per article: {all_avg:.2f}\n")
            f.write(f"  Minimum tokens: {min_seen}\n")
            f.write(f"  Maximum tokens: {max_seen}\n")
            f.write(f"  25th percentile: {all_percentiles[0]:.2f}\n")
            f.write(f"  Median (50th percentile): {all_percentiles[1]:.2f}\n")
            f.write(f"  75th percentile: {all_percentiles[2]:.2f}\n\n")
            
        # Token statistics for filtered articles
        if filtered_token_counts:
            filtered_min = min(filtered_token_counts)
            filtered_max = max(filtered_token_counts)
            
            # Calculate percentiles for filtered articles
            filtered_percentiles = np.percentile(filtered_token_counts, [25, 50, 75])
            
            f.write(f"Token statistics for FILTERED articles (those that were saved):\n")
            f.write(f"  Average tokens per filtered article: {filtered_avg:.2f}\n")
            f.write(f"  Minimum tokens: {filtered_min}\n")
            f.write(f"  Maximum tokens: {filtered_max}\n")
            f.write(f"  25th percentile: {filtered_percentiles[0]:.2f}\n")
            f.write(f"  Median (50th percentile): {filtered_percentiles[1]:.2f}\n")
            f.write(f"  75th percentile: {filtered_percentiles[2]:.2f}\n\n")
            
        # Token count distribution
        f.write(f"Token count distribution for ALL articles:\n")
        bins = [0, 300, 600, 1000, 1500, 2000, 3000, 5000, 10000, float('inf')]
        bin_names = ["<300", "300-599", "600-999", "1000-1499", "1500-1999", "2000-2999", 
                     "3000-4999", "5000-9999", "10000+"]
        
        hist, _ = np.histogram(all_token_counts, bins=bins)
        for i, count in enumerate(hist):
            percentage = count / len(all_token_counts) * 100
            f.write(f"  {bin_names[i]}: {count} articles ({percentage:.2f}%)\n")
    
    logger.info(f"Processing complete!")
    logger.info(f"Articles processed: {total_processed}")
    logger.info(f"Articles saved: {total_saved}")
    logger.info(f"Articles filtered out: {total_filtered}")
    logger.info(f"Average tokens in all articles: {all_avg:.2f}")
    logger.info(f"Average tokens in filtered articles: {filtered_avg:.2f}")
    logger.info(f"Processing time: {processing_time:.2f} seconds")
    logger.info(f"Processing rate: {total_processed/processing_time:.2f} articles/second")
    logger.info(f"See {summary_path} for details.")


def process_batch_with_indices(batch_indices, dataset, output_dir, json_dir, min_tokens, max_tokens):
    """
    Process a batch of articles by their indices
    
    Args:
        batch_indices: List of indices to process
        dataset: The dataset to pull articles from
        output_dir: Directory to save filtered articles
        json_dir: Directory to save JSON metadata
        min_tokens: Minimum token threshold
        max_tokens: Maximum token threshold
        
    Returns:
        dict: Processing statistics for this batch
    """
    results = {
        'processed': 0,
        'saved': 0,
        'filtered': 0,
        'all_token_counts': [],     # Token counts for all articles in batch
        'filtered_token_counts': [] # Token counts for articles that pass the filter
    }
    
    # Get the batch of articles
    batch_data = [dataset[idx] for idx in batch_indices]
    
    for article in batch_data:
        try:
            id = article['id']
            revid = article['revid']
            text = article['text']
            title = article.get('title', '')
            url = article.get('url', '')
            
            # Count tokens
            token_count = count_tokens(text)
            results['all_token_counts'].append(token_count)
            results['processed'] += 1
            
            # Filter by token count
            if min_tokens <= token_count <= max_tokens:
                # Create filename based on revid
                filename = f"{id}_{revid}.txt"
                filepath = os.path.join(output_dir, filename)
                
                # Save text article
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(text)
                
                # Save JSON metadata
                json_filepath = os.path.join(json_dir, f"{id}_{revid}.json")
                
                # Create JSON metadata object
                metadata = {
                    "file_name": filename,
                    "title": title,
                    "url": url,
                    "text": text,
                    "token_count": token_count
                }
                
                # Save JSON metadata
                with open(json_filepath, "w", encoding="utf-8") as f:
                    json.dump(metadata, f, ensure_ascii=False, indent=2)
                
                # Track token count for filtered articles
                results['filtered_token_counts'].append(token_count)
                results['saved'] += 1
            else:
                results['filtered'] += 1
                
        except Exception as e:
            logger.error(f"Error processing article with id {article.get('id', 'unknown')}: {str(e)}")
    
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process Vietnamese Wikipedia articles by token count in parallel")
    parser.add_argument("--output", type=str, default="viwiki_processed", 
                        help="Output directory for processed articles (default: viwiki_processed)")
    parser.add_argument("--json", type=str, default="viwiki_json", 
                        help="Output directory for JSON metadata (default: viwiki_json)")
    parser.add_argument("--min-tokens", type=int, default=600, 
                        help="Minimum number of tokens for inclusion (default: 600)")
    parser.add_argument("--max-tokens", type=int, default=3000, 
                        help="Maximum number of tokens for inclusion (default: 3000)")
    parser.add_argument("--max-articles", type=int, default=None, 
                        help="Maximum number of articles to process (default: all)")
    parser.add_argument("--workers", type=int, default=None,
                        help="Number of worker processes (default: auto-detect)")
    parser.add_argument("--batch-size", type=int, default=100,
                        help="Number of articles to process in each batch (default: 100)")
    
    args = parser.parse_args()
    
    process_wiki_articles(
        output_dir=args.output,
        json_dir=args.json,
        min_tokens=args.min_tokens,
        max_tokens=args.max_tokens,
        max_articles=args.max_articles,
        workers=args.workers,
        batch_size=args.batch_size
    )