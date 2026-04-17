# VSMRC

**Vietnamese Text Segmentation and Multiple-Choice Reading Comprehension Dataset Creation Pipeline**

[![arXiv](https://img.shields.io/badge/arXiv-2506.15978-b31b1b.svg)](https://arxiv.org/pdf/2506.15978)  
[![Hugging Face](https://img.shields.io/badge/%F0%9F%A4%97%20Hugging%20Face-VSMRC-blue)](https://huggingface.co/VSMRC)

## Overview

This repository contains the **official codebase** used to build the **VSMRC** dataset, introduced in our paper:

> **A Vietnamese Dataset for Text Segmentation and Multiple Choices Reading Comprehension**  
> Toan Nguyen Hai, Ha Nguyen Viet, Truong Quan Xuan, Duc Do Minh  
> arXiv: 2506.15978 (2025)

**VSMRC** is the first large-scale Vietnamese dataset that jointly supports two critical NLP tasks:
- **Text Segmentation** (15,942 fully segmented Wikipedia documents)
- **Multiple-Choice Machine Reading Comprehension (MRC)** (16,347 high-quality QA pairs)

The dataset was constructed from Vietnamese Wikipedia (2025-03-01 dump) using a fully automated two-phase pipeline with LLM assistance (Gemini 2.0 Flash Lite, GPT-4o-mini, DeepSeek-V3) and rigorous human validation.

**Key highlights from the paper:**
- First open-domain text segmentation benchmark for Vietnamese.
- Synthetic MCQs generated with four balanced question types (fact-check, fill-in-the-blank, reasoning, list).
- Human quality assurance on 37% of the QA data (error rate < 10%).
- Multilingual models (especially **mBERT**) significantly outperform monolingual Vietnamese models on both tasks.

The final dataset is publicly available on [Hugging Face → VSMRC](https://huggingface.co/VSMRC).

## Features

- **Segmentation pipeline**: Header-aware splitting, length filtering, quality validation, and optimized processing of Vietnamese Wikipedia articles.
- **QA pipeline**: LLM-based segment validation, question generation, distractor creation, multi-LLM verification, and JSON/XML structured I/O.
- Modular, reusable scripts with clear separation between segmentation and QA phases.
- Ready-to-run main entry points (`wiki_headers_main.py` and `question_generator_main.py`).
- Human-in-the-loop validation support.

## Directory Structure
