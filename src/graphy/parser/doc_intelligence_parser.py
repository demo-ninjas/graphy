import os
from pathlib import Path
from typing import Any

from azure.core.credentials import AzureKeyCredential
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeResult, ContentFormat

from .parser import Parser, ParsedDocument, DocumentChunk, DocumentChunkRect

class DocIntelligenceParser(Parser):
    def __init__(self, config:dict[str, any]):
        super().__init__(config)
        self.endpoint = config.get('recognizer-endpoint') or config.get('endpoint') or os.environ.get("AZURE_FORM_RECOGNIZER_ENDPOINT", None)
        if self.endpoint is None: raise Exception("'recognizer-endpoint' or 'AZURE_FORM_RECOGNIZER_ENDPOINT' is not set")

        self.key = config.get('recognizer-key') or config.get('key') or os.environ.get("AZURE_FORM_RECOGNIZER_KEY", "")
        self.title_height = float(config.get('title-height') or 0.27)
        self.subtitle_height = float(config.get('subtitle-height') or 0.22)
        self.paragraph_height = float(config.get('paragraph-height') or 0.08)
        self.min_chunk_chars = int(config.get('min-chunk-chars') or 1200)

        self.title_ignores = config.get('title-ignores') or ["journal", "volume", "issue", "page", "date", "doi", "abstract", "introduction", "conclusion", "acknowledgements", "references", "appendix", "figure", "table", "author", "editor", "reviewer", "keywords", "index", "bibliography", "publication", "submission", "correspondence", "contact", "about", "terms", "privacy", "policy", "license", "copyright"]

        self.client = DocumentIntelligenceClient(
            endpoint=self.endpoint, credential=AzureKeyCredential(self.key), api_version="2024-07-31-preview"
        )

    
    def _parse(self, file:Path) -> ParsedDocument:
        with open(file, "rb") as file_stream:
            poller = self.client.begin_analyze_document("prebuilt-layout", 
                                                        output_content_format=ContentFormat.MARKDOWN,
                                                        analyze_request=file_stream, 
                                                        content_type="application/octet-stream")
        result:AnalyzeResult = poller.result()

        parsed = ParsedDocument()
        
        title_h1 = ""
        title_h2 = ""
        
        current_txt = ""
        curr_rect:DocumentChunkRect = None
        prev_word_rect:DocumentChunkRect = None
        prev_style = None

        ## TODO: Finish implementing this - it's currently still based on the old code
        
        # if result.figures: 
        #     for figure in result.figures:
        #         print(f"Figure: {figure.caption}")
        #         for span in figure.spans:
        #             print(f"  Span: {span.offset} ({span.length})")
        #         print("Bounding Regions:")
        #         for region in figure.bounding_regions:
                    
        #         print("Elements:")
        #         for el in figure.elements:
        #             print(f"  - {el}")
                
        ## Go through each page
        for page_idx, page in enumerate(result.pages):
            # for figure in page.formulas:
            #     rect = DocumentChunkRect(figure.bounding_box[0].x, figure.bounding_box[0].y, figure.bounding_box[2].x, figure.bounding_box[2].y)
            #     parsed.chunks.append(DocumentChunk(type="formula", page=page_idx, page_chunk_idx=len(parsed.chunks), rect=rect, content=figure.content))
            for word in page.words:
                rect = DocumentChunkRect(word.polygon[0].x, word.polygon[0].y, word.polygon[2].x, word.polygon[2].y)
                height = rect.height
                if height < self.paragraph_height: continue

                dist = rect.distance_from(prev_word_rect)
                likely_new_line = dist > rect.width * 3.0 # More than 3x the width of the word, then it's likely a new line
                likely_new_paragraph = rect.y0 - prev_word_rect.y1 > self.paragraph_height  # More than a paragraph word height away, then it's likely a new paragraph
                
                style = "H1" if height >= self.title_height else "H2" if height >= self.subtitle_height else "P" if height >= self.paragraph_height else "X"
                if style == "X": continue   ## Likely a page number or other non-text element

                if curr_rect is None: 
                    curr_rect = rect
                    current_txt = word.content
                elif dist <= rect.width or (likely_new_line and not likely_new_paragraph):
                    if len(current_txt) == 0: 
                        current_txt = word.content
                        curr_rect = rect
                    elif len(word.content) == 1 and word.content in [".", ",", ":", ";", "!", "?"]:
                        current_txt += word.content
                        curr_rect = curr_rect.merge(rect)
                    else: 
                        current_txt += (" " + word.content) if len(current_txt) > 0 else word.content
                        curr_rect = curr_rect.merge(rect)

                    if len(current_txt) >= self.min_chunk_chars and (current_txt[-1] in [".", "!", "?"] or likely_new_paragraph):
                        parsed.chunks.append(DocumentChunk(type="text", page=page_idx, page_chunk_idx=len(parsed.chunks), rect=rect, content=current_txt, style=style))
                        if current_txt.lower() not in self.title_ignores:
                            if style == "H1" and len(title_h1) == 0:
                                title_h1 = current_txt
                            elif style == "H2" and len(title_h2) == 0:
                                title_h2 = current_txt
                            
                        current_txt = ""
                        curr_rect = None
                else:
                    ## Too far away, so save the current chunk + start a new one
                    parsed.chunks.append(DocumentChunk(type="text", page=page_idx, page_chunk_idx=len(parsed.chunks), rect=rect, content=current_txt, style=prev_style))
                    if prev_style is not None and current_txt.lower() not in self.title_ignores: 
                        if prev_style == "H1" and len(title_h1) == 0:
                            title_h1 = current_txt
                        elif prev_style == "H2" and len(title_h2) == 0:
                            title_h2 = current_txt

                    current_txt = word.content
                    curr_rect = rect

                prev_word_rect = rect
                prev_style = style

        if len(title_h1) > 0:
            parsed.title = title_h1
        
        if len(title_h2) > 0: 
            if parsed.title is None:
                parsed.title = title_h2
            else:
                parsed.subtitle = title_h2


        if parsed.title is None: 
            from pathlib import Path
            parsed.title = file.stem.title()


        ## Now do the tables
        for table_idx, table in enumerate(result.tables):
            pg_num = table.bounding_regions[0].page_number
            rect = DocumentChunkRect(word.polygon[0].x, word.polygon[0].y, word.polygon[2].x, word.polygon[2].y)

            current_chunk = DocumentChunk(type="table", page=pg_num,rect=rect)
            current_chunk.metadata = {
                "table_number": table_idx,
                "column_count": table.column_count,
                "row_count": table.row_count,
                "headings": None,
                "cells": None
            }

            table_headings = []
            table_data = []

            curr_row = 0
            curr_row_data = None
            for cell in table.cells:
                cell_content = cell.content
                if cell.row_index == 0:
                    table_headings.append(cell_content)
                elif curr_row != cell.row_index:
                    if curr_row_data is not None:
                        table_data.append(curr_row_data)
                    curr_row = cell.row_index
                    curr_row_data = [cell_content]
                else:
                    curr_row_data.append(cell_content)
                    
            if curr_row_data is not None:
                table_data.append(curr_row_data)

            current_chunk.metadata["headings"] = table_headings
            current_chunk.metadata["cells"] = table_data
            parsed.chunks.append(current_chunk)

