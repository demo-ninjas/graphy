import os
from pathlib import Path
from typing import Any
from concurrent.futures import ThreadPoolExecutor

from azure.core.credentials import AzureKeyCredential
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeResult, ContentFormat, DocumentAnalysisFeature

from graphrag.query.llm.oai.chat_openai import ChatOpenAI

from fitz import Document as PyMuPDFDocument
from fitz import Pixmap, Matrix
from fitz import open as FitzOpen

from .parser import Parser, ParsedDocument, DocumentChunkRect
from .img_analyser import analyse_image_data, analyse_image_data_iteratively

class PdfDocIntelligenceParser(Parser):
    def __init__(self, config:dict[str, any], llm:ChatOpenAI):
        super().__init__(config)
        self.endpoint = config.get('recognizer-endpoint') or config.get('endpoint') or os.environ.get("AZURE_FORM_RECOGNIZER_ENDPOINT", None)
        if self.endpoint is None: raise Exception("'recognizer-endpoint' or 'AZURE_FORM_RECOGNIZER_ENDPOINT' is not set")

        self.key = config.get('recognizer-key') or config.get('key') or os.environ.get("AZURE_FORM_RECOGNIZER_KEY", "")
        self.title_height = float(config.get('title-height') or 0.27)
        self.subtitle_height = float(config.get('subtitle-height') or 0.22)
        self.paragraph_height = float(config.get('paragraph-height') or 0.08)
        self.min_chunk_chars = int(config.get('min-chunk-chars') or 1200)

        self.title_ignores = config.get('title-ignores') or ["journal", "volume", "issue", "page", "date", "doi", "abstract", "introduction", "conclusion", "acknowledgements", "references", "appendix", "figure", "table", "author", "editor", "reviewer", "keywords", "index", "bibliography", "publication", "submission", "correspondence", "contact", "about", "terms", "privacy", "policy", "license", "copyright"]
        self.use_iterative_image_analyser = config.get('use-iterative-image-analyser', True)
        self.client = DocumentIntelligenceClient(
            endpoint=self.endpoint, credential=AzureKeyCredential(self.key), api_version="2024-07-31-preview"
        )

        self.llm = llm
        self.llm_workers = int(config.get('llm-workers') or 8)

    
    def _parse(self, file:Path) -> ParsedDocument:
        
        ## Open the PDF file with PyMuPDF
        pdf_document = FitzOpen(file)
        image_file_prefix = file.stem.replace(' ', '_')

        with open(file, "rb") as file_stream:
            poller = self.client.begin_analyze_document("prebuilt-layout", 
                                                        output_content_format=ContentFormat.MARKDOWN,
                                                        analyze_request=file_stream, 
                                                        features=[ DocumentAnalysisFeature.FORMULAS, DocumentAnalysisFeature.STYLE_FONT, DocumentAnalysisFeature.OCR_HIGH_RESOLUTION ],
                                                        content_type="application/octet-stream")
        result:AnalyzeResult = poller.result()    
        markdown = result.content
    
        pages_map = []
        for page in result.pages:
            pdf_page = pdf_document.load_page(page.page_number-1)
            xRatio = (pdf_page.rect.x1 - pdf_page.rect.x0) / page.width
            yRatio = (pdf_page.rect.y1 - pdf_page.rect.y0) / page.height
            pages_map.append({
                "num": page.page_number,
                "pdf_page": pdf_page,
                "doc_page": page,
                "xRatio": xRatio,
                "yRatio": yRatio
            })

        if result.figures: 
            replacements = []
            description_futures = []
            # Create executor
            with ThreadPoolExecutor(max_workers=self.llm_workers) as executor:
                for idx, figure in enumerate(result.figures):
                    print(f"  - Figure {idx}: {figure.caption.content[0:min(100, len(figure.caption.content))] if figure.caption is not None and figure.caption.content is not None else '<No Caption>'}...")
                    for span in figure.spans:
                        # print(f"  Span: {span.offset} ({span.length}) [In MD: {markdown[span.offset:span.offset+span.length]}]")
                        figure_content = markdown[span.offset:span.offset+span.length]
                        capttion_start = figure_content.find('<figcaption>') + len('<figcaption>')
                        caption_end = figure_content.find('</figcaption>')
                        caption = figure_content[capttion_start:caption_end]
                        replacements.append({
                            "content": caption,
                            "start": span.offset,
                            "end": span.offset+span.length,
                            "figure_id": idx,
                            "description": "<!-- No description available -->",
                            "image_name": ""
                        })

                    # print("Bounding Regions:")
                    for region_idx,region in enumerate(figure.bounding_regions):
                        try:
                            page_info = pages_map[region.page_number-1]
                            xRatio = page_info["xRatio"]
                            yRatio = page_info["yRatio"]
                            pdf_page = page_info["pdf_page"]
                            rect = DocumentChunkRect(region.polygon[0]*xRatio, region.polygon[1]*yRatio, region.polygon[4]*xRatio, region.polygon[5]*yRatio)
                            pix = pdf_page.get_pixmap(clip=[rect.x0, rect.y0, rect.x1, rect.y1], matrix=Matrix(2, 2))
                            image_name = f"{image_file_prefix}_{region.page_number}_{idx}_{region_idx}.png"
                            pix.save(os.path.join("images", image_name))
                            image_bytes = pix.tobytes("png")

                            if self.llm is not None:
                                section_name = self.determine_section_name_at_offset(markdown, span.offset)
                                prior_context = self.find_prior_context(markdown, span.offset)
                                post_context = self.find_post_context(markdown, span.offset+span.length)
                                def describe_image(image_bytes, figure_id, llm, section_name, prior_context, post_context, image_name):
                                    try:
                                        if self.use_iterative_image_analyser:
                                            result = analyse_image_data_iteratively(image_bytes, "png", llm, section_name=section_name, prior_context=prior_context, post_context=post_context)
                                        else: 
                                            result = analyse_image_data(image_bytes, "png", llm, section_name=section_name, prior_context=prior_context, post_context=post_context)
                                    except Exception as e:
                                        result = "<!-- There was an error analysing the image -->"
                                    return (figure_id, result, image_name)
                                
                                description_futures.append(executor.submit(describe_image, image_bytes, figure_id=idx, llm=self.llm, section_name=section_name, prior_context=prior_context, post_context=post_context, image_name=image_name))
                        except Exception as e:
                            print(f"Error processing region {region_idx} of figure {idx} in pdf {file.stem}: {e}")
                            continue
                            

                for future in description_futures:
                    figure_id, result, image_name = future.result()
                    for rep in replacements:
                        if rep["figure_id"] == figure_id:
                            rep["description"] = result
                            rep["image_name"] = image_name
                            break

            # Sort the replacements in reverse order of start index (to avoid messing up the indexes as we apply the replacements)
            replacements.sort(key=lambda x: x["start"], reverse=True)

            # Replace the content in the markdown
            for rep in replacements:
                markdown = markdown[:rep["start"]] + "<!-- Start of description of image at this position in the source document -->\n\n<!-- Image Path: " + rep["image_name"] + " -->\n\n**Caption:** " + rep["content"] + "\n\n**Description:** " + rep["description"] + "\n<!-- End of Image Description -->" + markdown[rep["end"]:]

        parsed = ParsedDocument()
        parsed.title = file.stem.title()
        # Find the first heading in the markdown
        if markdown.startswith("# "):
            h1_end = markdown.find("\n", 0)
            parsed.title = markdown[2:h1_end].strip()
        else: 
            h1_start = markdown.find("\n# ")
            if h1_start != -1:
                h1_end = markdown.find("\n", h1_start)
                parsed.title = markdown[h1_start+2:h1_end].strip()

        parsed.pre_parsed_md = markdown
        return parsed

    def determine_section_name_at_offset(self, markdown:str, offset:int) -> str:
        ## Find the nearest heading before the offset
        ## Find the nearest heading after the offset
        ## Return the heading with the smallest distance
        h1_name = None
        h2_name = None
        h3_name = None
        h4_name = None
        arr = markdown.split("\n")
        counter = 0
        for idx, line in enumerate(arr):
            if line.startswith("# "):
                h1_name = line[2:].strip()
            elif line.startswith("## "):
                h2_name = line[3:].strip()
            elif line.startswith("### "):
                h3_name = line[4:].strip()
            elif line.startswith("#### "):
                h4_name = line[5:].strip()
            counter += len(line)
            if counter >= offset: break

        section_name = ""
        if h1_name is not None: section_name = h1_name
        if h2_name is not None:
            if len(section_name) == 0: section_name = h2_name
            else: section_name = f"{section_name} / {h2_name}"
        if h3_name is not None:
            if len(section_name) == 0: section_name = h3_name
            else: section_name = f"{section_name} / {h3_name}"
        if h4_name is not None:
            if len(section_name) == 0: section_name = h4_name
            else: section_name = f"{section_name} / {h4_name}"
        return section_name

    def find_prior_context(self, markdown:str, offset:int) -> str:
        # Look back from the offset to find the last 2 paragraphs of text
        
        ## Start by looking for the previous word
        prev_space = markdown.rfind(" ", 0, offset)
        if prev_space == -1: return ""

        ## Then find the previous newline
        prev_newline = markdown.rfind("\n", 0, prev_space)
        if prev_newline == -1: return ""

        ## Then find the previous newline before that
        prev_newline2 = markdown.rfind("\n", 0, prev_newline)
        if prev_newline2 == -1: 
            return markdown[prev_newline:offset].strip()
        else:
            return markdown[prev_newline2:offset].strip()
    

    def find_post_context(self, markdown:str, offset:int) -> str:
        # Look forward from the offset to find the next 2 paragraphs of text
        
        # Start by looking for the next word
        next_space = markdown.find(" ", offset)

        ## Start by looking for the next newline
        next_newline = markdown.find("\n", next_space)
        if next_newline == -1: return ""

        ## Then find the next newline after that
        next_newline2 = markdown.find("\n", next_newline+1)
        if next_newline2 == -1: 
            return markdown[offset:next_newline].strip()
        else:
            return markdown[offset:next_newline2].strip()

        
