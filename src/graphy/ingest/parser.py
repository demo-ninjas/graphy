import os
from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient, AnalysisFeature, AnalyzeResult, Point

class DocumentParserConfig: 
    endpoint:str = None
    key:str = None
    title_height:float = None
    subtitle_height:float = None
    paragraph_height:float = None
    min_chunk_chars:int = None

    def __init__(self) -> None:
        self.endpoint = os.environ.get("AZURE_FORM_RECOGNIZER_ENDPOINT", None)
        if self.endpoint is None: raise Exception("AZURE_FORM_RECOGNIZER_ENDPOINT is not set")
        self.key = os.environ.get("AZURE_FORM_RECOGNIZER_KEY", "")
        self.title_height = 0.27
        self.subtitle_height = 0.22
        self.paragraph_height = 0.08
        self.min_chunk_chars = 1200



class ParsedDocumentChunkRect: 
    top_left:Point = None
    top_right:Point = None
    bottom_left:Point = None
    bottom_right:Point = None

    def __init__(self, top_left:Point, top_right:Point, bottom_left:Point, bottom_right:Point) -> None:
        self.top_left = top_left
        self.top_right = top_right
        self.bottom_left = bottom_left
        self.bottom_right = bottom_right

    @property
    def width(self):
        return self.top_right.x - self.top_left.x
    @property
    def height(self):
        return self.bottom_right.y - self.top_right.y
    @property
    def area(self):
        return self.width * self.height
    
    def merge(self, other:'ParsedDocumentChunkRect'):
        top_left = Point(min(self.top_left.x, other.top_left.x), min(self.top_left.y, other.top_left.y))
        top_right = Point(max(self.top_right.x, other.top_right.x), min(self.top_right.y, other.top_right.y))
        bottom_left = Point(min(self.bottom_left.x, other.bottom_left.x), max(self.bottom_left.y, other.bottom_left.y))
        bottom_right = Point(max(self.bottom_right.x, other.bottom_right.x), max(self.bottom_right.y, other.bottom_right.y))
        return ParsedDocumentChunkRect(top_left, top_right, bottom_left, bottom_right)

    
class ParsedDocumentChunk: 
    type:str = None
    page:int = None
    line:int = None
    content:str = None
    style:str = None
    rect:ParsedDocumentChunkRect = None

    table_number:int = None
    table_column_count:int = None
    table_row_count:int = None
    table_headings:list[str] = None
    table_data:list[list[str]] = None

    def __init__(self, type:str = None, page:int = None, line:int = None, content:str = None, style:str = None, rect:ParsedDocumentChunkRect = None) -> None:
        self.type = type
        self.page = page
        self.line = line
        self.content = content
        self.style = style
        self.rect = rect
    
    def get_table_content_as_md(self) -> str:
        # print the table out as markdown
        sub_header_row = ""
        content = ""
        for col in range(self.table_column_count):
            content += " | " + self.table_headings[col] if col < len(self.table_headings) else " | " 
            sub_header_row += "| --- "
        content += "| \n" + sub_header_row + " |\n"
        for row in self.table_data:
            for col in range(self.table_column_count):
                content += " | " + row[col] if col < len(row) else " | "
            content += "| \n"

        return content
    
    def get_as_markdown(self) -> str:
        if self.type == "table":
            return self.get_table_content_as_md()
        else:
            if self.style == "H1":
                return "# " + self.content
            elif self.style == "H2":
                return "## " + self.content
            else:
                return self.content

    def to_record(self, id:str = None, title:str = None ) -> dict:
        from uuid import uuid4
        if id is None or len(id) == 0: id = str(uuid4())
        if title is None or len(title) == 0: title = "Page " + str(self.page) + ", Line " + str(self.line)
        data = {
            "id": id,
            "title": title,
            "type": self.type,
            "page": self.page,
            "rect": {
                "tl": {"x": self.rect.top_left.x, "y": self.rect.top_left.y},
                "tr": {"x": self.rect.top_right.x, "y": self.rect.top_right.y},
                "bl": {"x": self.rect.bottom_left.x, "y": self.rect.bottom_left.y},
                "br": {"x": self.rect.bottom_right.x, "y": self.rect.bottom_right.y},
                "width": self.rect.width,
                "height": self.rect.height,
                "area": self.rect.area
            }
        }

        if self.type == "table":
            data["table_number"] = self.table_number
            data["table_column_count"] = self.table_column_count
            data["table_row_count"] = self.table_row_count
            # print the table out as markdown
            data["text"] = self.get_table_content_as_md()
        else: 
            data["text"] = self.content
            data["line"] = self.line
            data["style"] = self.style
        
        return data


class ParsedDocument: 
    raw_result:AnalyzeResult = None
    title:str = None
    chunks:list[ParsedDocumentChunk] = None
    
    def __init__(self, raw:AnalyzeResult) -> None:
        self.raw_result = raw
        self.chunks = []

    def _add_all_tables_in_page(self, content:str, page:int):
        for chunk in self.chunks:
            if chunk.page == page and chunk.type == "table":
                content += "\n> Table " + str(chunk.table_number) + "\n\n"
                content += chunk.get_table_content_as_md() + "\n\n"
        return content
    
    def as_text(self, all_tables_at_end:bool = False) -> str:
        content = ""
        content += "# " + self.title + "\n\n"
        curr_page = -1
        for chunk in self.chunks:
            record = chunk.get_as_markdown()
            if chunk.type == "table":
                content += "\n> Table " + str(chunk.table_number) + "\n\n"
                content += record + "\n\n"
            else:
                if chunk.page != curr_page:
                    if not all_tables_at_end:
                        self._add_all_tables_in_page(content, curr_page)

                    content += "\n\n> Page " + str(chunk.page) + "\n\n"
                    curr_page = chunk.page
                if chunk.style == "H1" or chunk.style == "H2":
                    content += record + "\n\n"
                else: 
                    content += record + "\n"
        return content

    def to_dict(self) -> dict:
        return {
            "title": self.title,
            "chunks": [chunk.to_record() for chunk in self.chunks]
        }

class DocumentParser:
    config:DocumentParserConfig = None
    client:DocumentAnalysisClient = None

    def __init__(self, config:DocumentParserConfig = None):
        if config is None: config = DocumentParserConfig()
        self.config = config

        self.client = DocumentAnalysisClient(
            endpoint=config.endpoint, credential=AzureKeyCredential(config.key)
        )

    def parse(self, file_path:str) -> ParsedDocument:
        file_stream = open(file_path, "rb")
        poller = self.client.begin_analyze_document("prebuilt-layout", file_stream, features=[AnalysisFeature.FORMULAS])
        result = poller.result()


        parsed = ParsedDocument(result)
        current_chunk = None
        title_h1 = ""
        title_h2 = ""

        ## Go through each page
        for page_idx, page in enumerate(result.pages):
            ## And each line on the page
            for line_idx, line in enumerate(page.lines):

                rect = ParsedDocumentChunkRect(line.polygon[0], line.polygon[1], line.polygon[3], line.polygon[2])
                height = rect.height
                if height < self.config.paragraph_height: continue  ## ignore any text that is smaller than the paragraph height

                style = "H1" if height >= self.config.title_height else "H2" if height >= self.config.subtitle_height else "P" if height >= self.config.paragraph_height else "X"
                if height > 2.0: style = "X"  ## Likely a page number or other non-text element
                if style == "X": continue

                if current_chunk is not None and current_chunk.style == style:
                    if rect.top_left.y - current_chunk.rect.top_left.y > self.config.subtitle_height:
                        parsed.chunks.append(current_chunk)    ## Likley new paragraph or page, so save the current chunk
                        current_chunk = None
                    else:
                        current_chunk.rect = current_chunk.rect.merge(rect)
                        current_chunk.content += " " + line.content
                        current_chunk.type = "paragraph"
                elif current_chunk is not None and current_chunk.style != style:
                    parsed.chunks.append(current_chunk)
                    current_chunk = None

                if current_chunk is None:
                    current_chunk = ParsedDocumentChunk(type="line", page=page_idx, line=line_idx, content=line.content, style=style, rect=rect)
                    if style == "H1":
                        if len(title_h1) > 0:
                            title_h1 += " "
                        title_h1 += line.content
                    elif style == "H2":
                        if len(title_h2) > 0:
                            title_h2 += " "
                        title_h2 += line.content

        if current_chunk is not None:
            parsed.chunks.append(current_chunk)
        

        ## Now, set the title of the document
        if len(title_h1) > 0:
            if "journal" not in title_h1.lower() and len(title_h1) > 5:
                parsed.title = title_h1
        if len(title_h2) > 0 and parsed.title is None:
            parsed.title = title_h2
        
        if parsed.title is None: 
            from pathlib import Path
            parsed.title = Path(file_path).stem.title()

        ## Now do the tables
        for table_idx, table in enumerate(result.tables):
            pg_num = table.bounding_regions[0].page_number
            rect = ParsedDocumentChunkRect(table.bounding_regions[0].polygon[0], table.bounding_regions[0].polygon[1], table.bounding_regions[0].polygon[3], table.bounding_regions[0].polygon[2])

            current_chunk = ParsedDocumentChunk(type="table", page=pg_num,rect=rect)
            current_chunk.table_number = table_idx
            current_chunk.table_column_count = table.column_count
            current_chunk.table_row_count = table.row_count
            current_chunk.table_headings = []
            current_chunk.table_data = []

            curr_row = 0
            curr_row_data = None
            for cell in table.cells:
                cell_content = cell.content
                if cell.row_index == 0:
                    current_chunk.table_headings.append(cell_content)
                elif curr_row != cell.row_index:
                    if curr_row_data is not None:
                        current_chunk.table_data.append(curr_row_data)
                    curr_row = cell.row_index
                    curr_row_data = [cell_content]
                else:
                    curr_row_data.append(cell_content)
                    
            if curr_row_data is not None:
                current_chunk.table_data.append(curr_row_data)

            parsed.chunks.append(current_chunk)

        return parsed