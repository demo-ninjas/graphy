
from abc import ABC, abstractmethod
from typing import Callable
from pathlib import Path

from graphrag.query.llm.oai.chat_openai import ChatOpenAI


class DocumentChunkRect:
    x0: float
    y0: float
    x1: float
    y1: float

    def __init__(self, x0:float, y0:float, x1:float, y1:float):
        self.x0 = x0
        self.y0 = y0
        self.x1 = x1
        self.y1 = y1
    
    @property
    def height(self) -> float:
        return self.y1 - self.y0
    
    @property
    def width(self) -> float:
        return self.x1 - self.x0
    
    @property
    def area(self) -> float:
        return self.height() * self.width()

    def merge(self, other:'DocumentChunkRect') -> 'DocumentChunkRect':
        return DocumentChunkRect(min(self.x0, other.x0), min(self.y0, other.y0), max(self.x1, other.x1), max(self.y1, other.y1))
    
    def contains(self, other:'DocumentChunkRect') -> bool:
        return self.x0 <= other.x0 and self.y0 <= other.y0 and self.x1 >= other.x1 and self.y1 >= other.y1

    def distance_from(self, other:'DocumentChunkRect') -> float:
        if other is None: return 0
        ## Test each corner of each rect against the other rect to find the closest distance between them
        d1 = abs(((self.x0 - other.x0)**2 + (self.y0 - other.y0)**2)**0.5)
        d2 = abs(((self.x1 - other.x1)**2 + (self.y1 - other.y1)**2)**0.5)
        d3 = abs(((self.x1 - other.x0)**2 + (self.y1 - other.y0)**2)**0.5)
        d4 = abs(((self.x0 - other.x1)**2 + (self.y0 - other.y1)**2)**0.5)
        return min(d1, d2, d3, d4)
        
    
    def to_json(self) -> dict[str, float]:
        return {
            "x0": self.x0,
            "y0": self.y0,
            "x1": self.x1,
            "y1": self.y1
        }

class DocumentChunk:
    type: str   # text, image, table, etc
    page: int
    page_chunk_idx: int
    rect: DocumentChunkRect
    content: str | bytes
    link: str
    metadata: dict[str, any]

    def is_image(self) -> bool:
        return self.type == "image"
    
    def is_text(self) -> bool:
        return self.type == "text"
    
    def is_table(self) -> bool:
        return self.type == "table"
    
    def get_as_markdown(self) -> str:
        if self.is_text():
            return self.content
        elif self.is_image():
            if type(self.content) == bytes:
                name = self.metadata.get("name", f"Image {self.page_chunk_idx}") if self.metadata is not None else f"Image {self.page_chunk_idx}"
                return f"![{name}]({self.link})"
            else:
                return self.content
        elif self.is_table():
            return self.content
        else:
            return ""
    
    def to_json(self) -> dict[str, any]:
        str_meta = None
        if self.metadata is not None: 
            import base64
            str_meta = {}
            for k,v in self.metadata.items():
                if type(v) is bytes: 
                    str_meta[k] = base64.b64encode(v).decode('utf-8')
                else: 
                    str_meta[k] = v
        

        str_content = base64.b64encode(self.content).decode('utf-8') if type(self.content) is bytes else self.content
        return {
            "type": self.type,
            "page": self.page,
            "page_chunk_idx": self.page_chunk_idx,
            "rect": self.rect.to_json(),
            "content": str_content,
            "link": self.link,
            "metadata": str_meta
        }
    


class ParsedDocument:
    title: str = None
    subtitle: str = None
    chunks: list[DocumentChunk] = []
    pre_parsed_md:str = None

    def analyse_images(self, llm:ChatOpenAI, custom_analysis_msg:str = None, progress_notifier:Callable = None):
        if self.pre_parsed_md is not None:
            return
        
        from .img_analyser import analyse_chunk_image
        total = float(len(self.chunks))
        for idx, chunk in enumerate(self.chunks):
            if chunk.is_image():
                if chunk.metadata is not None and chunk.metadata.get('image-analysed', False) == True: 
                    continue
                
                if progress_notifier is not None: 
                    progress_notifier(chunk, 'started', float(idx) / total)
                chunk.content = analyse_chunk_image(chunk, llm, analysis_msg=custom_analysis_msg)
                

                if progress_notifier is not None: 
                    progress_notifier(chunk, 'finished', float(idx) / total)

                if not chunk.metadata: 
                    chunk.metadata = {}
                chunk.metadata['image-analysed'] = True


    def to_markdown(self) -> str:
        if self.pre_parsed_md is not None:
            return self.pre_parsed_md

        content = ""
        content += "# " + self.title + "\n\n"
        curr_page = -1
        prev_chunk = None
        prev_chunk_style = None
        prev_chunk_text = None
        for chunk in self.chunks:
            record = chunk.get_as_markdown()

            if prev_chunk is not None and prev_chunk.is_text() and chunk.is_text() and prev_chunk.page == chunk.page:
                ## Check the locations of the two chunks and decide if they are on the same line or if a newline is needed between them
                chunk_distance = chunk.rect.distance_from(prev_chunk.rect)
                if chunk_distance < 8: ## TODO: Make this a config value and determine what the best default value would be
                    if prev_chunk_text[-1] in [".", ",", ":", ";", "!", "?"]:
                        record = " " + record
                    # else: No Change
                elif len(prev_chunk_text) == 1 and prev_chunk_text not in [".", ",", ":", ";", "!", "?"]:
                    record = record  # No change
                else: 
                    if prev_chunk_style in ("H1", "H2"):
                        record = "\n\n" + record
                    else: 
                        record = "\n" + record

            if chunk.type == "table":
                content += "\n> Table " + str(chunk.page_chunk_idx) + "\n\n"
                content += record + "\n\n"
            else:
                if chunk.page != curr_page:
                    content += "\n\n---\n> Page " + str(chunk.page) + "\n\n"
                    curr_page = chunk.page

                style = chunk.metadata.get("style", None) if chunk.metadata is not None else None
                if style is not None and style in ("H1", "H2"):
                    content += "## " if style == "H1" else "### "
                    content += record
                else: 
                    content += record
                prev_chunk = chunk
                prev_chunk_style = style
                prev_chunk_text = record
        return content

    def to_json(self) -> dict[str, any]:
        return {
            "title": self.title,
            "subtitle": self.subtitle,
            "chunks": [chunk.to_json() for chunk in self.chunks]
        }


class Parser(ABC):
    def __init__(self, config:dict[str, any]):
        self.config = config

    def parse(self, file_path:str) -> ParsedDocument:
        if not Path(file_path).is_file():
            raise FileNotFoundError(f"File {file_path} not found")

        return self._parse(Path(file_path))
    
    @abstractmethod
    def _parse(self, file:Path) -> ParsedDocument:
        pass