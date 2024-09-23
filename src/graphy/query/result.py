
from graphrag.query.structured_search.base import SearchResult
from azure.cosmos import DatabaseProxy

from graphy.data import SourceReference

class GraphySearchResult:
    result: SearchResult
    query: str
    community_level: int
    response_type: str
    query_type: str

    response:str = None
    sources: list[SourceReference] = None

    def __init__(self, result:SearchResult, 
                query:str, 
                community_level: int,
                response_type: str,
                query_type: str):
        
        self.result = result
        self.query = query
        self.community_level = community_level
        self.response_type = response_type
        self.query_type = query_type
        
        if type(result.response) == str:
            self.sources, self.response = SourceReference.parse_sources(result.response, update_txt_refs=True)
        else: 
            self.sources, self.response = [], str(result.response)
    
    def load_sources(self, db:DatabaseProxy, gather_documents:bool = False):
        if self.sources and len(self.sources) > 0:
            for source in self.sources:
                source.load(db=db, gather_documents=gather_documents)

    def __str__(self):
        new_line = "\n"
        return f"Response:\n{self.response}\n\nSources:\n{new_line.join([str(source) for source in self.sources])}\n\n - LLM Queries: {self.result.llm_calls}\n -      Tokens: {self.result.prompt_tokens}\n - Search Time: {int(self.result.completion_time)} seconds"