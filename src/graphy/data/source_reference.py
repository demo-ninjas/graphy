import re
import random
from concurrent.futures import ThreadPoolExecutor

from graphrag.query.structured_search.global_search.search import GlobalSearchResult
from azure.cosmos import ContainerProxy, DatabaseProxy

from .community import Community
from .entity import Entity
from .relationship import Relationship
from .text_unit import TextUnit
from .document import Document

EXTRACTOR = re.compile(r"\[Data\:\s+(Reports\s?\((?P<reports>[\d,\s(\+more)]+)\))?\;?\s?(Entities\s?\((?P<entities>[\d,\s(\+more)]+)\))?\;?\s?(Relationships\s?\((?P<relationsips>[\d\s,(\+more)]+)\))?\s?\]")

class SourceReference:
    id:int
    start:int
    end:int

    communities: list[int]
    entities: list[int]
    relationships: list[int]

    ref_prefix:str = "Ref:"

    _communities: list[Community] = None
    _entities: list[Entity] = None
    _relationships: list[Relationship] = None

    _texts: list[TextUnit] = None
    _documents: list[Document] = None
    _texts_truncated: bool = False
    _documents_truncated: bool = False

    def __init__(self, id:int = -1, communities: list[int] = None, entities: list[int] = None, relationships: list[int] = None, start: int = None, end: int = None):
        self.id = id
        self.communities = communities
        self.entities = entities
        self.relationships = relationships
        self.start = start
        self.end = end

    def __str__(self):
        detail_prefix = "\n    - "
        doc_detail_prefix = "\n      - "
        
        community_count = len(self.communities) if self.communities else 0
        entity_count = len(self.entities) if self.entities else 0
        relationship_count = len(self.relationships) if self.relationships else 0
        document_count = len(self._documents) if self._documents else 0
        
        doc_str = ""
        if document_count > 0:
            doc_str = f"\n    - Source Documents:{doc_detail_prefix}{doc_detail_prefix.join([str(x) for x in self._documents])}"
            if self._documents_truncated:
                doc_str += f"{doc_detail_prefix}(truncated)"

        ## Shortcuts for a single reference source
        if community_count == 1 and entity_count == 0 and relationship_count == 0:
            if self._communities and len(self._communities) == 1:
                return f"[{self.ref_prefix}{self.id}] Community: {self._communities[0]}{doc_str}"
            else: 
                return f"[{self.ref_prefix}{self.id}] Community: {self.communities[0]}{doc_str}"
        elif community_count == 0 and entity_count == 1 and relationship_count == 0:
            if self._entities and len(self._entities) == 1:
                return f"[{self.ref_prefix}{self.id}] Entity: {self._entities[0]}{doc_str}"
            else:
                return f"[{self.ref_prefix}{self.id}] Entity: {self.entities[0]}{doc_str}"
        elif community_count == 0 and entity_count == 0 and relationship_count == 1:
            if self._relationships and len(self._relationships) == 1:
                return f"[{self.ref_prefix}{self.id}] Relationship: {self._relationships[0]}{doc_str}"
            else:     
                return f"[{self.ref_prefix}{self.id}] Relationship: {self.relationships[0]}{doc_str}"

        
        ## Gather the details of the source reference
        
        community_str = ""
        if self._communities: 
            community_str = f"Communities:{detail_prefix}{detail_prefix.join([str(x) for x in self._communities])}"
        elif self.communities:
            community_str = f"Communities: {','.join([str(x) for x in self.communities])}" if self.communities else ""
        
        entity_str = ""
        if self._entities:
            entity_str = f"Entities:{detail_prefix}{detail_prefix.join([str(x) for x in self._entities])}"
        elif self.entities:
            entity_str = f"Entities: {','.join([str(x) for x in self.entities])}" if self.entities else ""
        
        relationship_str = ""
        if self._relationships:
            relationship_str = f"Relationships:{detail_prefix}{detail_prefix.join([str(x) for x in self._relationships])}"
        elif self.relationships:
            relationship_str = f"Relationships: {','.join([str(x) for x in self.relationships])}" if self.relationships else ""

        out_str = f"[{self.ref_prefix}{self.id}] "
        first_detail = True
        if community_str:
            out_str += f"{community_str}"
            first_detail = False
        
        if doc_str:
            if first_detail:
                out_str += doc_str
                first_detail = False
            else:
                out_str += doc_str

        if entity_str:
            if first_detail:
                out_str += entity_str
                first_detail = False
            else: 
                out_str += f"\n  - {entity_str}"

        if relationship_str:
            if first_detail:
                out_str += relationship_str
            else:
                out_str += f"\n  - {relationship_str}"

        return out_str

    def load(self, db: DatabaseProxy, gather_documents: bool = True, gather_entities: bool = False, gather_relationships: bool = False):
        """Load the source reference data from the database."""
        with ThreadPoolExecutor(20) as executor:
            community_futures = []
            entity_futures = []
            relationship_futures = []
            if self.communities:
                batch = []
                for community in self.communities:
                    batch.append(community)
                    if len(batch) == 20:
                        community_futures.append(executor.submit(self._load_community_batch, db, batch))
                        batch = []
                if len(batch) > 0:
                    community_futures.append(executor.submit(self._load_community_batch, db, batch))

            if self.entities:
                batch = []
                for entity in self.entities:
                    batch.append(entity)
                    if len(batch) == 20:
                        entity_futures.append(executor.submit(self._load_entity_batch, db, batch))
                        batch = []
                if len(batch) > 0:
                    entity_futures.append(executor.submit(self._load_entity_batch, db, batch))
                    
            if self.relationships:
                batch = []
                for relationship in self.relationships:
                    batch.append(relationship)
                    if len(batch) == 20:
                        relationship_futures.append(executor.submit(self._load_relationship_batch, db, batch))
                        batch = []
                if len(batch) > 0:
                    relationship_futures.append(executor.submit(self._load_relationship_batch, db, batch))
            
            executor.shutdown(wait=True)

            if len(community_futures) > 0:
                self._communities = []
                for future in community_futures:
                    self._communities.extend(future.result())
                community_futures = []

                if gather_documents:
                    for community in self._communities:
                        community.load_metadata(db)
                    
                    if gather_documents: 
                        self._texts = []
                        for community in self._communities:
                            if community.texts and len(community.texts) > 0:
                                text_ids = community.texts
                                if len(text_ids) > 20:
                                    self._texts_truncated = True
                                    text_ids = random.sample(text_ids, 20)
                                self._texts.extend(TextUnit.load_all(text_ids, db))
                        
                        doc_ids = set()
                        for text in self._texts:
                            for doc_id in text.document_ids:
                                doc_ids.add(doc_id)
                        doc_ids = list(doc_ids)
                        if len(doc_ids) > 20:
                            doc_ids = random.sample(doc_ids, 20)
                            self._documents_truncated = True

                        self._documents = Document.load_all(doc_ids, db)
                
            
            ## TODO: Implement the loading of entities and relationships + their documents 
            if len(community_futures) > 0:
                self._communities = []
                for future in community_futures:
                    self._communities.extend(future.result())
            
            if len(entity_futures) > 0:
                self._entities = []
                for future in entity_futures:
                    self._entities.extend(future.result())
            
            if len(relationship_futures) > 0:
                self._relationships = []
                for future in relationship_futures:
                    self._relationships.extend(future.result())

    def _load_community_batch(self, db: DatabaseProxy, batch: list[int]) -> list[Community]:
        """Load a batch of communities."""
        return Community.load_all(batch, db)

    def _load_entity_batch(self, db: DatabaseProxy, batch: list[int]) -> list[Entity]:
        """Load a batch of entities."""
        return Entity.load_all(batch, db)
    
    def _load_relationship_batch(self, db: DatabaseProxy, batch: list[int]) -> list[Relationship]:
        """Load a batch of relationships."""
        return Relationship.load_all(batch, db)
            
    def parse_sources(txt: str, update_txt_refs:bool = True) -> tuple[list['SourceReference'], str]:
        """Parse source references from a string."""
        sources = []
        counter = 0
        for match in re.finditer(EXTRACTOR, txt):
            counter+=1
            span = match.span()
            groups = match.groupdict()
            reports = [int(str(x).strip()) for x in groups["reports"].split(",") if x is not None and '+more' not in x] if groups.get("reports") else None
            enties = [int(str(x).strip()) for x in groups["entities"].split(",") if x is not None and '+more' not in x] if groups.get("entities") else None
            relationships = [int(str(x).strip()) for x in groups["relationsips"].split(",") if x is not None and '+more' not in x] if groups.get("relationsips") else None
            sources.append(SourceReference(id=counter, communities=reports, entities=enties, relationships=relationships, start=span[0], end=span[1]))

        if update_txt_refs:
            offset = 0
            for source in sources:
                txt, adjustment = source._update_txt_ref(txt, offset)
                offset += adjustment
        
        return sources, txt
    

    def _update_txt_ref(self, txt:str, offset:int = 0) -> tuple[str, int]:
        """Replace a source reference in the txt with the source id."""
        new_ref_str = '[' + self.ref_prefix + str(self.id) + ']'
        original_length = self.end - self.start
        new_length = len(new_ref_str)
        start_pos = self.start + offset
        end_pos = self.end + offset
        updated = txt[:start_pos] + new_ref_str + txt[end_pos:]
        return updated, new_length - original_length
        
if __name__ == '__main__':
    txt = "This is a test text with a reference to [Data: Reports(1); Entities(2, 3); Relationships(4, 5, 6)] and another reference to Reports(7). What do you think?"
    sources, updated_txt = SourceReference.parse_sources(txt)
    print(updated_txt)
    for source in sources:
        print(source)
        print()
        print()
        print()
        print()
        source.load(None)
        print(source)