import pandas as pd

from azure.cosmos import DatabaseProxy
from azure.cosmos.exceptions import CosmosResourceNotFoundError

from ..dataaccess import client_factory

from .entity import Entity

RELATIONSHIP_CONTAINER_NAME = "relationships"

MAX_TEXTS = 20_000

class Relationship:
    id:str
    uid:str
    source:str
    target:str
    weight:float
    description:str
    source_degree:int
    target_degree:int
    rank:float
    source_title:str
    target_title:str 
    truncated:bool = False
    texts:list[str]
    
    ### Following properties are transient props (loaded from other data collections) ###

    _source_entity:Entity = None
    _target_entity:Entity = None

    def __init__(self, data:dict = None):
        if data:
            self.id = data.get("id")
            self.uid = data.get("uid")
            self.source = data.get("source")
            self.target = data.get("target")
            self.weight = data.get("weight")
            self.description = data.get("description")
            self.source_degree = data.get("source_degree")
            self.target_degree = data.get("target_degree")
            self.rank = data.get("rank")
            self.texts = data.get("texts")
            self.source_title = data.get("source_title")
            self.target_title = data.get("target_title")
            self.truncated = data.get("truncated", False)
            

    def to_dict(self):
        return {
            "id": self.id,
            "uid": self.uid,
            "description": self.description,
            "source_title": self.source_title,
            "target_title": self.target_title,
            "source": self.source,
            "target": self.target,
            "weight": self.weight,
            "source_degree": self.source_degree,
            "target_degree": self.target_degree,
            "rank": self.rank,
            "texts": self.texts,
            "truncated": self.truncated
        }
    
    def __str__(self):
        return f"[{self.id}] {self.source_title} ({self.source}) -> {self.target_title} ({self.target})"

    def save(self, db:DatabaseProxy):
        """Save the Relationship to the database"""
        client = client_factory(RELATIONSHIP_CONTAINER_NAME, db)
        item = self.to_dict()

        if len(item["texts"]) > MAX_TEXTS:
            item["texts"] = item["texts"][:MAX_TEXTS]
            item["truncated"] = True
            self.truncated = True
            
        client.upsert_item(item)

    def load(id:str, db:DatabaseProxy) -> 'Relationship':
        """Load an Relationship from the database by either the Relationship ID or UID"""
        client = client_factory(RELATIONSHIP_CONTAINER_NAME, db)
        id = str(id)
        if not id.isnumeric():
            res = list(client.query_items(f"SELECT * FROM c WHERE c.uid = '{id}'", enable_cross_partition_query=True))
            if not res or len(res) == 0: return None
            rel = res[0]
        else: 
            try:
                rel = client.read_item(id, id)
            except CosmosResourceNotFoundError as e:
                return None

        if not rel: return None
        return Relationship(rel)

    def load_all(ids:list[str], db:DatabaseProxy) -> list['Relationship']:
        """Load all the specified relationships from the database (they must all be a Relationship ID or a UID, do not mix and match)"""
        if ids is None or len(ids) == 0: return []
        query = None

        check_id = str(ids[0])
        ids = ['"' + str(x).strip() + '"' for x in ids]
        if not check_id.isnumeric():
            query = f"SELECT * FROM c WHERE c.uid IN ({','.join(ids)})"
        else:
            query = f"SELECT * FROM c WHERE c.id IN ({','.join(ids)})"
        
        client = client_factory(RELATIONSHIP_CONTAINER_NAME, db)
        res = list(client.query_items(query, enable_cross_partition_query=True))
        if not res or len(res) == 0: return []
        return [Relationship(x) for x in res]

    def load_all_for_entity(entity_id:str, db:DatabaseProxy) -> tuple[list['Relationship'], list['Relationship']]:
        """Load all the relationships for a specified entity return a tuple of list of relationships for (source, target) - where the entity is the source or target of the relationship"""
        client = client_factory(RELATIONSHIP_CONTAINER_NAME, db)
        res = list(client.query_items(f"SELECT * FROM c WHERE c.source = '{entity_id}' OR c.target = '{entity_id}'", enable_cross_partition_query=True))
        if not res or len(res) == 0: return [], []
        return [Relationship(x) for x in res if x["source"] == entity_id], [Relationship(x) for x in res if x["target"] == entity_id]
    

    def load_source(self, db:DatabaseProxy) -> Entity:
        """Load the source Entity for this Relationship"""
        entity = Entity.load(self.source_id, db)
        self._source_entity = entity
        return entity
    
    def load_target(self, db:DatabaseProxy) -> Entity:
        """Load the target Entity for this Relationship"""
        entity = Entity.load(self.target_id, db)
        self._target_entity = entity
        return entity
    
    def load_from_df_row(df:any, entity_map:dict[str, str]) -> 'Relationship':
        """Load a Relationship from a pandas DataFrame Row (Named Tuple) that contains the Relationship record"""

        uid = df.id
        if uid is None: return None
        
        id = int(df.human_readable_id)
        source_title = df.source
        target_title = df.target
        weight = float(df.weight)
        description = df.description
        source_degree = int(df.source_degree)
        target_degree = int(df.target_degree)
        rank = float(df.rank)
        texts = df.text_unit_ids.tolist() if df.text_unit_ids is not None else []

        ## Find the source and target entities
        source_id = None
        target_id = None
        if entity_map is not None:
            source_id = entity_map.get(source_title, "")
            target_id = entity_map.get(target_title, "")

            
        return Relationship({
            "id": str(id),
            "uid": uid,
            "source": str(source_id),
            "target": str(target_id),
            "weight": weight,
            "description": description,
            "source_degree": source_degree,
            "target_degree": target_degree,
            "rank": rank,
            "texts": texts, 
            "source_title": source_title,
            "target_title": target_title
        })
