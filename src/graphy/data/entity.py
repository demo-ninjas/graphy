import pandas as pd

from azure.cosmos import DatabaseProxy
from azure.cosmos.exceptions import CosmosResourceNotFoundError

from ..dataaccess import client_factory
from ._pd_util import first_non_null

ENTITY_CONTAINER_NAME = "entities"
ENTITY_METADATA_CONTAINER_NAME = "entity-metadata"

MAX_SOURCES = 12_000
MAX_CLAIMS = 4_000


class EntityCommunity: 
    id:str
    community:int
    level:int
    x:int
    y:int
    size:int
    degree:int

    def __init__(self, data:dict = None):
        if data:
            self.id = data.get("id")
            self.community = str(data.get("community"))
            self.level = data.get("level")
            self.x = data.get("x")
            self.y = data.get("y")
            self.size = data.get("size")
            self.degree = data.get("degree")
    
    def to_dict(self):
        return {
            "id": self.id,
            "community": self.community,
            "level": self.level,
            "x": self.x,
            "y": self.y,
            "size": self.size,
            "degree": self.degree
        }

class EntityClaim: 
    id:str
    uid:str
    type:str
    claim_type:str
    description:str
    start_date:str
    end_date:str
    text_unit_id:str
    document_ids:list[str]
    n_tokens:int
    status:bool

    def __init__(self, data:dict = None):
        if data:
            self.id = data.get("id")
            self.uid = data.get("uid")
            self.type = data.get("type")
            self.claim_type = data.get("claim_type")
            self.description = data.get("description")
            self.start_date = data.get("start_date")
            self.end_date = data.get("end_date")
            self.text_unit_id = data.get("text_unit_id")
            self.document_ids = data.get("document_ids")
            self.n_tokens = data.get("n_tokens")
            self.status = data.get("status")
    
    def to_dict(self):
        return {
            "id": self.id,
            "uid": self.uid,
            "type": self.type,
            "claim_type": self.claim_type,
            "description": self.description,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "text_unit_id": self.text_unit_id,
            "document_ids": self.document_ids,
            "n_tokens": self.n_tokens,
            "status": self.status
        }

class Entity:
    id:str
    uid:str
    title:str
    type:str
    description:str
    community_ids:list[str]
    
    ## Following properties are metadata properties (loaded from metatdata collection + are loaded separately)
    metadata_loaded:bool = False
    communities:list[EntityCommunity]
    sources:list[str]
    entity_type:str
    description_embedding:list[float]
    claims:list[EntityClaim]
    truncated_sources:bool = False
    truncated_claims:bool = False
    
    def __init__(self, data:dict = None):
        if data:
            self.id = data.get("id")
            self.uid = data.get("uid")
            self.title = data.get("title")
            self.type = data.get("type")
            self.description = data.get("description")
            self.community_ids = data.get("community_ids")

            ## Metadata properties (if present)
            if data.get("communities") or data.get("sources") or data.get("entity_type") or data.get("description_embedding") or data.get("claims"):
                self.metadata_loaded = True
                self.communities = [ EntityCommunity(x) for x in data.get("communities") ] if data.get("communities") else []
                self.sources = data.get("sources")
                self.entity_type = data.get("entity_type")
                self.description_embedding = data.get("description_embedding")
                self.claims = [ EntityClaim(x) for x in data.get("claims") ] if data.get("claims") else []
                self.truncated_sources = data.get("truncated_sources") or False
                self.truncated_claims = data.get("truncated_claims") or False
            
    def to_dict(self) -> dict[str, any]:
        return {
            "id": self.id,
            "uid": self.uid,
            "title": self.title,
            "type": self.type,
            "description": self.description,
            "community_ids": self.community_ids
        }
    
    def to_meta_dict(self) -> dict[str, any]:
        return {
            "id": self.id,
            "uid": self.uid,
            "sources": self.sources,
            "claims": [x.to_dict() for x in self.claims],
            "entity_type": self.entity_type,
            "communities": [x.to_dict() for x in self.communities],
            "description_embedding": self.description_embedding,
            "truncated_sources": self.truncated_sources,
            "truncated_claims": self.truncated_claims
        }
    
    def __str__(self):
        return f"[{self.id}] {self.title} ({self.type})"

    def save(self, db:DatabaseProxy):
        """Save the Entity to the database"""
        client = client_factory(ENTITY_CONTAINER_NAME, db)
        item = self.to_dict()
        client.upsert_item(item)

        if self.metadata_loaded:
            client = client_factory(ENTITY_METADATA_CONTAINER_NAME, db)
            item = self.to_meta_dict()

            ## Truncate the sources and claims if they are too large
            if len(item.get("sources")) > MAX_SOURCES:
                item["sources"] = item.get("sources")[:MAX_SOURCES]
                item["truncated_sources"] = True
                self.truncated_sources = True
            if len(item.get("claims")) > MAX_CLAIMS:
                item["claims"] = item.get("claims")[:MAX_CLAIMS]
                item["truncated_claims"] = True
                self.truncated_claims = True

            client.upsert_item(item)
    
    def load_metadata(self, db:DatabaseProxy):
        """Load the metadata for the entity"""
        if self.metadata_loaded: return
        client = client_factory(ENTITY_METADATA_CONTAINER_NAME, db)
        metadata = client.read_item(self.id, self.id)
        if metadata is not None: 
            self.sources = metadata.get("sources")
            self.claims = [ EntityClaim(x) for x in metadata.get("claims") ] if metadata.get("claims") else []
            self.entity_type = metadata.get("entity_type")
            self.communities = metadata.get("communities")
            self.description_embedding = metadata.get("description_embedding")
            self.metadata_loaded = True
        

    def load(id:str, db:DatabaseProxy, include_metadata:bool = False) -> 'Entity':
        """Load an Entity from the database by either the ID or the UID"""
        client = client_factory(ENTITY_CONTAINER_NAME, db)
        id = str(id).strip()
        if not id.isnumeric():  ## Then it's a UID
            res = list(client.query_items(f"SELECT * FROM c WHERE c.uid = '{id}'", enable_cross_partition_query=True))
            if not res or len(res) == 0: return None
            entity = res[0]
        else: 
            try:
                entity = client.read_item(id, id)
            except CosmosResourceNotFoundError as e:
                return None

        if not entity: return None

        entity = Entity(entity)
        if include_metadata:
            entity.load_metadata(db)

        return entity

    def load_all(ids:list[str], db:DatabaseProxy, include_metadata:bool = False) -> list['Entity']:
        """Load all the specified entities from the database (they must all be Entity ID's or al Entity UID's, do not mix and match)"""
        if ids is None or len(ids) == 0: return []

        query = None
        check_id = str(ids[0])
        ids = ["'" + str(x).strip() + "'" for x in ids]
        if not check_id.isnumeric():
            query = f"SELECT * FROM c WHERE c.uid IN ({','.join(ids)})"
        else:
            query = f"SELECT * FROM c WHERE c.id IN ({','.join(ids)})"
        
        client = client_factory(ENTITY_CONTAINER_NAME, db)
        res = list(client.query_items(query, enable_cross_partition_query=True))
        if not res or len(res) == 0: return []

        entities = [Entity(x) for x in res]

        if include_metadata:
            for entity in entities:
                entity.load_metadata(db)

        return entities


    def load_community_entities(community_id:str, db:DatabaseProxy) -> list['Entity']:
        """Load all the entities in the specified community"""
        client = client_factory(ENTITY_CONTAINER_NAME, db)
        community_id = str(community_id).strip()
        res = list(client.query_items(f"SELECT * FROM c WHERE ARRAYCONTAINS(c.community_ids, {community_id})", enable_cross_partition_query=True))
        if not res or len(res) == 0: return []
        return [Entity(x) for x in res]
    
    def load_from_data_frame(df:pd.DataFrame, covariates:pd.DataFrame) -> 'Entity':
        """Load an entity from a pandas DataFrame that contains all the instances of this entity (at various levels)"""

        uid = first_non_null('id', df)
        if uid is None: return None
        
        title = first_non_null('title', df)
        type = first_non_null('type_x', df)
        if type is None: type = first_non_null('type_y', df)
        description = first_non_null('description_x', df)
        if description is None: description = first_non_null('description_y', df)
        human_readable_id = first_non_null('human_readable_id_x', df)
        if human_readable_id is None: human_readable_id = first_non_null('human_readable_id_y', df)
        entity_type = first_non_null('entity_type', df)
        description_embedding = first_non_null('description_embedding', df)
        description_embedding = description_embedding.tolist() if description_embedding is not None else None

        # Extract a unique set of Source IDs
        source_id_set = df[df["source_id"].notnull()]
        source_ids = set()
        for i, row in source_id_set.iterrows():
            for source_id in row["source_id"].split(","):
                if source_id not in source_ids:
                    source_ids.add(source_id.strip())
        
        ## Get the set as a list of values
        source_ids = list(source_ids)

        communities = Entity.__build_entity_communities_from_dataframe(df)
        community_ids = [str(x.get('community')) for x in communities if int(x.get('community')) > -1]

        ## Find any claims in the covariates (where subject_id == uid)
        claims = []
        if covariates is not None: 
            claim_set = covariates[covariates["subject_id"] == title]
            claims = [{ 
                "id": x.human_readable_id,
                "uid": x.id,
                "type": x.type,
                "claim_type": x.covariate_type,
                "description": x.description,
                "start_date": x.start_date or None,
                "end_date": x.end_date or None,
                "text_unit_id": x.text_unit_id,
                "document_ids": list(x.document_ids),
                "n_tokens": x.n_tokens,
                "status": str(x.status).upper() == "TRUE"
            } for x in claim_set.itertuples()]

        return Entity({
            "id": str(int(human_readable_id)),
            "uid": uid,
            "title": title,
            "type": type,
            "description": description,
            "communities": communities,
            "community_ids": community_ids,
            "sources": source_ids,
            "entity_type": entity_type,
            "description_embedding": description_embedding,
            "claims": claims
        })

    def __build_entity_communities_from_dataframe(df:pd.DataFrame) -> list['EntityCommunity']:
        """Build a list of entities from a pandas DataFrame that contains all the instances of this entity (at various levels)"""
        communities = []
        for i, row in df.iterrows():
            communities.append({
                "id": int(row.name),
                "community": int(row.get("community") or -1),
                "level": int(row.get("level") or -1),
                "x": int(row.get("x") or 0), 
                "y": int(row.get("y") or 0),
                "size": int(row.get("size") or 0),
                "degree": int(row.get("degree") or 0),
                "top_level_node_id": row.get("top_level_node_id", None)
            })

        ## Sort by level
        communities.sort(key=lambda x: x.get('level', -1) if x.get('level', -1) > -1 else 999)

        return communities