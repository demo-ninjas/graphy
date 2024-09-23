from concurrent.futures import ThreadPoolExecutor

from azure.cosmos import DatabaseProxy
from azure.cosmos.exceptions import CosmosResourceNotFoundError
from ..dataaccess import client_factory

COMMUNITY_CONTAINER_NAME = "communities"
COMMUNITY_METADATA_CONTAINER_NAME = "community-metadata"

MAX_RELATIONSHIPS = 40_000
MAX_TEXTS = 12_000

class CommunityFinding:
    explanation:str
    summary:str

    def __init__(self, data:dict = None):
        if data:
            self.explanation = data.get("explanation")
            self.summary = data.get("summary")

    def to_dict(self):
        return {
            "explanation": self.explanation,
            "summary": self.summary
        }

    def __str__(self):
        return self.summary


class Community:
    id:str
    uid:str
    title:str
    level:int
    rank:float
    summary:str
    full_content:str
    weight:float = None
    normalised_weight:float = None
    normalised_level_weight:float = None

    ## Following properties are metadata props (loaded from metadata collection - they are loaded separately)
    metadata_loaded:bool = False
    rank_explanation:str
    findings:list[CommunityFinding]
    relationships:list[str]
    texts:list[str]
    metadata_truncated:bool = False
    
    
    def __init__(self, data:dict = None):
        if data:
            self.id = data.get("id")
            self.uid = data.get("uid")
            self.title = data.get("title")
            self.level = data.get("level")
            self.rank = data.get("rank")
            self.summary = data.get("summary")
            self.full_content = data.get("full_content")
            self.weight = data.get("weight")
            self.normalised_weight = data.get("normalised_weight")
            self.normalised_level_weight = data.get("normalised_level_weight")

            if "rank_explanation" in data:
                self.metadata_loaded = True
                self.rank_explanation = data.get("rank_explanation")
                self.findings = [ CommunityFinding(x) for x in data.get("findings") ]
                self.relationships = data.get("relationships")
                self.texts = data.get("texts")
                self.metadata_truncated = data.get("truncated", False)
            


    def to_dict(self):
        return {
            "id": self.id,
            "uid": self.uid,
            "title": self.title,
            "level": self.level,
            "rank": self.rank,
            "summary": self.summary,
            "full_content": self.full_content,
            "weight": self.weight,
            "normalised_weight": self.normalised_weight,
            "normalised_level_weight": self.normalised_level_weight
        }
    
    def to_meta_dict(self):
        return {
            "id": self.id,
            "uid": self.uid,
            "rank_explanation": self.rank_explanation,
            "findings": [ x.to_dict() for x in self.findings ],
            "relationships": self.relationships,
            "texts": self.texts,
            "truncated": self.metadata_truncated
        }
    
    def __str__(self):
        return f"[{self.id}] {self.title} (L{self.level})"
    
    def save(self, db:DatabaseProxy):
        """Save the Community to the database"""
        client = client_factory(COMMUNITY_CONTAINER_NAME, db)
        item = self.to_dict()
        client.upsert_item(item)

        if self.metadata_loaded:
            client = client_factory(COMMUNITY_METADATA_CONTAINER_NAME, db)
            item = self.to_meta_dict()
            
            ## Limit the number of relationships and texts to avoid CosmosDB document size limit
            if len(item.get("relationships")) > MAX_RELATIONSHIPS:
                item["relationships"] = item.get("relationships")[:MAX_RELATIONSHIPS]
                item["truncated"] = True
                self.metadata_truncated = True
            if len(item.get("texts")) > MAX_TEXTS:
                item["texts"] = item.get("texts")[:MAX_TEXTS]
                item["truncated"] = True
                self.metadata_truncated = True
            
            client.upsert_item(item)

    def load_metadata(self, db:DatabaseProxy):
        if self.metadata_loaded: return
        client = client_factory(COMMUNITY_METADATA_CONTAINER_NAME, db)
        metadata = client.read_item(self.id, self.id)
        if not metadata: return
        self.rank_explanation = metadata.get("rank_explanation")
        self.findings = [ CommunityFinding(x) for x in metadata.get("findings") ]
        self.relationships = metadata.get("relationships")
        self.texts = metadata.get("texts")
        self.metadata_truncated = metadata.get("truncated", False)
        self.metadata_loaded = True


    def load(id:str, db:DatabaseProxy, include_metadata:bool = False) -> 'Community':
        """Load an Community from the database by the Community UID"""
        client = client_factory(COMMUNITY_CONTAINER_NAME, db)
        id = str(id)
        if not id.isnumeric():
            query = f"SELECT * FROM c WHERE c.uid = '{id}'"
            res = list(client.query_items(query, enable_cross_partition_query=True))
            if not res or len(res) == 0: return None
            community = res[0]
        else:
            try:
                community = client.read_item(id, id)
            except CosmosResourceNotFoundError as e: 
                return None
        
        if not community: return None

        community = Community(community)
        if include_metadata:
            community.load_metadata(db)
        return community
    
    def load_all(ids:list[str|int], db:DatabaseProxy, include_metadata:bool = False) -> list['Community']:
        """Load all the specified Communities from the database"""
        if ids is None or len(ids) == 0: return []

        if len(ids) == 1: return [Community.load(ids[0], db, include_metadata)]

        check_id = str(ids[0])
        id_arr = [f"'{x}'" for x in ids]
        if not check_id.isnumeric():
            query = f"SELECT * FROM c WHERE c.uid IN ({','.join(id_arr)})"
        else:
            query = f"SELECT * FROM c WHERE c.id IN ({','.join(id_arr)})"
    
        client = client_factory(COMMUNITY_CONTAINER_NAME, db)
        res = list(client.query_items(query, enable_cross_partition_query=True))
        if not res or len(res) == 0: return []

        communities = [Community(x) for x in res]
        if include_metadata:
            for c in communities:
                c.load_metadata(db)
        return communities
    

    def load_all_under_level(max_level:int, min_rank:float, db:DatabaseProxy, include_metadata:bool = False, only_fields:list[str] = None) -> list['Community']:
        """Load all the Communities under the specified level from the database"""
        client = client_factory(COMMUNITY_CONTAINER_NAME, db)
        if only_fields is not None:
            query = f"SELECT c.{',c.'.join(only_fields)} FROM c WHERE c.level <= {max_level} AND c.rank >= {min_rank}"
        else:
            query = f"SELECT * FROM c WHERE c.level <= {max_level} AND c.rank >= {min_rank}"

        communities = []
        for item in client.query_items(query, enable_cross_partition_query=True):
            communities.append(Community(item))

        # res = list(client.query_items(query, enable_cross_partition_query=True))
        if len(communities) == 0: return []

        # communities = [Community(x) for x in res]
        if include_metadata:
            for c in communities:
                c.load_metadata(db)

        return communities


    def load_from_df_row(df:any, raw_community:any) -> 'Community':
        """Load a community from a pandas DataFrame Row (Named Tuple) that contains the community report record"""

        uid = df.id
        if uid is None: return None
        
        community = int(df.community)
        title = df.title
        level = int(df.level)
        rank = float(df.rank)
        rank_explanation = df.rank_explanation
        summary = df.summary
        findings = df.findings
        full_content = df.full_content
        relationships = raw_community.relationship_ids.tolist() if raw_community is not None and raw_community.relationship_ids is not None else []
        texts = set()
        if raw_community is not None and raw_community.text_unit_ids is not None:
            for tmp in raw_community.text_unit_ids.tolist():
                arr = tmp.split(",")
                for t in arr:
                    texts.add(t)
        texts = list(texts)

        return Community({
            "id": str(community),
            "uid": uid,
            "title": title,
            "level": level,
            "rank": rank,
            "summary": summary,
            "full_content": full_content,
            "rank_explanation": rank_explanation,
            "findings": findings,
            "relationships": relationships,
            "texts": texts
        })


    
    