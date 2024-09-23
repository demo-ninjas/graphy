import pandas as pd

from azure.cosmos import DatabaseProxy
from azure.cosmos.exceptions import CosmosResourceNotFoundError

from ..dataaccess import client_factory

TEXT_UNIT_CONTAINER_NAME = "text-units"

class TextUnit:
    id:str
    text:str
    n_tokens:int
    document_ids:list[str]
    entity_ids:list[str]
    relationship_ids:list[str]
    covariate_ids:list[str]
    
    def __init__(self, data:dict = None):
        if data:
            self.id = data.get("id")
            self.text = data.get("text")
            self.n_tokens = data.get("n_tokens")
            self.document_ids = data.get("document_ids")
            self.entity_ids = data.get("entity_ids")
            self.relationship_ids = data.get("relationship_ids")
            self.covariate_ids = data.get("covariate_ids")

    def to_dict(self):
        return {
            "id": self.id,
            "text": self.text,
            "n_tokens": self.n_tokens,
            "document_ids": self.document_ids,
            "entity_ids": self.entity_ids,
            "relationship_ids": self.relationship_ids,
            "covariate_ids": self.covariate_ids
        }
    
    def __str__(self):
        return f"[{self.id}] {self.text}"
    
    def save(self, db:DatabaseProxy):
        """Save the TextUnit to the database"""
        client = client_factory(TEXT_UNIT_CONTAINER_NAME, db)
        item = self.to_dict()
        client.upsert_item(item)
    
    def load(id:str, db:DatabaseProxy) -> 'TextUnit':
        """Load an TextUnit from the database by the TextUnit ID"""
        client = client_factory(TEXT_UNIT_CONTAINER_NAME, db)
        id = str(id)
        if not id.isnumeric():
            res = list(client.query_items(f"SELECT * FROM c WHERE c.uid = '{id}'", enable_cross_partition_query=True))
            if not res or len(res) == 0: return None
            text_unit = res[0]
        else:
            try:
                text_unit = client.read_item(id, id)
            except CosmosResourceNotFoundError as e:
                return None
            
        
        if not text_unit: return None
        return TextUnit(text_unit)

    def load_all(ids:list[str], db:DatabaseProxy) -> list['TextUnit']:
        """Load all the specified TextUnits from the database"""
        if ids is None or len(ids) == 0: return []
        id_arr = [f"'{x}'" for x in ids]
        check_id = str(ids[0])
        if not check_id.isnumeric():
            query = f"SELECT * FROM c WHERE c.id IN ({','.join(id_arr)})"
        else: 
            ## We shouldn't get here - short ids are not currently implemented for text units in the database
            query = f"SELECT * FROM c WHERE c.id IN ({','.join(id_arr)})"
    
        client = client_factory(TEXT_UNIT_CONTAINER_NAME, db)
        res = list(client.query_items(query, enable_cross_partition_query=True))
        if not res or len(res) == 0: return []
        return [TextUnit(x) for x in res]
    

    def load_from_df_row(df:any, entity_map:dict[str, str] = None, relationship_map:dict[str, str] = None, covariates:pd.DataFrame = None, db:DatabaseProxy = None) -> 'TextUnit':
        """Load a Text Unit from a pandas DataFrame Row (Named Tuple) that contains the Text record"""

        uid = df.id
        if uid is None: return None
                     
        text = df.text
        n_tokens = int(df.n_tokens)
        document_ids = df.document_ids.tolist() if df.document_ids is not None else []
        entity_ids = df.entity_ids.tolist() if df.entity_ids is not None else []
        relationship_ids = df.relationship_ids.tolist() if df.relationship_ids is not None else []
        covariate_ids = []
        if hasattr(df, 'covariate_ids'):
            covariate_ids = df.covariate_ids.tolist() if df.covariate_ids is not None else []
        
        if entity_map is not None:
            ## Replace the entity ids with the actual entity ids (the id here is the UID)
            entity_ids = [entity_map.get(x) for x in entity_ids if entity_map.get(x) is not None]
        elif db is not None:
            ## Load the entities from the database
            from .entity import Entity
            entities = Entity.load_all(entity_ids, db)
            entity_ids = [x.id for x in entities]

        if relationship_map is not None:
            ## Replace the relationship ids with the actual relationship ids (the id here is the UID)
            relationship_ids = [relationship_map.get(x) for x in relationship_ids if relationship_map.get(x) is not None]
        elif db is not None:
            ## Load the relationships from the database
            from .relationship import Relationship
            relationships = Relationship.load_all(relationship_ids, db)
            relationship_ids = [x.id for x in relationships]
                                
        if db is not None:
            ## Load the documents from the database
            from .document import Document
            documents = Document.load_all(document_ids, db)
            document_ids = [x.id for x in documents]

        if covariates is not None:
            ## Replace the covariate ids with the actual covariate ids (the id here is the UID)
            cv_ids = []
            for cv in covariate_ids:
                cv = covariates[covariates.id == cv].human_readable_id
                if cv is not None and len(cv) > 0:
                    cv_ids.append(cv.values[0])
            # covariate_ids = [str(int(covariates[covariates.text_unit_id == x].human_readable_id.values[0])) for x in covariate_ids]
            covariate_ids = cv_ids
        # elif db is not None:
        #     ## Load the covariates from the database
        #     from .covariate import Covariate
        #     covariates = Covariate.load_all(covariate_ids, db)
        #     covariate_ids = [x.id for x in covariates]

        
        return TextUnit({
            "id": uid,
            "text": text,
            "n_tokens": int(n_tokens),
            "document_ids": document_ids,
            "entity_ids": entity_ids,
            "relationship_ids": relationship_ids,
            "covariate_ids": covariate_ids
        })

    
    