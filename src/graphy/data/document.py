
from azure.cosmos import DatabaseProxy
from azure.cosmos.exceptions import CosmosResourceNotFoundError


from ..dataaccess import client_factory

DOCUMENT_CONTAINER_NAME = "documents"

class Document:
    id:str
    uid:str
    name:str
    title:str
    content:str
    text_unit_ids:list[str]
    
    def __init__(self, data:dict = None):
        if data:
            self.id = data.get("id")
            self.uid = data.get("uid")
            self.name = data.get("name")
            self.title = data.get("title")
            self.content = data.get("content")
            self.text_unit_ids = data.get("text_unit_ids")

    def to_dict(self):
        return {
            "id": self.id,
            "uid": self.uid,
            "name": self.name,
            "title": self.title,
            "content": self.content,
            "text_unit_ids": self.text_unit_ids
        }
    
    def __str__(self):
        return f"[{self.id}] {self.title}"
    
    def save(self, db:DatabaseProxy):
        """Save the Document to the database"""
        client = client_factory(DOCUMENT_CONTAINER_NAME, db)
        client.upsert_item(self.to_dict())
    
    def load(id:str, db:DatabaseProxy) -> 'Document':
        """Load an Document from the database by the Document ID"""
        client = client_factory(DOCUMENT_CONTAINER_NAME, db)
        id = str(id)
        if not id.isnumeric():
            res = list(client.query_items(f"SELECT * FROM c WHERE c.uid = '{id}'", enable_cross_partition_query=True))
            if not res or len(res) == 0: return None
            document = res[0]
        else:
            try:
                document = client.read_item(id, id)
            except CosmosResourceNotFoundError as e:
                return None

        if not document: return None
        return Document(document)

    def load_all(ids:list[str], db:DatabaseProxy) -> list['Document']:
        """Load all the specified Documents from the database"""
        if ids is None or len(ids) == 0: return []
        id_arr = [f"'{x}'" for x in ids]
        check_id = str(ids[0])
        if not check_id.isnumeric():
            query = f"SELECT * FROM c WHERE c.uid IN ({','.join(id_arr)})"
        else: 
            query = f"SELECT * FROM c WHERE c.id IN ({','.join(id_arr)})"
    
        client = client_factory(DOCUMENT_CONTAINER_NAME, db)
        res = list(client.query_items(query, enable_cross_partition_query=True))
        if not res or len(res) == 0: return []
        return [Document(x) for x in res]
    
    def load_from_df_row(df:any, doc_id:int) -> 'Document':
        """Load a Document from a pandas DataFrame Row (Named Tuple) that contains the Document record"""

        uid = df.id
        if uid is None: return None
      
        content = df.raw_content
        name = df.title
        text_unit_ids = df.text_unit_ids.tolist() if df.text_unit_ids is not None else []

        ## Infer the title from the Header row of the content (assumingn markdown)
        # Find the first line that is not empty and starts with "# "
        title = None
        first_hash = content.find("# ")
        if first_hash != -1:
            first_newline = content.find("\n", first_hash)
            if first_newline != -1:
                title = content[first_hash+2:first_newline].strip()
            else: 
                if len(content) > 100:
                    title = content[first_hash+2:first_hash+100]
                else:
                    title = content[first_hash+2:]
        else: 
            end_first_line = content.find("\n")
            title = content[:end_first_line]
            if len(title) > 100:
                title = title[:100]

        return Document({
            "id": str(doc_id), 
            "uid": uid,
            "name": name,
            "title": title if title else name,
            "content": content,
            "text_unit_ids": text_unit_ids
        })

        
    
    