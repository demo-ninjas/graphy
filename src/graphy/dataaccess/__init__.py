from azure.cosmos import ContainerProxy, DatabaseProxy

__CLIENT_CACHE = {}

def client_factory(container_name:str, db:DatabaseProxy):
    global __CLIENT_CACHE
    key = f"{db.id}-{container_name}"
    if key in __CLIENT_CACHE:
        return __CLIENT_CACHE[key]
    else:
        client = db.get_container_client(container_name)
        __CLIENT_CACHE[key] = client
        return client
