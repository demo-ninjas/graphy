#!/usr/bin/env python
import os
from pathlib import Path
import asyncio

import pandas as pd
from pyvis.network import Network
from graphrag.query.indexer_adapters import (
    read_indexer_covariates,
    read_indexer_entities,
    read_indexer_relationships,
    read_indexer_reports,
    read_indexer_text_units,
)

LANCEDB_URI = f"lancedb"
COMMUNITY_REPORT_TABLE = "create_final_community_reports"
ENTITY_TABLE = "create_final_nodes"                                 ## Nodes
ENTITY_EMBEDDING_TABLE = "create_final_entities"
RELATIONSHIP_TABLE = "create_final_relationships"                   ## Edges
COVARIATE_TABLE = "create_final_covariates"
TEXT_UNIT_TABLE = "create_final_text_units"
COMMUNITY_LEVEL = 2

async def main():
    # Check if there's a command line argument called "--run"
    args = _parse_args()

    if "--help" in args:
        print("Usage: python visualise-graph --run=<run_id> --response-type=<response_type> --query-type=<query_type> --query=<query>")
        print("")
        print("Options:")
        print("  --run=<run_id>                        The run ID to use (aka. the folder name) - defaults to the latest run in the output directory")
        print("  --response-type=<response_type>       The response type to return (eg. 'Multiple Paragraphs', 'Single Sentence', 'List of 3-7 Points', 'Single Page', 'Multi-Page Report')")
        print("  --query-type=<query_type>             The query type to use (eg. 'global', 'local') [Default: local]")
        print("  --local                               Use local search")
        print("  --global                              Use global search")
        print("  --query=<query>                       To run a single query immediately (otherwise, you will be prompted for a query)")
        return

    ARTIFACT_DIR = None
    if "--run" in args:
        run_id = args["--run"]
        ARTIFACT_DIR = f"output/{run_id}/artifacts"
    else: 
        ARTIFACT_DIR = _infer_artifact_dir(".")

    print(f"Loading data from: {ARTIFACT_DIR}")
    data_path = Path(ARTIFACT_DIR)
    final_nodes: pd.DataFrame = pd.read_parquet(f"{data_path.as_posix()}/{ENTITY_TABLE}.parquet")
    final_entities: pd.DataFrame = pd.read_parquet(f"{data_path.as_posix()}/{ENTITY_EMBEDDING_TABLE}.parquet")
    final_community_reports: pd.DataFrame = pd.read_parquet(f"{data_path.as_posix()}/{COMMUNITY_REPORT_TABLE}.parquet")
    # final_text_units: pd.DataFrame = pd.read_parquet(f"{data_path.as_posix()}/{TEXT_UNIT_TABLE}.parquet")
    final_relationships: pd.DataFrame = pd.read_parquet(f"{data_path.as_posix()}/{RELATIONSHIP_TABLE}.parquet")
    # final_covariates: pd.DataFrame = pd.read_parquet(f"{data_path.as_posix()}/{COVARIATE_TABLE}.parquet")
    
    net = Network(height="100%", width="100%", bgcolor="#222222", font_color="white")
    net.barnes_hut()

    final_nodes["shape"] = "dot"
    print("Reading relationships")
    edges = read_indexer_relationships(final_relationships)
    print("Reading entities")
    entities = read_indexer_entities(final_nodes, final_entities, COMMUNITY_LEVEL)

    titles = [entity.title for entity in entities]
    descriptions = [entity.description for entity in entities]

    edge_list = [ (int(edge.source) if edge.source.isnumeric() else edge.source, int(edge.target) if edge.target.isnumeric() else edge.target, edge.weight) for edge in edges]
    # nodes = final_nodes[["id", "title", "size", "shape"]]
    # filtered_edges = final_relationships.query('col1 <= 1 & 1 <= col1')
    # edges = filtered_edges[["source", "target", "weight"]].values.tolist()

    # for edge in edges:
    #     if edge[0] not in final_nodes["title"].values or edge[1] not in final_nodes["title"].values:
    #         continue
    #     filtered_edges.append(edge)

    print("Adding nodes to network")
    net.add_nodes(titles, title=descriptions)
    print("Adding edges to network")
    net.add_edges(edge_list)
    print("Showing graph")
    net.show("graph.html")

# Relationships
# Index(['source', 'target', 'weight', 'description', 'text_unit_ids', 'id',
#        'human_readable_id', 'source_degree', 'target_degree', 'rank'],
#       dtype='object')

# Nodes
# Index(['level', 'title', 'type', 'description', 'source_id', 'community',
#        'degree', 'human_readable_id', 'id', 'size', 'graph_embedding',
#        'entity_type', 'top_level_node_id', 'x', 'y', 'shape'],
#       dtype='object')

# Entities
# Index(['id', 'name', 'type', 'description', 'human_readable_id',
#        'graph_embedding', 'text_unit_ids', 'description_embedding'],
#       dtype='object')

# Community Reports
# Index(['community', 'full_content', 'level', 'rank', 'title',
#        'rank_explanation', 'summary', 'findings', 'full_content_json', 'id'],
#       dtype='object')

def _parse_args() -> dict[str, str]:
    args = sys.argv[1:]
    if len(args) == 0:
        return {}
    res = {}
    for arg in args: 
        if arg.startswith("--"):
            arr = arg.split("=")
            key = arr[0]
            value = arr[1] if len(arr) > 1 else True
            res[key] = value
    return res

def _infer_artifact_dir(root: str) -> str:
    output = Path(root) / "output"
    # use the latest data-run folder
    if output.exists():
        folders = sorted(output.iterdir(), key=os.path.getmtime, reverse=True)
        if len(folders) > 0:
            folder = folders[0]
            return str((folder / "artifacts").absolute())
    msg = f"Could not infer data directory from root={root}"
    raise ValueError(msg)

# map community to a colour
def community_to_colour(community):
    """Map a community to a color."""
    colors = [
        "crimson",
        "darkorange",
        "indigo",
        "cornflowerblue",
        "cyan",
        "teal",
        "green",
    ]
    return (
        colors[int(community) % len(colors)] if community is not None else "lightgray"
    )


def edge_to_source_community(edge):
    """Get the community of the source node of an edge."""
    source_node = next(
        (entry for entry in w.nodes if entry["properties"]["title"] == edge["start"]),
        None,
    )
    source_node_community = source_node["properties"]["community"]
    return source_node_community if source_node_community is not None else None





# INPUT_DIR = "output/20240904-220431/artifacts"
# LANCEDB_URI = f"{INPUT_DIR}/lancedb"

# COMMUNITY_REPORT_TABLE = "create_final_community_reports"
# ENTITY_TABLE = "create_final_nodes"
# ENTITY_EMBEDDING_TABLE = "create_final_entities"
# RELATIONSHIP_TABLE = "create_final_relationships"
# COVARIATE_TABLE = "create_final_covariates"
# TEXT_UNIT_TABLE = "create_final_text_units"
# COMMUNITY_LEVEL = 2

# # read nodes table to get community and degree data
# entity_df = pd.read_parquet(f"{INPUT_DIR}/{ENTITY_TABLE}.parquet")
# entity_embedding_df = pd.read_parquet(f"{INPUT_DIR}/{ENTITY_EMBEDDING_TABLE}.parquet")

# relationship_df = pd.read_parquet(f"{INPUT_DIR}/{RELATIONSHIP_TABLE}.parquet")
# relationships = read_indexer_relationships(relationship_df)



# w = GraphWidget()
# w.directed = True
# w.nodes = convert_entities_to_dicts(entity_df)
# w.edges = convert_relationships_to_dicts(relationship_df)

# # show title on the node
# w.node_label_mapping = "title"


# # map community to a color
# def community_to_color(community):
#     """Map a community to a color."""
#     colors = [
#         "crimson",
#         "darkorange",
#         "indigo",
#         "cornflowerblue",
#         "cyan",
#         "teal",
#         "green",
#     ]
#     return (
#         colors[int(community) % len(colors)] if community is not None else "lightgray"
#     )


# def edge_to_source_community(edge):
#     """Get the community of the source node of an edge."""
#     source_node = next(
#         (entry for entry in w.nodes if entry["properties"]["title"] == edge["start"]),
#         None,
#     )
#     source_node_community = source_node["properties"]["community"]
#     return source_node_community if source_node_community is not None else None


# w.node_color_mapping = lambda node: community_to_color(node["properties"]["community"])
# w.edge_color_mapping = lambda edge: community_to_color(edge_to_source_community(edge))
# # map size data to a reasonable factor
# w.node_scale_factor_mapping = lambda node: 0.5 + node["properties"]["size"] * 1.5 / 20
# # use weight for edge thickness
# w.edge_thickness_factor_mapping = "weight"

# # Use the circular layout for this visualization. For larger graphs, the default organic layout is often preferrable.
# w.circular_layout()


def run_main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    
if __name__ ==  '__main__':
    run_main()