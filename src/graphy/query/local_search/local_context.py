# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""Local Context Builder."""

from collections import defaultdict
from typing import Any, cast

import pandas as pd
import tiktoken

from graphrag.query.llm.text_utils import num_tokens

from azure.cosmos import DatabaseProxy

from graphy.data import Entity, Relationship
from graphy.data.entity import EntityClaim


def build_entity_context(
    db: DatabaseProxy,
    selected_entities: list[Entity],
    token_encoder: tiktoken.Encoding | None = None,
    max_tokens: int = 8000,
    include_entity_rank: bool = True,
    rank_description: str = "number of relationships",
    column_delimiter: str = "|",
    context_name="Entities",
) -> tuple[str, pd.DataFrame]:
    """Prepare entity data table as context data for system prompt."""
    if len(selected_entities) == 0:
        return "", pd.DataFrame()

    # add headers
    current_context_text = f"-----{context_name}-----" + "\n"
    header = ["id", "entity", "description"]
    if include_entity_rank:
        header.append(rank_description)
    # attribute_cols = (
    #     list(selected_entities[0].attributes.keys())
    #     if selected_entities[0].attributes
    #     else []
    # )
    # header.extend(attribute_cols)
    current_context_text += column_delimiter.join(header) + "\n"
    current_tokens = num_tokens(current_context_text, token_encoder)

    all_context_records = [header]
    for entity in selected_entities:
        new_context = [
            entity.id,
            entity.title,
            entity.description if entity.description else "",
        ]
        if include_entity_rank:
            entity.load_relationships(db)
            rank = len(entity.outbound_relationships) + len(entity.inbound_relationships)
            new_context.append(str(rank))

        # for field in attribute_cols:
        #     field_value = (
        #         str(entity.attributes.get(field))
        #         if entity.attributes and entity.attributes.get(field)
        #         else ""
        #     )
        #     new_context.append(field_value)
        new_context_text = column_delimiter.join(new_context) + "\n"
        new_tokens = num_tokens(new_context_text, token_encoder)
        if current_tokens + new_tokens > max_tokens:
            break
        current_context_text += new_context_text
        all_context_records.append(new_context)
        current_tokens += new_tokens

    if len(all_context_records) > 1:
        record_df = pd.DataFrame(
            all_context_records[1:], columns=cast(Any, all_context_records[0])
        )
    else:
        record_df = pd.DataFrame()

    return current_context_text, record_df


def build_covariates_context(
    db: DatabaseProxy,
    selected_entities: list[Entity],
    token_encoder: tiktoken.Encoding | None = None,
    max_tokens: int = 8000,
    column_delimiter: str = "|",
    context_name: str = "Covariates",
) -> tuple[str, pd.DataFrame]:
    """Prepare covariate data tables as context data for system prompt."""
    # create an empty list of covariates
    if len(selected_entities) == 0:
        return "", pd.DataFrame()

    selected_covariates = list[EntityClaim]()
    record_df = pd.DataFrame()

    # add context header
    current_context_text = f"-----{context_name}-----" + "\n"

    # add header
    header = ["id", "entity", "type", "claim_type", "description", "start_date", "end_date", "status" ]
    current_context_text += column_delimiter.join(header) + "\n"
    current_tokens = num_tokens(current_context_text, token_encoder)

    all_context_records = [header]
    for entity in selected_entities:
        entity.load_metadata(db)
        if entity.claims is None or len(entity.claims) == 0:
            continue

        for claim in entity.claims: 
            new_context = [
                claim.id,
                entity.title,
                claim.type,
                claim.claim_type,
                claim.description if claim.description else "",
                claim.start_date if claim.start_date else "",
                claim.end_date if claim.end_date else "",
                claim.status if claim.status else "",
            ]
            new_context_text = column_delimiter.join(new_context) + "\n"
            new_tokens = num_tokens(new_context_text, token_encoder)
            if current_tokens + new_tokens > max_tokens:
                break
            current_context_text += new_context_text
            all_context_records.append(new_context)
            current_tokens += new_tokens

        if len(all_context_records) > 1:
            record_df = pd.DataFrame(
                all_context_records[1:], columns=cast(Any, all_context_records[0])
            )
        else:
            record_df = pd.DataFrame()

    return current_context_text, record_df


def build_relationship_context(
    db: DatabaseProxy,
    selected_entities: list[Entity],
    token_encoder: tiktoken.Encoding | None = None,
    include_relationship_weight: bool = False,
    max_tokens: int = 8000,
    top_k_relationships: int = 10,
    relationship_ranking_attribute: str = "rank",
    column_delimiter: str = "|",
    context_name: str = "Relationships",
) -> tuple[str, pd.DataFrame]:
    """Prepare relationship data tables as context data for system prompt."""
    selected_relationships = _filter_relationships(
        db=db,
        selected_entities=selected_entities,
        top_k_relationships=top_k_relationships,
        relationship_ranking_attribute=relationship_ranking_attribute,
    )

    if len(selected_entities) == 0 or len(selected_relationships) == 0:
        return "", pd.DataFrame()

    # add headers
    current_context_text = f"-----{context_name}-----" + "\n"
    header = ["id", "source", "target", "description"]
    if include_relationship_weight:
        header.append("weight")
    # attribute_cols = (
    #     list(selected_relationships[0].attributes.keys())
    #     if selected_relationships[0].attributes
    #     else []
    # )
    # attribute_cols = [col for col in attribute_cols if col not in header]
    # header.extend(attribute_cols)

    current_context_text += column_delimiter.join(header) + "\n"
    current_tokens = num_tokens(current_context_text, token_encoder)

    all_context_records = [header]
    for rel in selected_relationships:
        new_context = [
            rel.id,
            rel.source_title,
            rel.target_title,
            rel.description if rel.description else "",
        ]
        if include_relationship_weight:
            new_context.append(str(rel.weight if rel.weight else ""))
        # for field in attribute_cols:
        #     field_value = (
        #         str(rel.attributes.get(field))
        #         if rel.attributes and rel.attributes.get(field)
        #         else ""
        #     )
        #     new_context.append(field_value)
        new_context_text = column_delimiter.join(new_context) + "\n"
        new_tokens = num_tokens(new_context_text, token_encoder)
        if current_tokens + new_tokens > max_tokens:
            break
        current_context_text += new_context_text
        all_context_records.append(new_context)
        current_tokens += new_tokens

    if len(all_context_records) > 1:
        record_df = pd.DataFrame(
            all_context_records[1:], columns=cast(Any, all_context_records[0])
        )
    else:
        record_df = pd.DataFrame()

    return current_context_text, record_df


def _filter_relationships(
    db: DatabaseProxy,
    selected_entities: list[Entity],
    top_k_relationships: int = 10,
    relationship_ranking_attribute: str = "rank",
) -> list[Relationship]:
    """Filter and sort relationships based on a set of selected entities and a ranking attribute."""
    # First priority: in-network relationships (i.e. relationships between selected entities)
    # Second priority: out-of-network relationships (i.e. relationships with entities not in the selected entities)
    selected_entity_map = {entity.id: entity for entity in selected_entities}
    in_network_relationships, out_network_relationships = get_network_relationships(
        db=db,
        selected_entity_map=selected_entity_map,
    )

    if len(out_network_relationships) <= 1:
        return in_network_relationships + out_network_relationships

    # within out-of-network relationships, prioritize mutual relationships
    # (i.e. relationships with out-network entities that are shared with multiple selected entities)
    out_network_source_ids = [
        relationship.source
        for relationship in out_network_relationships
        if relationship.source not in selected_entity_map
    ]
    out_network_target_ids = [
        relationship.target
        for relationship in out_network_relationships
        if relationship.target not in selected_entity_map
    ]
    out_network_entity_ids = list(
        set(out_network_source_ids + out_network_target_ids)
    )
    out_network_entity_links = defaultdict(int)
    for entity_id in out_network_entity_ids:
        targets = [
            relationship.target
            for relationship in out_network_relationships
            if relationship.source == entity_id
        ]
        sources = [
            relationship.source
            for relationship in out_network_relationships
            if relationship.target == entity_id
        ]
        out_network_entity_links[entity_id] = len(set(targets + sources))

    # sort out-network relationships by number of links and rank_attributes
    for rel in out_network_relationships:
        rel._links = (
            out_network_entity_links[rel.source]
            if rel.source in out_network_entity_links
            else out_network_entity_links[rel.target]
        )

    # sort by attributes[links] first, then by ranking_attribute
    if relationship_ranking_attribute == "weight":
        out_network_relationships.sort(
            key=lambda x: (x._links, x.weight),  # type: ignore
            reverse=True,  # type: ignore
        )
    else:
        out_network_relationships.sort(
            key=lambda x: (
                x._links,  # type: ignore
                x.rank,  # type: ignore ## TODO: Do we want to support using user-defined ranking attributes?
            ),  # type: ignore
            reverse=True,
        )

    relationship_budget = top_k_relationships * len(selected_entities)
    return in_network_relationships + out_network_relationships[:relationship_budget]


def get_network_relationships(
    db: DatabaseProxy,
    selected_entity_map: dict[str,Entity],
) -> tuple[list[Relationship], list[Relationship]]:
    """Get all directed relationships between selected entities, sorted by ranking_attribute."""

    ## Grab all the relationships that are in the selected entities
    in_selected_relationships_map = dict[str, Relationship]()
    out_selected_relationships_map = dict[str, Relationship]()
    for entity in selected_entity_map.values():
        entity.load_relationships(db)
        for rel in entity.outbound_relationships:   ## aka. where the entity is the source
            if rel.target not in selected_entity_map:   
                ## This is an out of network relationship
                if rel.id not in out_selected_relationships_map:
                    rel._source_entity = entity
                    out_selected_relationships_map[rel.id] = rel
                else: 
                    out_selected_relationships_map[rel.id]._source_entity = entity
            else: 
                if rel.id not in in_selected_relationships_map:
                    rel._source_entity = entity
                    in_selected_relationships_map[rel.id] = rel
                else: 
                    in_selected_relationships_map[rel.id]._source_entity = entity
    

        for rel in entity.inbound_relationships:    ## aka. where the entity is the target
            if rel.source not in selected_entity_map:
                ## This is an out of network relationship
                if rel.id not in out_selected_relationships_map:
                    rel._target_entity = entity
                    out_selected_relationships_map[rel.id] = rel
                else: 
                    out_selected_relationships_map[rel.id]._target_entity = entity
            else: 
                if rel.id not in in_selected_relationships_map:
                    rel._target_entity = entity
                    in_selected_relationships_map[rel.id] = rel
                else: 
                    in_selected_relationships_map[rel.id]._target_entity = entity


    in_selected_relationships = list(in_selected_relationships_map.values())
    out_selected_relationships = list(out_selected_relationships_map.values())
    
    # sort by ranking attribute
    in_selected_relationships.sort(key=lambda x: x.rank, reverse=True)
    out_selected_relationships.sort(key=lambda x: x.rank, reverse=True)

    return in_selected_relationships, out_selected_relationships
    