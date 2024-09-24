from graphrag.query.structured_search.global_search.callbacks import GlobalSearchLLMCallback

STATE_IDLE = "idle"
STATE_LOADING_COMMUNITY_CONTEXT = "loading_community_context"
STATE_MAP_RESPONSE = "mapping_responses"
STATE_REDUCE_RESPONSE = "reducing_responses"
STATE_COMPLETE = "complete"

class QueryCallback:
    def on_state_change(self, state: str):
        pass
    def on_llm_token(self, token: str):
        pass
    
    def on_map_response_start(self, for_contexts: list[str]):
        pass

    def on_map_response_end(self, map_outputs: list[str]):
        pass



class _GlobalSearchCallbackToQueryCallback(GlobalSearchLLMCallback):
    def __init__(self, query_callback: QueryCallback):
        super().__init__()
        self.query_callback = query_callback
        self.state = STATE_IDLE
        self.map_counter = 0

    def on_state_change(self, state: str):
        self.state = state
        self.query_callback.on_state_change(state)

    def on_llm_token(self, token: str):
        self.query_callback.on_llm_token(token)

    def on_map_response_start(self, for_contexts: list[str]):
        if self.state == STATE_IDLE or self.state == STATE_LOADING_COMMUNITY_CONTEXT:
            self.on_state_change(STATE_MAP_RESPONSE)
        self.map_counter += 1
        self.query_callback.on_map_response_start(for_contexts)

    def on_map_response_end(self, map_outputs: list[str]):
        self.map_counter -= 1
        self.query_callback.on_map_response_end(map_outputs)
        if self.map_counter <= 0:
            self.on_state_change(STATE_REDUCE_RESPONSE)