from core.vector_store_utils import semantic_search as vector_search, chunk_schema_elements
from core.graph_builder import GraphBuilder
from core.schema_parser import Schema
import re
from collections import deque
from difflib import SequenceMatcher

# --- Enhanced Keyword-based search with fuzzy matching ---
def fuzzy_match(a, b):
    return SequenceMatcher(None, a, b).ratio()

def keyword_search(query: str, schema: Schema, top_k=5, fuzzy_threshold=0.7):
    query_lower = query.lower()
    results = []
    for table_name, table in schema.tables.items():
        score = max(query_lower in table_name.lower(), fuzzy_match(query_lower, table_name.lower()))
        if score >= fuzzy_threshold:
            results.append({'type': 'table', 'name': table_name, 'score': score, 'text': f'Table: {table_name}'})
        for col_name, col in table.columns.items():
            score = max(query_lower in col_name.lower(), fuzzy_match(query_lower, col_name.lower()))
            if score >= fuzzy_threshold:
                results.append({'type': 'column', 'name': f'{table_name}.{col_name}', 'score': score, 'text': f'Column: {col_name} in {table_name}'})
    return sorted(results, key=lambda x: x['score'], reverse=True)[:top_k]

# --- Enhanced Graph-based retrieval: multi-hop FK/PK traversal ---
def graph_search(query: str, schema: Schema, top_k=5, max_hops=2):
    query_lower = query.lower()
    # Find all tables matching the query
    start_tables = [name for name in schema.tables if query_lower in name.lower()]
    visited = set(start_tables)
    queue = deque([(t, 0) for t in start_tables])
    related = []
    while queue:
        table_name, hops = queue.popleft()
        if hops > max_hops:
            continue
        if hops > 0:
            related.append({'type': 'table', 'name': table_name, 'score': 1.0/(hops+1), 'text': f'Table: {table_name} (distance {hops})', 'graph_distance': hops})
        table = schema.tables.get(table_name)
        if not table:
            continue
        # Traverse FK out
        for fk in table.foreign_keys.values():
            if fk.ref_table not in visited:
                visited.add(fk.ref_table)
                queue.append((fk.ref_table, hops+1))
        # Traverse FK in (reverse)
        for t2 in schema.tables.values():
            for fk in t2.foreign_keys.values():
                if fk.ref_table == table_name and t2.name not in visited:
                    visited.add(t2.name)
                    queue.append((t2.name, hops+1))
    return sorted(related, key=lambda x: x['score'], reverse=True)[:top_k]

# --- Feature extraction and re-ranking ---
def extract_features(vector_results, graph_results, keyword_results):
    # Combine all results, deduplicate by name, and attach features
    all_candidates = {}
    for r in vector_results:
        all_candidates[r['metadata']['name']] = {
            'name': r['metadata']['name'],
            'type': r['metadata']['type'],
            'text': r['text'],
            'vector_score': 1.0 - r['distance'],  # invert distance for similarity
            'graph_score': 0.0,
            'keyword_score': 0.0,
            'graph_distance': None,
            'trace': {'vector': r['distance']}
        }
    for r in graph_results:
        if r['name'] not in all_candidates:
            all_candidates[r['name']] = r
            all_candidates[r['name']]['vector_score'] = 0.0
            all_candidates[r['name']]['graph_score'] = r['score']
            all_candidates[r['name']]['keyword_score'] = 0.0
            all_candidates[r['name']]['graph_distance'] = r.get('graph_distance', None)
            all_candidates[r['name']]['trace'] = {'graph': r.get('graph_distance', 1)}
        else:
            all_candidates[r['name']]['graph_score'] = r['score']
            all_candidates[r['name']]['graph_distance'] = r.get('graph_distance', None)
            all_candidates[r['name']]['trace']['graph'] = r.get('graph_distance', 1)
    for r in keyword_results:
        if r['name'] not in all_candidates:
            all_candidates[r['name']] = r
            all_candidates[r['name']]['vector_score'] = 0.0
            all_candidates[r['name']]['graph_score'] = 0.0
            all_candidates[r['name']]['keyword_score'] = r['score']
            all_candidates[r['name']]['graph_distance'] = None
            all_candidates[r['name']]['trace'] = {'keyword': r['score']}
        else:
            all_candidates[r['name']]['keyword_score'] = r['score']
            all_candidates[r['name']]['trace']['keyword'] = r['score']
    return list(all_candidates.values())

# --- Simple re-ranker (weighted sum, document for future ML model) ---
def rerank(candidates, w_vector=0.6, w_graph=0.2, w_keyword=0.2):
    """
    Combine features into a final score. In future, replace with ML model.
    """
    for c in candidates:
        c['final_score'] = w_vector * c.get('vector_score', 0) + w_graph * c.get('graph_score', 0) + w_keyword * c.get('keyword_score', 0)
    return sorted(candidates, key=lambda x: x['final_score'], reverse=True)

# --- Unified hybrid retrieval interface ---
def hybrid_retrieve(query: str, schema: Schema, top_k=5):
    vector_results = vector_search(query, top_k=top_k)
    graph_results = graph_search(query, schema, top_k=top_k)
    keyword_results = keyword_search(query, schema, top_k=top_k)
    candidates = extract_features(vector_results, graph_results, keyword_results)
    ranked = rerank(candidates)
    # Add trace logging for diagnostics
    for c in ranked:
        c['trace_log'] = c.get('trace', {})
    # Diagnostics stub for RAGxplorer
    diagnostics = {
        'vector_results': vector_results,
        'graph_results': graph_results,
        'keyword_results': keyword_results,
        'final_ranking': ranked[:top_k]
    }
    return ranked[:top_k], diagnostics
