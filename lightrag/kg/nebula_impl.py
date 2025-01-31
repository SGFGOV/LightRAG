import asyncio
from dataclasses import dataclass
from typing import Any, Union, Tuple, List, Dict
from nebula3.gclient.net import ConnectionPool  # type: ignore
from nebula3.Config import Config # type: ignore
from nebula3.gclient.net import Session
from lightrag.utils import logger
from ..base import BaseGraphStorage

@dataclass
class NebulaGraphStorage(BaseGraphStorage):
    def __init__(self, namespace, global_config, embedding_func,**kwargs):
        super().__init__(namespace=namespace, global_config=global_config, embedding_func=embedding_func)
        self._config = Config()
        self._config.max_connection_pool_size = 10
        self._pool = ConnectionPool()
        host = kwargs.get("host", "127.0.0.1")
        port = kwargs.get("port", 9669)
        username = kwargs.get("username", "root")
        password = kwargs.get("password", "nebula")
        db_name = kwargs.get("db_name", "graph_db")
        self._pool.init([(host, port)], self._config)
        self._session = self._pool.get_session(username, password)
        self._DATABASE = db_name

    async def close(self):
        self._session.release()

    async def has_node(self, node_id: str) -> bool:
        query = f"LOOKUP ON `{node_id}` YIELD id(vertex) AS vid"
        result = self._session.execute(query)
        return result.is_succeeded() and result.row_size() > 0

    async def has_edge(self, source_node_id: str, target_node_id: str) -> bool:
        query = f"GO FROM '{source_node_id}' OVER * YIELD dst(edge) AS dst WHERE dst == '{target_node_id}'"
        result = self._session.execute(query)
        return result.is_succeeded() and result.row_size() > 0

    async def get_node(self, node_id: str) -> Union[dict, None]:
        query = f"FETCH PROP ON * '{node_id}'"
        result = self._session.execute(query)
        if result.is_succeeded() and result.row_size() > 0:
            return dict(result.row_values(0))
        return None

    async def node_degree(self, node_id: str) -> int:
        query = f"GO FROM '{node_id}' OVER * YIELD count(*) AS degree"
        result = self._session.execute(query)
        if result.is_succeeded() and result.row_size() > 0:
            return result.row_values(0)[0].as_int()
        return 0

    async def edge_degree(self, src_id: str, tgt_id: str) -> int:
        src_degree = await self.node_degree(src_id)
        tgt_degree = await self.node_degree(tgt_id)
        return src_degree + tgt_degree

    async def get_edge(self, source_node_id: str, target_node_id: str) -> Union[dict, None]:
        query = f"GO FROM '{source_node_id}' TO '{target_node_id}' OVER * YIELD properties(edge)"
        result = self._session.execute(query)
        if result.is_succeeded() and result.row_size() > 0:
            return dict(result.row_values(0))
        return None

    async def get_node_edges(self, source_node_id: str) -> List[Tuple[str, str]]:
        query = f"GO FROM '{source_node_id}' OVER * YIELD dst(edge) AS dst"
        result = self._session.execute(query)
        edges = []
        if result.is_succeeded():
            for row in result.rows():
                edges.append((source_node_id, row.values[0].as_string()))
        return edges

    async def upsert_node(self, node_id: str, node_data: Dict[str, Any]):
        properties = ', '.join([f"{k}: '{v}'" for k, v in node_data.items()])
        query = f"INSERT VERTEX `{node_id}` ({properties})"
        result = self._session.execute(query)
        if not result.is_succeeded():
            logger.error(f"Error during upsert: {result.error_msg()}")

    async def upsert_edge(self, source_node_id: str, target_node_id: str, edge_data: Dict[str, Any]):
        properties = ', '.join([f"{k}: '{v}'" for k, v in edge_data.items()])
        query = f"INSERT EDGE `{source_node_id}` -> `{target_node_id}` ({properties})"
        result = self._session.execute(query)
        if not result.is_succeeded():
            logger.error(f"Error during edge upsert: {result.error_msg()}")

    async def get_knowledge_graph(self, node_label: str, max_depth: int = 5) -> Dict[str, List[Dict]]:
        result: Dict[str, List[Dict]] = {"nodes": [], "edges": []}
        query = f"GO FROM '{node_label}' OVER * YIELD dst(edge) AS dst"
        result_set = self._session.execute(query)
        if result_set.is_succeeded():
            for row in result_set.rows():
                node = row.values[0].as_string()
                result["nodes"].append({"id": node})
                result["edges"].append({"source": node_label, "target": node})
        return result

    async def get_all_labels(self) -> List[str]:
        query = "SHOW TAGS"
        result = self._session.execute(query)
        labels = []
        if result.is_succeeded():
            for row in result.rows():
                labels.append(row.values[0].as_string())
        return labels
