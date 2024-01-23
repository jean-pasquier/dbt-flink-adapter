from dataclasses import dataclass
from typing import Dict, List, Any, Optional

from dbt.events import AdapterLogger

logger = AdapterLogger("Flink")


@dataclass
class SqlGatewayResult:
    rows: List[Dict[str, Any]]
    next_result_url: Optional[str]
    column_names: List[str]
    is_end_of_stream: bool


class SqlGatewayResultParser:
    """Parses data Python dictionary into `SqlGatewayResult`"""

    @staticmethod
    def parse_result(data: Dict[str, Any]) -> SqlGatewayResult:
        columns = data["results"]["columns"]
        next_result_url = data.get("nextResultUri")
        column_names = [c["name"] for c in columns]
        is_end_of_stream = data["resultType"] == "EOS"

        rows = [
            {col: record["fields"][idx] for idx, col in enumerate(column_names)}
            for record in data["results"]["data"]
        ]

        return SqlGatewayResult(rows, next_result_url, column_names, is_end_of_stream)
