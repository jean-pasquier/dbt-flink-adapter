from datetime import datetime
from time import sleep
from typing import Sequence, Tuple, Optional, Any, List

from dbt.events import AdapterLogger

from dbt.adapters.flink.query_hints_parser import QueryHints, QueryHintsParser, QueryMode
from flink.sqlgateway.client import FlinkSqlGatewayClient
from flink.sqlgateway.operation import SqlGatewayOperation
from flink.sqlgateway.result_parser import SqlGatewayResult
from flink.sqlgateway.session import SqlGatewaySession

logger = AdapterLogger("Flink")


class FlinkCursor:
    fetch_interval: float = 0.1

    def __init__(self, session: SqlGatewaySession):
        logger.info("Creating new cursor for session {}".format(session))
        self.session = session

        # Init state
        self.last_operation: Optional[SqlGatewayOperation] = None
        self.result_buffer: List[Tuple] = []
        self.buffered_results_counter: int = 0
        self.last_result: Optional[SqlGatewayResult] = None
        self.last_query_hints: QueryHints = QueryHints()
        self.last_query_start_time: Optional[float] = None

    def cancel(self) -> None:
        pass

    def close(self) -> None:
        pass

    def fetchall(self) -> Sequence[Tuple]:
        if self.last_result is None:
            self._buffer_results()

        if self.last_result is None:
            raise Exception("No result after fetch")

        while not self.last_result.is_end_of_stream and not self._buffered_fetch_max() and not self._exceeded_timeout():
            sleep(self.fetch_interval)
            self._buffer_results()

        result = self.result_buffer
        logger.info(f"Fetched results from Flink: {result}")

        if self.last_query_hints.test_query is True:
            result = self._handle_test_query(result)
        logger.info("Returned results from adapter: {}".format(result))
        self._close()
        self._clean()
        return result

    def _handle_test_query(self, result: List[Tuple]) -> List[Tuple]:
        if self.last_query_hints.mode == QueryMode.STREAMING:
            if len(result) > 0:
                return [result[-1]]
            else:
                return [(0, False, False)]
        return result

    def _close(self):
        if self.last_query_hints.mode == QueryMode.STREAMING:
            self.last_operation.close()

    def _buffered_fetch_max(self):
        return (
            self.last_query_hints.fetch_max is not None
            and self.buffered_results_counter >= self.last_query_hints.fetch_max
        )

    def _exceeded_timeout(self):
        return (
            self.last_query_hints.fetch_timeout_ms is not None
            and self._get_current_timestamp()
            > self.last_query_start_time + self.last_query_hints.fetch_timeout_ms / 1000
        )

    def fetchone(self) -> Optional[Tuple]:
        if len(self.result_buffer) == 0:
            if self.last_result is None or not self.last_result.is_end_of_stream:
                self._buffer_results()
                if len(self.result_buffer) > 0:
                    return self.result_buffer.pop(0)
        self._clean()
        return None

    def execute(self, sql: str, bindings: Optional[Sequence[Any]] = None) -> None:
        logger.debug(f'Preparing statement "{sql}"')
        if bindings is not None:
            sql = sql.format(*[self._convert_binding(binding) for binding in bindings])

        logger.info(f'Executing statement "{sql}"')

        self.last_query_hints = QueryHintsParser.parse(sql)
        self._set_query_mode()
        self.last_query_start_time = self._get_current_timestamp()

        operation = FlinkSqlGatewayClient.execute_statement(self.session, sql)
        self.last_operation = operation

        status = self._wait_till_finished(operation)
        logger.info(f"Statement executed. Status {status}, operation handle: {operation.operation_handle}")
        if status == "ERROR":
            raise Exception("Statement execution failed")

    def _convert_binding(self, binding):
        if isinstance(binding, str):
            return "'{}'".format(binding)
        if isinstance(binding, datetime):
            return "TIMESTAMP '{}'".format(binding)
        return binding

    @property
    def description(self) -> Tuple[Tuple[str], ...]:
        if self.last_result is None:
            self._buffer_results()
        result = []

        if self.last_result is None:
            raise Exception("No result after fetch")

        for column_name in self.last_result.column_names:
            result.append((column_name,))
        return tuple(result)

    def _buffer_results(self):
        if self.last_result is not None:
            next_page = self.last_result.next_result_url
        else:
            next_page = None

        result = self.last_operation.get_result(next_page=next_page)
        for record in result.rows:
            self.buffered_results_counter += 1
            self.result_buffer.append(tuple(record.values()))
            if self._buffered_fetch_max():
                logger.info("Reached fetch max record")
                break
        logger.info(f"Buffered: {len(result.rows)} rows")
        self.last_result = result

    @staticmethod
    def _wait_till_finished(operation_handle: SqlGatewayOperation) -> str:
        status = operation_handle.get_status()
        while status == "RUNNING":
            sleep(FlinkCursor.fetch_interval)
            status = operation_handle.get_status()
        return status

    def _set_query_mode(self):
        runtime_mode = "batch"
        if self.last_query_hints.mode is not None:
            runtime_mode = self.last_query_hints.mode.value
        logger.info("Setting 'execution.runtime-mode' to '{}'".format(runtime_mode))
        FlinkSqlGatewayClient.execute_statement(
            self.session, "SET 'execution.runtime-mode' = '{}'".format(runtime_mode)
        )

    def get_status(self) -> str:
        if self.last_operation is not None:
            return self.last_operation.get_status()
        return "UNKNOWN"

    def _clean(self):
        self.result_buffer = []
        self.last_result = None
        self.last_operation = None
        self.buffered_results_counter = 0

    @staticmethod
    def _get_current_timestamp() -> float:
        return datetime.timestamp(datetime.utcnow())


class FlinkHandler:
    session: SqlGatewaySession

    def __init__(self, session: SqlGatewaySession):
        self.session = session

    def cursor(self) -> FlinkCursor:
        return FlinkCursor(self.session)
