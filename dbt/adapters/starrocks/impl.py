#! /usr/bin/python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
import re
import time
import uuid
from concurrent.futures import Future
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set, FrozenSet, Tuple
from typing_extensions import TypeAlias

import agate
import dbt.exceptions
from mysql.connector import Error as MySQLError
from dbt.adapters.base import available
from dbt.adapters.base.impl import _expect_row_value, catch_as_completed
from dbt.adapters.base.relation import InformationSchema
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.protocol import AdapterConfig
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.sql.impl import LIST_RELATIONS_MACRO_NAME, LIST_SCHEMAS_MACRO_NAME
from dbt_common.clients.agate_helper import table_from_rows
from dbt_common.utils import executor
from typing_extensions import override

from dbt.adapters.starrocks.column import StarRocksColumn
from dbt.adapters.starrocks.connections import StarRocksConnectionManager
from dbt.adapters.starrocks.relation import StarRocksRelation


logger = AdapterLogger("starrocks")

SQLQueryResult: TypeAlias = Tuple[AdapterResponse, "agate.Table"]


SUBMIT_TASK_TEMPLATE = "submit /*+set_var(query_timeout={timeout})*/ task {task_id} as {sql}"
POLL_TASK_TEMPLATE = "select * from information_schema.task_runs where task_name = '{task_id}'"
MIN_ASYNC_POLL_SECONDS = 1800
TRANSIENT_MYSQL_CONNECTION_ERRORS = {2002, 2003, 2006, 2013, 2055}
MV_TASK_STATES = {"MERGED", "SKIPPED"}
PRIVATE_BACKUP_CTAS = re.compile(
    r'^\s*create\s+table\s+(?:`[^`]+`\.)*`[^`]*__dbt_backup_[0-9a-f]{32}`(?=\s|\()', re.IGNORECASE
)

class StarRocksConfig(AdapterConfig):
    engine: Optional[str] = None
    table_type: Optional[str] = None  # DUPLICATE/PRIMARY/UNIQUE/AGGREGATE
    keys: Optional[List[str]] = None
    partition_by: Optional[List[str]] = None
    partition_by_init: Optional[List[str]] = None
    distributed_by: Optional[List[str]] = None
    buckets: Optional[int] = None
    properties: Optional[Dict[str, str]] = None
    microbatch_use_dynamic_overwrite: Optional[bool] = None


class StarRocksAdapter(SQLAdapter):
    ConnectionManager = StarRocksConnectionManager
    Relation = StarRocksRelation
    AdapterSpecificConfigs = StarRocksConfig
    Column = StarRocksColumn
    
    _running_tasks: Dict[str, str] = {}

    @staticmethod
    def _compute_poll_delay(attempt: int, poll_interval: int = 1, poll_factor: float = 2.0, poll_max_delay: int = 600) -> float:
        """
        Compute the polling delay using configurable exponential backoff.

        :param attempt: The current attempt number (starting from 1).
        :param poll_interval: Base delay in seconds.
        :param poll_factor: Growth multiplier.
        :param poll_max_delay: Maximum delay cap in seconds.
        :return: The computed delay in seconds.
        """
        return min(poll_max_delay, poll_interval * (poll_factor ** attempt))

    @staticmethod
    def _is_submittable_etl(sql: str) -> bool:
        """
        Evaluates if the SQL statement matches supported StarRocks ETL patterns.

        Supported patterns:
        - CREATE TABLE ... SELECT
        - INSERT INTO/OVERWRITE
        - CACHE SELECT

        Reference: https://docs.starrocks.io/docs/sql-reference/sql-statements/loading_unloading/ETL/SUBMIT_TASK/

        :param sql: The SQL statement to evaluate.
        :return: True or False depending on whether the SQL statement is a submittable ETL.
        """
        # Remove newlines and normalize whitespace
        sql_clean = sql.strip().replace('\n', '')
        sql_clean = re.sub(r'\s+', ' ', sql_clean).strip().lower()

        # Note: # Supported ETL patterns from StarRocks documentation
        # https://docs.starrocks.io/docs/sql-reference/sql-statements/loading_unloading/ETL/SUBMIT_TASK/
        #
        # The goal here is to soft-match the ETL patterns for dbt-generated sql queries.
        # It is not intended to be exhaustive nor to validate SQL statement, this will be left to the engine.
        suitable_etl_patterns = [
            r'^create\s+table.*select',
            r'^insert\s+(into|overwrite)\b',
            r'^cache\s+select\b'
        ]

        return any(re.search(pattern, sql_clean) for pattern in suitable_etl_patterns)

    def _cancel_task(self, task_id: str) -> None:
        """
        Cancel running task.

        :param task_id: The task ID to cancel.
        """
        try:
            _cancel_sql = f"DROP TASK `{task_id}`"
            super().execute(sql=_cancel_sql, fetch=False)
        except Exception as e:
            logger.warning(f"Failed to cancel task [{task_id}]: {e}")

    @staticmethod
    def _is_transient_poll_error(error: Optional[BaseException]) -> bool:
        # The driver error is normally wrapped by the dbt connection manager.
        while error is not None:
            if isinstance(error, MySQLError) and error.errno in TRANSIENT_MYSQL_CONNECTION_ERRORS:
                return True
            message = str(error).lower()
            if "_statistics_.task_run_history" in message and "repoexecutorexecute sql failed" in message:
                return True
            error = error.__cause__ or error.__context__
        return False

    def _poll_for_complete_task(
        self, task_id: str, pending_file: Optional[Path] = None, private_backup: bool = False
    ) -> SQLQueryResult:
        """Wait for explicit completion of this task before allowing the next statement."""
        poll_sql = POLL_TASK_TEMPLATE.format(task_id=task_id)
        connection = self.connections.get_if_exists()
        credentials = self.config.credentials
        poll_timeout = max(MIN_ASYNC_POLL_SECONDS, (credentials.async_query_timeout or 0) + 60)
        deadline = time.monotonic() + poll_timeout
        attempt = 1
        detail = "task status unavailable"

        while time.monotonic() < deadline:
            try:
                self.connections.open(connection)
                response, rows = super().execute(sql=poll_sql, fetch=True, limit=1)
            except Exception as error:
                if not self._is_transient_poll_error(error):
                    raise dbt.exceptions.DbtRuntimeError(
                        f"StarRocks task [{task_id}] unresolved: status poll failed: {error}"
                    ) from error
                detail = f"transient status poll error: {error}"
            else:
                if response.code != "SUCCESS":
                    raise dbt.exceptions.DbtRuntimeError(
                        f"StarRocks task [{task_id}] unresolved: poll returned [{response.code}]"
                    )
                row = rows[0] if rows else {}
                status = row.get("STATE")
                detail = (
                    f"state={status}, progress={row.get('PROGRESS', 'unknown')}"
                    if status else "task status missing"
                )
                if time.monotonic() >= deadline:
                    break
                if status in MV_TASK_STATES:
                    raise dbt.exceptions.DbtRuntimeError(
                        f"StarRocks task [{task_id}] unresolved: unexpected ETL state [{status}]"
                    )
                if status in {"SUCCESS", "FAILED"}:
                    if pending_file is not None:
                        pending_file.unlink()
                    if status == "FAILED":
                        retry_hint = " (isolated full-refresh backup)" if private_backup else ""
                        raise dbt.exceptions.DbtRuntimeError(
                            f"StarRocks task [{task_id}] finished with [{status}]{retry_hint}: "
                            f"{row.get('ERROR_MESSAGE') or 'no error message'}"
                        )
                    logger.info(f"Task [{task_id}] finished with status [{status}]")
                    return response, rows

            self.connections.close(connection)
            delay = min(
                self._compute_poll_delay(
                    attempt, credentials.poll_interval, credentials.poll_factor, credentials.poll_max_delay
                ),
                deadline - time.monotonic(),
            )
            if delay <= 0:
                break
            logger.info(f"Task [{task_id}] {detail}. Waiting {delay:g} seconds...")
            time.sleep(delay)
            attempt += 1

        raise dbt.exceptions.DbtRuntimeError(
            f"StarRocks task [{task_id}] unresolved after {poll_timeout}s: {detail}. "
            "Check the server-side task before retrying or cleaning up."
        )

    def _execute_async_task(self, sql: str, auto_begin: bool = False, fetch: bool = False, limit: Optional[int] = None) -> SQLQueryResult:
        """
        Executes an SQL statement asynchronously and wait for completion.

        :param str sql: The sql to execute.
        :param bool auto_begin: If set, and dbt is not currently inside a
            transaction, automatically begin one.
        :param bool fetch: If set, fetch results.
        :param Optional[int] limit: If set, only fetch n number of rows
        :return: A tuple of the query status and results (empty if fetch=False).
        :rtype: Tuple[AdapterResponse, "agate.Table"]
        """
        _task_id = str(uuid.uuid4()).replace('-', '')
        _timeout = self.config.credentials.async_query_timeout

        _submit_sql = SUBMIT_TASK_TEMPLATE.format(
            timeout=_timeout,
            task_id=_task_id,
            sql=sql,
        )

        _connection = self.connections.get_if_exists()
        # A retry in a second dbt process needs evidence even if the first is killed.
        pending_dir = os.environ.get("DBT_STARROCKS_PENDING_TASK_DIR")
        pending_file = Path(pending_dir) / _task_id if pending_dir else None
        if pending_file is not None:
            pending_file.touch(exist_ok=False)
        if _connection:
            self._running_tasks[_connection.name] = _task_id

        try:
            logger.info(f"Submitting StarRocks task [{_task_id}]")
            try:
                response, _ = super().execute(
                    sql=_submit_sql, auto_begin=auto_begin, fetch=fetch, limit=limit
                )
            except Exception as error:
                raise dbt.exceptions.DbtRuntimeError(
                    f"StarRocks task [{_task_id}] unresolved: submission failed: {error}"
                ) from error
            if response.code != "SUCCESS":
                raise dbt.exceptions.DbtRuntimeError(
                    f"StarRocks task [{_task_id}] unresolved: submit returned [{response.code}]"
                )
            return self._poll_for_complete_task(_task_id, pending_file, bool(PRIVATE_BACKUP_CTAS.match(sql)))
        finally:
            if _connection and _connection.name in self._running_tasks:
                del self._running_tasks[_connection.name]

    @override
    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: Optional[int] = None,
    ) -> SQLQueryResult:
        """
        Execute a SQL statement against the database.

        This method overrides the base class method to support submittable ETL.

        Only if the adapter is configured to be async:
        - it will intercept the original SQL statement and prefix it with a `submit task` statement if the SQL is a submittable ETL
        - it will then poll the task status until the task is completed using basic exponential backoff

        :param str sql: The sql to execute.
        :param bool auto_begin: If set, and dbt is not currently inside a
            transaction, automatically begin one.
        :param bool fetch: If set, fetch results.
        :param Optional[int] limit: If set, only fetch n number of rows
        :return: A tuple of the query status and results (empty if fetch=False).
        :rtype: Tuple[AdapterResponse, "agate.Table"]
        """
        if not self.config.credentials.is_async or not self._is_submittable_etl(sql):
            return super().execute(sql=sql, auto_begin=auto_begin, fetch=fetch, limit=limit)
        return self._execute_async_task(sql=sql, auto_begin=auto_begin, fetch=fetch, limit=limit)

    def cancel_open_connections(self):
        """Cancel all running tasks when dbt is interrupted."""
        logger.warning(f"Canceling {len(self._running_tasks)} running StarRocks tasks...")
        for conn_name, task_id in list(self._running_tasks.items()):
            logger.warning(f"Canceling task [{task_id}] on connection [{conn_name}]")
            self._cancel_task(task_id)
        return super().cancel_open_connections()

    @classmethod
    def date_function(cls) -> str:
        return "current_date()"

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "datetime"

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "string"

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        # The base SQLAdapter returns "float8" for decimal columns, which is a
        # Postgres alias StarRocks doesn't accept (its parser treats the "8" as
        # an expected precision and complains about the missing "("). Use the
        # standard StarRocks type instead.
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "double" if decimals else "bigint"

    def quote(self, identifier):
        return "`{}`".format(identifier)

    def check_schema_exists(self, database, schema):
        results = self.execute_macro(
            LIST_SCHEMAS_MACRO_NAME, kwargs={"database": database}
        )

        exists = True if schema in [row[0] for row in results] else False
        return exists

    def get_relation(self, database: Optional[str], schema: str, identifier: str):
        if not self.Relation.get_default_include_policy().database:
            database = None

        return super().get_relation(database, schema, identifier)

    def list_relations_without_caching(
        self, schema_relation: StarRocksRelation
    ) -> List[StarRocksRelation]:
        kwargs = {"schema_relation": schema_relation}
        results = self.execute_macro(LIST_RELATIONS_MACRO_NAME, kwargs=kwargs)

        relations = []
        for row in results:
            if len(row) != 4:
                raise dbt.exceptions.DbtRuntimeError(
                    f"Invalid value from 'show table extended ...', "
                    f"got {len(row)} values, expected 4"
                )
            _database, name, schema, type_info = row
            relation = self.Relation.create(
                database=None,
                schema=schema,
                identifier=name,
                type=self.Relation.get_relation_type(type_info),
            )
            relations.append(relation)

        return relations

    def get_catalog(self, manifest, used_schemas):
        schema_map = self._get_catalog_schemas(manifest)
        if len(schema_map) > 1:
            dbt.exceptions.CompilationError(
                f"Expected only one database in get_catalog, found "
                f"{list(schema_map)}"
            )

        with executor(self.config) as tpe:
            futures: List[Future[agate.Table]] = []
            for info, schemas in schema_map.items():
                for schema in schemas:
                    for d, s in used_schemas:
                        if schema.lower() == s.lower():
                            schema = s
                            break
                    futures.append(
                        tpe.submit_connected(
                            self,
                            schema,
                            self._get_one_catalog,
                            info,
                            [schema],
                            used_schemas,
                        )
                    )
            catalogs, exceptions = catch_as_completed(futures)
        return catalogs, exceptions

    @classmethod
    def _catalog_filter_table(
        cls, table: "agate.Table", used_schemas: FrozenSet[Tuple[str, str]]
    ) -> agate.Table:
        table = table_from_rows(
            table.rows,
            table.column_names,
            text_only_columns=["table_schema", "table_name"],
        )
        return table.where(_catalog_filter_schemas(used_schemas))

    @available
    def is_before_version(self, version: str) -> bool:
        conn = self.connections.get_if_exists()
        if conn:
            server_version = conn.handle.server_version
            server_version_tuple = tuple(server_version)
            version_detail_tuple = tuple(
                int(part) for part in version.split(".") if part.isdigit())
            if version_detail_tuple > server_version_tuple:
                return True
        return False

    @available
    def current_version(self):
        conn = self.connections.get_if_exists()
        if conn:
            server_version = conn.handle.server_version
            if server_version != (999, 999, 999):
                return "{}.{}.{}".format(server_version[0], server_version[1], server_version[2])
        return 'UNKNOWN'

    def _get_one_catalog(
        self,
        information_schema: InformationSchema,
        schemas: Set[str],
        used_schemas: FrozenSet[Tuple[str, str]],
    ) -> agate.Table:
        if len(schemas) != 1:
            dbt.exceptions.CompilationError(
                f"Expected only one schema in StarRocks _get_one_catalog, found "
                f"{schemas}"
            )

        return super()._get_one_catalog(information_schema, schemas, used_schemas)

    @override
    def valid_incremental_strategies(self):
        return ["default", "insert_overwrite", "dynamic_overwrite", "microbatch"]


def _catalog_filter_schemas(used_schemas: FrozenSet[Tuple[str, str]]) -> Callable[[agate.Row], bool]:
    schemas = frozenset((None, s.lower()) for d, s in used_schemas)

    def test(row: agate.Row) -> bool:
        table_database = _expect_row_value("table_database", row)
        table_schema = _expect_row_value("table_schema", row)
        if table_schema is None:
            return False
        return (table_database, table_schema.lower()) in schemas

    return test
