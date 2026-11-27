from contextlib import ExitStack
from hashlib import md5
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock, patch

from dbt.adapters.sql import SQLAdapter
from dbt.exceptions import DbtRuntimeError
from jinja2 import Environment
from mysql.connector import OperationalError

from dbt.adapters.starrocks import impl
from dbt.adapters.starrocks.impl import StarRocksAdapter
from dbt.adapters.starrocks.connections import StarRocksConnectionManager
from dbt.adapters.sql import SQLConnectionManager
from dbt_common.exceptions import DbtDatabaseError


class TestAsyncTaskLifecycle(TestCase):
    ctas = "create table sample as select 1"

    def setUp(self):
        self.adapter = object.__new__(StarRocksAdapter)
        self.adapter.config = SimpleNamespace(credentials=SimpleNamespace(
            is_async=True, async_query_timeout=600, poll_interval=5, poll_factor=1.5, poll_max_delay=30
        ))
        self.adapter.connections = Mock()
        self.adapter.connections.get_if_exists.return_value = SimpleNamespace(name="sample")
        self.adapter._running_tasks = {}
        self.response = SimpleNamespace(code="SUCCESS")
        self.clock = SimpleNamespace(now=0)
        self.sql = []
        self.poll = Mock(return_value=self.poll_result("SUCCESS"))

        stack = ExitStack()
        self.addCleanup(stack.close)
        stack.enter_context(patch.object(SQLAdapter, "execute", self.execute_sql))
        stack.enter_context(patch.object(impl, "logger"))
        stack.enter_context(patch.object(impl, "time", SimpleNamespace(
            monotonic=lambda: self.clock.now, sleep=self.sleep
        )))
        self.pending = Path(stack.enter_context(TemporaryDirectory()))
        stack.enter_context(patch.dict("os.environ", {"DBT_STARROCKS_PENDING_TASK_DIR": str(self.pending)}))

    def sleep(self, delay):
        self.clock.now += delay

    def execute_sql(self, sql, **kwargs):
        self.sql.append(sql)
        if sql.startswith("submit "):
            task_id = sql.split(" task ", 1)[1].split()[0]
            self.assertTrue((self.pending / task_id).is_file())
        if sql.startswith("select * from information_schema.task_runs"):
            return self.poll()
        return self.response, []

    def poll_result(self, state=None, error=None):
        return self.response, [{"STATE": state, "ERROR_MESSAGE": error}] if state else []

    def assert_single_submission(self):
        submissions = [sql for sql in self.sql if sql.startswith("submit ")]
        polls = [sql for sql in self.sql if sql.startswith("select * from information_schema.task_runs")]
        self.assertEqual(len(submissions), 1)
        task_id = submissions[0].split(" task ", 1)[1].split()[0]
        self.assertEqual(set(polls), {impl.POLL_TASK_TEMPLATE.format(task_id=task_id)})
        return task_id

    def test_success(self):
        self.assertEqual(self.adapter.execute(self.ctas), self.poll_result("SUCCESS"))
        self.assert_single_submission()
        self.assertEqual(list(self.pending.iterdir()), [])

    def test_empty_and_unknown_status_are_retried(self):
        self.poll.side_effect = [
            self.poll_result(), self.poll_result("unknown"), self.poll_result("SUCCESS")
        ]
        self.adapter.execute(self.ctas)
        self.assert_single_submission()
        self.assertEqual(self.poll.call_count, 3)
        self.assertEqual(self.adapter.connections.open.call_count, 3)
        self.assertEqual(self.adapter.connections.close.call_count, 2)

    def test_transient_archive_and_connection_errors_are_retried(self):
        self.poll.side_effect = [
            DbtRuntimeError("RepoExecutorexecute sql failed: SELECT ... FROM _statistics_.task_run_history"),
            OperationalError("Lost connection", errno=2013),
            self.poll_result("SUCCESS"),
        ]
        self.adapter.execute(self.ctas)
        self.assert_single_submission()
        self.assertEqual(list(self.pending.iterdir()), [])

    def test_terminal_failure_reports_the_error_and_clears_pending(self):
        self.poll.return_value = self.poll_result("FAILED", "backend unavailable")
        with self.assertRaisesRegex(DbtRuntimeError, r"\[FAILED\]: backend unavailable"):
            self.adapter.execute(self.ctas)
        self.assert_single_submission()
        self.assertEqual(list(self.pending.iterdir()), [])

    def test_only_private_backup_ctas_has_a_retryable_failure_marker(self):
        self.poll.return_value = self.poll_result("FAILED", "synthetic failure")
        backup = "sample__dbt_backup_" + "a" * 32
        with self.assertRaisesRegex(DbtRuntimeError, r"\[FAILED\] \(isolated full-refresh backup\)"):
            self.adapter.execute(f"create table `marts`.`{backup}` PRIMARY KEY (id) as select 1 as id")
        with self.assertRaisesRegex(DbtRuntimeError, r"\[FAILED\]: synthetic failure"):
            self.adapter.execute("insert into `marts`.`sample` values (1)")
        self.assertEqual(list(self.pending.iterdir()), [])

    def test_mv_states_keep_pending_etl_evidence(self):
        for status in ("MERGED", "SKIPPED"):
            with self.subTest(status=status):
                self.sql.clear()
                self.poll.return_value = self.poll_result(status)
                with self.assertRaisesRegex(DbtRuntimeError, rf"unexpected ETL state \[{status}\]"):
                    self.adapter.execute(self.ctas)
                self.assertTrue((self.pending / self.assert_single_submission()).is_file())

    def test_persistent_empty_status_leaves_pending_task_for_reconciliation(self):
        self.poll.return_value = self.poll_result()
        with self.assertRaisesRegex(DbtRuntimeError, "unresolved after 1800s: task status missing"):
            self.adapter.execute(self.ctas)
        task_id = self.assert_single_submission()
        self.assertTrue((self.pending / task_id).exists())
        self.assertEqual(self.clock.now, 1800)

    def test_permission_error_is_not_retried(self):
        self.poll.side_effect = DbtRuntimeError("Access denied")
        with self.assertRaisesRegex(DbtRuntimeError, r"unresolved: status poll failed:[\s\S]*Access denied"):
            self.adapter.execute(self.ctas)
        self.assertEqual(self.poll.call_count, 1)
        self.assertTrue((self.pending / self.assert_single_submission()).exists())

    def render_full_refresh(self, invocation_id):
        # Exercise the actual materialization branch with adapter calls wired to our SQL mock.
        path = Path(__file__).resolve().parents[2] / (
            "dbt/include/starrocks/macros/materializations/models/incremental.sql"
        )
        branch = path.read_text(encoding="utf-8").split("{%- elif full_refresh_mode -%}", 1)[1]
        branch = branch.split("{% else %}", 1)[0]
        relation = SimpleNamespace(identifier="sample")
        relation.incorporate = lambda path: path["identifier"]
        env = Environment(extensions=["jinja2.ext.do"])

        def statement(name, caller):
            self.adapter.execute(caller().strip())
            return ""

        env.from_string(branch).render(
            target={"is_async": True}, target_relation="sample", existing_relation=relation,
            invocation_id=invocation_id, model=SimpleNamespace(unique_id="model.sample"),
            local_md5=lambda text: md5(text.encode()).hexdigest(),
            statement=statement,
            compiled_code="select 1", is_external=False,
            starrocks__create_table_as=lambda _, name, code, external: f"create table {name} as {code}",
            starrocks__exchange_relation=lambda target, backup: self.adapter.execute(
                f"alter table {target} swap with {backup}"
            ),
            drop_relation_if_exists=lambda name: self.adapter.execute(f"drop table if exists {name}"),
            adapter=SimpleNamespace(drop_relation=lambda name: self.adapter.execute(f"drop table {name}")),
        )

    def test_running_task_never_swaps_or_drops_and_later_invocation_uses_new_backup(self):
        self.poll.return_value = self.poll_result("RUNNING")
        with self.assertRaisesRegex(DbtRuntimeError, "unresolved after 1800s: state=RUNNING"):
            self.render_full_refresh("first")
        first_task = self.assert_single_submission()
        first_backup = self.sql[0].split("create table ", 1)[1].split()[0]
        self.assertTrue(all(sql.startswith(("submit ", "select ")) for sql in self.sql))

        self.sql.clear()
        self.poll.side_effect = [self.poll_result(), self.poll_result("SUCCESS")]
        self.render_full_refresh("second")
        second_task = self.assert_single_submission()
        second_backup = self.sql[0].split("create table ", 1)[1].split()[0]
        self.assertNotEqual(first_task, second_task)
        self.assertNotEqual(first_backup, second_backup)
        self.assertEqual(self.sql[-2:], [
            f"alter table sample swap with {second_backup}", f"drop table {second_backup}"
        ])
        self.assertTrue(all(first_backup not in sql for sql in self.sql))
        self.assertEqual([path.name for path in self.pending.iterdir()], [first_task])

    def test_interrupted_status_read_leaves_pending_task(self):
        self.poll.side_effect = KeyboardInterrupt
        with self.assertRaises(KeyboardInterrupt):
            self.adapter.execute(self.ctas)
        self.assertTrue((self.pending / self.assert_single_submission()).exists())

    def test_ambiguous_submit_failure_leaves_pending_task_and_never_resubmits(self):
        def lose_submit_response(adapter, sql, **kwargs):
            self.sql.append(sql)
            raise OperationalError("Lost connection", errno=2013)

        with patch.object(SQLAdapter, "execute", lose_submit_response):
            with self.assertRaisesRegex(DbtRuntimeError, "unresolved: submission failed"):
                self.adapter.execute(self.ctas)
        self.assertEqual(len(self.sql), 1)
        task_id = self.sql[0].split(" task ", 1)[1].split()[0]
        self.assertTrue((self.pending / task_id).exists())
        self.poll.assert_not_called()

    def test_late_success_cannot_publish(self):
        def late_poll():
            self.clock.now = 1801
            return self.poll_result("SUCCESS")

        self.poll.side_effect = late_poll
        with self.assertRaisesRegex(DbtRuntimeError, "unresolved after 1800s"):
            self.adapter.execute(self.ctas)
        self.assertTrue((self.pending / self.assert_single_submission()).exists())

    def test_sync_still_executes_sql_directly(self):
        self.adapter.config.credentials.is_async = False
        self.assertEqual(self.adapter.execute(self.ctas), (self.response, []))
        self.assertEqual(self.sql, [self.ctas])
        self.poll.assert_not_called()


class TestStartupWriteEvidence(TestCase):
    def setUp(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.evidence = Path(temporary.name)
        environment = patch.dict("os.environ", {"DBT_STARROCKS_WRITE_EVIDENCE_DIR": temporary.name})
        environment.start()
        self.addCleanup(environment.stop)
        self.manager = object.__new__(StarRocksConnectionManager)
        macro = Path(__file__).resolve().parents[2] / "dbt/include/starrocks/macros/adapters/metadata.sql"
        self.catalog = (
            macro.read_text().split("fetch_result=True) %}", 1)[1].split("{% endcall %}", 1)[0]
            .replace("{{ schema_relation.schema }}", "marts")
        )

    def test_exact_catalog_planner_failure_records_positive_evidence(self):
        sql = '/* {"app": "dbt"} */ ' + self.catalog
        with patch.object(SQLConnectionManager, "add_query", side_effect=DbtDatabaseError(
            "1064: StarRocks planner use long time 30298 ms in logical phase"
        )):
            with self.assertRaises(DbtDatabaseError):
                self.manager.add_query(sql)
        self.assertTrue((self.evidence / "catalog-planner-failed").is_file())
        self.assertFalse((self.evidence / "write-possible").exists())

    def test_schema_listing_is_read_only_but_other_sql_latches_before_dispatch(self):
        with patch.object(SQLConnectionManager, "add_query"):
            self.manager.add_query("SELECT DISTINCT schema_name FROM information_schema.schemata")
        self.assertFalse((self.evidence / "write-possible").exists())

        def check_before_dispatch(*args, **kwargs):
            self.assertTrue((self.evidence / "write-possible").is_file())
            raise DbtDatabaseError("lost connection")

        with patch.object(SQLConnectionManager, "add_query", side_effect=check_before_dispatch):
            with self.assertRaises(DbtDatabaseError):
                self.manager.add_query(self.catalog + "; DROP TABLE marts.unsafe")
        self.assertFalse((self.evidence / "catalog-planner-failed").exists())

    def test_direct_create_database_latches_before_dispatch(self):
        handle = Mock()
        connection = SimpleNamespace(state="init", credentials=SimpleNamespace(
            host="starrocks", username="dbt", password="", catalog="default_catalog",
            schema="marts", auth_plugin="", port=9030, use_pure=None, version="3.5.21",
        ))

        def check_before_dispatch(sql):
            self.assertEqual(sql, "CREATE DATABASE default_catalog.marts")
            self.assertTrue((self.evidence / "write-possible").is_file())

        handle.cursor.return_value.execute.side_effect = check_before_dispatch
        with patch("mysql.connector.connect", side_effect=[OperationalError("missing schema"), handle, handle]):
            self.manager.open(connection)
