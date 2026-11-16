"""状态快照批量清理与状态快照数据清单的行为校验。

运行：backend/.venv/bin/python -m unittest discover -s backend/tests -v
"""
from __future__ import annotations

import threading
import unittest

from fastapi.testclient import TestClient

from app.main import app
from app.services import state as state_service
from app.store import store


def reset_store() -> None:
    """每个用例都从同一份种子数据出发，避免用例之间互相污染。"""
    from app.seed import SEED_ROWS

    store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}


class StateCleanupTest(unittest.TestCase):
    def setUp(self) -> None:
        reset_store()
        self.client = TestClient(app)

    def cleanup(self, values: dict) -> dict:
        response = self.client.post("/api/state/manifests/cleanup", json={"values": values})
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    def test_acceptance_sample_protects_referenced_snapshot(self) -> None:
        """STAT-0001、STAT-0002 与 CHEC-0003 一起清理时只删除无引用目录。"""
        payload = self.cleanup({
            "state_ids": ["STAT-0001", "STAT-0002"],
            "检查点": ["CHEC-0003"],
        })
        self.assertTrue(payload["ok"])
        entry = payload["entry"]
        self.assertEqual(entry["受保护大小"], "1.2GB")
        self.assertEqual(entry["受保护字节"], 1288490189)
        self.assertEqual(entry["整组进度"]["已保留"], 1)
        self.assertEqual(entry["整组进度"]["已删除"], 1)
        self.assertEqual(entry["失败项"], [])

        deleted = [item["路径"] for item in entry["删除结果"]]
        self.assertEqual(deleted, ["s3://stream-state/prod/orders-archive/ckpt-099/"])
        protected = [item["路径"] for item in entry["保护项"]]
        self.assertIn("s3://stream-state/prod/orders/ckpt-100/", protected)
        self.assertIn("s3://stream-state/prod/orders/", protected)

        states = {row["配置编号"]: row for row in self.client.get("/api/state").json()["items"]}
        self.assertEqual(states["STAT-0001"]["状态大小"], "1.2GB")
        self.assertEqual(states["STAT-0001"]["状态大小字节"], 1288490189)
        self.assertEqual(states["STAT-0002"]["状态大小"], "0B")

        checkpoints = {row["记录编号"]: row for row in self.client.get("/api/checkpoint").json()["items"]}
        self.assertTrue(checkpoints["CHEC-0001"]["可恢复"])
        self.assertTrue(checkpoints["CHEC-0003"]["可恢复"])
        self.assertEqual(checkpoints["CHEC-0003"]["关联清单编号"], "SNAP-0001")

    def test_manifest_records_deletions_protection_and_failures(self) -> None:
        payload = self.cleanup({
            "state_ids": ["STAT-0001", "STAT-0002", "STAT-0003"],
            "失败路径": ["s3://stream-state/prod/orders-archive/ckpt-099/"],
        })
        entry = payload["entry"]
        self.assertEqual(entry["受保护大小"], "1.2GB")
        self.assertEqual(len(entry["失败项"]), 1)
        self.assertEqual(entry["失败项"][0]["路径"], "s3://stream-state/prod/orders-archive/ckpt-099/")
        self.assertTrue(entry["失败项"][0]["保留目录"])
        self.assertEqual(entry["整组进度"]["失败"], 1)
        self.assertFalse(entry["整组进度"]["完成"])

        manifest = self.client.get("/api/state/manifests/2").json()
        self.assertEqual(manifest["状态"], "部分完成")
        self.assertEqual([item["路径"] for item in manifest["失败项"]],
                         ["s3://stream-state/prod/orders-archive/ckpt-099/"])
        self.assertEqual(manifest["受保护大小"], "0B")

    def test_duplicate_submission_returns_single_manifest_set(self) -> None:
        first = self.cleanup({"state_ids": ["STAT-0002"], "提交编号": "req-1"})
        second = self.cleanup({"state_ids": ["STAT-0002"], "提交编号": "req-1"})
        self.assertFalse(first["entry"]["重复提交"])
        self.assertTrue(second["entry"]["重复提交"])
        self.assertEqual(len(second["entry"]["清单列表"]), 1)
        self.assertEqual(len(second["entry"]["删除结果"]), 1)
        manifests = self.client.get("/api/state/manifests").json()
        # 种子里的 SNAP-0001 加上本次清理新建的一条，重复提交没有再建清单。
        self.assertEqual(manifests["total"], 2)

    def test_same_path_concurrent_cleanup_succeeds_once(self) -> None:
        service = state_service.StateService()
        barrier = threading.Barrier(2)
        results: list[dict] = []

        def run(tag: str) -> None:
            barrier.wait()
            results.append(service.cleanup_snapshots({"state_ids": ["STAT-0003"], "提交编号": tag}))

        threads = [threading.Thread(target=run, args=(f"req-{index}",)) for index in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        deletions = [
            item for result in results for item in result["批量处理结果"] if item["动作"] == "删除"
        ]
        skips = [
            item for result in results for item in result["批量处理结果"] if item["动作"] == "跳过"
        ]
        self.assertEqual(len(deletions), 1)
        self.assertEqual(len(skips), 1)

    def test_failed_paths_keep_original_directory_and_retry_only_unfinished(self) -> None:
        self.cleanup({
            "state_ids": ["STAT-0003"],
            "失败路径": ["s3://stream-state/staging/metrics/"],
        })
        manifest = self.client.get("/api/state/manifests/2").json()
        self.assertEqual(len(manifest["失败项"]), 1)
        states = {row["配置编号"]: row for row in self.client.get("/api/state").json()["items"]}
        self.assertEqual(states["STAT-0003"]["状态大小"], "80MB")
        self.assertEqual(states["STAT-0003"]["快照状态"], "清理失败")

        retried = self.client.post("/api/state/manifests/2/retry", json={"values": {}}).json()
        self.assertTrue(retried["ok"])
        self.assertEqual(retried["entry"]["失败项"], [])
        self.assertEqual(retried["entry"]["整组进度"]["已删除"], 1)
        states = {row["配置编号"]: row for row in self.client.get("/api/state").json()["items"]}
        self.assertEqual(states["STAT-0003"]["状态大小"], "0B")

        again = self.client.post("/api/state/manifests/2/retry", json={"values": {}}).json()
        self.assertIn("无需续跑", again["message"])

    def test_retry_rechecks_references_before_deleting(self) -> None:
        """失败项在续跑前新增了恢复引用时，只保留目录，不能继续删除。"""
        self.cleanup({
            "state_ids": ["STAT-0003"],
            "失败路径": ["s3://stream-state/staging/metrics/"],
        })
        todo = store.rows("_state_restore_todo")
        todo.append({
            "id": 2,
            "待办编号": "TODO-0002",
            "检查点编号": "CHEC-0099",
            "存储路径": "s3://stream-state/staging/metrics/",
            "状态": "待恢复",
        })
        store.rows("checkpoint").append({
            "id": 99,
            "记录编号": "CHEC-0099",
            "状态": "已过期",
            "记录状态": "已过期",
            "状态大小": "80MB",
            "保存路径": "s3://stream-state/staging/metrics/",
            "检查点ID": "ckpt-099",
            "流作业名": "metrics-rollup",
        })
        retried = self.client.post("/api/state/manifests/2/retry", json={"values": {}}).json()
        self.assertTrue(retried["ok"])
        self.assertEqual(retried["entry"]["失败项"], [])
        self.assertEqual(retried["entry"]["整组进度"]["已保留"], 1)
        self.assertEqual(retried["entry"]["整组进度"]["已删除"], 0)
        self.assertEqual(retried["entry"]["受保护大小"], "80MB")
        states = {row["配置编号"]: row for row in self.client.get("/api/state").json()["items"]}
        self.assertEqual(states["STAT-0003"]["状态大小"], "80MB")

    def test_cross_link_conflict_keeps_directory(self) -> None:
        """已登记删除的目录仍被恢复待办引用时，不能一边删除一边保留可恢复引用。"""
        for row in store.rows("_state_cleanup"):
            if "ckpt-100" in row["路径"]:
                row["结果"] = "已删除"
        payload = self.cleanup({"state_ids": ["STAT-0001"]})
        entry = payload["entry"]
        self.assertEqual(entry["整组进度"]["失败"], 1)
        self.assertEqual(entry["删除结果"], [])
        self.assertTrue(entry["失败项"][0]["保留目录"])
        self.assertIn("跨链路冲突", entry["失败项"][0]["结果"])
        states = {row["配置编号"]: row for row in self.client.get("/api/state").json()["items"]}
        self.assertEqual(states["STAT-0001"]["状态大小"], "1.2GB")

    def test_manifest_and_references_roll_back_together(self) -> None:
        before_state = [dict(row) for row in store.rows("state")]
        before_ledger = [dict(row) for row in store.rows("_state_cleanup")]
        original = state_service._collect_references

        def boom() -> list[dict]:
            raise RuntimeError("模拟写入失败")

        state_service._collect_references = boom
        try:
            with self.assertRaises(RuntimeError):
                state_service.StateService().cleanup_snapshots({"state_ids": ["STAT-0002"]})
        finally:
            state_service._collect_references = original

        self.assertEqual([dict(row) for row in store.rows("state")], before_state)
        self.assertEqual([dict(row) for row in store.rows("_state_cleanup")], before_ledger)

    def test_consumer_version_tracks_manifest(self) -> None:
        self.cleanup({"state_ids": ["STAT-0002"]})
        version = self.client.get("/api/state/manifests/2").json()["版本标识"]
        consumers = self.client.get("/api/state/manifests/consumers").json()
        self.assertGreaterEqual(version, 2)
        self.assertEqual(consumers[0]["当前清单版本"], version)
        synced = self.client.post("/api/state/manifests/2/sync", json={"values": {}}).json()
        self.assertTrue(synced["ok"])
        self.assertEqual(synced["entry"]["版本标识"], version)
        consumers = self.client.get("/api/state/manifests/consumers").json()
        self.assertEqual(consumers[0]["已同步版本"], version)
        self.assertEqual(consumers[0]["待同步"], 0)

    def test_references_released_allow_cleanup_again(self) -> None:
        """引用集合变化后，同一个提交可以真正删除此前被保护的目录。"""
        first = self.cleanup({"state_ids": ["STAT-0001"], "提交编号": "req-release"})
        self.assertEqual(first["entry"]["整组进度"]["已保留"], 1)
        for row in store.rows("checkpoint"):
            if row["记录编号"] in {"CHEC-0001", "CHEC-0003"}:
                row["status"] = "已清理"
                row["记录状态"] = "已清理"
        store.rows("_state_restore_todo").clear()
        second = self.cleanup({"state_ids": ["STAT-0001"], "提交编号": "req-release"})
        self.assertFalse(second["entry"]["重复提交"])
        self.assertEqual(second["entry"]["整组进度"]["已删除"], 1)
        states = {row["配置编号"]: row for row in self.client.get("/api/state").json()["items"]}
        self.assertEqual(states["STAT-0001"]["状态大小"], "0B")

    def test_dry_run_rolls_back_everything(self) -> None:
        before_state = [dict(row) for row in store.rows("state")]
        before_ledger = [dict(row) for row in store.rows("_state_cleanup")]
        before_manifests = len(store.rows("_state_manifest"))
        payload = self.cleanup({"state_ids": ["STAT-0002", "STAT-0003"], "预演": True})
        self.assertEqual(payload["entry"]["整组进度"]["已删除"], 2)
        self.assertEqual([dict(row) for row in store.rows("state")], before_state)
        self.assertEqual([dict(row) for row in store.rows("_state_cleanup")], before_ledger)
        self.assertEqual(len(store.rows("_state_manifest")), before_manifests)
        self.assertIn("预演", payload["message"])

    def test_legacy_manifest_fields_are_repaired(self) -> None:
        store.rows("_state_manifest").append({
            "id": 9,
            "清单编号": "SNAP-0009",
            "存储路径": "s3://stream-state/legacy/",
            "删除结果": [{"路径": "s3://stream-state/legacy/old/", "结果": "已删除"}],
            "受保护大小": "300MB",
        })
        manifest = self.client.get("/api/state/manifests/9").json()
        self.assertEqual(manifest["受保护字节"], 314572800)
        self.assertEqual(manifest["受保护大小"], "300MB")
        self.assertEqual(manifest["版本标识"], 1)
        self.assertEqual(manifest["失败项"], [])
        self.assertEqual(manifest["整组进度"]["已删除"], 1)


if __name__ == "__main__":
    unittest.main()
