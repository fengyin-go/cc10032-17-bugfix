"""序号17 回归测试：分层采样覆盖、截断台账、去重与幂等、调试复核拦截。

覆盖场景：
  * 尾部分层不再因整体截断被清空，每个非空分层至少一条可追踪样本；
  * 样本上限不足覆盖全部分层时的两种取舍（expand / compress）都可消费；
  * 截断损失写入样本报告，且报告与分层样本数据一致；
  * 同一分层/同一事件重复采样不重复累计；
  * 并发/重复调用同一策略只生效一次，事务中断回到采样前版本；
  * 调试追踪发现缺失分层时阻止异常结论。
"""
from __future__ import annotations

import copy
import threading

import pytest

from app.sampling import (
    dedupe_and_group,
    plan_allocation,
    random_sample,
    stratified_sample,
)
from app.services.data_sampler import DataSamplerService
from app.services.debug_trace import DebugTraceService
from app.store import Store, store


# ---------------------------------------------------------------------------
# 辅助
# ---------------------------------------------------------------------------

def _events(per_stratum: int, strata: int = 5, key: str = "region") -> list[dict]:
    return [
        {
            "event_id": f"e{s}_{i}",
            "topic": "t",
            "payload": {key: f"region_{s}", "value": 10 + i},
            "event_time": i * 1.5,
            "source": "test",
        }
        for i in range(per_stratum)
        for s in range(strata)
    ]


def _register_strategy(target_store, **overrides) -> str:
    rows = target_store.rows("data_sampler")
    row = {
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "status": "已启用",
        "pending": False,
        "abnormal": False,
        "策略名称": f"序号17策略_{len(rows)}",
        "采样方式": "stratified",
        "采样比例": "0.5",
        "分组键": "region",
        "保留特征": "value",
        "样本上限": "4",
        "采样窗口": "60",
    }
    row.update(overrides)
    rows.append(row)
    return row["策略名称"]


@pytest.fixture()
def fresh_store(monkeypatch):
    """每个用例用独立的内存仓库，避免批次互相污染。"""
    isolated = Store.__new__(Store)
    Store.__init__(isolated)
    monkeypatch.setattr("app.store.store", isolated)
    monkeypatch.setattr("app.services.data_sampler.store", isolated)
    monkeypatch.setattr("app.services.debug_trace.store", isolated)
    monkeypatch.setattr("app.pipeline.store", isolated)
    return isolated


# ---------------------------------------------------------------------------
# 纯逻辑：配额与采样
# ---------------------------------------------------------------------------

class TestAllocation:
    def test_every_non_empty_stratum_kept(self):
        sizes = {f"r{i}": 5 for i in range(5)}
        plan = plan_allocation(sizes, 0.5, 4)
        assert all(plan["allocation"][key] >= 1 for key in sizes)
        assert plan["cap_expanded"] is True
        assert plan["effective_limit"] == 5

    def test_tail_strata_not_truncated_away(self):
        """旧实现按层顺序累计后整体截断，尾部分层会被清空。"""
        sizes = {"head": 200, **{f"tail{i}": 2 for i in range(6)}}
        plan = plan_allocation(sizes, 0.01, 130)
        assert plan["allocation"]["head"] < 200
        for i in range(6):
            assert plan["allocation"][f"tail{i}"] >= 1
        assert plan["cap_expanded"] is False

    def test_limit_within_coverage_respects_cap(self):
        sizes = {f"r{i}": 10 for i in range(4)}
        plan = plan_allocation(sizes, 0.5, 10)
        assert sum(plan["allocation"].values()) <= 10
        assert plan["cap_expanded"] is False

    def test_compress_policy_keeps_limit(self):
        sizes = {f"r{i}": 5 for i in range(5)}
        plan = plan_allocation(sizes, 0.5, 3, cap_policy="compress")
        assert plan["effective_limit"] == 3
        assert plan["limit_overrun"] == 2
        assert all(value >= 1 for value in plan["allocation"].values())

    def test_empty_sizes(self):
        plan = plan_allocation({}, 0.5, 10)
        assert plan["allocation"] == {}


class TestStratifiedSample:
    def test_report_covers_every_stratum(self):
        sampled, report = stratified_sample(_events(5), "region", 0.5, 4)
        assert report["strata_covered"] == report["strata_total"] == 5
        assert report["uncovered_strata"] == []
        assert report["coverage_complete"] is True
        # 上限 4 小于 5 层，扩容到每层一份
        assert report["cap_expanded"] is True
        assert report["sampled_total"] == 5

    def test_truncation_loss_recorded(self):
        sampled, report = stratified_sample(_events(5), "region", 0.5, 8)
        assert report["truncated_total"] == 25 - report["sampled_total"] > 0
        assert report["truncated_strata"]
        assert sum(item["loss"] for item in report["strata"]) == report["truncated_total"]
        assert all(item["covered"] for item in report["strata"])

    def test_selected_ids_match_sample(self):
        sampled, report = stratified_sample(_events(4), "region", 0.5, 6)
        report_ids = {
            sample_id
            for item in report["strata"]
            for sample_id in item["selected_ids"]
        }
        actual_ids = {f"event_id:{item['event_id']}" for item in sampled}
        assert report_ids == actual_ids
        assert len(actual_ids) == len(sampled)

    def test_duplicate_events_counted_once(self):
        events = _events(5)
        sampled, report = stratified_sample(events + copy.deepcopy(events), "region", 0.5, 8)
        assert report["duplicates_dropped"] == len(events)
        assert report["total_events"] == len(events)
        assert len({item["event_id"] for item in sampled}) == len(sampled)

    def test_same_event_in_two_strata_counted_once(self):
        events = [
            {"event_id": "dup", "payload": {"region": "a"}},
            {"event_id": "dup", "payload": {"region": "b"}},
            {"event_id": "x", "payload": {"region": "b"}},
        ]
        _, duplicates, strata = dedupe_and_group(events, "region")
        assert duplicates == 1
        assert set(strata) == {"a", "b"}

    def test_stable_across_runs(self):
        first, _ = stratified_sample(_events(6), "region", 0.3, 9, "seed")
        second, _ = stratified_sample(_events(6), "region", 0.3, 9, "seed")
        assert [item["event_id"] for item in first] == [item["event_id"] for item in second]

    def test_no_group_key_single_stratum(self):
        sampled, report = stratified_sample(_events(5), "", 0.5, 100)
        assert report["strata_total"] == 1
        assert report["strata"][0]["stratum"] == "__default__"
        assert len(sampled) >= 1

    def test_empty_data(self):
        sampled, report = stratified_sample([], "region", 0.5, 10)
        assert sampled == []
        assert report["coverage_complete"] is True


class TestRandomSample:
    def test_random_dedupes(self):
        events = _events(4, strata=3)
        sampled, report = random_sample(events + copy.deepcopy(events), 0.5, 100)
        assert report["duplicates_dropped"] == len(events)
        assert len(sampled) <= len(events)


# ---------------------------------------------------------------------------
# 服务层：幂等、并发、事务、报告
# ---------------------------------------------------------------------------

class TestSamplerService:
    def test_repeat_call_hits_same_batch(self, fresh_store):
        service = DataSamplerService()
        name = _register_strategy(fresh_store)
        events = _events(5)
        before = len(fresh_store.rows("data_sampler"))
        first = service.apply_sampling(name, events)
        second = service.apply_sampling(name, events)
        assert first["batch_id"] == second["batch_id"]
        assert second["deduplicated"] is True
        assert len(fresh_store.rows("data_sampler")) == before + 1

    def test_concurrent_retries_apply_once(self, fresh_store):
        service = DataSamplerService()
        name = _register_strategy(fresh_store)
        events = _events(5)
        before = len(fresh_store.rows("data_sampler"))
        results: list[dict] = []

        def worker() -> None:
            results.append(service.apply_sampling(name, events))

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert len({result["batch_id"] for result in results}) == 1
        assert len(fresh_store.rows("data_sampler")) == before + 1
        batch = fresh_store.find("data_sampler", results[0]["batch_id"])
        assert batch["_sample_count"] == results[0]["sampled"]
        assert batch["_sample_report"]["duplicates_dropped"] == 0

    def test_duplicate_payload_not_double_counted(self, fresh_store):
        service = DataSamplerService()
        name = _register_strategy(fresh_store)
        events = _events(5)
        first = service.apply_sampling(name, events, idempotency_key="k1")
        batch = fresh_store.find("data_sampler", first["batch_id"])
        count_before = batch["_sample_count"]
        # 同一事件再来一遍：显式幂等键相同，命中同一批次
        again = service.apply_sampling(
            name, events + copy.deepcopy(events), idempotency_key="k1",
        )
        assert again["deduplicated"] is True
        assert batch["_sample_count"] == count_before
        # 即使重新按数据指纹计算，重复数据也只计一次事件
        third = service.apply_sampling(
            name, events + copy.deepcopy(events), idempotency_key="k2",
        )
        assert third["duplicates_dropped"] == len(events)
        assert third["total"] == len(events)

    def test_transaction_rollback_on_write_failure(self, fresh_store):
        service = DataSamplerService()
        name = _register_strategy(fresh_store)
        events = _events(5)
        rows = fresh_store.rows("data_sampler")
        before = [copy.deepcopy(row) for row in rows]
        original_mutation = list.append

        class BrokenRows(list):
            def append(self, item):  # noqa: D401 - 模拟写库失败
                raise RuntimeError("write failed")

        broken = BrokenRows(before)
        fresh_store._tables["data_sampler"] = broken
        with pytest.raises(RuntimeError):
            service.apply_sampling(name, events, idempotency_key="tx")
        assert list(broken) == before, "事务中断后应回到修复前版本，不残留半份样本"

    def test_dry_run_does_not_write(self, fresh_store):
        service = DataSamplerService()
        name = _register_strategy(fresh_store)
        before = len(fresh_store.rows("data_sampler"))
        result = service.apply_sampling(name, _events(5), dry_run=True)
        assert result["dry_run"] is True
        assert len(fresh_store.rows("data_sampler")) == before

    def test_report_and_stratum_counts(self, fresh_store):
        service = DataSamplerService()
        name = _register_strategy(fresh_store)
        result = service.apply_sampling(name, _events(5))
        report = service.get_sample_report(result["batch_id"])
        counts = service.list_stratum_counts(result["batch_id"])
        assert report["batch_id"] == result["batch_id"]
        assert len(counts) == 5
        assert sum(item["selected"] for item in counts) == result["sampled"]
        assert all(item["covered"] for item in counts)

    def test_threshold_breach_uses_effective_limit(self, fresh_store):
        service = DataSamplerService()
        name = _register_strategy(fresh_store, 样本上限="4")
        service.apply_sampling(name, _events(5))
        breaches = service.list_threshold_breaches(name)
        # 扩容后的有效上限就是 5，不应再报超限
        assert breaches == []

    def test_batches_hidden_from_strategy_list(self, fresh_store):
        service = DataSamplerService()
        name = _register_strategy(fresh_store)
        visible_before, total_before = service.list_entries(size=200)
        service.apply_sampling(name, _events(5))
        visible_after, total_after = service.list_entries(size=200)
        assert total_after == total_before, "采样批次不应混进策略列表"
        assert all(row.get("策略名称") != "" for row in visible_after)
        assert all("_sample_report" not in row for row in visible_after)

    def test_fix_ledger_records_apply_and_idempotent(self, fresh_store):
        service = DataSamplerService()
        name = _register_strategy(fresh_store)
        first = service.apply_sampling(name, _events(5), idempotency_key="ledger")
        service.apply_sampling(name, _events(5), idempotency_key="ledger")
        ledger = service.fix_ledger()
        assert ledger[-1]["idempotent"] is False
        assert ledger[0]["idempotent"] is True
        assert ledger[0]["batch_id"] == first["batch_id"]
        assert ledger[0]["issue"] == "17"

    def test_read_events_from_source(self, fresh_store):
        service = DataSamplerService()
        fresh_store.rows("stream_source").append({
            "id": 99, "源端名称": "断连源", "status": "异常断连", "pending": False, "abnormal": True,
        })
        blocked = service.read_events_from_source("断连源")
        assert blocked["ok"] is False
        ok = service.read_events_from_source("test_source", 6)
        assert ok["ok"] is True and len(ok["events"]) == 6

    def test_compress_policy_materialised(self, fresh_store):
        service = DataSamplerService()
        name = _register_strategy(fresh_store)
        result = service.apply_sampling(
            name, _events(5), cap_policy="compress", idempotency_key="compress",
        )
        assert result["cap_policy"] == "compress"
        assert result["limit_overrun"] == 1
        assert result["strata_covered"] == 5


# ---------------------------------------------------------------------------
# 调试追踪：覆盖复核与结论拦截
# ---------------------------------------------------------------------------

class TestDebugTraceCoverage:
    def _session(self, fresh_store, name: str) -> dict:
        row = {
            "id": max((int(r.get("id", 0)) for r in fresh_store.rows("debug_trace")), default=0) + 1,
            "会话名称": name,
            "目标算子": "window_agg",
            "采样数据": "",
            "中间结果": "0",
            "异常标记": "none",
            "开始时间": "2026-11-18 10:00:00",
            "结束时间": "",
            "status": "运行中",
            "pending": True,
            "abnormal": False,
        }
        fresh_store.rows("debug_trace").append(row)
        return row

    def test_complete_after_full_coverage(self, fresh_store):
        sampler = DataSamplerService()
        tracer = DebugTraceService()
        name = _register_strategy(fresh_store)
        batch = sampler.apply_sampling(name, _events(5))
        self._session(fresh_store, "会话完整")
        assert tracer.attach_sampling_batch("会话完整", batch["batch_id"])["ok"]
        coverage = tracer.check_sample_coverage("会话完整")
        assert coverage["ok"] is True
        result = tracer.complete_session_with_analysis("会话完整")
        assert result["ok"] is True
        assert result["analysis"]["coverage_complete"] is True
        assert result["analysis"]["truncated_strata"]

    def test_missing_stratum_blocks_conclusion(self, fresh_store):
        sampler = DataSamplerService()
        tracer = DebugTraceService()
        name = _register_strategy(fresh_store)
        batch = sampler.apply_sampling(name, _events(5))
        self._session(fresh_store, "会话缺失")
        tracer.attach_sampling_batch("会话缺失", batch["batch_id"])

        # 模拟报告与分层样本数据不一致：某层有事件但没有样本
        entry = fresh_store.find("data_sampler", batch["batch_id"])
        report = entry["_sample_report"]
        dropped = report["strata"][-1]
        dropped["selected"] = 0
        dropped["selected_ids"] = []
        dropped["covered"] = False
        entry["_sampled_data"] = [
            event for event in entry["_sampled_data"]
            if str(event["payload"]["region"]) != dropped["stratum"]
        ]

        coverage = tracer.check_sample_coverage("会话缺失")
        assert coverage["ok"] is False
        assert dropped["stratum"] in coverage["missing_strata"]

        session = fresh_store.rows("debug_trace")[-1]
        status_before = session["status"]
        result = tracer.complete_session_with_analysis("会话缺失")
        assert result["ok"] is False and result["blocked"] is True
        assert session["status"] == status_before
        assert tracer.analyze_anomaly_patterns("会话缺失")["blocked"] is True

    def test_no_batch_blocks_conclusion(self, fresh_store):
        tracer = DebugTraceService()
        self._session(fresh_store, "会话未绑定")
        coverage = tracer.check_sample_coverage("会话未绑定")
        assert coverage["ok"] is False
        assert coverage["reason"] == "no_sample_batch"
        result = tracer.complete_session_with_analysis("会话未绑定")
        assert result["blocked"] is True

    def test_report_sample_mismatch_detected(self, fresh_store):
        sampler = DataSamplerService()
        tracer = DebugTraceService()
        name = _register_strategy(fresh_store)
        batch = sampler.apply_sampling(name, _events(5))
        self._session(fresh_store, "会话错位")
        tracer.attach_sampling_batch("会话错位", batch["batch_id"])
        entry = fresh_store.find("data_sampler", batch["batch_id"])
        entry["_sampled_data"] = entry["_sampled_data"][1:]
        coverage = tracer.check_sample_coverage("会话错位")
        assert coverage["ok"] is False
        assert coverage["missing_strata"]

        entry["_sampled_data"] = entry["_sampled_data"] + [copy.deepcopy(entry["_sampled_data"][0])]
        coverage = tracer.check_sample_coverage("会话错位")
        assert coverage["ok"] is False
        assert coverage["count_mismatch"]

    def test_coverage_note_lists_truncated(self, fresh_store):
        sampler = DataSamplerService()
        tracer = DebugTraceService()
        name = _register_strategy(fresh_store)
        batch = sampler.apply_sampling(name, _events(5))
        self._session(fresh_store, "会话截断")
        tracer.attach_sampling_batch("会话截断", batch["batch_id"])
        result = tracer.complete_session_with_analysis("会话截断")
        assert result["ok"] is True
        note = result["analysis"]["coverage_note"]
        for stratum in result["analysis"]["truncated_strata"]:
            assert stratum in note
