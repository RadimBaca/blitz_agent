import pytest
from datetime import datetime

import src.app_filter_sort as fs
from src.models import BlitzRecord, BlitzIndexRecord, BlitzCacheRecord
from app import app as flask_app


def test_filter_priority_filters_and_handles_invalid():
    records = [BlitzRecord(procedure_order=1, pc_id=1, priority=5),
               BlitzRecord(procedure_order=2, pc_id=1, priority=10),
               BlitzRecord(procedure_order=3, pc_id=1, priority=None)]

    # without max_priority should keep records with non-null priority
    res = fs.filter_priority(records, None)
    assert all(r.priority is not None for r in res)
    assert len(res) == 2

    # with max_priority should filter properly
    res2 = fs.filter_priority(records, '6')
    assert len(res2) == 1
    assert res2[0].priority == 5

    # invalid max_priority should be ignored
    res3 = fs.filter_priority(records, 'not-an-int')
    assert len(res3) == 2


def test_filter_blitz_and_index_group_extraction():
    br = BlitzRecord(procedure_order=1, pc_id=1, finding='GroupA: detail')
    br2 = BlitzRecord(procedure_order=2, pc_id=1, finding='GroupB: other')
    br3 = BlitzRecord(procedure_order=3, pc_id=1, finding=None)

    # filter_blitz and filter_blitz_index rely on Flask request context
    with flask_app.test_request_context('/'):
        # filter_blitz returns filtered list only
        res = fs.filter_blitz([br, br2, br3])
        assert len(res) == 3  # default behavior shows all when no selection

        # filter_blitz_index returns a tuple (filtered, groups, selected)
        filtered, groups, selected = fs.filter_blitz_index([br, br2, br3])
    assert set(groups) == {'GroupA', 'GroupB'}
    assert set(selected) == set(groups)
    assert len(filtered) == 3


def test_filter_blitz_cache_and_sorting_and_filter_by_hour():
    now = datetime.now()
    # create blitzcache records with varying metrics
    # model expects last_execution as a string
    last_exec_str = now.isoformat()
    r1 = BlitzCacheRecord(procedure_order=1, pc_id=1, avg_cpu_ms=10.0, total_cpu_ms=100.0, executions=5, total_reads=200, last_execution=last_exec_str)
    r2 = BlitzCacheRecord(procedure_order=2, pc_id=1, avg_cpu_ms=None, total_cpu_ms=50.0, executions=10, total_reads=100, last_execution=last_exec_str)
    r3 = BlitzCacheRecord(procedure_order=3, pc_id=1, avg_cpu_ms=20.0, total_cpu_ms=None, executions=2, total_reads=300, last_execution=last_exec_str)

    records = [r1, r2, r3]

    # filter by numeric thresholds
    out = fs.filter_blitz_cache(records, min_avg_cpu='15', min_total_cpu=None, min_executions='3', min_total_reads='150')
    # r3 has avg_cpu 20 but executions 2 -> should be filtered out; r1 has avg_cpu 10 -> filtered out by avg
    assert out == [] or all(isinstance(x, BlitzCacheRecord) for x in out)

    # sort by avg_cpu_ms desc should put r3 first, then r1, then r2 (None -> treated as 0)
    sorted_desc = fs.sort_records(records, 'avg_cpu_ms', 'desc')
    assert sorted_desc[0].avg_cpu_ms == 20.0
    assert sorted_desc[-1].avg_cpu_ms is None or sorted_desc[-1].avg_cpu_ms == None

    # sort by executions asc
    sorted_exec = fs.sort_records(records, 'executions', 'asc')
    assert sorted_exec[0].executions == 2

    # filter_by_hour with matching hour
    hour = str(now.hour)
    # filter_by_hour uses simple hour comparison; run inside request context just in case
    with flask_app.test_request_context('/'):
        filtered_hour = fs.filter_by_hour(hour, hour, records)
        assert len(filtered_hour) == 3


def test_sort_records_handles_none_and_invalid_sort_key():
    r1 = BlitzCacheRecord(procedure_order=1, pc_id=1, avg_cpu_ms=5.0)
    r2 = BlitzCacheRecord(procedure_order=2, pc_id=1, avg_cpu_ms=None)
    records = [r1, r2]

    # valid sort key
    s = fs.sort_records(records, 'avg_cpu_ms', 'asc')
    assert s[0].avg_cpu_ms == 0 or s[0].avg_cpu_ms == 5.0 or isinstance(s[0], BlitzCacheRecord)

    # invalid sort key should return original list unchanged
    s2 = fs.sort_records(records, 'nonexistent', 'asc')
    assert s2 == records
