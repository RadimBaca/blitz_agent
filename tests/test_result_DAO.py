import os
import tempfile
import shutil
import pytest
import src.result_DAO as dao

@pytest.fixture(autouse=True)
def temp_cwd(monkeypatch):
    # Create a temporary directory and switch to it for the test duration
    orig_dir = os.getcwd()
    temp_dir = tempfile.mkdtemp()
    monkeypatch.chdir(temp_dir)
    yield
    os.chdir(orig_dir)
    shutil.rmtree(temp_dir)

def test_store_and_get_all_records():
    records = [{"foo": "bar"}, {"baz": "qux"}]
    dao.store_records("testproc", records)
    result = dao.get_all_records("testproc")
    assert len(result) == 2
    assert result[0]["foo"] == "bar"
    assert result[1]["baz"] == "qux"

def test_get_and_store_chat_history():
    chat_history = [("user", "hello"), ("ai", "hi")]
    dao.store_chat_history("testproc", 0, chat_history)
    loaded = dao.get_chat_history("testproc", 0)
    assert loaded == chat_history

def test_get_record():
    records = [{"foo": "bar"}]
    dao.store_records("testproc", records)
    rec = dao.get_record("testproc", 0)
    assert rec["foo"] == "bar"

def test_clear_all_and_delete_chat_sessions():
    records = [{"foo": "bar"}]
    dao.store_records("testproc", records)
    dao.store_chat_history("testproc", 0, [("user", "hi")])
    # Should remove all kv and chat files
    dao.clear_all()
    assert dao.get_all_records("testproc") == []
    assert dao.get_chat_history("testproc", 0) is None