# results_DAO.py

import shelve
import glob
import os


def _kv_path(proc_name: str) -> str:
    return f"kv_{proc_name}"


def _kv_chat_path(proc_name: str, rec_id: int) -> str:
    return f"kv_chat_{proc_name}_{rec_id}"


def get_all_records(proc_name: str):
    kv_name = _kv_path(proc_name)
    records = []

    with shelve.open(kv_name) as kv:
        for i, val in enumerate(kv.values()):
            rec = dict(val)
            rec["_rec_id"] = i
            chat_path = _kv_chat_path(proc_name, i) + ".dat"
            rec["_analyzed"] = os.path.exists(chat_path)
            records.append(rec)

    return records


def get_record(proc_name: str, rec_id: int):
    kv_name = _kv_path(proc_name)
    with shelve.open(kv_name) as kv:
        return kv[str(rec_id)]


def store_records(proc_name: str, records: list):
    kv_name = _kv_path(proc_name)
    with shelve.open(kv_name, writeback=True) as kv:
        kv.clear()
        for i, record in enumerate(records):
            kv[str(i)] = record


def get_chat_history(proc_name: str, rec_id: int):
    kv_chat = _kv_chat_path(proc_name, rec_id)
    if not os.path.exists(kv_chat + ".dat"):
        return None
    with shelve.open(kv_chat) as kv:
        return kv.get("history", [])


def store_chat_history(proc_name: str, rec_id: int, chat_history):
    kv_chat = _kv_chat_path(proc_name, rec_id)
    with shelve.open(kv_chat, writeback=True) as kv:
        kv["history"] = chat_history


def clear_all():
    # Remove all shelve files for records
    for ext in [".dat", ".bak", ".dir"]:
        for file in glob.glob(f"kv_*{ext}"):
            try:
                os.remove(file)
            except Exception:
                pass
    delete_all_chat_sessions()


def delete_all_chat_sessions():
    for file in glob.glob("kv_chat_*.dat") + glob.glob("kv_chat_*.bak") + glob.glob("kv_chat_*.dir"):
        try:
            os.remove(file)
        except Exception:
            pass


def delete_chat_sessions(proc_name: str):
    patterns = [
        f"kv_chat_{proc_name}_*.db",
        f"kv_chat_{proc_name}_*.bak",
        f"kv_chat_{proc_name}_*.dat",
        f"kv_chat_{proc_name}_*.dir"
    ]
    for pattern in patterns:
        for file in glob.glob(pattern):
            try:
                os.remove(file)
            except Exception:
                pass
