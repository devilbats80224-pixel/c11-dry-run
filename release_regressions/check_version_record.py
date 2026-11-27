"""Fail closed when a PR has no new, traceable version record."""
import os
from pathlib import Path
import re
import subprocess
import sys

RECORD_PATH = re.compile(r"docs/version-records/[a-z0-9][a-z0-9-]*\.md")
SECTIONS = ("基於哪一版", "改了什麼", "解決了什麼", "尚未解決或未驗證", "保留項目與驗證")
PLACEHOLDERS = {"todo", "tbd", "n/a", "待補", "無", "none", "略"}


def git(*args):
    return subprocess.check_output(["git", *args])


def check(base_ref, head_ref):
    base = git("merge-base", base_ref, head_ref).decode("ascii").strip()
    changes = git("diff", "--name-status", "--no-renames", "-z", base, head_ref, "--")
    tokens = changes.decode("utf-8", "surrogateescape").split("\0")[:-1]
    records = [path for status, path in zip(tokens[::2], tokens[1::2])
               if status == "A" and RECORD_PATH.fullmatch(path)]
    if len(records) != 1:
        return "VERSION_RECORD_MISSING", "此 PR 必須新增恰好一份版本紀錄", f"找到 {len(records)} 份新紀錄"
    body = git("show", f"{head_ref}:{records[0]}").decode("utf-8")
    parts = re.split(r"(?m)^## ([^\n]+)\n", body)
    names = parts[1::2]
    for label in SECTIONS:
        if names.count(label) != 1:
            return "VERSION_RECORD_SECTION_MISSING", f"{label}須恰好出現一次", f"找到 {names.count(label)} 次"
        value = parts[2::2][names.index(label)].strip()
        if label == SECTIONS[0]:
            if value.splitlines()[0:1] != [base]:
                return "VERSION_RECORD_BASE_MISMATCH", f"第一行應為本次 Git 分支共同基底 {base}", "未填寫正確的基底提交"
        else:
            value = re.sub(r"<!--.*?-->", "", value, flags=re.S).strip()
            value = re.sub(r"^[-* ]+", "", value).strip()
            if len(value) < 8 or value.casefold().rstrip("。.!") in PLACEHOLDERS:
                return "VERSION_RECORD_SECTION_EMPTY", f"{label}須有具體說明", "欄位空白、過短或仍是佔位字樣"
    return None


def main():
    try:
        failure = check(*sys.argv[1:]) if len(sys.argv) == 3 else None
        if len(sys.argv) != 3:
            raise ValueError
    except (ValueError, OSError, subprocess.CalledProcessError):
        failure = ("VERSION_RECORD_UNREADABLE", "可讀取 Git 比對及 UTF-8 版本紀錄", "無法讀取輸入")
    if failure is None:
        print("VERSION_RECORD_PASS")
        return 0
    case, expected, actual = failure
    message = (f"版本紀錄檢查失敗：{case}\n\n"
               f"- 預期：{expected}\n- 實際／首個確認原因：{actual}\n")
    print(message)
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(summary).open("a", encoding="utf-8") as report:
            report.write(message)
    return 1


if __name__ == "__main__":
    sys.exit(main())
