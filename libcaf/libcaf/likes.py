""" Likes — on disk representation and primitives."""
from pathlib import Path
from .constants import HASH_CHARSET, HASH_LENGTH, LIKES_DIR, LIKES_USERS_DIR, LIKES_COMMITS_DIR, LIKES_JOURNALS_FILE, LIKES_JOURNAL_TMP_SUFFIX
import os
from contextlib import contextmanager
from typing import Iterator
import fcntl
import hashlib
import time
import shutil


def _write_like(path: Path, ts_ns: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        try:
            if path.stat().st_size != 0:
                return
        except FileNotFoundError:
            return
        fd = os.open(path, os.O_WRONLY | os.O_TRUNC, 0o600)

    try:
        os.write(fd, f"{ts_ns}\n".encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)


def rebuild_commit_likes_cache(repo_path: Path) -> None:
    with _lock_journal(repo_path, exclusive=True):
        _recover_journal_locked(repo_path)
        likes_users_base = _likes_users_base(repo_path)
        likes_commits_base = _likes_commits_base(repo_path)
        if likes_commits_base.exists():
            shutil.rmtree(likes_commits_base)
        likes_commits_base.mkdir(parents=True, exist_ok=True)
        if not likes_users_base.exists():
            return
        for user_dir in likes_users_base.iterdir():
            if not user_dir.is_dir():
                continue
            username = user_dir.name
            for like_file in user_dir.iterdir():
                if not like_file.is_file():
                    continue
                commit_hash = like_file.name
                ts_ns = like_file.read_text(encoding="utf-8").strip()
                commit_side = _commit_like_path(repo_path, commit_hash, username)
                _write_like(commit_side, int(ts_ns))


def _journal_path(repo_path: Path) -> Path:
    return _likes_base(repo_path) / LIKES_JOURNALS_FILE


def _journal_lock_path(repo_path: Path) -> Path:
    return _likes_locks_base(repo_path) / "journal.lock"

def _truncate_journal_locked(repo_path: Path) -> None:
    jp = _journal_path(repo_path)
    jp.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(jp, os.O_CREAT | os.O_WRONLY , 0o600)
    try:
        os.ftruncate(fd, 0)
        os.fsync(fd)
    finally:
        os.close(fd)


def _append_journal(repo_path: Path, line: str) -> None:
    jp = _journal_path(repo_path)
    jp.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(jp, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o600)
    try:
        os.write(fd, line.encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)


def journal_has_pending(repo_path: Path) -> bool:
    jp = _journal_path(repo_path)
    if not jp.exists():
        return False
    try:
        return jp.exists() and jp.stat().st_size > 0
    except FileNotFoundError:
        return False

def _maybe_recover_from_journal(repo_path: Path) -> None:
    with _lock_journal(repo_path, exclusive=True):
        if not journal_has_pending(repo_path):
            return
        _recover_journal_locked(repo_path)


def _recover_journal_locked(repo_path: Path) -> None:
    jp = _journal_path(repo_path)
    if not jp.exists():
        return
    try:
        if jp.stat().st_size == 0:
            return
    except FileNotFoundError:
        return
    try:
        with jp.open("r", encoding="utf-8") as f:
            for line in f:
                parts = line.strip().split()
                if not parts:
                    continue
                op = parts[0]
                if op == "ADD":
                    if len(parts) < 4:
                        continue
                    username, commit_hash = parts[1], parts[2]
                    try:
                        ts_ns = int(parts[3])
                    except ValueError:
                        ts_ns = time.time_ns()
                    
                    user_side = (_likes_users_base(repo_path) / username) / commit_hash
                    commit_side = _commit_like_path(repo_path, commit_hash, username)
                    legacy_commit_side = _likes_commits_base(repo_path) / commit_hash / username
                    _write_like(user_side, ts_ns)
                    _write_like(commit_side, ts_ns)
                    if legacy_commit_side.exists():
                        legacy_commit_side.unlink()
                elif op == "DEL":
                    if len(parts) < 3:
                        continue
                    username, commit_hash = parts[1], parts[2]
                    user_side = (_likes_users_base(repo_path) / username) / commit_hash
                    if user_side.exists():
                        user_side.unlink()

                    commit_side = _commit_like_path(repo_path, commit_hash, username)
                    legacy_commit_side = _likes_commits_base(repo_path) / commit_hash / username
                    if commit_side.exists():
                        commit_side.unlink()
                    if legacy_commit_side.exists():
                        legacy_commit_side.unlink()
    except FileNotFoundError:
        return
    _truncate_journal_locked(repo_path)



def init_likes(repo_path: Path) -> None:
    (repo_path / LIKES_DIR).mkdir(parents=True, exist_ok=True)
    (repo_path / LIKES_DIR / LIKES_USERS_DIR).mkdir(parents=True, exist_ok=True)
    (repo_path / LIKES_DIR / LIKES_COMMITS_DIR).mkdir(parents=True, exist_ok=True)


def _validate_username(username: str) -> str:
    username = username.strip()
    if not username:
        raise ValueError("Username is required")
    if "/" in username or "\\" in username:
        raise ValueError("Invalid username: path separators are not allowed")
    if username in {".", ".."}:
        raise ValueError("Invalid username")
    return username


def _validate_commit_hash(commit_hash: str) -> str:
    commit_hash = commit_hash.strip()
    if not commit_hash:
        raise ValueError("Commit hash is required")
    if len(commit_hash) != HASH_LENGTH or any(c not in HASH_CHARSET for c in commit_hash):
        raise ValueError("Invalid commit hash")
    return commit_hash

def _commit_bucket(username: str, n: int = 2) -> str:
    return hashlib.sha256(username.encode("utf-8")).hexdigest()[:n]


def _commit_like_path(repo_path: Path, commit_hash: str, username: str) -> Path:
    bucket = _commit_bucket(username)
    return _likes_commits_base(repo_path) / commit_hash / bucket / username


def _likes_base(repo_path: Path) -> Path:
    return repo_path / LIKES_DIR


def _likes_users_base(repo_path: Path) -> Path:
    return _likes_base(repo_path) / LIKES_USERS_DIR

def _likes_commits_base(repo_path: Path) -> Path:
    return _likes_base(repo_path) / LIKES_COMMITS_DIR

def _likes_locks_base(repo_path: Path) -> Path:
    return _likes_base(repo_path) / ".locks"


@contextmanager
def _lock_journal(repo_path: Path, *, exclusive: bool = True) -> Iterator[None]:
    lock_path = _journal_lock_path(repo_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def add_like(repo_path: Path, username: str, commit_hash: str) -> None:
    username = _validate_username(username)
    commit_hash = _validate_commit_hash(commit_hash)
    ts_ns = time.time_ns()
    with _lock_journal(repo_path, exclusive=True):
        if journal_has_pending(repo_path):
            _recover_journal_locked(repo_path)
        _append_journal(repo_path, f"ADD {username} {commit_hash} {ts_ns}\n")
        user_side = _likes_users_base(repo_path) / username / commit_hash
        commit_side = _commit_like_path(repo_path, commit_hash, username)
        legacy_commit_side = _likes_commits_base(repo_path) / commit_hash / username
        _write_like(user_side, ts_ns)
        _write_like(commit_side, ts_ns)
        if legacy_commit_side.exists():
            legacy_commit_side.unlink()
        _truncate_journal_locked(repo_path)


def remove_like(repo_path: Path, username: str, commit_hash: str) -> None:
    username = _validate_username(username)
    commit_hash = _validate_commit_hash(commit_hash)
    with _lock_journal(repo_path, exclusive=True):
        if journal_has_pending(repo_path):
            _recover_journal_locked(repo_path)
        _append_journal(repo_path, f"DEL {username} {commit_hash}\n")
        user_side = _likes_users_base(repo_path) / username / commit_hash
        if user_side.exists():
            user_side.unlink()

        commit_side = _commit_like_path(repo_path, commit_hash, username)
        legacy_commit_side = _likes_commits_base(repo_path) / commit_hash / username
        if commit_side.exists():
            commit_side.unlink()
        if legacy_commit_side.exists():
            legacy_commit_side.unlink()
        _truncate_journal_locked(repo_path)


def likes_by_user(repo_path: Path, username: str) -> set[str]:
    username = _validate_username(username)
    _maybe_recover_from_journal(repo_path)
    user_dir = _likes_users_base(repo_path) / username
    if not user_dir.exists() or not user_dir.is_dir():
        return set()
    return {p.name for p in user_dir.iterdir() if p.is_file()}


def likes_by_commit(repo_path: Path, commit_hash: str) -> set[str]:
    commit_hash = _validate_commit_hash(commit_hash)
    _maybe_recover_from_journal(repo_path)
    commit_dir = _likes_commits_base(repo_path) / commit_hash
    if not commit_dir.exists() or not commit_dir.is_dir():
        return set()
    users_set: set[str] = set()
    for p in commit_dir.iterdir():
        if p.is_file():
            users_set.add(p.name)
    for bucket_dir in commit_dir.iterdir():
        if not bucket_dir.is_dir():
            continue
        for p in bucket_dir.iterdir():
            if p.is_file():
                users_set.add(p.name)
    return users_set
    