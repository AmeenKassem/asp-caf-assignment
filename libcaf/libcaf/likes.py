""" Likes — on disk representation and primitives."""
from pathlib import Path
from .constants import HASH_CHARSET, HASH_LENGTH, LIKES_DIR, LIKES_USERS_DIR, LIKES_COMMITS_DIR
import os
from contextlib import contextmanager
from typing import Iterator
import fcntl
import hashlib
import time
import shutil


def _write_like_sot_with_ts(path: Path, ts_ns: int) -> None:
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
    with _acquire_locks(_journal_lock_path(repo_path), exclusive=True):
        _recover_journal_locked(repo_path)

        commits_base = _likes_commits_base(repo_path)
        if commits_base.exists():
            shutil.rmtree(commits_base)
        commits_base.mkdir(parents=True, exist_ok=True)

        users_base = _likes_users_base(repo_path)
        if not users_base.exists():
            return

        for user_dir in users_base.iterdir():
            if not user_dir.is_dir():
                continue
            username = user_dir.name
            for p in user_dir.iterdir():
                if not p.is_file():
                    continue
                commit_hash = p.name
                commit_side = _commit_like_path(repo_path, commit_hash, username)
                commit_side.parent.mkdir(parents=True, exist_ok=True)
                commit_side.touch(exist_ok=True)


def _journal_path(repo_path: Path) -> Path:
    return _likes_base(repo_path) / "journal.log"


def _journal_lock_path(repo_path: Path) -> Path:
    return _likes_locks_base(repo_path) / "journal.lock"


def _new_txid() -> str:
    return f"{time.time_ns()}-{os.getpid()}"


def _append_journal(repo_path: Path, line: str) -> None:
    jp = _journal_path(repo_path)
    jp.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(jp, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o600)
    try:
        os.write(fd, line.encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)


def _recover_journal_locked(repo_path: Path) -> None:
    jp = _journal_path(repo_path)
    if not jp.exists():
        return

    pending: dict[str, tuple[str, str, str, int | None]] = {}

    with jp.open("r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split()
            if not parts:
                continue

            txid = parts[0]

            if len(parts) >= 2 and parts[1] == "DONE":
                pending.pop(txid, None)
                continue

            if len(parts) == 4 and parts[1] == "DEL":
                pending[txid] = ("DEL", parts[2], parts[3], None)
                continue

            if len(parts) == 5 and parts[1] == "ADD":
                try:
                    ts_ns = int(parts[4])
                except ValueError:
                    ts_ns = None
                pending[txid] = ("ADD", parts[2], parts[3], ts_ns)
                continue

            if len(parts) == 4 and parts[1] == "ADD":
                pending[txid] = ("ADD", parts[2], parts[3], None)
                continue

    for (op, username, commit_hash, ts_ns) in pending.values():
        user_side = (_likes_users_base(repo_path) / username) / commit_hash

        commit_side = _commit_like_path(repo_path, commit_hash, username)
        legacy_commit_side = _likes_commits_base(repo_path) / commit_hash / username

        if op == "ADD":
            _write_like_sot_with_ts(user_side, ts_ns if ts_ns is not None else time.time_ns())

            commit_side.parent.mkdir(parents=True, exist_ok=True)
            commit_side.touch(exist_ok=True)

            if legacy_commit_side.exists():
                legacy_commit_side.unlink()

        elif op == "DEL":
            if user_side.exists():
                user_side.unlink()

            if commit_side.exists():
                commit_side.unlink()

            if legacy_commit_side.exists():
                legacy_commit_side.unlink()
                
    tmp = jp.with_suffix(".log.tmp")
    fd = os.open(tmp, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp, jp)



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


def _user_lock_path(repo_path: Path, username: str) -> Path:
    return _likes_locks_base(repo_path) / "users" / f"{username}.lock"


def _commit_lock_path(repo_path: Path, commit_hash: str) -> Path:
    return _likes_locks_base(repo_path) / "commits" / f"{commit_hash}.lock"


@contextmanager
def _acquire_locks(*lock_paths: Path, exclusive: bool) -> Iterator[None]:
    paths = sorted(lock_paths)
    lock_type = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH

    fds: list[int] = []
    try:
        for p in paths:
            p.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(p, os.O_CREAT | os.O_RDWR, 0o600)
            fds.append(fd)
            fcntl.flock(fd, lock_type)
        yield
    finally:
        for fd in reversed(fds):
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

def add_like(repo_path: Path, username: str, commit_hash: str) -> None:
    username = _validate_username(username)
    commit_hash = _validate_commit_hash(commit_hash)
    with _acquire_locks(
        _journal_lock_path(repo_path),
        _user_lock_path(repo_path, username),
        _commit_lock_path(repo_path, commit_hash),
        exclusive=True,
    ):
        _recover_journal_locked(repo_path)
        txid = _new_txid()
        ts_ns = time.time_ns()
        _append_journal(repo_path, f"{txid} ADD {username} {commit_hash} {ts_ns}\n")

        user_side = (_likes_users_base(repo_path) / username) / commit_hash
        _write_like_sot_with_ts(user_side, ts_ns)

    
        commit_side = _commit_like_path(repo_path,commit_hash,username)
        commit_side.parent.mkdir(parents=True, exist_ok=True)
        commit_side.touch(exist_ok=True)

        _append_journal(repo_path, f"{txid} DONE\n")
    
def remove_like(repo_path: Path, username: str, commit_hash: str) -> None:
    username = _validate_username(username)
    commit_hash = _validate_commit_hash(commit_hash)
    with _acquire_locks(
        _journal_lock_path(repo_path),
        _user_lock_path(repo_path, username),
        _commit_lock_path(repo_path, commit_hash),
        exclusive=True,
    ):
        _recover_journal_locked(repo_path)
        txid = _new_txid()
        _append_journal(repo_path, f"{txid} DEL {username} {commit_hash}\n")
        
        users_dir = _likes_users_base(repo_path) / username
        user_side = users_dir / commit_hash
        if user_side.exists():
            user_side.unlink()
        
        commit_side = _commit_like_path(repo_path,commit_hash,username)
        legacy_commit_side = _likes_commits_base(repo_path) / commit_hash / username
        if commit_side.exists():
            commit_side.unlink()
        if legacy_commit_side.exists():
            legacy_commit_side.unlink()
        
        _append_journal(repo_path, f"{txid} DONE\n")


def list_likes_by_user(repo_path: Path, username: str) -> list[str]:
    username = _validate_username(username)
    with _acquire_locks(
        _user_lock_path(repo_path, username),
        exclusive=False,
    ):
        user_dir = _likes_users_base(repo_path) / username
        if not user_dir.exists() or not user_dir.is_dir():
            return []
        commits: list[str] = [p.name for p in user_dir.iterdir() if p.is_file()]
        return commits


def list_likes_by_commit(repo_path: Path, commit_hash: str) -> list[str]:
    commit_hash = _validate_commit_hash(commit_hash)
    with _acquire_locks(
        _commit_lock_path(repo_path, commit_hash),
        exclusive=False,
    ):
        commit_dir = _likes_commits_base(repo_path) / commit_hash
        if not commit_dir.exists() or not commit_dir.is_dir():
            return []
        users_set : set[str] = set()
        for p in commit_dir.iterdir():
            if p.is_file():
                users_set.add(p.name)
        for bucket_dir in commit_dir.iterdir():
            if not bucket_dir.is_dir():
                continue
            for p in bucket_dir.iterdir():
                if p.is_file():
                    users_set.add(p.name) 
        return list(users_set)
    