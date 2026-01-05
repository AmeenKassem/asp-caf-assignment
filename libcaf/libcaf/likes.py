""" Likes — on disk representation and primitives."""
from pathlib import Path
from .constants import HASH_CHARSET, HASH_LENGTH, LIKES_DIR, LIKES_USERS_DIR, LIKES_COMMITS_DIR
import os
from contextlib import contextmanager
from typing import Iterator


def init_likes(repo_path: Path) -> None:
    (repo_path / LIKES_DIR).mkdir(parents=True, exist_ok=True)
    (repo_path / LIKES_DIR / LIKES_USERS_DIR).mkdir(parents=True, exist_ok=True)
    (repo_path / LIKES_DIR / LIKES_COMMITS_DIR).mkdir(parents=True, exist_ok=True)


class LikeError(Exception):
    """Raised for like-related errors."""


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


def _likes_base(repo_path: Path) -> Path:
    return repo_path / LIKES_DIR


def _likes_users_base(repo_path: Path) -> Path:
    return _likes_base(repo_path) / LIKES_USERS_DIR

def _likes_commits_base(repo_path: Path) -> Path:
    return _likes_base(repo_path) / LIKES_COMMITS_DIR

def _likes_lock_path(repo_path: Path) -> Path:
    # Lock file lives under .caf/likes/.lock
    return _likes_base(repo_path) / ".lock"


@contextmanager
def _acquire_likes_lock(repo_path: Path) -> Iterator[None]:
    """
    Acquire an exclusive repo-wide lock for likes operations.

    This serializes all mutating operations (add/remove/update), guaranteeing
    the two-sided on-disk representation remains consistent under concurrency.
    """
    lock_path = _likes_lock_path(repo_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    f = open(lock_path, "a+", encoding="utf-8")

    try:
        try:
            import fcntl  # type: ignore
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        except ImportError:
            import msvcrt  # type: ignore
            f.seek(0)
            msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 1)
        yield
    finally:
        try:
            try:
                import fcntl  # type: ignore
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except ImportError:
                import msvcrt  # type: ignore
                f.seek(0)
                msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
        finally:
            f.close()


def add_like(repo_path: Path, username: str, commit_hash: str) -> None:
    """
    Add a like: username likes commit_hash.

    Creates both sides:
      - .caf/likes/likes_users/<username>/<commit_hash>
      - .caf/likes/likes_commits/<commit_hash>/<username>

    Idempotent: if the like already exists, this is a no-op.
    """
    username = _validate_username(username)
    commit_hash = _validate_commit_hash(commit_hash)
    with _acquire_likes_lock(repo_path):
        users_dir = _likes_users_base(repo_path) / username
        commits_dir = _likes_commits_base(repo_path) / commit_hash

        user_side = users_dir / commit_hash
        commit_side = commits_dir / username

        users_dir.mkdir(parents=True, exist_ok=True)
        commits_dir.mkdir(parents=True, exist_ok=True)

        user_side.touch(exist_ok=True)
        commit_side.touch(exist_ok=True)

    
def remove_like(repo_path: Path, username: str, commit_hash: str) -> None:
    """
    Remove a like: username unlikes commit_hash.

    Removes both sides if present:
      - .caf/likes/likes_users/<username>/<commit_hash>
      - .caf/likes/likes_commits/<commit_hash>/<username>

    Idempotent: if the like (or either side) is missing, this is a no-op.
    Also removes now-empty per-user / per-commit directories.
    """
    username = _validate_username(username)
    commit_hash = _validate_commit_hash(commit_hash)
    with _acquire_likes_lock(repo_path):
        users_dir = _likes_users_base(repo_path) / username
        commits_dir = _likes_commits_base(repo_path) / commit_hash

        user_side = users_dir / commit_hash
        commit_side = commits_dir / username

        if user_side.exists():
            user_side.unlink()

        if commit_side.exists():
            commit_side.unlink()

        try:
            if users_dir.exists() and users_dir.is_dir() and not any(users_dir.iterdir()):
                users_dir.rmdir()
        except OSError:
            pass

        try:
            if commits_dir.exists() and commits_dir.is_dir() and not any(commits_dir.iterdir()):
                commits_dir.rmdir()
        except OSError:
            pass

def list_likes_by_user(repo_path: Path, username: str) -> list[str]:
    """
    List commit hashes liked by the given user (sorted).
    """
    username = _validate_username(username)
    user_dir = _likes_users_base(repo_path) / username

    if not user_dir.exists() or not user_dir.is_dir():
        return []

    commits: list[str] = [p.name for p in user_dir.iterdir() if p.is_file()]
    commits.sort()
    return commits


def list_likes_by_commit(repo_path: Path, commit_hash: str) -> list[str]:
    """
    List usernames who liked the given commit (sorted).
    """
    commit_hash = _validate_commit_hash(commit_hash)
    commit_dir = _likes_commits_base(repo_path) / commit_hash

    if not commit_dir.exists() or not commit_dir.is_dir():
        return []

    users: list[str] = [p.name for p in commit_dir.iterdir() if p.is_file()]
    users.sort()
    return users

    """
    Update a like for a user: replace old_commit_hash with new_commit_hash.

    Implemented as remove(old) then add(new).
    """
    username = _validate_username(username)
    old_commit_hash = _validate_commit_hash(old_commit_hash)
    new_commit_hash = _validate_commit_hash(new_commit_hash)

    if old_commit_hash == new_commit_hash:
        return

    remove_like(repo_path, username, old_commit_hash)
    add_like(repo_path, username, new_commit_hash)
