from libcaf.constants import (
    HASH_LENGTH,
    LIKES_COMMITS_DIR,
    LIKES_DIR,
    LIKES_USERS_DIR,
)
from libcaf.likes import (
    add_like,
    list_likes_by_commit,
    list_likes_by_user,
    remove_like,
)
from libcaf.repository import Repository
from pytest import raises


def _fake_hash(ch: str) -> str:
    return (ch * HASH_LENGTH)[:HASH_LENGTH]


def test_list_likes_by_user_initially_empty(temp_repo: Repository) -> None:
    assert list_likes_by_user(temp_repo.repo_path(), "goku") == []


def test_list_likes_by_commit_initially_empty(temp_repo: Repository) -> None:
    commit = _fake_hash("a")
    assert list_likes_by_commit(temp_repo.repo_path(), commit) == []


def test_add_like_creates_both_sides(temp_repo: Repository) -> None:
    repo_path = temp_repo.repo_path()
    user = "goku"
    commit = _fake_hash("b")

    add_like(repo_path, user, commit)

    assert list_likes_by_user(repo_path, user) == [commit]
    assert list_likes_by_commit(repo_path, commit) == [user]


def test_add_like_idempotent(temp_repo: Repository) -> None:
    repo_path = temp_repo.repo_path()
    user = "vegeta"
    commit = _fake_hash("c")

    add_like(repo_path, user, commit)
    add_like(repo_path, user, commit)

    assert list_likes_by_user(repo_path, user) == [commit]
    assert list_likes_by_commit(repo_path, commit) == [user]


def test_remove_like_removes_both_sides(temp_repo: Repository) -> None:
    repo_path = temp_repo.repo_path()
    user = "cell"
    commit = _fake_hash("d")

    add_like(repo_path, user, commit)
    remove_like(repo_path, user, commit)

    assert list_likes_by_user(repo_path, user) == []
    assert list_likes_by_commit(repo_path, commit) == []


def test_remove_like_idempotent(temp_repo: Repository) -> None:
    repo_path = temp_repo.repo_path()
    user = "goku"
    commit = _fake_hash("e")

    remove_like(repo_path, user, commit)

    add_like(repo_path, user, commit)
    remove_like(repo_path, user, commit)
    remove_like(repo_path, user, commit)

    assert list_likes_by_user(repo_path, user) == []
    assert list_likes_by_commit(repo_path, commit) == []

    repo_path = temp_repo.repo_path()
    user = "vegeta"
    old_commit = _fake_hash("f")
    new_commit = _fake_hash("1")

    add_like(repo_path, user, old_commit)
    remove_like(repo_path, user, old_commit)
    add_like(repo_path, user, new_commit)

    assert list_likes_by_user(repo_path, user) == [new_commit]
    assert list_likes_by_commit(repo_path, old_commit) == []
    assert list_likes_by_commit(repo_path, new_commit) == [user]

def test_invalid_username_raises_value_error(temp_repo: Repository) -> None:
    repo_path = temp_repo.repo_path()
    commit = _fake_hash("a")

    with raises(ValueError, match="Username is required"):
        add_like(repo_path, "", commit)

    with raises(ValueError, match="Invalid username"):
        add_like(repo_path, "..", commit)

    with raises(ValueError, match="path separators"):
        add_like(repo_path, "a/b", commit)


def test_invalid_commit_hash_raises_value_error(temp_repo: Repository) -> None:
    repo_path = temp_repo.repo_path()

    with raises(ValueError, match="Commit hash is required"):
        add_like(repo_path, "goku", "")

    with raises(ValueError, match="Invalid commit hash"):
        add_like(repo_path, "goku", "not-a-hash")
