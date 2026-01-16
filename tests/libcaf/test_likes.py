from libcaf.constants import HASH_LENGTH
from libcaf.likes import (
    add_like,
    likes_by_commit,
    likes_by_user,
    remove_like,
)
from libcaf.repository import Repository
from pytest import raises


def test_likes_by_user_initially_empty(temp_repo: Repository) -> None:
    assert likes_by_user(temp_repo.repo_path(), "goku") == set()


def test_likes_by_commit_initially_empty(temp_repo: Repository) -> None:
    commit = "a" * HASH_LENGTH
    assert likes_by_commit(temp_repo.repo_path(), commit) == set()


def test_add_like_creates_both_sides(temp_repo: Repository) -> None:
    repo_path = temp_repo.repo_path()
    user = "goku"
    commit = "b" * HASH_LENGTH

    add_like(repo_path, user, commit)

    assert set(likes_by_user(repo_path, user)) == {commit}
    assert set(likes_by_commit(repo_path, commit)) == {user}


def test_add_like_idempotent(temp_repo: Repository) -> None:
    repo_path = temp_repo.repo_path()
    user = "vegeta"
    commit = "c" * HASH_LENGTH

    add_like(repo_path, user, commit)
    add_like(repo_path, user, commit)

    assert set(likes_by_user(repo_path, user)) == {commit}
    assert set(likes_by_commit(repo_path, commit)) == {user}


def test_remove_like_removes_both_sides(temp_repo: Repository) -> None:
    repo_path = temp_repo.repo_path()
    user = "cell"
    commit = "d" * HASH_LENGTH

    add_like(repo_path, user, commit)
    remove_like(repo_path, user, commit)

    assert likes_by_user(repo_path, user) == set()
    assert likes_by_commit(repo_path, commit) == set()


def test_remove_like_missing_is_idempotent(temp_repo: Repository) -> None:
    repo_path = temp_repo.repo_path()
    user = "goku"
    commit = "e" * HASH_LENGTH
    remove_like(repo_path, user, commit)
    remove_like(repo_path, user, commit)

    assert likes_by_user(repo_path, user) == set()
    assert likes_by_commit(repo_path, commit) == set()


def test_remove_like_existing_is_idempotent(temp_repo: Repository) -> None:
    repo_path = temp_repo.repo_path()
    user = "goku"
    commit = "e" * HASH_LENGTH

    add_like(repo_path, user, commit)

    remove_like(repo_path, user, commit)
    remove_like(repo_path, user, commit)

    assert likes_by_user(repo_path, user) == set()
    assert likes_by_commit(repo_path, commit) == set()


def test_invalid_username_raises_value_error(temp_repo: Repository) -> None:
    repo_path = temp_repo.repo_path()
    commit = "a" * HASH_LENGTH

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
