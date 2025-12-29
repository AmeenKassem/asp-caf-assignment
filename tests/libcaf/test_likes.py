from libcaf.repository import Repository, RepositoryError
from pytest import raises


def test_user_likes_initially_empty(temp_repo: Repository) -> None:
    temp_repo.add_user("alice")

    assert temp_repo.user_likes("alice") == []


def test_user_likes_unknown_user_raises(temp_repo: Repository) -> None:
    with raises(RepositoryError, match='User "alice" does not exist'):
        temp_repo.user_likes("alice")


def test_user_likes_returns_all_likes_for_user(temp_repo: Repository) -> None:
    temp_repo.add_user("alice")

    # No "like" API yet, so we seed the on-disk representation directly.
    user_dir = temp_repo.likes_users_dir() / "alice"
    user_dir.mkdir(parents=True)

    (user_dir / "bob").touch()
    (user_dir / "goku00ssj").touch()

    likes = temp_repo.user_likes("alice")

    # order is filesystem-dependent
    assert set(likes) == {"bob", "goku00ssj"}


def test_user_likes_ignores_non_files(temp_repo: Repository) -> None:
    temp_repo.add_user("alice")

    user_dir = temp_repo.likes_users_dir() / "alice"
    user_dir.mkdir(parents=True)

    (user_dir / "bob").touch()
    (user_dir / "nested").mkdir()

    likes = temp_repo.user_likes("alice")

    assert set(likes) == {"bob"}


def test_user_likes_invalid_username_raises_value_error(temp_repo: Repository) -> None:
    with raises(ValueError, match="Username is required"):
        temp_repo.user_likes("")

    with raises(ValueError, match="Invalid username"):
        temp_repo.user_likes("..")

    with raises(ValueError, match="path separators"):
        temp_repo.user_likes("a/b")
