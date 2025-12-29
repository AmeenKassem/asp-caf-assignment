from libcaf.constants import DEFAULT_REPO_DIR
from libcaf.repository import Repository
from pytest import CaptureFixture

from caf import cli_commands


def test_user_likes_lists_likes_for_user(temp_repo: Repository, capsys: CaptureFixture[str]) -> None:
    cli_commands.add_user(working_dir_path=temp_repo.working_dir, username="alice")

    # Seed likes directly (no like API yet)
    likes_user_dir = temp_repo.working_dir / DEFAULT_REPO_DIR / "likes" / "likes_users" / "alice"
    likes_user_dir.mkdir(parents=True)

    (likes_user_dir / "commit1").touch()
    (likes_user_dir / "commit2").touch()

    assert cli_commands.user_likes(working_dir_path=temp_repo.working_dir, username="alice") == 0
    out = capsys.readouterr().out
    assert "commit1" in out
    assert "commit2" in out


def test_user_likes_no_likes_prints_message(temp_repo: Repository, capsys: CaptureFixture[str]) -> None:
    cli_commands.add_user(working_dir_path=temp_repo.working_dir, username="alice")

    assert cli_commands.user_likes(working_dir_path=temp_repo.working_dir, username="alice") == 0
    out = capsys.readouterr().out.lower()
    assert ("no likes" in out) or (out.strip() == "")


def test_user_likes_missing_user_fails(temp_repo: Repository, capsys: CaptureFixture[str]) -> None:
    assert cli_commands.user_likes(working_dir_path=temp_repo.working_dir, username="alice") == -1
    assert "does not exist" in capsys.readouterr().err.lower()


def test_user_likes_missing_username_fails(temp_repo: Repository, capsys: CaptureFixture[str]) -> None:
    assert cli_commands.user_likes(working_dir_path=temp_repo.working_dir, username="") == -1
    assert "username is required" in capsys.readouterr().err.lower()
