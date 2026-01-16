from libcaf.constants import HASH_LENGTH
from libcaf.likes import add_like
from libcaf.repository import Repository
from pytest import CaptureFixture

from caf import cli_commands



def test_user_likes_lists_likes_for_user(temp_repo: Repository, capsys: CaptureFixture[str]) -> None:
    cli_commands.add_user(working_dir_path=temp_repo.working_dir, username="goku")

    commit1 = "a" * HASH_LENGTH
    commit2 = "b" * HASH_LENGTH

    add_like(temp_repo.repo_path(), "goku", commit1)
    add_like(temp_repo.repo_path(), "goku", commit2)

    assert cli_commands.user_likes(working_dir_path=temp_repo.working_dir, username="goku") == 0
    out = capsys.readouterr().out
    assert commit1 in out
    assert commit2 in out


def test_user_likes_no_likes_prints_message(temp_repo: Repository, capsys: CaptureFixture[str]) -> None:
    cli_commands.add_user(working_dir_path=temp_repo.working_dir, username="vegeta")

    assert cli_commands.user_likes(working_dir_path=temp_repo.working_dir, username="vegeta") == 0
    out = capsys.readouterr().out.lower()
    assert ("0 likes" in out)


def test_user_likes_missing_user_fails(temp_repo: Repository, capsys: CaptureFixture[str]) -> None:
    assert cli_commands.user_likes(working_dir_path=temp_repo.working_dir, username="cell") == -1
    assert "does not exist" in capsys.readouterr().err.lower()


def test_user_likes_missing_username_fails(temp_repo: Repository, capsys: CaptureFixture[str]) -> None:
    assert cli_commands.user_likes(working_dir_path=temp_repo.working_dir, username="") == -1
    assert "username is required" in capsys.readouterr().err.lower()
