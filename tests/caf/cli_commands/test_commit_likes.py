from libcaf.constants import HASH_LENGTH
from libcaf.likes import add_like
from libcaf.repository import Repository
from pytest import CaptureFixture

from caf import cli_commands




def test_commit_likes_lists_users_for_commit(temp_repo: Repository, capsys: CaptureFixture[str]) -> None:
    cli_commands.add_user(working_dir_path=temp_repo.working_dir, username="goku")
    cli_commands.add_user(working_dir_path=temp_repo.working_dir, username="vegeta")
    assert cli_commands.commit(
        working_dir_path=temp_repo.working_dir,
        author="goku",
        message="gokuIsGOAT",
    ) == 0
    commit_hash = str(temp_repo.head_commit())
    add_like(temp_repo.repo_path(), "goku", commit_hash)
    add_like(temp_repo.repo_path(), "vegeta", commit_hash)
    assert cli_commands.commit_likes(working_dir_path=temp_repo.working_dir, commit_hash=commit_hash) == 0
    out = capsys.readouterr().out
    assert "goku" in out
    assert "vegeta" in out


def test_commit_likes_no_likes_prints_message(temp_repo: Repository, capsys: CaptureFixture[str]) -> None:
    assert cli_commands.commit(
        working_dir_path=temp_repo.working_dir,
        author="goku",
        message="gokuIsGOAT",
    ) == 0
    commit_hash = str(temp_repo.head_commit())
    assert cli_commands.commit_likes(working_dir_path=temp_repo.working_dir, commit_hash=commit_hash) == 0
    out = capsys.readouterr().out.lower()
    assert ("0 likes" in out) or (out.strip() == "")


def test_commit_likes_missing_commit_hash_fails(temp_repo: Repository, capsys: CaptureFixture[str]) -> None:
    assert cli_commands.commit_likes(working_dir_path=temp_repo.working_dir, commit_hash="") == -1
    assert "commit hash is required" in capsys.readouterr().err.lower()


def test_commit_likes_nonexistent_commit_hash_fails(temp_repo: Repository, capsys: CaptureFixture[str]) -> None:
    fake_commit_hash = "a" * HASH_LENGTH
    assert cli_commands.commit_likes(working_dir_path=temp_repo.working_dir, commit_hash=fake_commit_hash) == -1
    assert "does not exist" in capsys.readouterr().err.lower()