"""modal_app.py deploys and runs only in the `critical` Modal environment."""

import os
import subprocess
import sys
from pathlib import Path

import modal
import pytest

import modal_app

REPO_ROOT = Path(__file__).resolve().parent.parent

# Runs the real Modal CLI, stubbing only what comes after it imports the app
# (the deploy RPC, and resolving the `modal run` target). Sockets are blocked,
# so any attempt to reach the network fails loudly instead of dialing out.
_CLI = """
import socket
import sys

def no_network(*args, **kwargs):
    print("NETWORK-ATTEMPT")
    raise OSError("network blocked in test")

socket.socket.connect = no_network
socket.socket.connect_ex = no_network
socket.create_connection = no_network
socket.getaddrinfo = no_network

import modal.__main__
import modal.cli.run as run

def reached(*args, **kwargs):
    print("REACHED-DOWNSTREAM")
    sys.exit(0)

run.deploy_app = reached
run._get_runnable_app = reached
sys.argv = ["modal", *sys.argv[1:]]
modal.__main__.main()
"""


def _modal_cli(
    tmp_path: Path, *args: str, extra_env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("MODAL_")}
    env.update(
        MODAL_CONFIG_PATH=str(tmp_path / ".modal.toml"),  # no profile, no credentials
        COLUMNS="400",
        **(extra_env or {}),
    )
    return subprocess.run(
        [sys.executable, "-c", _CLI, *args],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )


@pytest.mark.parametrize("command", [["deploy"], ["run"]], ids=["deploy", "run"])
@pytest.mark.parametrize(
    "env_args", [[], ["--env", "main"], ["--env", "staging"]], ids=["missing", "main", "staging"]
)
def test_cli_rejects_non_critical_environment(
    tmp_path: Path, command: list[str], env_args: list[str]
) -> None:
    target = "modal_app.py" if command == ["deploy"] else "modal_app.py::main"
    result = _modal_cli(tmp_path, *command, *env_args, target)
    output = result.stdout + result.stderr

    assert result.returncode != 0
    assert "must target the 'critical' Modal environment" in output
    assert "REACHED-DOWNSTREAM" not in output
    assert "NETWORK-ATTEMPT" not in output


@pytest.mark.parametrize("command", [["deploy"], ["run"]], ids=["deploy", "run"])
def test_cli_env_flag_overrides_critical_env_var(tmp_path: Path, command: list[str]) -> None:
    """`--env main` wins over MODAL_ENVIRONMENT=critical, and is rejected."""
    target = "modal_app.py" if command == ["deploy"] else "modal_app.py::main"
    result = _modal_cli(
        tmp_path, *command, "--env", "main", target, extra_env={"MODAL_ENVIRONMENT": "critical"}
    )
    output = result.stdout + result.stderr

    assert result.returncode != 0
    assert "must target the 'critical' Modal environment, got 'main'" in output
    assert "REACHED-DOWNSTREAM" not in output
    assert "NETWORK-ATTEMPT" not in output


@pytest.mark.parametrize(
    "args",
    [
        ["deploy", "--env", "critical", "modal_app.py"],
        ["run", "--env", "critical", "modal_app.py::main"],
    ],
    ids=["deploy", "run"],
)
def test_cli_critical_environment_imports_app(tmp_path: Path, args: list[str]) -> None:
    result = _modal_cli(tmp_path, *args)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "REACHED-DOWNSTREAM" in result.stdout
    assert "NETWORK-ATTEMPT" not in result.stdout


@pytest.mark.parametrize("environment", ["", "main", "staging"])
def test_guard_rejects_non_critical_config(
    monkeypatch: pytest.MonkeyPatch, environment: str
) -> None:
    monkeypatch.setenv("MODAL_ENVIRONMENT", environment)

    with pytest.raises(RuntimeError, match="must target the 'critical' Modal environment"):
        modal_app._require_critical_environment()


def test_guard_is_local_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MODAL_ENVIRONMENT", "main")
    monkeypatch.setattr(modal, "is_local", lambda: False)

    modal_app._require_critical_environment()


def test_deploy_workflow_targets_critical_and_is_held() -> None:
    workflow = (REPO_ROOT / ".github/workflows/deploy.yml").read_text()
    checks = (REPO_ROOT / ".github/workflows/checks.yml").read_text()
    on, jobs = workflow.split("\njobs:\n")
    deploy = jobs[jobs.index("  deploy:\n") :]
    gate = deploy.index("- name: Require critical deploys to be enabled")
    credentials = deploy.index("MODAL_TOKEN_SECRET")

    assert "pull_request" not in on
    assert "  pull_request:\n  workflow_call:\n" in checks
    assert "  checks:\n    # Pull requests run checks.yml directly.\n" in jobs
    assert "    uses: ./.github/workflows/checks.yml\n" in jobs
    assert "    if: github.ref == 'refs/heads/main'\n    # " in deploy
    assert "    needs: checks\n" in deploy
    # A failing step, not a job-level skip, before the step that holds credentials.
    assert deploy.count("MODAL_CRITICAL_DEPLOY_ENABLED") == 1
    assert (
        "        if: vars.MODAL_CRITICAL_DEPLOY_ENABLED != 'true'\n"
        "        run: |\n"
        "          echo '::error::Critical deployment is held; "
        "complete the operator cutover before enabling.'\n"
        "          exit 1\n"
    ) in deploy[gate:credentials]
    assert "run: uv run modal deploy --env critical modal_app.py\n" in deploy[credentials:]
    assert "modal deploy modal_app.py" not in workflow
