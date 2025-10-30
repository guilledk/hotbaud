import time
import json
import signal

from os import kill, killpg
from pathlib import Path
from typing import Literal

import msgspec


class ProcMeta(msgspec.Struct, frozen=True):
    pid: int
    log_file: str | None
    repo: str | None
    branch: str | None
    commit: str | None


def write_proc_meta(
    path: Path,
    pid: int,
    *,
    log_file: str | None = None,
    repo: str | None = None,
    branch: str | None = None,
    commit: str | None = None
) -> None:
    path.write_text(
        json.dumps(
            msgspec.to_builtins(
                ProcMeta(
                    pid=pid,
                    log_file=log_file,
                    repo=repo,
                    branch=branch,
                    commit=commit
                )
            ),
            indent=4
        )
    )


def read_proc_meta(path: Path) -> ProcMeta | None:
    try:
        return msgspec.json.decode(
            path.read_bytes(),
            type=ProcMeta
        )

    except Exception:
        return None


def is_pid_alive(meta: int | ProcMeta) -> bool:
    try:
        kill(meta if isinstance(meta, int) else meta.pid, 0)
        return True
    except OSError:
        return False


ProcStatus = Literal['meta-missing', 'not-running', 'stopped', 'forced', 'still-running']


def stop_by_proc_meta(path: Path, grace: int) -> tuple[ProcStatus, ProcMeta | None]:
    '''
    Stop the process recorded in pidfile.
    Returns (status, pid) where status is one of:
      'meta-missing' | 'not-running' | 'stopped' | 'forced' | 'still-running'
    '''
    pmeta = read_proc_meta(path)
    if not pmeta:
        return ('meta-missing', None)
    if not is_pid_alive(pmeta):
        path.unlink(missing_ok=True)
        return ('not-running', pmeta)
    try:
        killpg(pmeta.pid, signal.SIGTERM)
    except Exception:
        try:
            kill(pmeta.pid, signal.SIGTERM)
        except ProcessLookupError:
            path.unlink(missing_ok=True)
            return ('not-running', pmeta)
    deadline = time.time() + grace
    while time.time() < deadline:
        if not is_pid_alive(pmeta):
            path.unlink(missing_ok=True)
            return ('stopped', pmeta)
        time.sleep(0.2)
    # escalate
    try:
        killpg(pmeta.pid, signal.SIGKILL)
    except Exception:
        try:
            kill(pmeta.pid, signal.SIGKILL)
        except Exception:
            pass
    for _ in range(20):
        if not is_pid_alive(pmeta):
            path.unlink(missing_ok=True)
            return ('forced', pmeta)
        time.sleep(0.1)
    return ('still-running', pmeta)
