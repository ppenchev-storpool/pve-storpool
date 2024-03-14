# SPDX-FileCopyrightText: StorPool <support@storpool.com>
# SPDX-License-Identifier: BSD-2-Clause
"""Make sure that `sp-pve-utils run` starts up at least."""

from __future__ import annotations

import json
import pathlib
import socket
import subprocess  # noqa: S404
import sys
import tempfile
import typing


if typing.TYPE_CHECKING:
    from typing import IO, Final


def test_run_help_noop() -> None:
    """Make sure that the `--help` and `--noop` options work."""
    watchdog_help: Final = subprocess.check_output(
        [sys.executable, "-m", "sp_pve_utils.watchdog_mux", "run", "--help"],  # noqa: S603
        encoding="UTF-8",
    )
    assert "--noop" in watchdog_help
    assert "Would listen on " not in watchdog_help

    watchdog_noop: Final = subprocess.check_output(
        [sys.executable, "-m", "sp_pve_utils.watchdog_mux", "run", "--noop"],  # noqa: S603
        encoding="UTF-8",
    )
    assert "--noop" not in watchdog_noop
    assert "Would listen on " in watchdog_noop

    status_help: Final = subprocess.check_output(
        [sys.executable, "-m", "sp_pve_utils.vm_status", "show", "--help"],  # noqa: S603
        encoding="UTF-8",
    )
    assert "VMID" in status_help


def test_wd_connect_disconnect() -> None:  # noqa: PLR0915
    """Test some basic functionality of `sp-watchdog-mux`."""
    print()
    with tempfile.TemporaryDirectory(prefix="sp-watchdog-mux-conndisc-") as tempd_obj:
        tempd: Final = pathlib.Path(tempd_obj)
        path_listen: Final = tempd / "listen.sock"
        path_active: Final = tempd / "active"

        def check_paths(*, listen: bool, active: bool) -> None:
            """Make sure the listening socket and the active marker are present or not."""
            assert not path_listen.is_symlink()
            if listen:
                assert path_listen.is_socket()
            else:
                assert not path_listen.exists()

            assert not path_active.is_symlink()
            if active:
                assert path_active.is_dir()
            else:
                assert not path_active.exists()

        print("Asking sp-watchdog-mux for its configuration")
        check_paths(listen=False, active=False)
        conf_raw: Final = subprocess.check_output(
            [  # noqa: S603
                sys.executable,
                "-m",
                "sp_pve_utils.watchdog_mux",
                "-l",
                path_listen,
                "-a",
                path_active,
                "show",
                "config",
            ],
            encoding="UTF-8",
        )
        check_paths(listen=False, active=False)
        conf: Final = json.loads(conf_raw)
        assert conf["format"]["version"] == {"major": 0, "minor": 1}
        assert conf["config"]["paths"]["listen"] == str(path_listen)
        assert conf["config"]["paths"]["active"] == str(path_active)

        print("Starting a sp-watchdog-mux process")
        check_paths(listen=False, active=False)
        with subprocess.Popen(
            [  # noqa: S603
                sys.executable,
                "-m",
                "sp_pve_utils.watchdog_mux",
                "-l",
                path_listen,
                "-a",
                path_active,
                "-d",
                "run",
            ],
            bufsize=1,
            encoding="UTF-8",
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        ) as proc:
            assert proc.stdout is not None
            assert proc.stderr is None

            def wait_for_line(stream: IO[str], expected: str, lines: list[str]) -> list[str]:
                """Wait for a line to show up on the child's standard output stream."""
                while True:
                    while lines:
                        line = lines.pop(0)
                        if expected in line:
                            print(f"Found {line!r}")
                            return lines

                    print("Waiting for output lines")
                    lines = [line for line in (stream.readline(),) if line]
                    print(f"Got {lines!r}")
                    assert lines

            try:
                print(f"Started sp-watchdog-mux as pid {proc.pid}")
                print("Waiting for the 'Starting up' line")
                lines = wait_for_line(proc.stdout, f"Starting up to listen on {path_listen}", [])

                print("Waiting for the 'listening on' line")
                lines = wait_for_line(proc.stdout, f"Listening on {path_listen}", lines)
                check_paths(listen=True, active=False)

                print("Waiting for a 'tick' line")
                lines = wait_for_line(proc.stdout, "Tick!", lines)
                check_paths(listen=True, active=False)

                print("Connecting to the server")
                conn: Final = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, 0)
                conn.connect(str(path_listen))
                print(f"Got a connection: {conn!r}")

                print("Waiting for a 'connection' line")
                lines = wait_for_line(proc.stdout, "New connection", lines)

                print("Waiting for a 'client connection' line")
                lines = wait_for_line(proc.stdout, "New client connection 0", lines)

                print("Waiting for a 'marker created' line")
                lines = wait_for_line(proc.stdout, "Active marker created", lines)
                check_paths(listen=True, active=True)

                print("Sending a zero byte")
                assert conn.send(bytes([0])) == 1

                print("Waiting for a 'got data' line")
                lines = wait_for_line(proc.stdout, "Client 0 said ", lines)
                check_paths(listen=True, active=True)

                print("Waiting for a 'checked in' line")
                lines = wait_for_line(proc.stdout, "Client 0 checked in", lines)
                check_paths(listen=True, active=True)

                print("Connecting to the server again")
                conn_too: Final = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, 0)
                conn_too.connect(str(path_listen))
                print(f"Got a connection: {conn_too!r}")

                print("Waiting for a 'connection' line")
                lines = wait_for_line(proc.stdout, "New connection", lines)

                print("Waiting for a 'client connection' line")
                lines = wait_for_line(proc.stdout, "New client connection 1", lines)
                check_paths(listen=True, active=True)

                print("Letting the server know the first connection is going away")
                assert conn.send(b"V") == 1

                print("Waiting for a 'got data' line")
                lines = wait_for_line(proc.stdout, "Client 0 said ", lines)

                print("Waiting for a 'going away' line")
                lines = wait_for_line(proc.stdout, "Client 0 said it might go away soon", lines)
                check_paths(listen=True, active=True)

                print("Closing the first connection")
                conn.close()

                print("Waiting for a 'gone' line")
                lines = wait_for_line(proc.stdout, "Client 0 went away gracefully", lines)
                check_paths(listen=True, active=True)

                print("Waiting for a 'cleaned up' line")
                lines = wait_for_line(proc.stdout, "Cleaned up after client 0", lines)
                check_paths(listen=True, active=True)

                print("Closing the second connection")
                conn_too.close()

                print("Waiting for a 'gone' line")
                lines = wait_for_line(
                    proc.stdout,
                    "Client 1 went away without saying goodbye",
                    lines,
                )

                print("Waiting for a 'marker removed' line")
                lines = wait_for_line(proc.stdout, "Removed the active marker", lines)
                check_paths(listen=True, active=False)

                print("Waiting for a 'cleaned up' line")
                lines = wait_for_line(proc.stdout, "Cleaned up after client 1", lines)
                check_paths(listen=True, active=False)

                print("Telling the child process to shut down gracefully")
                proc.terminate()

                print("Waiting for a 'going away' line")
                lines = wait_for_line(
                    proc.stdout,
                    "Somebody told us to go away via signal 15",
                    lines,
                )

                print("Reading the rest of the child process output")
                while True:
                    line = proc.stdout.readline()
                    if not line:
                        print("Done!")
                        break

                    print(f"Got {line!r}")

                print("Waiting for the child process to go away")
                res_int: Final = proc.wait()
                print(f"Got exit code {res_int}")
            finally:
                proc.kill()

        res: Final = proc.wait()
        assert res == 0
