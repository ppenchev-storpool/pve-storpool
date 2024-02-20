# SPDX-FileCopyrightText: StorPool <support@storpool.com>
# SPDX-License-Identifier: BSD-2-Clause
"""Do things, or do other things."""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import functools
import itertools
import json
import pathlib
import signal
import socket
import subprocess  # noqa: S404
import sys
import typing

import click

from . import defs
from . import util


if typing.TYPE_CHECKING:
    from collections.abc import Iterator
    from typing import Final


WD_ACTIVE_MARKER: Final = pathlib.Path("/run/watchdog-mux.active")
"""The default listening socket for the `sp-watchdog-mux` service."""

WD_SOCK_PATH: Final = pathlib.Path("/run/watchdog-mux.sock")
"""The default active connections marker for the `sp-watchdog-mux` service."""


@dataclasses.dataclass(frozen=True)
class Config(defs.Config):
    """Runtime configuration for the `sp-watchdog-mux` service."""

    active_marker: pathlib.Path
    """The path to the active connections marker directory."""

    listen_socket: pathlib.Path
    """The path to the Unix socket to listen on."""


@dataclasses.dataclass
class ConfigHolder:
    """Hold a `Config` object."""

    cfg: Config | None = None
    """The `Config` object stashed by the main function."""


def extract_cfg(ctx: click.Context) -> Config:
    """Extract the `Config` object that the main function built."""
    cfg_hold: Final = ctx.find_object(ConfigHolder)
    if cfg_hold is None:
        sys.exit("Internal error: no click config holder object")

    cfg: Final = cfg_hold.cfg
    if cfg is None:
        sys.exit("Internal error: no config in the click config holder")

    return cfg


@dataclasses.dataclass(frozen=True)
class MainMsg:
    """The top-level class for messages sent to the main thread."""


@dataclasses.dataclass(frozen=True)
class TimerMsg(MainMsg):
    """A second went by."""


@dataclasses.dataclass(frozen=True)
class NewConnMsg(MainMsg):
    """A new client connection has been established."""

    reader: asyncio.StreamReader
    """The stream to read incoming bytes from."""


@dataclasses.dataclass(frozen=True)
class ClientDoneMsg(MainMsg):
    """A client connection is done."""

    client_id: int
    """The internal ID of the client connection."""

    warned: bool
    """Did the client warn us that it might be going away?"""


@dataclasses.dataclass(frozen=True)
class SigStopMsg(MainMsg):
    """A "please stop" signal was received."""

    sig: int
    """The signal we received."""


@dataclasses.dataclass(frozen=True)
class WDTask:
    """A task to wait for: either a client connection or the listening task."""

    name: str
    """The internal name of the task."""

    task: asyncio.Task[None]
    """The task itself."""


@dataclasses.dataclass(frozen=True)
class WDClientTask(WDTask):
    """A client connection task."""

    client_id: int
    """The internal ID of this client connection."""


@dataclasses.dataclass(frozen=True)
class GlobalState:
    """The current state of the main loop: tasks, etc."""

    cfg: Config
    """The runtime configuration of the program."""

    mainq: asyncio.Queue[MainMsg]
    """The queue that tasks send messages on."""

    tasks: list[WDTask]
    """The tasks themselves."""

    tasks_lock: asyncio.Lock
    """The mutex that MUST be held while accessing `tasks` in ANY way."""

    client_id: Iterator[int]
    """The generator for successive connection IDs."""


async def do_signal(state: GlobalState, sig_sock: socket.socket) -> None:
    """Send a "oof, got a signal" message to the main thread when a signal arrives."""
    reader, _ = await asyncio.open_unix_connection(sock=sig_sock)
    state.cfg.log.debug("Waiting for signals on %(reader)r", {"reader": reader})
    while True:
        data = await reader.read(1)
        state.cfg.log.warning("Got some sort of signal: %(data)r", {"data": data})
        await state.mainq.put(SigStopMsg(data[0]))


async def do_timer(state: GlobalState) -> None:
    """Send a "hey, some time passed" message to the main queue every now and then."""
    while True:
        await asyncio.sleep(1)
        await state.mainq.put(TimerMsg())


async def do_listen(state: GlobalState) -> None:
    """Listen for incoming client connections, pass them on to the main thread."""

    async def got_client(reader: asyncio.StreamReader, _writer: asyncio.StreamWriter) -> None:
        """Handle a client connection."""
        state.cfg.log.info("New connection")
        await state.mainq.put(NewConnMsg(reader))

    srv: Final = await asyncio.start_unix_server(got_client, path=state.cfg.listen_socket)
    state.cfg.log.info(
        "Listening on %(sockets)s",
        {"sockets": ", ".join(sock.getsockname() for sock in srv.sockets)},
    )
    async with srv:
        await srv.serve_forever()


async def client_read(state: GlobalState, client_id: int, reader: asyncio.StreamReader) -> None:
    """Read data from a client."""
    warned = False
    state.cfg.log.debug("Waiting for stuff to come in via %(reader)r", {"reader": reader})
    while True:
        data = await reader.read(1)
        if not data:
            break
        state.cfg.log.debug(
            "Client %(client_id)d said %(data)r",
            {"client_id": client_id, "data": data},
        )
        if b"\x00" in data:
            state.cfg.log.debug("Client %(client_id)d checked in", {"client_id": client_id})
        if b"V" in data:
            state.cfg.log.info(
                "Client %(client_id)d said it might go away soon",
                {"client_id": client_id},
            )
            warned = True

    if warned:
        state.cfg.log.info("Client %(client_id)d went away gracefully", {"client_id": client_id})
    else:
        state.cfg.log.warning(
            "Client %(client_id)d went away without saying goodbye",
            {"client_id": client_id},
        )
    await state.mainq.put(ClientDoneMsg(client_id, warned))


@functools.singledispatch
async def handle_msg(msg: MainMsg, _state: GlobalState) -> bool:
    """Handle a single message."""
    sys.exit(f"sp-watchdog-mux internal error: unexpected mainq message {msg!r}")


@handle_msg.register
async def handle_msg_timer(_msg: TimerMsg, state: GlobalState) -> bool:
    """Check the active tasks."""
    async with state.tasks_lock:
        to_remove: Final[list[int]] = []
        for idx, task in ((idx, task) for idx, task in enumerate(state.tasks) if task.task.done()):
            state.cfg.log.warning(
                "Task %(name)s went away: %(task)r",
                {"name": task.name, "task": task},
            )
            with contextlib.suppress(asyncio.CancelledError):
                await task.task
            to_remove.append(idx)

        if to_remove:
            had_clients: Final = any(task for task in state.tasks if isinstance(task, WDClientTask))

            state.cfg.log.info("Removing %(count)d tasks", {"count": len(to_remove)})
            for idx in reversed(to_remove):
                state.tasks.pop(idx)

            if (
                had_clients
                and not any(task for task in state.tasks if isinstance(task, WDClientTask))
                and state.cfg.active_marker.exists()
            ):
                state.cfg.active_marker.rmdir()
                state.cfg.log.info("Removed the active marker")

        if not state.tasks:
            state.cfg.log.warning("Tick: no tasks left!")
            return False

        state.cfg.log.debug("Tick!")
        return True


@handle_msg.register
async def handle_msg_new_conn(msg: NewConnMsg, state: GlobalState) -> bool:
    """Spawn a "read bytes from the new connection" task."""
    client_id: Final = next(state.client_id)
    state.cfg.log.info("New client connection %(client_id)d", {"client_id": client_id})
    conn_task: Final = asyncio.create_task(client_read(state, client_id, msg.reader))
    task: Final = WDClientTask(f"client/{client_id}", conn_task, client_id)
    async with state.tasks_lock:
        if not any(task for task in state.tasks if isinstance(task, WDClientTask)):
            state.cfg.active_marker.mkdir(mode=0o700)
            state.cfg.log.info("Active marker created")

        state.tasks.append(task)

    return True


@handle_msg.register
async def handle_msg_client_done(msg: ClientDoneMsg, state: GlobalState) -> bool:
    """Remove a client connection fro the tasks list."""
    client_id: Final = msg.client_id
    async with state.tasks_lock:
        to_remove: Final = list(
            reversed(
                [
                    idx
                    for idx, task in enumerate(state.tasks)
                    if isinstance(task, WDClientTask) and task.client_id == client_id
                ],
            ),
        )
        if to_remove:
            state.cfg.log.info(
                "Removing %(count)d tasks, indices: %(indices)s",
                {"count": len(to_remove), "indices": to_remove},
            )
            for idx in to_remove:
                state.tasks.pop(idx)
            state.cfg.log.info("Left with %(count)d tasks", {"count": len(state.tasks)})

        if not any(task for task in state.tasks if isinstance(task, WDClientTask)):
            state.cfg.active_marker.rmdir()
            state.cfg.log.info("Removed the active marker")

    state.cfg.log.info("Cleaned up after client %(client_id)d", {"client_id": client_id})
    return True


@handle_msg.register
async def handle_msg_sig_stop(msg: SigStopMsg, state: GlobalState) -> bool:
    """Cancel all the tasks, go away."""
    state.cfg.log.warning("Somebody told us to go away via signal %(sig)d", {"sig": msg.sig})
    async with state.tasks_lock:
        for task in state.tasks:
            state.cfg.log.warning("- cancelling task %(name)s", {"name": task.name})
            task.task.cancel()

    return True


def setup_signalfd_task(state: GlobalState) -> asyncio.Task[None]:
    """Set up the signal handlers and the notification socket pair."""
    sig_socket_send, sig_socket_recv = socket.socketpair()
    state.cfg.log.debug(
        "Signal socket pair: send: %(send)r recv: %(recv)r",
        {"send": sig_socket_send, "recv": sig_socket_recv},
    )

    def handle_signal(sig: int) -> None:
        """Send a 'got a signal' message to the asyncio waiter."""
        sig_socket_send.send(bytes([sig]))

    for sig in (signal.SIGINT, signal.SIGQUIT, signal.SIGTERM):
        asyncio.get_running_loop().add_signal_handler(sig, functools.partial(handle_signal, sig))

    return asyncio.create_task(do_signal(state, sig_socket_recv))


async def do_run(cfg: Config) -> None:
    """Accept client connections, listen for their messages, report them."""
    cfg.log.info("Starting up to listen on %(listen)s", {"listen": cfg.listen_socket})

    state: Final = GlobalState(
        cfg=cfg,
        mainq=asyncio.Queue(maxsize=16),
        tasks=[],
        tasks_lock=asyncio.Lock(),
        client_id=itertools.count(),
    )

    async with state.tasks_lock:
        signalfd_task: Final = setup_signalfd_task(state)
        timer_task: Final = asyncio.create_task(do_timer(state))
        listen_task: Final = asyncio.create_task(do_listen(state))
        state.tasks.append(WDTask("listen", listen_task))

    while True:
        state.cfg.log.debug(
            "Waiting for something to happen with %(count)d tasks: %(tasks)s",
            {
                "count": len(state.tasks),
                "tasks": ", ".join(f"'{task.name}'" for task in state.tasks),
            },
        )
        msg = await state.mainq.get()
        if await handle_msg(msg, state):
            state.mainq.task_done()
        else:
            break

    for name, task in [("timer", timer_task), ("signalfd", signalfd_task)] + [
        (task.name, task.task) for task in state.tasks
    ]:
        if not task.done():
            state.cfg.log.info("Cancelling the %(name)s task", {"name": name})
            task.cancel()

        state.cfg.log.info("Waiting for the %(name)s task", {"name": name})
        try:
            await task
            state.cfg.log.info("The %(name)s task completed", {"name": name})
        except asyncio.CancelledError:
            state.cfg.log.warning("The %(name)s task was cancelled", {"name": name})

    state.cfg.log.info("Looks like we are done here")


def ensure_not_there(cfg: Config, lsock: pathlib.Path) -> None:
    """Make sure a path does not exist on the filesystem."""
    cfg.log.debug("Making sure %(path)s does not exist on the filesystem", {"path": lsock})
    if lsock.is_symlink():
        lsock.unlink()
    elif lsock.is_dir():
        subprocess.check_call(["rm", "-rf", "--", lsock])  # noqa: S603,S607
    elif lsock.exists():
        lsock.unlink()


@click.command(name="run")
@click.option("--noop", "-N", is_flag=True, help="no-operation mode; display what would be done")
@click.pass_context
def cmd_run(ctx: click.Context, *, noop: bool) -> None:
    """Accept client connections, listen for their messages, report them."""
    cfg: Final = extract_cfg(ctx)

    if noop:
        cfg.log.info(
            "Would listen on %(listen)s and create the %(active)s marker",
            {"listen": cfg.listen_socket, "active": cfg.active_marker},
        )
        return

    ensure_not_there(cfg, cfg.listen_socket)
    ensure_not_there(cfg, cfg.active_marker)

    asyncio.run(do_run(cfg))


@click.command(name="config")
@click.pass_context
def cmd_show_config(ctx: click.Context) -> None:
    """Show the current configuration settings."""
    cfg: Final = extract_cfg(ctx)

    print(  # noqa: T201
        json.dumps(
            {
                "format": {"version": {"major": 0, "minor": 1}},
                "config": {
                    "paths": {
                        "listen": str(cfg.listen_socket),
                        "active": str(cfg.active_marker),
                    },
                },
            },
        ),
    )


@click.group(name="show")
@click.pass_context
def cmd_show(_ctx: click.Context) -> None:
    """Display program parameters as requested."""


cmd_show.add_command(cmd_show_config)


@click.group(name="sp-watchdog-mux")
@click.option(
    "--features",
    is_flag=True,
    is_eager=True,
    callback=util.arg_features,
    help="display program features information and exit",
)
@click.option(
    "--active-marker",
    "-a",
    type=click.Path(file_okay=True, dir_okay=True, resolve_path=False, path_type=pathlib.Path),
    default=WD_ACTIVE_MARKER,
    help="the path to the directory to use as an active clients marker",
)
@click.option("--debug", "-d", is_flag=True, help="debug mode; display diagnostic output")
@click.option(
    "--listen-socket",
    "-l",
    type=click.Path(file_okay=True, dir_okay=True, resolve_path=False, path_type=pathlib.Path),
    default=WD_SOCK_PATH,
    help="the path to the Unix-domain socket to listen on",
)
@click.pass_context
def main(
    ctx: click.Context,
    *,
    active_marker: pathlib.Path,
    debug: bool,
    features: bool,
    listen_socket: pathlib.Path,
) -> None:
    """Do something, or do something else, who cares anyway."""
    if features:
        sys.exit("Internal error: how did we get to main() with features=True?")

    sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

    ctx.ensure_object(ConfigHolder)
    ctx.obj.cfg = Config(
        active_marker=active_marker,
        listen_socket=listen_socket,
        log=util.build_logger(debug=debug),
        debug=debug,
    )


main.add_command(cmd_run)
main.add_command(cmd_show)


if __name__ == "__main__":
    main()
