# SPDX-FileCopyrightText: StorPool <support@storpool.com>
# SPDX-License-Identifier: BSD-2-Clause
"""Do things, or do other things."""

from __future__ import annotations

import dataclasses
import json
import pathlib
import platform
import re
import subprocess  # noqa: S404
import sys
import typing

import click

from . import defs
from . import util


if sys.version_info >= (3, 11):
    import enum as strenum
else:
    from backports import strenum


if typing.TYPE_CHECKING:
    from typing import Final


PVE_NODES: Final = pathlib.Path("/etc/pve/nodes")
"""The root of PVE's cluster nodes status tree."""

RE_QM_STATUS: Final = re.compile(r"^ status: \s+ (?P<status> \S+ ) \s* $", re.X)
"""The pattern to match the output of `qm status`."""


class VMStatus(strenum.StrEnum):
    """The VM states we can handle."""

    RUNNING = "running"

    STOPPED = "stopped"

    UNKNOWN = "unknown"

    def __str__(self) -> str:
        """Use the state name as the stringified output."""
        return self.value


def status_from_string(value: str) -> VMStatus:
    """Parse a string into a status value."""
    for known in (VMStatus.RUNNING, VMStatus.STOPPED):
        if value == known:
            return known

    return VMStatus.UNKNOWN


@dataclasses.dataclass(frozen=True)
class Config(defs.Config):
    """Runtime configuration for the `sp-vm-status` tool."""


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


def get_local_status(
    cfg: Config,
    vmid: int,
    node: str,
    pve_nodes: list[str],
) -> dict[str, VMStatus]:
    """Query the local Proxmox node for the VM status."""
    if node not in pve_nodes:
        return {}

    return {node: get_qm_status_output(cfg, vmid, [], f"local VM {vmid}")}


def get_qm_status_output(cfg: Config, vmid: int, remote_cmd: list[str], tag: str) -> VMStatus:
    """Get the status for a single VM, local or remote."""
    cfg.log.info("Asking the local `qm` instance about VM %(vmid)d", {"vmid": vmid})
    prog: Final = remote_cmd[0] if remote_cmd else "qm"
    res: Final = subprocess.run(
        [  # noqa: S603,S607
            "timelimit",
            "-pq",
            "-t",
            "1",
            "-T",
            "1",
            "--",
            *remote_cmd,
            "qm",
            "status",
            "--",
            str(vmid),
        ],
        check=False,
        encoding="UTF-8",
        stdout=subprocess.PIPE,
    )
    if res.returncode:
        cfg.log.error(
            "Could not check the %(tag)s: `%(prog)s` exit code %(res)d",
            {"tag": tag, "prog": prog, "res": res.returncode},
        )
        return VMStatus.UNKNOWN
    if res.stdout is None:
        raise RuntimeError(repr((vmid, remote_cmd, res)))
    lines: Final = res.stdout.splitlines()
    if len(lines) != 1:
        cfg.log.error(
            "Unexpected output from `%(prog)s` for %(tag)s: %(lines)r",
            {"tag": tag, "prog": prog, "lines": lines},
        )
        return VMStatus.UNKNOWN
    status_data: Final = RE_QM_STATUS.match(lines[0])
    if not status_data:
        cfg.log.error(
            "Unexpected output from `%(prog)s` for %(tag)s: %(lines)r",
            {"tag": tag, "prog": prog, "lines": lines},
        )
        return VMStatus.UNKNOWN

    reported: Final = status_data.group("status")
    cfg.log.info(
        "- got status '%(reported)s' for %(tag)s",
        {"tag": tag, "reported": reported},
    )
    status: Final = status_from_string(reported)
    if status == VMStatus.UNKNOWN:
        cfg.log.warning(
            "Unexpected status '%(reported)s' for %(tag)s",
            {"tag": tag, "reported": reported},
        )
    return status


def get_remote_status(
    cfg: Config,
    vmid: int,
    node: str,
    pve_nodes: list[str],
) -> dict[str, VMStatus]:
    """Query remote Proxmox nodes for the VM status."""
    return {
        remote: get_qm_status_output(
            cfg,
            vmid,
            ["ssh", "--", f"root@{remote}"],
            f"remote VM {vmid} on remote {remote}",
        )
        for remote in pve_nodes
        if remote != node
    }


@click.command(name="show")
@click.argument("vmid", type=int)
@click.pass_context
def cmd_show(ctx: click.Context, *, vmid: int) -> None:
    """Show the current configuration settings."""
    cfg: Final = extract_cfg(ctx)

    node: Final = platform.node()

    cfg.log.info("Looking for the configuration of VM %(vmid)d", {"vmid": vmid})
    pve_nodes: Final = sorted(
        path.relative_to(PVE_NODES).parts[0]
        for path in (
            subdir / "qemu-server" / f"{vmid}.conf"
            for subdir in PVE_NODES.iterdir()
            if subdir.is_dir()
        )
        if path.is_file()
    )

    local: Final = get_local_status(cfg, vmid, node, pve_nodes)
    cfg.log.debug("Got local status %(local)r", {"local": local})

    remote: Final = get_remote_status(cfg, vmid, node, pve_nodes)
    cfg.log.debug("Got remote status %(remote)r", {"remote": remote})

    print(json.dumps({"data": {"vmid": vmid, "local": local, "remote": remote}}, indent=2))  # noqa: T201


@click.group(name="sp-pve-utils")
@click.option(
    "--features",
    is_flag=True,
    is_eager=True,
    callback=util.arg_features,
    help="display program features information and exit",
)
@click.option("--debug", "-d", is_flag=True, help="debug mode; display diagnostic output")
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="verbose operation; display informational messages",
)
@click.pass_context
def main(
    ctx: click.Context,
    *,
    debug: bool,
    features: bool,
    verbose: bool,
) -> None:
    """Do something, or do something else, who cares anyway."""
    if features:
        sys.exit("Internal error: how did we get to main() with features=True?")

    ctx.ensure_object(ConfigHolder)
    ctx.obj.cfg = Config(
        log=util.build_logger(debug=debug, verbose=verbose, all_stderr=True),
        debug=debug,
    )


main.add_command(cmd_show)


if __name__ == "__main__":
    main()
