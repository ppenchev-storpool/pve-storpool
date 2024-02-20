# SPDX-FileCopyrightText: StorPool <support@storpool.com>
# SPDX-License-Identifier: BSD-2-Clause
"""Basic test for file importing."""

from __future__ import annotations

import pathlib
import re
import sys
import typing

import feature_check
from packaging import version as pkg_version

from sp_pve_utils import defs


_RE_FEATURE_VALUE: Final = re.compile(
    r"^ (?P<major> 0 | [1-9][0-9]* ) \. (?P<minor> 0 | [1-9][0-9]* )",
    re.X,
)


if typing.TYPE_CHECKING:
    from typing import Final


def test_version() -> None:
    """Make sure the `VERSION` variable has a sane value."""
    version: Final = pkg_version.Version(defs.VERSION)
    assert version > pkg_version.Version("0")


def test_features() -> None:
    """Make sure that the list of features looks right.

    It must include the program's name, and each value must be a X.Y number pair.
    """
    assert defs.FEATURES["sp-pve-utils"] == defs.VERSION
    for value in (value for name, value in defs.FEATURES.items() if name != "sp-pve-utils"):
        assert _RE_FEATURE_VALUE.match(value)


def test_features_run() -> None:
    """Run the command-line tool with the `--features` option, see what happens."""
    test_program: Final = pathlib.Path(sys.executable).parent / "sp-watchdog-mux"
    features: Final = feature_check.obtain_features(str(test_program))
    assert features == {
        name: feature_check.parse_version(version) for name, version in defs.FEATURES.items()
    }
