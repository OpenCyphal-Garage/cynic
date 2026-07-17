"""End-to-end tests for cynic. The same cases run over UDP and over SocketCAN (vcan0)."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
import signal
import struct
import subprocess
import sys
import time
import uuid
from datetime import datetime
from importlib.machinery import SourceFileLoader
from importlib.util import module_from_spec, spec_from_loader
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any

import pytest
import pydsdl  # type: ignore[import-untyped]
from pycyphal2 import DeliveryError
from rich.console import Console

CYNIC = Path(__file__).parent.parent / "cynic.py"
VCAN = "vcan0"
TIMEOUT = 3.0
SETTLE = 10.0
GOSSIP_PERIOD = 5.0  # pycyphal2._node.GOSSIP_PERIOD

_spec = spec_from_loader("cynic", SourceFileLoader("cynic", str(CYNIC)))
assert _spec and _spec.loader
cynic = module_from_spec(_spec)
sys.modules["cynic"] = cynic
_spec.loader.exec_module(cynic)

RESPONDER = """
import asyncio, sys
from pycyphal2 import Instant, Node

async def main(iface, topic, reply, count):
    if iface:
        from pycyphal2.can import CANTransport
        from pycyphal2.can.socketcan import SocketCANInterface
        transport = CANTransport.new(SocketCANInterface(iface))
    else:
        from pycyphal2.udp import UDPTransport
        transport = UDPTransport.new()
    node = Node.new(transport, "responder/")
    sub = node.subscribe(topic)
    print("READY", flush=True)
    async for arrival in sub:
        for i in range(count):
            await arrival.breadcrumb(Instant.now() + 5.0, reply + str(i).encode(), reliable=True)

asyncio.run(main(sys.argv[1], sys.argv[2], sys.argv[3].encode(), int(sys.argv[4])))
"""


def vcan_up() -> bool:
    r = subprocess.run(["ip", "link", "show", VCAN], capture_output=True)
    return r.returncode == 0


@pytest.fixture(params=["udp", "vcan"])
def link(request: pytest.FixtureRequest) -> list[str]:
    """CLI args selecting the transport, plus the equivalent responder argv[1]."""
    if request.param == "vcan":
        if not vcan_up():
            pytest.skip(
                f"{VCAN} absent; create it with: sudo ip link add dev {VCAN} type vcan && sudo ip link set up {VCAN}"
            )
        return ["--can", VCAN]
    return []


@pytest.fixture
def topic() -> str:
    return f"cynictest/{uuid.uuid4().hex[:12]}"


def payload_eq(received: bytes, sent: bytes) -> bool:
    """Cyphal/CAN FD pads frames up to the next DLC, so trailing zeros may be appended."""
    return received[: len(sent)] == sent and set(received[len(sent) :]) <= {0}


def run_cynic(
    link: list[str], *args: str, timeout: float = 30.0, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    argv = [sys.executable, str(CYNIC), *link, "--timeout", str(TIMEOUT), *args]
    return subprocess.run(argv, capture_output=True, text=True, timeout=timeout, env={**os.environ, **(env or {})})


def run_cynic_bytes(
    link: list[str], *args: str, input: bytes | None = None, timeout: float = 30.0
) -> subprocess.CompletedProcess[bytes]:
    argv = [sys.executable, str(CYNIC), *link, "--timeout", str(TIMEOUT), *args]
    return subprocess.run(argv, input=input, capture_output=True, timeout=timeout)


def make_dsdl(tmp_path: Path, service: bool = False) -> tuple[Path, pydsdl.CompositeType]:
    root = tmp_path / "types" / "demo"
    root.mkdir(parents=True)
    file = root / "Test.1.0.dsdl"
    file.write_text(
        "uint8 request\n@sealed\n---\nbyte[<=3] response\n@sealed\n"
        if service
        else "uint8 value\nbyte[<=3] data\n@sealed\n"
    )
    return file, pydsdl.read_files(file, root)[0][0]


def test_config_defaults_from_file_override_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    (tmp_path / "cynic.toml").write_text(
        'can = "from-config"\nbitrate = 500000\ntimeout = 2.5\nverbose = 1\nunused = "ignored"\n'
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["cynic", "ls"])
    monkeypatch.setenv("CYNIC_CAN", "from-environment")
    monkeypatch.setenv("CYNIC_BITRATE", "250000")

    config = cynic.resolve_config()

    assert isinstance(config, cynic.LsConfig)
    assert (config.can, config.bitrate, config.timeout, config.verbose) == ("from-config", 500000, 2.5, 1)
    assert config.output is cynic.json_output


def test_config_defaults_from_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["cynic", "ls"])
    monkeypatch.setenv("CYNIC_CAN", "from-environment")
    monkeypatch.setenv("CYNIC_BITRATE", "250000")

    config = cynic.resolve_config()

    assert isinstance(config, cynic.LsConfig)
    assert (config.can, config.bitrate, config.timeout, config.verbose) == ("from-environment", 250000, 10.0, 0)
    assert config.output is cynic.json_output


def test_empty_environment_bitrate_is_ignored(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["cynic", "ls"])
    monkeypatch.setenv("CYNIC_BITRATE", "")

    config = cynic.resolve_config()

    assert isinstance(config, cynic.LsConfig)
    assert (config.can, config.bitrate, config.timeout, config.verbose) == (None, 1_000_000, 10.0, 0)


def test_cli_options_override_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    (tmp_path / "cynic.toml").write_text('can = "/does/not/exist/*"\nbitrate = 500000\ntimeout = 2.5\nverbose = 1\n')
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        ["cynic", "--can", "literal*", "--bitrate", "1000000", "--timeout", "3", "-vv", "ls"],
    )
    monkeypatch.setenv("CYNIC_CAN", "from-environment")
    monkeypatch.setenv("CYNIC_BITRATE", "250000")

    config = cynic.resolve_config()

    assert isinstance(config, cynic.LsConfig)
    assert (config.can, config.bitrate, config.timeout, config.verbose) == ("literal*", 1_000_000, 3.0, 2)


def test_options_may_surround_the_subcommand(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("CYNIC_CAN", raising=False)
    monkeypatch.delenv("CYNIC_BITRATE", raising=False)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "cynic",
            "-v",
            "--can",
            "before-command",
            "ls",
            "--can",
            "after-command",
            "--bitrate",
            "500000",
            "--ns",
            "lab",
            "--remap",
            "foo=/bar",
            "--timeout",
            "2.5",
            "-v",
        ],
    )

    config = cynic.resolve_config()

    assert isinstance(config, cynic.LsConfig)
    assert (config.can, config.bitrate, config.ns, config.remap, config.verbose, config.timeout) == (
        "after-command",
        500000,
        "lab",
        "foo=/bar",
        2,
        2.5,
    )


def test_command_handler_receives_a_complete_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("CYNIC_CAN", raising=False)
    monkeypatch.delenv("CYNIC_BITRATE", raising=False)
    monkeypatch.setattr(sys, "argv", ["cynic", "--timeout", "2", "pub", "first", "second", "--", "payload"])

    config = cynic.resolve_config()

    assert isinstance(config, cynic.PubConfig)
    assert (config.topics, config.timeout) == (("first", "second"), 2.0)
    assert config.output is cynic.json_output
    assert config.input() == b"payload"


@pytest.mark.parametrize("args", [["-o", "raw", "sub", "first", "second"], ["sub", "--dec=raw", "first", "second"]])
def test_raw_output_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, args: list[str]) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["cynic", *args])

    config = cynic.resolve_config()

    assert isinstance(config, cynic.SubConfig)
    assert config.topics == ("first", "second")
    assert config.output is cynic.raw_output


@pytest.mark.parametrize(("option", "command"), [("--enc=raw", "pub"), ("--dec=raw", "sub"), ("--io=raw", "req")])
@pytest.mark.parametrize("before", [False, True])
def test_format_selectors_before_or_after_command(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, option: str, command: str, before: bool
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys, "argv", ["cynic", option, command, "topic"] if before else ["cynic", command, option, "topic"]
    )

    assert isinstance(
        cynic.resolve_config(), {"pub": cynic.PubConfig, "sub": cynic.SubConfig, "req": cynic.ReqConfig}[command]
    )


@pytest.mark.parametrize("command", ["pub", "sub", "req"])
def test_io_raw_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, command: str) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys, "argv", ["cynic", command, "--io=raw", "topic"] + ([] if command == "sub" else ["--", r"\x09"])
    )

    config = cynic.resolve_config()

    if command != "sub":
        assert config.input() == rb"\x09"
    assert (config.output is cynic.raw_output) == (command != "pub")


@pytest.mark.parametrize(
    ("args", "error"),
    [
        (["sub", "--enc=raw", "topic"], "--enc is only valid with pub/req"),
        (["pub", "--dec=raw", "topic"], "--dec is only valid with sub/req"),
        (["ls", "--io=raw"], "--io is only valid with pub/sub/req"),
        (["req", "--io=raw", "--enc=raw", "topic"], "--io cannot be combined"),
        (["req", "--dec=raw", "--dec=raw", "topic"], "--dec cannot be repeated"),
        (["--enc=raw", "req", "--enc=raw", "topic"], "--enc cannot be repeated"),
        (["pub", "--dsdl", "root", "topic"], "unrecognized arguments: --dsdl"),
    ],
)
def test_format_selector_errors(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str], args: list[str], error: str
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["cynic", *args])

    with pytest.raises(SystemExit, match="2"):
        cynic.resolve_config()
    assert error in capsys.readouterr().err


def test_dsdl_message_input_and_req_default_output(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    file, schema = make_dsdl(tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "cynic",
            "--dsdl-root",
            str(file.parent),
            "req",
            "topic",
            "--enc",
            str(file),
            "--",
            "{value: 7, data: [1, 2]}",
        ],
    )

    config = cynic.resolve_config()

    assert isinstance(config, cynic.ReqConfig)
    assert pydsdl.deserialize(schema, config.input()) == {"value": 7, "data": b"\x01\x02"}
    assert config.output is cynic.json_output

    monkeypatch.setattr(
        sys,
        "argv",
        ["cynic", "req", "--enc=raw", "topic", "--", r"\x09"],
    )
    config = cynic.resolve_config()
    assert isinstance(config, cynic.ReqConfig)
    assert config.input() == rb"\x09"
    assert config.output is cynic.json_output


def test_dsdl_service_and_raw_input(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    file, schema = make_dsdl(tmp_path, service=True)
    assert isinstance(schema, pydsdl.ServiceType)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "cynic",
            "req",
            "--enc=raw",
            "--dsdl-root",
            str(file.parent),
            "--dec",
            str(file),
            "topic",
            "--",
            r"\x09",
        ],
    )

    config = cynic.resolve_config()

    assert isinstance(config, cynic.ReqConfig)
    assert config.input() == rb"\x09"
    config.output(
        cynic.Response(cynic.Instant.now(), 123, 0, pydsdl.serialize(schema.response_type, {"response": [8]})),
        type("T", (), {"name": "topic"})(),
    )
    event = json.loads(capsys.readouterr().out)
    assert event["response"] == [8]
    assert event["_meta_"]["remote"] == 123
    assert event["_meta_"]["topic"] == "topic"
    assert event["_meta_"]["seqno"] == 0
    datetime.fromisoformat(event["_meta_"]["ts"])


def test_dsdl_service_pub_sub_use_request(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    file, schema = make_dsdl(tmp_path, service=True)
    assert isinstance(schema, pydsdl.ServiceType)
    roots = ["--dsdl-root", str(file.parent)]
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["cynic", "pub", *roots, "--io", str(file), "topic", "--", "request: 7"])

    config = cynic.resolve_config()

    assert isinstance(config, cynic.PubConfig)
    assert pydsdl.deserialize(schema.request_type, config.input()) == {"request": 7}

    monkeypatch.setattr(sys, "argv", ["cynic", "sub", *roots, "--io", str(file), "topic"])
    config = cynic.resolve_config()
    assert isinstance(config, cynic.SubConfig)
    config.output(
        cynic.Response(cynic.Instant.now(), 123, 0, pydsdl.serialize(schema.request_type, {"request": 8})),
        type("T", (), {"name": "topic"})(),
    )
    assert json.loads(capsys.readouterr().out)["request"] == 8


def test_io_dsdl_service_uses_req_response(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    file, schema = make_dsdl(tmp_path, service=True)
    assert isinstance(schema, pydsdl.ServiceType)
    load_dsdl, loaded = cynic.load_dsdl, []

    def counted_load(file: Path, roots: list[Path]) -> pydsdl.CompositeType:
        loaded.append(file)
        return load_dsdl(file, roots)

    monkeypatch.setattr(cynic, "load_dsdl", counted_load)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        ["cynic", "req", "--dsdl-root", str(file.parent), "--io", str(file), "topic", "--", "request: 7"],
    )

    config = cynic.resolve_config()

    assert isinstance(config, cynic.ReqConfig)
    assert loaded == [file.resolve()]
    assert pydsdl.deserialize(schema.request_type, config.input()) == {"request": 7}
    config.output(
        cynic.Response(cynic.Instant.now(), 123, 0, pydsdl.serialize(schema.response_type, {"response": [8]})),
        type("T", (), {"name": "topic"})(),
    )
    assert json.loads(capsys.readouterr().out)["response"] == [8]


def test_req_accepts_different_dsdl_types(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    enc_file, enc_schema = make_dsdl(tmp_path / "enc", service=True)
    dec_file, dec_schema = make_dsdl(tmp_path / "dec", service=True)
    assert isinstance(enc_schema, pydsdl.ServiceType) and isinstance(dec_schema, pydsdl.ServiceType)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "cynic",
            "--dsdl-root",
            str(enc_file.parent),
            "req",
            "--dsdl-root",
            str(dec_file.parent),
            "--enc",
            str(enc_file),
            "--dec",
            str(dec_file),
            "topic",
            "--",
            "request: 7",
        ],
    )

    config = cynic.resolve_config()

    assert isinstance(config, cynic.ReqConfig)
    assert pydsdl.deserialize(enc_schema.request_type, config.input()) == {"request": 7}
    config.output(
        cynic.Response(cynic.Instant.now(), 123, 0, pydsdl.serialize(dec_schema.response_type, {"response": [8]})),
        type("T", (), {"name": "topic"})(),
    )
    assert json.loads(capsys.readouterr().out)["response"] == [8]


def test_dsdl_discovery_is_lazy(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["cynic", "ls"])
    monkeypatch.setenv("CYPHAL_PATH", str(tmp_path))
    monkeypatch.setattr(Path, "iterdir", lambda _: (_ for _ in ()).throw(OSError("inaccessible")))

    assert isinstance(cynic.resolve_config(), cynic.LsConfig)


def test_dsdl_path_resolution_error_is_reported(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["cynic", "pub", "--enc=loop.dsdl", "topic"])
    monkeypatch.setattr(Path, "resolve", lambda _: (_ for _ in ()).throw(RuntimeError("symlink loop")))

    with pytest.raises(SystemExit, match="2"):
        cynic.resolve_config()
    assert "cannot load DSDL 'loop.dsdl': symlink loop" in capsys.readouterr().err


def test_dsdl_accepts_multiple_roots(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    demo, dep = tmp_path / "demo", tmp_path / "dep"
    demo.mkdir(parents=True)
    dep.mkdir(parents=True)
    file = demo / "Test.1.0.dsdl"
    file.write_text("dep.Value.1.0 item\n@sealed\n")
    (dep / "Value.1.0.dsdl").write_text("uint8 value\n@sealed\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "cynic",
            "--dsdl-root",
            str(demo),
            "pub",
            "--dsdl-root",
            str(dep),
            "--enc",
            str(file),
            "topic",
            "--",
            "item: {value: 42}",
        ],
    )

    config = cynic.resolve_config()

    schema = pydsdl.read_files(file, demo, [dep])[0][0]
    assert isinstance(config, cynic.PubConfig)
    assert pydsdl.deserialize(schema, config.input()) == {"item": {"value": 42}}


def test_dsdl_roots_are_merged(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    demo, dep, base = (tmp_path / x for x in ("demo", "dep", "base"))
    for root in (demo, dep, base):
        root.mkdir()
    file = demo / "Test.1.0.dsdl"
    file.write_text("dep.Value.1.0 left\nbase.Value.1.0 right\n@sealed\n")
    (dep / "Value.1.0.dsdl").write_text("uint8 value\n@sealed\n")
    (base / "Value.1.0.dsdl").write_text("uint8 value\n@sealed\n")
    (tmp_path / "cynic.toml").write_text(f'dsdl_root = ["{dep}"]\n')
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        ["cynic", "pub", "--dsdl-root", str(demo), "--enc", str(file), "topic", "--", "{left: {}, right: {}}"],
    )
    monkeypatch.setenv("DSDL_ROOT", str(base))
    monkeypatch.delenv("CYPHAL_PATH", raising=False)

    config = cynic.resolve_config()

    schema = pydsdl.read_files(file, demo, [dep, base])[0][0]
    assert isinstance(config, cynic.PubConfig)
    assert pydsdl.deserialize(schema, config.input()) == {"left": {"value": 0}, "right": {"value": 0}}


def test_cyphal_path_is_absorbed_with_dsdl_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    current, legacy = tmp_path / "current", tmp_path / "legacy"
    demo = current / "demo"
    for path in (legacy / "dep", demo):
        path.mkdir(parents=True)
    (legacy / "dep" / "Value.1.0.dsdl").write_text("uint8 value\n@sealed\n")
    file = demo / "Test.1.0.dsdl"
    file.write_text("dep.Value.1.0 item\n@sealed\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["cynic", "pub", "--enc", str(file), "topic", "--", "item: {value: 42}"])
    monkeypatch.setenv("DSDL_ROOT", str(demo))
    monkeypatch.setenv("CYPHAL_PATH", str(legacy))

    config = cynic.resolve_config()

    schema = pydsdl.read_files(file, demo, [legacy / "dep"])[0][0]
    assert isinstance(config, cynic.PubConfig)
    assert pydsdl.deserialize(schema, config.input()) == {"item": {"value": 42}}


def test_cyphal_path_is_converted_to_roots(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    container, demo, dep = tmp_path / "types", tmp_path / "types" / "demo", tmp_path / "types" / "dep"
    demo.mkdir(parents=True)
    dep.mkdir()
    file = demo / "Test.1.0.dsdl"
    file.write_text("dep.Value.1.0 item\n@sealed\n")
    (dep / "Value.1.0.dsdl").write_text("uint8 value\n@sealed\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["cynic", "pub", "--enc", str(file), "topic", "--", "item: {value: 42}"])
    monkeypatch.delenv("DSDL_ROOT", raising=False)
    monkeypatch.setenv("CYPHAL_PATH", str(container))

    config = cynic.resolve_config()

    assert isinstance(config, cynic.PubConfig)
    assert pydsdl.deserialize(pydsdl.read_files(file, demo, [dep])[0][0], config.input()) == {"item": {"value": 42}}


def test_dsdl_and_roots_from_config_read_yaml_stdin(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    file, schema = make_dsdl(tmp_path)
    (tmp_path / "cynic.toml").write_text(f'dsdl = "/ignored"\ndsdl_root = ["{file.parent}"]\n')
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["cynic", "pub", "--enc", str(file), "topic"])
    monkeypatch.setattr(sys, "stdin", type("Stdin", (), {"buffer": BytesIO(b"value: 3\ndata: [4]\n")})())

    config = cynic.resolve_config()

    assert isinstance(config, cynic.PubConfig)
    assert pydsdl.deserialize(schema, config.input()) == {"value": 3, "data": b"\x04"}


def test_dsdl_config_is_ignored(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    (tmp_path / "cynic.toml").write_text('dsdl = "/missing"\n')
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["cynic", "ls"])

    assert isinstance(cynic.resolve_config(), cynic.LsConfig)


def test_dsdl_path_environment_is_ignored(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    file, _ = make_dsdl(tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["cynic", "pub", "--enc", str(file), "topic", "--", "{}"])
    monkeypatch.setenv("DSDL_PATH", str(file.parent.parent))
    monkeypatch.delenv("DSDL_ROOT", raising=False)
    monkeypatch.delenv("CYPHAL_PATH", raising=False)

    with pytest.raises(SystemExit):
        cynic.resolve_config()
    error = capsys.readouterr().err
    assert "not under any DSDL root" in error
    assert "--dsdl-root, dsdl_root in cynic.toml, or DSDL_ROOT" in error


def test_dsdl_does_not_search_cwd_or_ancestors(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    file, _ = make_dsdl(tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("DSDL_ROOT", raising=False)
    monkeypatch.delenv("CYPHAL_PATH", raising=False)

    with pytest.raises(ValueError, match="not under any DSDL root"):
        cynic.load_dsdl(file, [])


def test_data_separator_requires_one_argument(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["cynic", "pub", "topic", "--"])

    with pytest.raises(SystemExit) as ex:
        cynic.resolve_config()

    assert ex.value.code == 2
    assert "expected exactly one DATA argument after --" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("content", "message"),
    [
        ("can = 1\n", "'can' has an invalid type"),
        ("bitrate = true\n", "'bitrate' has an invalid type"),
        ("timeout = false\n", "'timeout' has an invalid type"),
        ("verbose = 1.5\n", "'verbose' has an invalid type"),
        ('dsdl_root = "root"\n', "'dsdl_root' has an invalid type"),
        ("dsdl_root = [1]\n", "'dsdl_root' has an invalid type"),
        ("can = [\n", "cannot read"),
    ],
)
def test_invalid_config_is_a_usage_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str], content: str, message: str
) -> None:
    (tmp_path / "cynic.toml").write_text(content)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["cynic", "ls"])

    with pytest.raises(SystemExit) as ex:
        cynic.resolve_config()

    assert ex.value.code == 2
    assert message in capsys.readouterr().err


def test_invalid_environment_bitrate_is_a_usage_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["cynic", "ls"])
    monkeypatch.setenv("CYNIC_BITRATE", "nope")

    with pytest.raises(SystemExit) as ex:
        cynic.resolve_config()

    assert ex.value.code == 2
    assert "CYNIC_BITRATE must be an integer" in capsys.readouterr().err


def test_config_can_glob_requires_exactly_one_match(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    pattern = tmp_path / "serial-*"
    (tmp_path / "serial-one").touch()
    (tmp_path / "cynic.toml").write_text(f'can = "{pattern}"\n')
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["cynic", "ls"])

    config = cynic.resolve_config()
    assert config.can == str(tmp_path / "serial-one")

    (tmp_path / "serial-two").touch()
    with pytest.raises(SystemExit) as ex:
        cynic.resolve_config()
    assert ex.value.code == 2
    assert "matched 2 paths" in capsys.readouterr().err

    (tmp_path / "serial-one").unlink()
    (tmp_path / "serial-two").unlink()
    with pytest.raises(SystemExit) as ex:
        cynic.resolve_config()
    assert ex.value.code == 2
    assert "matched 0 paths" in capsys.readouterr().err


def wait_for(path: Path, needle: str, what: str) -> None:
    deadline = time.monotonic() + SETTLE
    while time.monotonic() < deadline:
        if path.exists() and needle in path.read_text(errors="replace"):
            return
        time.sleep(0.05)
    raise AssertionError(f"{what} not ready within {SETTLE} s; log:\n{path.read_text(errors='replace')}")


def collect(path: Path, count: int, deadline_s: float) -> list[dict[str, Any]]:
    deadline = time.monotonic() + deadline_s
    while time.monotonic() < deadline:
        lines = [x for x in path.read_text().splitlines() if x.startswith("{")]
        if len(lines) >= count:
            return [json.loads(x) for x in lines]
        time.sleep(0.05)
    return [json.loads(x) for x in path.read_text().splitlines() if x.startswith("{")]


class Subscriber:
    """`cynic sub` in the background; readiness is taken from its own INFO log line."""

    def __init__(self, tmp: Path, link: list[str], *topics: str) -> None:
        self.out = tmp / "out.jsonl"
        self.err = tmp / "err.log"
        argv = [sys.executable, str(CYNIC), *link, "-v", "sub", *topics]
        self._fo, self._fe = self.out.open("w"), self.err.open("w")
        self.proc = subprocess.Popen(argv, stdout=self._fo, stderr=self._fe)
        wait_for(self.err, "Subscribed to", "subscriber")

    def close(self) -> None:
        self.proc.terminate()
        self.proc.wait(timeout=10)
        self._fo.close()
        self._fe.close()


class FileServer:
    """`cynic fs` in the background; it serves relative paths from ``tmp``."""

    def __init__(self, tmp: Path, link: list[str], *topics: str) -> None:
        self.out = tmp / "file-server.out"
        self.err = tmp / "file-server.err"
        argv = [sys.executable, str(CYNIC), *link, "-v", "fs", *topics]
        self._fo, self._fe = self.out.open("w"), self.err.open("w")
        self.proc = subprocess.Popen(argv, stdout=self._fo, stderr=self._fe, cwd=tmp)
        wait_for(self.err, "File server ready", "file server")

    def close(self) -> None:
        self.proc.terminate()
        self.proc.wait(timeout=10)
        self._fo.close()
        self._fe.close()


@pytest.fixture
def subscriber(tmp_path: Path, link: list[str], topic: str) -> Any:
    sub = Subscriber(tmp_path, link, topic)
    yield sub
    sub.close()


def test_no_args_prints_help() -> None:
    r = subprocess.run([sys.executable, str(CYNIC)], capture_output=True, text=True, timeout=30)
    assert r.returncode == 0
    assert all(x in r.stdout for x in ("sub", "pub", "req", "fs", "ls"))
    # The module docstring is rendered from Markdown.
    assert "cn sub topic/foo topic/bar | jq '.msg'\n" in r.stdout, r.stdout
    assert r'''cn pub topic/foo topic/bar -- "Hex-encoded string\n\x0d"''' in r.stdout, r.stdout
    assert "cn fs my/file/topic /other/topic\n" in r.stdout, r.stdout
    assert "```" not in r.stdout


@pytest.mark.parametrize("args", [["--version"], ["ls", "--version"]])
def test_version(args: list[str]) -> None:
    r = subprocess.run([sys.executable, str(CYNIC), *args], capture_output=True, text=True, timeout=30)
    assert r.returncode == 0
    assert r.stdout == f"cynic {cynic.__version__}\n"
    assert not r.stderr


def test_fs_help_documents_multiple_topics() -> None:
    r = subprocess.run([sys.executable, str(CYNIC), "fs", "--help"], capture_output=True, text=True, timeout=30)
    assert r.returncode == 0
    assert "TOPIC [TOPIC ...]" in r.stdout
    assert "zubax.file.Read" in r.stdout


def test_runs_without_coloredlogs(tmp_path: Path) -> None:
    """coloredlogs is optional; without it, logging falls back to the stdlib formatter."""
    shim = tmp_path / "shim"
    shim.mkdir()
    (shim / "coloredlogs.py").write_text('raise ImportError("simulated: not installed")\n')
    env = {**os.environ, "PYTHONPATH": str(shim)}
    argv = [sys.executable, str(CYNIC), "--timeout", "1", "-v", "ls"]
    r = subprocess.run(argv, capture_output=True, text=True, timeout=60, env=env)
    assert r.returncode == 0, r.stderr
    assert "INFO" in r.stderr and "Discovered" in r.stderr, r.stderr


@pytest.mark.parametrize("flag", ["-t", "--timeout"])
def test_timeout_flag_aliases(flag: str) -> None:
    started = time.monotonic()
    r = subprocess.run([sys.executable, str(CYNIC), flag, "1", "ls"], capture_output=True, text=True, timeout=30)
    assert r.returncode == 0, r.stderr
    assert time.monotonic() - started < 10.0


def test_help_into_closed_pipe() -> None:
    """`cynic | head -n 0` must exit 0 quietly, not 120 with an ignored BrokenPipeError."""
    cmd = f"set -o pipefail; {sys.executable} {CYNIC} | head -n 0"  # pipefail: surface the producer's status.
    r = subprocess.run(cmd, shell=True, executable="/bin/bash", capture_output=True, text=True, timeout=30)
    assert r.returncode == 0
    assert "BrokenPipe" not in r.stderr, r.stderr


def test_emit_is_compact_json_off_terminal(capsys: pytest.CaptureFixture[str]) -> None:
    cynic.emit(foo="bar")

    assert capsys.readouterr().out == '{"foo": "bar"}\n'


def test_emit_error(capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level("DEBUG", logger="cynic"):
        try:
            raise ValueError("bad")
        except ValueError as ex:
            cynic.emit_error(ex)

    assert json.loads(capsys.readouterr().out) == {"error": "ValueError", "info": "bad"}
    assert "Exception reported as JSON" in caplog.text
    assert "Traceback" in caplog.text
    assert "ValueError: bad" in caplog.text


def test_emit_preserves_nan(capsys: pytest.CaptureFixture[str]) -> None:
    cynic.emit(value=float("nan"))

    value = json.loads(capsys.readouterr().out)["value"]
    assert value != value


def test_emit_is_colored_compact_json_on_terminal(monkeypatch: pytest.MonkeyPatch) -> None:
    class TtyStringIO(StringIO):
        def isatty(self) -> bool:
            return True

    output = TtyStringIO()
    monkeypatch.setattr(sys, "stdout", output)
    monkeypatch.setattr(
        cynic, "console", Console(file=output, force_terminal=True, color_system="standard", soft_wrap=True)
    )

    cynic.emit(foo="bar")
    colored = output.getvalue()
    assert "\x1b[" in colored
    assert re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", colored) == '{"foo": "bar"}\n'


@pytest.mark.parametrize("raw", [b"", b"a", bytes(range(256)), b'q" b\\s', b"hello\rworld!"])
def test_escape_round_trip(raw: bytes) -> None:
    assert cynic.unescape(cynic.escape(raw)) == raw


@pytest.mark.parametrize(
    "text,raw",
    [(r"\x0d", b"\r"), (r"\n", b"\n"), (r"\t", b"\t"), (r"\\", b"\\"), ("plain", b"plain"), (r"\xff", b"\xff")],
)
def test_unescape_accepts_python_escapes(text: str, raw: bytes) -> None:
    assert cynic.unescape(text) == raw


@pytest.mark.parametrize("raw,text", [(b"\r", r"\x0d"), (b"\\", r"\x5c"), (b"\x00", r"\x00"), (b'"', '"')])
def test_escape_output(raw: bytes, text: str) -> None:
    assert cynic.escape(raw) == text


def test_timestamp_maps_monotonic_instant_onto_wall_clock() -> None:
    now = cynic.Instant.now()
    assert abs(datetime.fromisoformat(cynic.timestamp(now)).timestamp() - time.time()) < 0.05
    aged = cynic.Instant(ns=now.ns - 5_000_000_000)
    assert abs(datetime.fromisoformat(cynic.timestamp(aged)).timestamp() - (time.time() - 5.0)) < 0.05


def serialize_file_read_request(seek: int, size: int, path: str) -> bytes:
    """Manual zubax.file.Read.0.1 request serialization, independent of cynic's codec."""
    encoded_path = path.encode("utf8")
    return seek.to_bytes(6, "little", signed=True) + struct.pack("<H2xH", size, len(encoded_path)) + encoded_path


def deserialize_file_read_response(payload: bytes) -> tuple[int, int, bool, bytes]:
    """Manual zubax.file.Read.0.1 response deserialization, accepting CAN-FD zero padding."""
    assert len(payload) >= 12
    seek = int.from_bytes(payload[:6], "little", signed=True)
    error, end, data_length = struct.unpack_from("<BB2xH", payload, 6)
    data_end = 12 + data_length
    assert len(payload) >= data_end
    assert set(payload[data_end:]) <= {0}
    return seek, error & 0x0F, bool(end & 1), payload[12:data_end]


def test_file_read_wire_codec() -> None:
    request = serialize_file_read_request(-7, 123, "f\u00f8\u00f8")
    assert cynic.deserialize_file_read_request(request) == (-7, 123, "f\u00f8\u00f8")
    assert cynic.deserialize_file_read_request(request[:11]) is None
    assert cynic.deserialize_file_read_request(request + b"\0") is None

    excessive_path = b"x" * 1025
    malformed = b"\0" * 10 + struct.pack("<H", len(excessive_path)) + excessive_path
    assert cynic.deserialize_file_read_request(malformed) is None
    assert cynic.deserialize_file_read_request(b"\0" * 10 + b"\x01\x00\xff") is None

    response = cynic.serialize_file_read_response(5, 6, True, b"abc")
    assert deserialize_file_read_response(response) == (5, 6, True, b"abc")


def test_file_read_chunking_and_errors(tmp_path: Path) -> None:
    file_path = tmp_path / "data.bin"
    file_path.write_bytes(b"abcdef")

    assert cynic.read_file_chunk(str(file_path), 0, 3) == (0, 0, False, b"abc")
    assert cynic.read_file_chunk(str(file_path), 3, 99) == (3, 0, True, b"def")
    assert cynic.read_file_chunk(str(file_path), -2, 3) == (5, 0, True, b"f")
    assert cynic.read_file_chunk(str(file_path), -1, 3) == (6, 0, True, b"")
    assert cynic.read_file_chunk(str(file_path), 99, 3) == (99, 0, True, b"")
    assert cynic.read_file_chunk(str(file_path), -8, 3) == (0, 1, False, b"")
    assert cynic.read_file_chunk(str(tmp_path / "missing"), 0, 3) == (0, 3, False, b"")
    assert cynic.read_file_chunk(str(tmp_path), 0, 3) == (0, 4, False, b"")


def test_file_read_response_failure_is_logged_and_ignored(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    class FailingBreadcrumb:
        remote_id = 0
        tag = 0

        class topic:
            name = "file/read"

        async def __call__(self, *args: Any, **kwargs: Any) -> None:
            raise cynic.Error("simulated response failure")

    class FailingArrival:
        timestamp = cynic.Instant.now()
        breadcrumb = FailingBreadcrumb()

    with caplog.at_level("WARNING", logger="cynic"):
        asyncio.run(cynic.serve_file_read_request(FailingArrival(), 0, 1, str(tmp_path / "missing"), 1.0))
    assert "File-read response failed" in caplog.text


def test_fs_serves_multiple_topics_after_malformed_request(tmp_path: Path, link: list[str]) -> None:
    topics = [f"cynictest/{uuid.uuid4().hex[:12]}" for _ in range(2)]
    (tmp_path / "served.bin").write_bytes(b"abcdef")
    server = FileServer(tmp_path, link, *topics)
    try:
        # The subscriber acknowledges this malformed publication at the transport layer, but the server must not
        # emit an RPC response or stop serving later valid requests.
        assert run_cynic(link, "pub", topics[0], "--", cynic.escape(b"malformed")).returncode == 0
        request = serialize_file_read_request(-2, 10, "served.bin")
        result = run_cynic(link, "req", *topics, "--", cynic.escape(request))
    finally:
        server.close()

    assert result.returncode == 0, result.stderr
    decoded = {x["topic"]: deserialize_file_read_response(cynic.unescape(x["msg"])) for x in responses(result)}
    assert decoded == {topic: (5, 0, True, b"f") for topic in topics}
    assert "Dropping malformed file-read request" in server.err.read_text()
    processed = [json.loads(line) for line in server.out.read_text().splitlines()]
    assert {event["topic"] for event in processed} == set(topics)
    assert all(
        event["path"] == "served.bin"
        and event["seek"] == 5
        and event["error"] == 0
        and event["end"] is True
        and "data" not in event
        for event in processed
    )


def test_pub_sub(subscriber: Subscriber, link: list[str], topic: str) -> None:
    before = time.time()
    assert run_cynic(link, "pub", topic, "--", r"hello\x0dworld!").returncode == 0

    (msg,) = collect(subscriber.out, 1, SETTLE)
    assert payload_eq(cynic.unescape(msg["msg"]), b"hello\rworld!")
    assert msg["topic"] == topic
    assert isinstance(msg["remote"], int) and isinstance(msg["tag"], int)
    ts = datetime.fromisoformat(msg["ts"])
    assert ts.tzinfo is not None
    assert before - 1.0 <= ts.timestamp() <= time.time() + 1.0


def test_pub_multiple_topics(tmp_path: Path, link: list[str]) -> None:
    """One `pub` invocation fans the same message out to every topic, concurrently."""
    topics = [f"cynictest/{uuid.uuid4().hex[:12]}" for _ in range(3)]
    sub = Subscriber(tmp_path, link, *topics)
    try:
        assert run_cynic(link, "pub", *topics, "--", "fanout").returncode == 0
        msgs = collect(sub.out, len(topics), SETTLE)
    finally:
        sub.close()

    assert {m["topic"] for m in msgs} == set(topics)
    assert all(payload_eq(cynic.unescape(m["msg"]), b"fanout") for m in msgs)


def test_sub_multiple_topics_independently(tmp_path: Path, link: list[str]) -> None:
    """A single `sub` listens to every topic asynchronously, emitting messages as they arrive."""
    topics = [f"cynictest/{uuid.uuid4().hex[:12]}" for _ in range(3)]
    sub = Subscriber(tmp_path, link, *topics)
    try:
        for i, t in enumerate(topics):  # Separate publishers, one topic each.
            assert run_cynic(link, "pub", t, "--", f"msg{i}").returncode == 0
        msgs = collect(sub.out, len(topics), SETTLE)
    finally:
        sub.close()

    by_topic = {m["topic"]: cynic.unescape(m["msg"]) for m in msgs}
    assert set(by_topic) == set(topics)
    assert all(payload_eq(by_topic[t], f"msg{i}".encode()) for i, t in enumerate(topics))


def test_sub_raw_output(tmp_path: Path, link: list[str], topic: str) -> None:
    output = tmp_path / "out.raw"
    errors = tmp_path / "err.log"
    with output.open("wb") as fo, errors.open("w") as fe:
        proc = subprocess.Popen(
            [sys.executable, str(CYNIC), *link, "-v", "sub", "--dec=raw", topic], stdout=fo, stderr=fe
        )
        try:
            wait_for(errors, "Subscribed to", "raw subscriber")
            assert run_cynic(link, "pub", topic, "--", r"raw\x00output").returncode == 0
            deadline = time.monotonic() + SETTLE
            while not output.read_bytes() and time.monotonic() < deadline:
                time.sleep(0.05)
        finally:
            proc.terminate()
            proc.wait(timeout=10)

    assert payload_eq(output.read_bytes(), b"raw\0output")


def test_pub_with_subscriber(subscriber: Subscriber, link: list[str], topic: str) -> None:
    assert run_cynic(link, "pub", topic, "--", "ack me").returncode == 0
    (msg,) = collect(subscriber.out, 1, SETTLE)
    assert payload_eq(cynic.unescape(msg["msg"]), b"ack me")


def test_pub_reads_raw_stdin(subscriber: Subscriber, link: list[str], topic: str) -> None:
    payload = b"raw\0input\xff"
    assert run_cynic_bytes(link, "pub", topic, input=payload).returncode == 0
    (msg,) = collect(subscriber.out, 1, SETTLE)
    assert payload_eq(cynic.unescape(msg["msg"]), payload)


def test_pub_reliable_without_subscriber(link: list[str], topic: str) -> None:
    r = run_cynic(link, "pub", topic, "--", "nobody home")
    assert r.returncode == 1
    assert len(error_events(r, "DeliveryError")) == 1


def test_pub_reliability_flag_is_removed(link: list[str], topic: str) -> None:
    r = run_cynic(link, "pub", "--reliable", topic, "--", "deprecated")
    assert r.returncode == 2
    assert "unrecognized arguments: --reliable" in r.stderr


@contextlib.contextmanager
def responder(tmp_path: Path, link: list[str], topic: str, reply: bytes, count: int = 1) -> Any:
    """Replies `reply`+seqno, `count` times, to every request on `topic`."""
    iface = link[1] if link else ""
    log = tmp_path / f"responder-{uuid.uuid4().hex[:6]}.log"
    with log.open("w") as fo:
        argv = [sys.executable, "-c", RESPONDER, iface, topic, reply.decode(), str(count)]
        proc = subprocess.Popen(argv, stdout=fo, stderr=fo)
        try:
            wait_for(log, "READY", f"responder on {topic}")
            yield proc
        finally:
            proc.terminate()
            proc.wait(timeout=10)


def responses(r: subprocess.CompletedProcess[str]) -> list[dict[str, Any]]:
    return [json.loads(x) for x in r.stdout.splitlines() if x.startswith("{")]


def error_events(r: subprocess.CompletedProcess[str], name: str) -> list[dict[str, Any]]:
    events = [x for x in responses(r) if x.get("error") == name]
    assert all(isinstance(x["info"], str) for x in events)
    return events


def test_req_delivery_error_is_json(capsys: pytest.CaptureFixture[str]) -> None:
    class FailingPublisher:
        topic = type("Topic", (), {"name": "test/topic"})()

        async def request(self, deadline: Any, timeout: float, message: bytes) -> None:
            raise DeliveryError("Acknowledgment timeout")

    config = cynic.ReqConfig(None, 0, "", "", 0, lambda: b"", cynic.json_output, (), 1.0)
    assert asyncio.run(cynic.request(FailingPublisher(), config, b"request")) == 0
    assert json.loads(capsys.readouterr().out) == {"error": "DeliveryError", "info": "Acknowledgment timeout"}


def test_req(tmp_path: Path, link: list[str], topic: str) -> None:
    with responder(tmp_path, link, topic, b"pong"):
        r = run_cynic(link, "req", topic, "--", "ping")
    assert r.returncode == 0, r.stderr
    lines = responses(r)
    assert len(lines) == 1
    assert payload_eq(cynic.unescape(lines[0]["msg"]), b"pong0")
    assert lines[0]["seqno"] == 0
    assert isinstance(lines[0]["remote"], int)
    assert lines[0]["topic"] == topic
    assert "tag" not in lines[0]


def test_req_reads_raw_stdin(tmp_path: Path, link: list[str], topic: str) -> None:
    with responder(tmp_path, link, topic, b"pong"):
        r = run_cynic_bytes(link, "req", topic, input=b"ping")
    assert r.returncode == 0, r.stderr.decode()
    (response,) = [json.loads(line) for line in r.stdout.decode().splitlines()]
    assert payload_eq(cynic.unescape(response["msg"]), b"pong0")


def test_req_raw_output(tmp_path: Path, link: list[str], topic: str) -> None:
    with responder(tmp_path, link, topic, b"pong"):
        r = run_cynic_bytes(link, "req", "--dec=raw", topic, "--", "ping")
    assert r.returncode == 0, r.stderr.decode()
    assert payload_eq(r.stdout, b"pong0")
    assert not r.stderr


def test_req_raw_output_writes_json_errors_to_stderr(link: list[str], topic: str) -> None:
    r = run_cynic_bytes(link, "req", "-o", "raw", topic, "--", "anyone?")
    assert r.returncode == 1
    assert not r.stdout
    assert json.loads(r.stderr) == {"error": "LivenessError", "info": "Response timeout"}


def test_req_streams_multiple_responses(tmp_path: Path, link: list[str], topic: str) -> None:
    """A responder may stream; seqno increments per responder and every chunk is printed."""
    with responder(tmp_path, link, topic, b"chunk", count=3):
        r = run_cynic(link, "req", topic, "--", "ping")
    assert r.returncode == 0, r.stderr
    lines = responses(r)
    assert [x["seqno"] for x in lines] == [0, 1, 2]
    assert [cynic.unescape(x["msg"])[:6] for x in lines] == [b"chunk0", b"chunk1", b"chunk2"]


def test_req_multiple_topics(tmp_path: Path, link: list[str]) -> None:
    """One `req` fans out to every topic; each response is tagged with the topic that answered."""
    topics = [f"cynictest/{uuid.uuid4().hex[:12]}" for _ in range(2)]
    with contextlib.ExitStack() as stack:
        for i, t in enumerate(topics):
            stack.enter_context(responder(tmp_path, link, t, f"pong{i}".encode()))
        r = run_cynic(link, "req", *topics, "--", "ping")

    assert r.returncode == 0, r.stderr
    by_topic = {x["topic"]: cynic.unescape(x["msg"]) for x in responses(r)}
    assert set(by_topic) == set(topics)
    assert all(payload_eq(by_topic[t], f"pong{i}0".encode()) for i, t in enumerate(topics))


def test_req_succeeds_if_any_topic_answers(tmp_path: Path, link: list[str]) -> None:
    answered, silent = (f"cynictest/{uuid.uuid4().hex[:12]}" for _ in range(2))
    with responder(tmp_path, link, answered, b"pong"):
        r = run_cynic(link, "req", answered, silent, "--", "ping")
    assert r.returncode == 0, r.stderr
    assert [x["topic"] for x in responses(r) if "topic" in x] == [answered]
    assert len(error_events(r, "LivenessError")) == 1


def test_req_without_responder(link: list[str], topic: str) -> None:
    r = run_cynic(link, "req", topic, "--", "anyone?")
    assert r.returncode == 1
    assert len(error_events(r, "LivenessError")) == 1


def test_ls(tmp_path: Path, link: list[str]) -> None:
    """`ls` discovers live topics, reports each exactly once, and exits once discovery goes idle."""
    topics = [f"cynictest/{uuid.uuid4().hex[:12]}" for _ in range(2)]
    sub = Subscriber(tmp_path, link, *topics)
    try:
        started = time.monotonic()
        r = run_cynic(link, "ls")
        elapsed = time.monotonic() - started
    finally:
        sub.close()

    assert r.returncode == 0, r.stderr
    found = [json.loads(x) for x in r.stdout.splitlines() if x.startswith("{")]
    names = [x["name"] for x in found]
    assert set(topics) <= set(names), f"missing topics; saw {names}"
    assert len(names) == len(set(names)), f"duplicate topics reported: {names}"
    assert all(isinstance(x[k], int) for x in found for k in ("hash", "subject", "evictions"))
    assert elapsed < 3 * TIMEOUT, f"ls ran {elapsed:.1f} s with an idle timeout of {TIMEOUT} s"


def test_ls_idle_timer_ignores_duplicate_gossip(tmp_path: Path, link: list[str]) -> None:
    """With an idle timeout longer than the gossip period, resetting on arrival instead of on
    discovery would keep `ls` alive forever. It must still exit."""
    idle = GOSSIP_PERIOD * 1.5
    sub = Subscriber(tmp_path, link, f"cynictest/{uuid.uuid4().hex[:12]}")
    argv = [sys.executable, str(CYNIC), *link, "--timeout", str(idle), "ls"]
    try:
        started = time.monotonic()
        proc = subprocess.run(argv, capture_output=True, text=True, timeout=idle + SETTLE + GOSSIP_PERIOD)
        elapsed = time.monotonic() - started
    finally:
        sub.close()

    assert proc.returncode == 0, proc.stderr
    assert elapsed < idle + SETTLE, f"ls ran {elapsed:.1f} s; duplicate gossip is restarting the idle timer"


def test_ls_empty_network_exits(link: list[str]) -> None:
    r = run_cynic(link, "ls")
    assert r.returncode == 0, r.stderr


def test_ls_idle_timer_restarts_on_late_discovery(tmp_path: Path, link: list[str]) -> None:
    """A topic appearing mid-run must be reported and must push the idle deadline out."""
    late = f"cynictest/{uuid.uuid4().hex[:12]}"
    argv = [sys.executable, str(CYNIC), *link, "--timeout", str(TIMEOUT), "ls"]
    started = time.monotonic()
    proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        appears_at = TIMEOUT * 0.6
        time.sleep(appears_at)
        sub = Subscriber(tmp_path, link, late)
        try:
            out = proc.communicate(timeout=SETTLE + TIMEOUT)[0]
        finally:
            sub.close()
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
    elapsed = time.monotonic() - started

    assert proc.returncode == 0
    assert late in [json.loads(x)["name"] for x in out.splitlines() if x.startswith("{")]
    assert elapsed > appears_at + TIMEOUT, f"idle deadline did not restart on the late topic ({elapsed:.1f} s)"


@pytest.mark.parametrize(
    "iface,marker",
    [("/dev/cynic-nonexistent", "CanInitializationError"), ("cynic-nosuch0", "OSError")],
)
def test_can_iface_heuristic(iface: str, marker: str) -> None:
    """A leading `/` selects the python-can slcan backend; anything else selects SocketCAN."""
    argv = [sys.executable, str(CYNIC), "--can", iface, "sub", "x"]
    r = subprocess.run(argv, capture_output=True, text=True, timeout=30)
    assert r.returncode == 1
    assert marker in r.stderr, r.stderr


def test_sub_survives_closed_stdout(tmp_path: Path, link: list[str], topic: str) -> None:
    """`cynic sub TOPIC | head -1` must exit 0 quietly rather than reporting a broken pipe."""
    err = tmp_path / "err.log"
    with err.open("w") as fe:
        proc = subprocess.Popen(
            [sys.executable, str(CYNIC), *link, "-v", "sub", topic], stdout=subprocess.PIPE, stderr=fe, text=True
        )
        assert proc.stdout is not None
        try:
            wait_for(err, "Subscribed to", "subscriber")
            assert run_cynic(link, "pub", topic, "--", "one").returncode == 0
            assert proc.stdout.readline().startswith("{")
            proc.stdout.close()  # The reader goes away, as `head` would.
            for _ in range(3):
                run_cynic(link, "pub", topic, "--", "two")
            returncode = proc.wait(timeout=SETTLE)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=10)

    assert returncode == 0
    text = err.read_text()
    assert "Traceback" not in text and "BrokenPipeError" not in text, text


def test_namespace_and_remap(tmp_path: Path, link: list[str], topic: str) -> None:
    """--ns and --remap are applied on top of the environment; verified via the resolver's own log."""
    # Publishing is reliable by default, so subscribe to the resolved names to acknowledge it.
    sub = Subscriber(tmp_path, link, "remapped", f"lab/{topic}")
    try:
        r = run_cynic(link, "--ns", "lab", "--remap", f"{topic}=/remapped", "-v", "pub", topic, "--", "x")
        assert r.returncode == 0
        assert "-> 'remapped'" in r.stderr

        r = run_cynic(link, "--ns", "lab", "-v", "pub", topic, "--", "x")
        assert r.returncode == 0
        assert f"-> 'lab/{topic}'" in r.stderr
    finally:
        sub.close()


def test_namespace_and_remap_from_environment(tmp_path: Path, link: list[str], topic: str) -> None:
    # The subscriber must use the resolved names because it intentionally has no matching environment configuration.
    sub = Subscriber(tmp_path, link, f"envns/{topic}", "envremap", "flagremap")
    try:
        r = run_cynic(link, "-v", "pub", topic, "--", "x", env={"CYPHAL_NAMESPACE": "envns"})
        assert r.returncode == 0
        assert f"-> 'envns/{topic}'" in r.stderr

        r = run_cynic(link, "-v", "pub", topic, "--", "x", env={"CYPHAL_REMAP": f"{topic}=/envremap"})
        assert r.returncode == 0
        assert "-> 'envremap'" in r.stderr

        env = {"CYPHAL_NAMESPACE": "envns", "CYPHAL_REMAP": f"{topic}=/envremap"}
        r = run_cynic(link, "--ns", "flagns", "--remap", f"{topic}=/flagremap", "-v", "pub", topic, "--", "x", env=env)
        assert r.returncode == 0
        assert "-> 'flagremap'" in r.stderr  # The flag wins over CYPHAL_REMAP.
    finally:
        sub.close()


@pytest.mark.parametrize("command", [["sub", "cynictest/sigint"], ["fs", "cynictest/sigint"], ["ls"]])
def test_sigint_exits_quietly(link: list[str], command: list[str]) -> None:
    """Long-running commands must exit 0 on Ctrl-C without a traceback."""
    argv = [sys.executable, str(CYNIC), *link, "--timeout", "60", *command]
    proc = subprocess.Popen(
        argv,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        preexec_fn=lambda: signal.signal(signal.SIGINT, signal.SIG_DFL),
    )
    time.sleep(2.0)
    proc.send_signal(signal.SIGINT)
    try:
        out = proc.communicate(timeout=SETTLE)[0]
    except subprocess.TimeoutExpired:
        proc.kill()
        pytest.fail(f"{command[0]} ignored SIGINT")
    assert proc.returncode == 0
    assert "Traceback" not in out
