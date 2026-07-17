#!/usr/bin/env python3
r"""
# Cyphal Network Investigation Console (CYNIC)

The tool can be invoked as `cynic` or `cn` equivalently.

## Configure

By default Cynic uses UDP.
To select CAN, pass `--can=IFACE`, where IFACE is either an SLCAN serial port or a SocketCAN interface.
CAN also accepts optional `--bitrate`.

Optional `./cynic.toml` is looked for in the current working directory; it sets CLI defaults as shown below.
`CYNIC_*` environment variables are also supported for defaults.

```toml
# cynic.toml
can = "/dev/serial/by-id/usb-Zubax*Babel*if00"  # or SocketCAN iface name
bitrate = 1_000_000
```

## List topics

List all topics in the network as JSON lines and quit:

```shell
cn ls
```

## Publish a message

Specify one or more topics to send the message on. Reliable publication is the default.
The message payload is read via stdin by default as a binary blob;
alternatively, it can be passed as a hex-encoded string after `--`.

```shell
cn pub topic/foo topic/bar -- "Hex-encoded string\n\x0d"
printf '\x01\x02' | cn pub topic/foo
```

## Subscribe to topics

Subscribe to a topic or several and print messages as they arrive.
The output is JSON lines with metadata by default;
say `-o raw` to output message payloads as undelimited binary blobs without metadata.

```shell
cn sub topic/foo topic/bar | jq '.msg'
cn sub -o raw topic/foo > messages.bin
```

Subscribe to a Cyphal/CAN v1.0 subject 1234 (compatibility mode):

```shell
cn sub '1234#1234'
```

## Send requests

The `req` command accepts the request payload like `pub` and prints the response like `sub` (same options).

```shell
printf '\x01\x02' | cn req topic/foo topic/bar
```

## Serialization

By default, Cynic does not attempt to make sense of the exchanged data, treating it as opaque blobs.
Serialization has to be opted into as shown below.
When serialization is used, structured inputs can be either JSON or YAML (superset of JSON); outputs are always JSON.

Input encoding is selected with `-i`/`--enc`, and output decoding with `-o`/`--dec`.
Use `--io` to apply the same selection to every direction supported by the command.
Each selector accepts either a DSDL file or `raw`.
Raw encoding disables escape expansion for DATA after `--`; raw decoding emits undelimited payload bytes.
With an RPC DSDL, `req` encodes the request and decodes the response; `pub` and `sub` use the request part.

### DSDL

Use `--enc=path/to/File.1.1.dsdl`, `--dec=path/to/File.1.1.dsdl`, or `--io=path/to/File.1.1.dsdl`.
Supply DSDL namespace root directories via CLI as `--dsdl-root`, TOML `dsdl_root`, or envvar `DSDL_ROOT`.

The old `CYPHAL_PATH` envvar is also supported (back from the days when DSDL was part of Cyphal) but its use is
discouraged; its semantics is also different: instead of listing *roots*, it lists directories that *contain roots*.

```shell
cn pub --dsdl-root=/path/to/vendor --enc=/path/to/vendor/Type.1.0.dsdl topic/foo -- '{value: 42}'
cn req --dsdl-root=/path/to/vendor --io=/path/to/vendor/Service.1.0.dsdl topic/foo -- '{value: 42}'
```

DSDL objects are specified either via the full key-value notation, or positionally;
single-field objects can also be assigned by specifying the field alone directly.
The following are equivalent: `{foo: [1,2,3], bar: 456}`, `[[1,2,3], 456]`.
Likewise, for single-field objects: `{foo: 456}`, `[456]`, `456`.

## Serve files

Run a file server on the specified topic(s) using `zubax.file.Read` out of the current working directory.
Whenever a response is sent, a JSON line is printed via stdout.

```shell
cn fs my/file/topic /other/topic
```

---

Distributed under the MIT License. Author: Pavel Kirienko `pavel@opencyphal.org`
"""

from __future__ import annotations
import argparse
import asyncio
import errno
import glob
import json
import logging
import os
import struct
import sys
import time
import tomllib
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from typing import Any, Callable
import pydsdl  # type: ignore[import-untyped]
import yaml  # type: ignore[import-untyped]
from pycyphal2 import Arrival, Error, Instant, LivenessError, Node, Publisher, Response, Subscriber, Topic, Transport
from rich.console import Console
from rich.markdown import CodeBlock, Markdown
from rich.syntax import Syntax

__version__ = "0.1.0"

HOME = "cynic/"  # Trailing slash to generate a unique home.
DEFAULT_TIMEOUT = 10.0
SCOUT_PATTERN = "/>"

Input = Callable[[], bytes]
Output = Callable[[Arrival | Response | Exception, Topic | None], None]


@dataclass(frozen=True)
class Config:
    can: str | None
    bitrate: int
    ns: str
    remap: str
    verbose: int
    input: Input
    output: Output


@dataclass(frozen=True)
class SubConfig(Config):
    topics: tuple[str, ...]


@dataclass(frozen=True)
class PubConfig(Config):
    topics: tuple[str, ...]
    timeout: float


@dataclass(frozen=True)
class ReqConfig(Config):
    topics: tuple[str, ...]
    timeout: float


@dataclass(frozen=True)
class FsConfig(Config):
    topics: tuple[str, ...]
    timeout: float


@dataclass(frozen=True)
class LsConfig(Config):
    timeout: float


logger = logging.getLogger("cynic")
console = Console(soft_wrap=True)


class HelpCodeBlock(CodeBlock):
    def __rich_console__(self, console: Console, options: Any) -> Any:
        yield Syntax(str(self.text).rstrip(), self.lexer_name, theme=self.theme, word_wrap=True)


class HelpMarkdown(Markdown):
    elements = {**Markdown.elements, "fence": HelpCodeBlock}


class DSDLJSONEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        return list(obj) if isinstance(obj, bytes) else super().default(obj)


def escape(data: bytes) -> str:
    return "".join(chr(b) if 0x20 <= b < 0x7F and b != 0x5C else f"\\x{b:02x}" for b in data)


def unescape(text: str) -> bytes:
    return text.encode("latin-1").decode("unicode_escape").encode("latin-1")


def load_dsdl(file: Path, roots: list[Path]) -> pydsdl.CompositeType:
    file, roots = file.resolve(), [x.resolve() for x in roots]
    if not file.is_file():
        raise FileNotFoundError(file)
    if invalid := next((x for x in roots if not x.is_dir()), None):
        raise NotADirectoryError(invalid)
    roots = list(dict.fromkeys(roots))
    root = next((x for x in roots if file.is_relative_to(x)), None)
    if root is None:
        raise ValueError(f"{file} is not under any DSDL root; use --dsdl-root, dsdl_root in cynic.toml, or DSDL_ROOT")
    direct, _ = pydsdl.read_files(file, root, [x for x in roots if x != root])
    logger.info("Loaded DSDL %s from root %s", direct[0].full_name, root)
    return direct[0]


def deserialize_file_read_request(payload: bytes) -> tuple[int, int, str] | None:
    """Deserialize zubax.file.Read request without relying on generated DSDL code."""
    if len(payload) < 12:
        return None
    size, path_length = struct.unpack_from("<H2xH", payload, 6)
    if path_length > 1024:
        return None
    path_end = 12 + path_length
    if len(payload) != path_end:
        return None
    try:
        path = payload[12:path_end].decode("utf8")
    except UnicodeDecodeError:
        return None
    return int.from_bytes(payload[:6], "little", signed=True), size, path


def serialize_file_read_response(seek: int, error: int, end: bool, data: bytes) -> bytes:
    """Serialize zubax.file.Read response without relying on generated DSDL code."""
    return seek.to_bytes(6, "little", signed=True) + struct.pack("<BBHH", error, int(end), 0, len(data)) + data


def file_error_from_exception(ex: BaseException) -> int:
    if isinstance(ex, FileNotFoundError):
        return 3  # ERROR_EXISTENCE
    if isinstance(ex, IsADirectoryError):
        return 4  # ERROR_KIND
    if isinstance(ex, PermissionError):
        return 6  # ERROR_PERMISSION
    if isinstance(ex, (ValueError, OverflowError)):
        return 1  # ERROR_SEEK
    if isinstance(ex, OSError):
        if ex.errno in (errno.EISDIR, errno.ENOTDIR):
            return 4  # ERROR_KIND
        if ex.errno in (errno.EINVAL, errno.ESPIPE):
            return 1  # ERROR_SEEK
    return 7  # ERROR_RUNTIME


def read_file_chunk(file_path: str, seek: int, size: int) -> tuple[int, int, bool, bytes]:
    """Return (resolved seek, error, end, data) using zubax.file.Read semantics."""
    try:
        if os.path.isdir(file_path):
            return 0, 4, False, b""  # ERROR_KIND
        with open(file_path, "rb") as file:
            if seek < 0:
                file.seek(seek + 1, os.SEEK_END)
            else:
                file.seek(seek)
            resolved_seek = file.tell()
            data = file.read(size)
            file.seek(0, os.SEEK_END)
            end = file.tell() <= resolved_seek + len(data)
    except (OSError, ValueError, OverflowError) as ex:
        return 0, file_error_from_exception(ex), False, b""
    return resolved_seek, 0, end, data  # ERROR_OK


def setup_logging(verbosity: int) -> None:
    level = [logging.WARNING, logging.INFO, logging.DEBUG][min(verbosity, 2)]
    fmt = "%(asctime)s %(levelname)-5.5s %(name)s: %(message)s"
    try:
        import coloredlogs  # type: ignore[import-untyped]
    except ImportError:
        logging.basicConfig(level=level, format=fmt)
    else:
        coloredlogs.install(level=level, fmt=fmt)


def emit(stream: Any = None, **fields: Any) -> None:
    stream = stream or sys.stdout
    payload = json.dumps(fields, cls=DSDLJSONEncoder)
    if stream is sys.stdout and sys.stdout.isatty():
        console.print_json(json=payload, indent=None)
    else:
        stream.write(payload + "\n")
    stream.flush()


def emit_error(ex: Exception) -> None:
    logger.debug("Exception reported as JSON; traceback:", exc_info=ex)
    emit(error=type(ex).__name__, info=str(ex))


def json_output(value: Arrival | Response | Exception, topic: Topic | None) -> None:
    if isinstance(value, Exception):
        emit_error(value)
    elif isinstance(value, Arrival):
        bc = value.breadcrumb
        emit(
            ts=timestamp(value.timestamp),
            remote=bc.remote_id,
            topic=bc.topic.name,
            tag=bc.tag,
            msg=escape(value.message),
        )
    else:
        assert topic is not None
        emit(
            ts=timestamp(value.timestamp),
            remote=value.remote_id,
            topic=topic.name,
            seqno=value.seqno,
            msg=escape(value.message),
        )


def dsdl_output(schema: pydsdl.CompositeType, value: Arrival | Response | Exception, topic: Topic | None) -> None:
    if isinstance(value, Exception):
        emit_error(value)
    else:
        try:
            obj = pydsdl.deserialize(schema, value.message)
            meta: dict[str, Any] = {"ts": timestamp(value.timestamp)}
            if isinstance(value, Arrival):
                bc = value.breadcrumb
                meta.update(remote=bc.remote_id, topic=bc.topic.name, tag=bc.tag)
            else:
                assert topic is not None
                meta.update(remote=value.remote_id, topic=topic.name, seqno=value.seqno)
            obj["_meta_"] = meta
            emit(**obj)
        except Exception as ex:
            emit_error(ex)


def raw_output(value: Arrival | Response | Exception, topic: Topic | None) -> None:
    if isinstance(value, Exception):
        emit(sys.stderr, error=type(value).__name__, info=str(value))
    else:
        sys.stdout.buffer.write(value.message)
        sys.stdout.buffer.flush()


def timestamp(ts: Instant) -> str:
    """Instant is monotonic, so it is stamped against the wall clock at the moment of arrival."""
    epoch = time.time() - (time.monotonic() - ts.s)
    return datetime.fromtimestamp(epoch, timezone.utc).astimezone().isoformat(timespec="microseconds")


def make_transport(iface: str | None, bitrate: int) -> Transport:
    if iface is None:
        from pycyphal2.udp import UDPTransport

        return UDPTransport.new()

    from pycyphal2.can import CANTransport, Interface

    if iface.startswith("/") or iface.upper().startswith("COM"):
        import can
        from pycyphal2.can.pythoncan import PythonCANInterface

        itf: Interface = PythonCANInterface(can.ThreadSafeBus(interface="slcan", channel=iface, bitrate=bitrate))
    else:
        from pycyphal2.can.socketcan import SocketCANInterface

        logger.debug("SocketCAN %r: --bitrate ignored, the kernel owns it", iface)
        itf = SocketCANInterface(iface)
    return CANTransport.new(itf)


async def drain(sub: Subscriber, output: Output) -> None:
    async for arrival in sub:
        output(arrival, None)


async def cmd_sub(node: Node, config: SubConfig) -> int:
    subs = [node.subscribe(x) for x in config.topics]
    logger.info("Subscribed to %s", config.topics)
    try:
        await asyncio.gather(*(drain(sub, config.output) for sub in subs))
    finally:
        for sub in subs:
            sub.close()
    return 0


async def cmd_pub(node: Node, config: PubConfig) -> int:
    pubs = [node.advertise(x) for x in config.topics]
    try:
        deadline = Instant.now() + config.timeout
        message = config.input()
        await asyncio.gather(*(pub(deadline, message, reliable=True) for pub in pubs))
        logger.info("Published on %s", config.topics)
    except Exception as ex:
        config.output(ex, None)
        return 1
    finally:
        for pub in pubs:
            pub.close()
    return 0


async def request(pub: Publisher, config: ReqConfig, message: bytes) -> int:
    """Requests on one topic and prints the responses as they arrive; returns how many were received."""
    count = 0
    stream = None
    try:
        stream = await pub.request(Instant.now() + config.timeout, config.timeout, message)
        async for response in stream:
            config.output(response, pub.topic)
            count += 1
    except LivenessError as ex:
        if not count:
            config.output(ex, pub.topic)
    except Error as ex:
        config.output(ex, pub.topic)
    finally:
        if stream is not None:
            stream.close()
    return count


async def cmd_req(node: Node, config: ReqConfig) -> int:
    pubs = [node.advertise(x) for x in config.topics]
    try:
        message = config.input()
        counts = await asyncio.gather(*(request(pub, config, message) for pub in pubs))
    except Exception as ex:
        config.output(ex, None)
        return 1
    finally:
        for pub in pubs:
            pub.close()
    return 0 if sum(counts) > 0 else 1


async def serve_file_read_request(arrival: Arrival, seek: int, size: int, path: str, timeout: float) -> None:
    resolved_seek, error, end, data = read_file_chunk(path, seek, size)
    payload = serialize_file_read_response(resolved_seek, error, end, data)
    bc = arrival.breadcrumb
    emit(
        ts=timestamp(arrival.timestamp),
        remote=bc.remote_id,
        topic=bc.topic.name,
        tag=bc.tag,
        path=path,
        seek=resolved_seek,
        error=error,
        end=end,
    )
    logger.info("File read: file=%r seek=%d size=%d error=%d", path, resolved_seek, len(data), error)
    try:
        await arrival.breadcrumb(arrival.timestamp + timeout, payload, reliable=True)
    except Error as ex:
        logger.warning("File-read response failed: remote=%016x file=%r: %s", arrival.breadcrumb.remote_id, path, ex)


async def serve_file_reads(sub: Subscriber, timeout: float) -> None:
    async for arrival in sub:
        request = deserialize_file_read_request(arrival.message)
        if request is None:
            logger.warning("Dropping malformed file-read request of size %d on %r", len(arrival.message), sub.pattern)
            continue
        try:
            await serve_file_read_request(arrival, *request, timeout)
        except Exception:
            logger.exception("File-read request failed on %r", sub.pattern)


async def cmd_fs(node: Node, config: FsConfig) -> int:
    subs = [node.subscribe(x) for x in config.topics]
    logger.info("File server ready on %s", config.topics)
    try:
        await asyncio.gather(*(serve_file_reads(sub, config.timeout) for sub in subs))
    finally:
        for sub in subs:
            sub.close()
    return 0


async def cmd_ls(node: Node, config: LsConfig) -> int:
    queue: asyncio.Queue[Topic] = asyncio.Queue()
    handle = node.monitor(queue.put_nowait)
    modulus = node.transport.subject_id_modulus
    seen: set[int] = set()
    try:
        await node.scout(SCOUT_PATTERN)
        # Known topics keep gossiping, so the idle timer must track discoveries, not arrivals.
        deadline = time.monotonic() + config.timeout
        while (remaining := deadline - time.monotonic()) > 0:
            try:
                topic = await asyncio.wait_for(queue.get(), timeout=remaining)
            except TimeoutError:
                break
            if topic.hash in seen:
                continue
            seen.add(topic.hash)
            deadline = time.monotonic() + config.timeout
            emit(
                hash=topic.hash,
                evictions=topic.evictions,
                subject=topic.subject_id(modulus),
                name=topic.name,
            )
    finally:
        handle.close()
    logger.info("Discovered %d topic(s)", len(seen))
    return 0


async def run(config: Config) -> int:
    transport = make_transport(config.can, config.bitrate)
    try:
        node = Node.new(transport, HOME, config.ns)
        try:
            if config.remap:
                node.remap(config.remap)
            if isinstance(config, SubConfig):
                return await cmd_sub(node, config)
            if isinstance(config, PubConfig):
                return await cmd_pub(node, config)
            if isinstance(config, ReqConfig):
                return await cmd_req(node, config)
            if isinstance(config, FsConfig):
                return await cmd_fs(node, config)
            if isinstance(config, LsConfig):
                return await cmd_ls(node, config)
            raise AssertionError(type(config))
        finally:
            node.close()
    finally:
        transport.close()


def resolve_config() -> Config | None:
    class Verbosity(argparse.Action):
        def __init__(self, option_strings: list[str], dest: str, **kwargs: Any) -> None:
            self.count = 0
            super().__init__(option_strings, dest, nargs=0, **kwargs)

        def __call__(
            self,
            parser: argparse.ArgumentParser,
            namespace: argparse.Namespace,
            values: Any,
            option_string: str | None = None,
        ) -> None:
            self.count += 1
            setattr(namespace, self.dest, self.count)

    class Append(argparse.Action):
        items: list[str] = []

        def __call__(self, parser: Any, namespace: Any, value: Any, option_string: Any = None) -> None:
            self.items.append(value)
            setattr(namespace, self.dest, self.items)

    class Once(argparse.Action):
        items: dict[str, str] = {}

        def __call__(self, parser: Any, namespace: Any, value: Any, option_string: Any = None) -> None:
            if self.dest in self.items:
                parser.error(f"{option_string} cannot be repeated")
            self.items[self.dest] = value
            setattr(namespace, self.dest, value)

    class Parser(argparse.ArgumentParser):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            kwargs["allow_abbrev"] = False
            super().__init__(*args, **kwargs)

    options = Parser(add_help=False)
    options.add_argument(
        "--can",
        metavar="IFACE_NAME",
        default=argparse.SUPPRESS,
        help="Use Cyphal/CAN: SocketCAN name or serial port (config 'can', envvar CYNIC_CAN)",
    )
    options.add_argument(
        "--bitrate",
        type=int,
        default=argparse.SUPPRESS,
        help="CAN bitrate (config 'bitrate', envvar CYNIC_BITRATE), default 1000000, no effect with SocketCAN",
    )
    options.add_argument(
        "--ns", metavar="NAMESPACE", default=argparse.SUPPRESS, help="defaults to envvar CYPHAL_NAMESPACE or empty"
    )
    options.add_argument(
        "--remap",
        metavar="REMAP_STRING",
        default=argparse.SUPPRESS,
        help="Whitespace-separated from=to topic name pairs, applied atop CYPHAL_REMAP envvar",
    )
    options.add_argument(
        "-t",
        "--timeout",
        metavar="SECOND",
        type=float,
        default=argparse.SUPPRESS,
        help=f"pub deadline, response/ls timeout (config 'timeout', default {DEFAULT_TIMEOUT} s)",
    )
    options.add_argument(
        "-v", "--verbose", action=Verbosity, default=argparse.SUPPRESS, help="INFO, or DEBUG if repeated"
    )
    for flags, help_ in (
        (("-i", "--enc"), "input encoding: raw DATA without escape expansion, or YAML serialized using DSDL"),
        (("-o", "--dec"), "output decoding: raw payload bytes, or JSON deserialized using DSDL"),
        (("--io",), "use the same encoding/decoding for every direction supported by the command"),
    ):
        options.add_argument(*flags, metavar="raw|FILE.dsdl", action=Once, default=argparse.SUPPRESS, help=help_)
    options.add_argument(
        "--dsdl-root",
        metavar="DIRECTORY",
        action=Append,
        default=argparse.SUPPRESS,
        help="DSDL root namespace (config 'dsdl_root', envvar DSDL_ROOT)",
    )
    options.add_argument("--version", action="version", version=f"cynic {__version__}")
    with console.capture() as capture:
        console.print(HelpMarkdown(__doc__))
    description = "\n".join(line.rstrip() for line in capture.get().splitlines())
    parser = Parser(
        prog="cynic",
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        parents=[options],
    )
    sub = parser.add_subparsers(dest="command", parser_class=Parser)

    p_sub = sub.add_parser("sub", parents=[options], help="subscribe to each TOPIC and emit messages into stdout")
    p_sub.add_argument("topic", nargs="+")

    p_pub = sub.add_parser("pub", parents=[options], help="publish stdin or DATA after '--' on each TOPIC once")
    p_pub.add_argument("topic", nargs="+")

    p_req = sub.add_parser(
        "req",
        parents=[options],
        help="publish stdin or DATA after -- on each TOPIC, print responses",
    )
    p_req.add_argument("topic", nargs="+")

    p_fs = sub.add_parser("fs", parents=[options], help="serve file-read requests on each TOPIC")
    p_fs.add_argument("topic", metavar="TOPIC", nargs="+", help="topic that carries zubax.file.Read requests")

    sub.add_parser("ls", parents=[options], help="discover topics, print each once as it appears; exits when idle")

    argv = sys.argv[1:]
    source: Input = sys.stdin.buffer.read
    input_ = source
    if data_supplied := "--" in argv:
        separator = argv.index("--")
        if len(argv) != separator + 2:
            parser.error("expected exactly one DATA argument after --")
        data = os.fsencode(argv[-1])
        source = lambda: data
        input_ = lambda: unescape(data.decode("latin-1"))
        argv = argv[:separator]
    args = parser.parse_args(argv)
    if args.command is None:
        try:
            parser.print_help()
            sys.stdout.flush()
        except BrokenPipeError:
            os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        return None
    cli = vars(args)
    cli.update(Once.items)
    if Append.items:
        cli["dsdl_root"] = Append.items
    if data_supplied and cli["command"] not in ("pub", "req"):
        parser.error("DATA after -- is only valid with pub or req")

    values: dict[str, Any] = {}
    if (path := Path.cwd() / "cynic.toml").exists():
        try:
            with path.open("rb") as file:
                values = tomllib.load(file)
        except (OSError, tomllib.TOMLDecodeError) as ex:
            parser.error(f"cannot read {path!r}: {ex}")

    for name in ("can", "bitrate", "timeout", "verbose"):
        if name not in values:
            continue
        value = values[name]
        if name == "can":
            valid = isinstance(value, str)
        else:
            valid = isinstance(value, (int, float) if name == "timeout" else int) and not isinstance(value, bool)
        if not valid:
            parser.error(f"{path!r}: {name!r} has an invalid type")
    if "dsdl_root" in values and not (
        isinstance(values["dsdl_root"], list) and all(isinstance(x, str) for x in values["dsdl_root"])
    ):
        parser.error(f"{path!r}: 'dsdl_root' has an invalid type")

    env_bitrate: int | None = None
    if value := os.environ.get("CYNIC_BITRATE"):
        try:
            env_bitrate = int(value)
        except ValueError:
            parser.error("CYNIC_BITRATE must be an integer")

    can = cli.get("can", values.get("can", os.environ.get("CYNIC_CAN")))
    if "can" not in cli and "can" in values:
        if isinstance(can, str) and os.path.isabs(can) and glob.has_magic(can):
            matches = sorted(glob.glob(can))
            if len(matches) != 1:
                parser.error(f"CAN path pattern {can!r} matched {len(matches)} paths: {matches!r}")
            can = matches[0]

    bitrate = cli.get("bitrate", values.get("bitrate", env_bitrate))
    timeout = float(cli.get("timeout", values.get("timeout", DEFAULT_TIMEOUT)))
    verbose = cli.get("verbose", values.get("verbose", 0))
    command = cli["command"]
    enc, dec, io = cli.get("enc"), cli.get("dec"), cli.get("io")
    for name, selector, commands in (
        ("enc", enc, ("pub", "req")),
        ("dec", dec, ("sub", "req")),
        ("io", io, ("pub", "sub", "req")),
    ):
        if selector is not None and command not in commands:
            parser.error(f"--{name} is only valid with {'/'.join(commands)}")
    if io is not None and (enc is not None or dec is not None):
        parser.error("--io cannot be combined with --enc or --dec")
    enc, dec = (
        (io if command in ("pub", "req") else None, io if command in ("sub", "req") else None)
        if io is not None
        else (enc, dec)
    )

    roots: list[Path] | None = None
    schemas: dict[Path, pydsdl.CompositeType] = {}

    def resolve_schema(selector: str, output_direction: bool) -> pydsdl.CompositeType:
        nonlocal roots
        try:
            file = Path(selector).resolve()
            if roots is None:
                containers = [Path(x) for x in os.environ.get("CYPHAL_PATH", "").split(os.pathsep) if x]
                roots = [
                    Path(x)
                    for x in cli.get("dsdl_root", [])
                    + values.get("dsdl_root", [])
                    + [x for x in os.environ.get("DSDL_ROOT", "").split(os.pathsep) if x]
                ] + [
                    x
                    for p in containers
                    if p.is_dir()
                    for x in p.iterdir()
                    if x.is_dir() and not x.name.startswith(".")
                ]
            if file not in schemas:
                schemas[file] = load_dsdl(file, roots)
            schema = schemas[file]
        except (OSError, RuntimeError, ValueError, pydsdl.Error) as ex:
            parser.error(f"cannot load DSDL {selector!r}: {ex}")
        if isinstance(schema, pydsdl.ServiceType):
            return schema.response_type if command == "req" and output_direction else schema.request_type
        return schema

    if enc == "raw":
        input_ = source
    elif enc is not None:
        input_schema = resolve_schema(enc, False)
        input_ = lambda: pydsdl.serialize(input_schema, yaml.safe_load(source()), relaxed=True)

    output: Output = json_output
    if dec == "raw":
        output = raw_output
    elif dec is not None:
        output = partial(dsdl_output, resolve_schema(dec, True))
    common = dict(
        can=can,
        bitrate=1_000_000 if bitrate is None else bitrate,
        ns=cli.get("ns", ""),
        remap=cli.get("remap", ""),
        verbose=verbose,
        output=output,
        input=input_,
    )
    match cli["command"]:
        case "sub":
            return SubConfig(topics=tuple(cli["topic"]), **common)
        case "pub":
            return PubConfig(topics=tuple(cli["topic"]), timeout=timeout, **common)
        case "req":
            return ReqConfig(topics=tuple(cli["topic"]), timeout=timeout, **common)
        case "fs":
            return FsConfig(topics=tuple(cli["topic"]), timeout=timeout, **common)
        case "ls":
            return LsConfig(timeout=timeout, **common)
    raise AssertionError(cli["command"])


def main() -> int:
    config = resolve_config()
    if config is None:
        return 0
    setup_logging(config.verbose)
    try:
        return asyncio.run(run(config))
    except KeyboardInterrupt:
        return 0
    except BrokenPipeError:
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())  # Silence the flush at interpreter exit.
        return 0
    except Exception as ex:
        logger.error("%s: %s", type(ex).__name__, ex)
        logger.info("Unhandled exception", exc_info=ex)
        return 1


if __name__ == "__main__":
    sys.exit(main())
