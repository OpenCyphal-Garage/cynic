# Cynic

[![PyPI](https://img.shields.io/pypi/v/cyphal-cynic?logo=pypi&color=ffff00)](https://pypi.org/project/cyphal-cynic/)
[![Forum](https://img.shields.io/discourse/https/forum.opencyphal.org/users.svg?logo=discourse&color=1700b3)](https://forum.opencyphal.org)

Cynic (Cyphal Network Investigation Console) is a simple command-line tool for inspecting and exercising
[Cyphal](https://opencyphal.org) networks.

Cynic is built for use with Cyphal v1.1, which introduces named topics.
It is fully interoperable with Cyphal/CAN v1.0 via pinned topics, where the topic name is constructed from its
subject-ID, like `1234#1234`.

## Usage

Install:

```
pip install cyphal-cynic
```

Run `cn --help` for full usage info, or read the code -- it is simple and compact. Some basic examples are shown below.

### List topics visible in the network

```shell
cn ls
cn --can=can0 ls
cn --can=COM8 --bitrate=125000 ls
```

### Publish/subscribe

```shell
printf '\x01\x02' | cn pub topic/foo
cn sub -o raw topic/foo > messages.bin
```

Set defaults per directory in `cynic.toml`, or use environment variables:

```toml
can = "/dev/serial/by-id/usb-Zubax*Babel*if00"  # or SocketCAN iface name
bitrate = 1_000_000
dsdl_root = ["/home/user/public_regulated_data_types/uavcan", "/home/user/zubax_dsdl"]
```

### RPC/streaming

Send a request, print response(s):

```shell
cn req topic/foo/bar --io=zubax/primitive/String1K.1.0.dsdl -- 'Hello world!'
```

## Showcase

### Firmware update via Cyphal/CAN

Put this in `./cynic.toml` so that you don't have to say `--can=slcan0` with every command (optional):

```toml
can = "slcan0"  # Can also be a file wildcard for SLCAN
```

Run the file server in the background (or in a second terminal); the topic name here is arbitrarily chosen as `fwupd`:

```shell
cn fs fwupd &
```

Command the remote node to download the specified firmware file from topic `fwupd`:

```shell
echo 'upd com.zubax.fluxgrip-1-1.0.41c608fbec4f54d9.16c6d0fc4c1e972a.app.release.bin fwupd' | \
    cn pub -i ~/zubax/zubax_dsdl/zubax/primitive/String256.1.0.dsdl command
```
