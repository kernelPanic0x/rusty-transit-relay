# Rusty Transit Relay

A Rust implementation of the Magic Wormhole Transit Relay server. This relay helps establish direct connections between peers when direct connectivity isn't possible due to NAT or firewalls.

## Features

- Support for both legacy and modern handshake protocols
- IPv4 and IPv6 support

## Installation

Clone the repository and build using Cargo:

```bash
git clone https://github.com/yourusername/rusty-transit-relay
cd rusty-transit-relay
cargo build --release
```

## Usage

Run the relay server with default settings (listens on 0.0.0.0:4001 and [::]:4001):

```bash
./target/release/rusty-transit-relay
```

Specify custom listen addresses:

```bash
./target/release/rusty-transit-relay --listen 127.0.0.1:4001 --listen [::1]:4001
```

## Configuration

The following constants can be modified in the source code:

- `BUFFER_SIZE`: Maximum buffer size for data transfer (default: 1MB)
- `PEER_TIMEOUT`: Connection timeout duration (default: 5 seconds)
- `GC_INTERVAL`: Garbage collection interval (default: 10 seconds)

## License

MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.