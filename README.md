## :lizard: :control_knobs: **zig mpsc**

[![CI][ci-shield]][ci-url]
[![LC][lc-shield]][lc-url]

### Zig port of the [intrusive wait-free MPSC queue](https://www.1024cores.net/home/lock-free-algorithms/queues/intrusive-mpsc-node-based-queue) created by [Dmitry Vyukov](https://github.com/dvyukov).

### :rocket: Usage

1. Add `mpsc` as a dependency in your `build.zig.zon`.

    <details>

    <summary><code>build.zig.zon</code> example</summary>

    ```zig
    .{
        .name = "<name_of_your_package>",
        .version = "<version_of_your_package>",
        .dependencies = .{
            .mpsc = .{
                .url = "https://github.com/tensorush/zig-mpsc/archive/<git_tag_or_commit_hash>.tar.gz",
                .hash = "<package_hash>",
            },
        },
    }
    ```

    Set `<package_hash>` to `12200000000000000000000000000000000000000000000000000000000000000000`, and Zig will provide the correct found value in an error message.

    </details>

2. Add `mpsc` as a module in your `build.zig`.

    <details>

    <summary><code>build.zig</code> example</summary>

    ```zig
    const mpsc = b.dependency("mpsc", .{});
    exe.addModule("mpsc", mpsc.module("mpsc"));
    ```

    </details>

<!-- MARKDOWN LINKS -->

[ci-shield]: https://img.shields.io/github/actions/workflow/status/tensorush/zig-mpsc/ci.yaml?branch=main&style=for-the-badge&logo=github&label=CI&labelColor=black
[ci-url]: https://github.com/tensorush/zig-mpsc/blob/main/.github/workflows/ci.yaml
[lc-shield]: https://img.shields.io/github/license/tensorush/zig-mpsc.svg?style=for-the-badge&labelColor=black
[lc-url]: https://github.com/tensorush/zig-mpsc/blob/main/LICENSE.md
