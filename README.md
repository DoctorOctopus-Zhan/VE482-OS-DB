# LemonDB Recovered Files
![CI status](https://focs.ji.sjtu.edu.cn:2222/api/badges/ve482-21/p2-group-08/status.svg?ref=refs/heads/master)

## Introduction

- ./src
   Contains the source code for Group 8  of lemnonDB.
   Multi-thread.

## Developer Quick Start

See INSTALL.md for instructions on building from source.

`ClangFormat` and `EditorConfig` are used to format codes.

Hint to using `ClangFormat`: need to install `clang-format-12` first.
```shell
apt install clang-format-12
find . -name "*.cpp" -o -name "*.h" | sed 's| |\\ |g' | xargs clang-format-12 -i
```

And make sure your code editor has `EditorConfig` support.

## Copyright

Lemonion Inc. 2018

