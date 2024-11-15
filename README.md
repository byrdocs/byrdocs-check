# byrdocs-check

## Usage

First, compile the binaries of PDFium and place them in the `./lib` directory with the appropriate filename, such as `libpdfium.xxx`.

Then

```sh
cargo run --bin check-pr -d "./path_you_want_to_check" ...
```

Or

```sh
cargo run --bin check-cmt -d "./path_you_want_to_check" ...
```

check-pr: Check the PRs in the specified directory.(Check format)

check-cmt: Check the comments in the specified directory.(Upload cover and metadata)
