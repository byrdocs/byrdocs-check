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

`upload-metadata` supports an optional environment variable `SITEMAP_MIN_LASTMOD`.
When set, any sitemap `lastmod` earlier than this value will be replaced by it.
Supported formats: `YYYY-MM-DD` or RFC3339, for example `2024-01-01` or `2024-01-01T00:00:00+08:00`.

Homepage and `/about` sitemap `lastmod` are fetched from GitHub.
You can override the target repository with `BYRDOCS_SITE_REPO_OWNER` and `BYRDOCS_SITE_REPO_NAME`; both default to `byrdocs`.
