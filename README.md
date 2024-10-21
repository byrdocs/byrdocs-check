# byrdocs-check

## 少女折寿中
![少女祈祷中](https://img.moegirl.org.cn/common/1/11/%E8%90%83%E9%A6%99%E7%A5%88%E7%A5%B7%E4%B8%AD.gif)

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