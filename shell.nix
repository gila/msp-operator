{ pkgs ? import <nixpkgs> {} }:
with pkgs;
mkShell {
  buildInputs = [
    clang
    openssl
    llvmPackages_11.libclang
    pkg-config
    pre-commit
  ];

  LIBCLANG_PATH = "${llvmPackages_11.libclang.lib}/lib";
  PROTOC = "${protobuf}/bin/protoc";
}
