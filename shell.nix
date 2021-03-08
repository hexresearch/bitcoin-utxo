with import ./nix/pkgs.nix {};

stdenv.mkDerivation rec {
  name = "rust-env";
  env = buildEnv { name = name; paths = buildInputs; };

  buildInputs = [
    rustup
    clang
    llvm
    llvmPackages.libclang
  ];
  shellHook = ''
  export LIBCLANG_PATH="${llvmPackages.libclang}/lib"
  '';
}
