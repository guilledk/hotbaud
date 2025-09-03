{ pkgs ? import <nixpkgs-25.05> {} }:
let
  nativeBuildInputs = with pkgs; [
    libgcc.lib
  ];

in
pkgs.mkShell {
  inherit nativeBuildInputs;

  LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath nativeBuildInputs;
  TMPDIR = "/tmp";
}
