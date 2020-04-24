with import <nixpkgs> {};

stdenv.mkDerivation {
  name = "java_tracklib";

  buildInputs = [
    openjdk
    maven
  ];
}
