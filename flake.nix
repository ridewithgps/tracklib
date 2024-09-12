{
  description = "RWGPS Tracklib";

  outputs = { self, nixpkgs, ... }:
    let
      pkgs = nixpkgs.legacyPackages.x86_64-linux;
    in {
      devShells.x86_64-linux.default = pkgs.mkShell {
        name = "tracklib";
        packages = with pkgs; [
          stdenv.cc.cc.lib
        ];
      };
      devShells.x86_64-linux.ruby = pkgs.mkShell {
        name = "tracklib";
        packages = with pkgs; [
          ruby_3_3
          clang
        ];
        LIBCLANG_PATH = "${pkgs.libclang.lib}/lib";
        shellHook = ''
          mkdir -p .nix-gems
          export GEM_HOME=$PWD/.nix-gems
          export GEM_PATH=$GEM_HOME
          export PATH=$GEM_HOME/bin:$PATH
        '';
      };
    };
}
