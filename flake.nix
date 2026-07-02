{
  description = "magic-wormhole transit relay server written in rust";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
  };

  outputs =
    { self, nixpkgs }:
    let
      cargoToml = builtins.fromTOML (builtins.readFile ./Cargo.toml);
      pname = cargoToml.package.name;
      version = cargoToml.package.version;

      allSystems = [
        "x86_64-linux"
        "aarch64-linux"
      ];

      # Helper function to generate attributes for all systems
      forAllSystems =
        f:
        nixpkgs.lib.genAttrs allSystems (
          system:
          f {
            pkgs = import nixpkgs { inherit system; };
          }
        );
    in
    {
      packages = forAllSystems (
        { pkgs }: {
          default = pkgs.rustPlatform.buildRustPackage {
            inherit pname version;
            src = self;

            meta.mainProgram = pname;

            cargoHash = "sha256-St1nIZabP7JkGYRTh3qE2zgxZRkAiK0jb6ltZZ/BNlQ=";
          };
        }
      );

      nixosModules.default =
        {
          config,
          lib,
          pkgs,
          ...
        }:
        let
          cfg = config.services.rusty-transit-relay;
        in
        {
          options.services.rusty-transit-relay = {
            enable = lib.mkEnableOption "rusty-transit-relay service";

            package = lib.mkOption {
              type = lib.types.package;
              default = self.packages.${pkgs.stdenv.hostPlatform.system}.default;
              defaultText = lib.literalExpression "self.packages.\${pkgs.stdenv.hostPlatform.system}.default";
              description = "The rusty-transit-relay package to use.";
            };

            listen = lib.mkOption {
              type = lib.types.listOf lib.types.str;
              default = [ ];
              example = [
                "127.0.0.1:4001"
                "[::]:4001"
              ];
              description = ''
                List of socket addresses to listen on. 
                If empty, the application will use its default listen address.
              '';
            };
          };

          config = lib.mkIf cfg.enable {
            systemd.services.rusty-transit-relay = {
              description = "rusty-transit-relay server";
              after = [ "network.target" ];
              wantedBy = [ "multi-user.target" ];

              serviceConfig = {
                Type = "notify";
                TimeoutStartSec = "15s";
                ExecStart = lib.escapeShellArgs (
                  [ (lib.getExe cfg.package) ]
                  ++ lib.concatMap (addr: [
                    "--listen"
                    addr
                  ]) cfg.listen
                );
                Restart = "on-failure";
                # Best practice: run the service with a dynamically allocated unprivileged user
                DynamicUser = true;
              };
            };
          };
        };

      devShells = forAllSystems (
        { pkgs }: {
          default = pkgs.mkShell {
            packages = with pkgs; [
              cargo
              rustc
              rust-analyzer
              clippy
              rustfmt
              nixfmt-rfc-style
              nil
            ];
          };
        }
      );
    };
}
