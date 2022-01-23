{
  description = "A very basic flake";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }: flake-utils.lib.eachDefaultSystem (system:
    let
      pkgs = import nixpkgs { inherit system; };
      pythonVersion = pkgs.python39;
    in
    rec {
      devShell = pkgs.mkShell rec {
        venvDir = "venv";
        dontUseVenvShellHook=1;
        buildInputs =
          (with pythonVersion.pkgs; [ virtualenv pip setuptools venvShellHook ]) ++
          (with pkgs; [ spark hadoop ]);
        postVenvCreation = ''
          python -m pip install -r requirements.txt
        '';
        # postShellHook = ''
        #   export AIRFLOW_HOME=`pwd`;
        #   export JAVA_HOME="${pkgs.jdk8}";
        # '';
        postShellHook = ''
          echo postShellHook
          # allow pip to install wheels
          unset SOURCE_DATE_EPOCH
          export AIRFLOW_HOME=`pwd`;
          export JAVA_HOME="${pkgs.jdk8}";
        '';
      };
    });
}
