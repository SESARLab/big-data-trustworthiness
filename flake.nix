{
  description = "A very basic flake";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-21.11";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }: flake-utils.lib.eachDefaultSystem (system:

    let
      pkgs = import nixpkgs { inherit system; };
      python = "python39";
      pythonPackages = pkgs.${python}.pkgs;
    in
    {
      devShell = pkgs.mkShell {
        name = "impurePythonEnv";
        venvDir = "./venv";
        buildInputs = with pkgs; [
          # A Python interpreter including the 'venv' module is required to bootstrap
          # the environment.
          pythonPackages.python

          # This execute some shell code to initialize a venv in $venvDir before
          # dropping into the shell
          pythonPackages.venvShellHook

          # Those are dependencies that we would like to use from nixpkgs, which will
          # add them to PYTHONPATH and thus make them accessible from within the venv.
          pythonPackages.virtualenv
          # pythonPackages.ipython

          pythonPackages.numpy
          pythonPackages.pyspark
          # pythonPackages.apache-airflow # crash while running webserver

          # hadoop
          # spark
        ];

        JAVA_HOME = "${pkgs.jdk8}";
        HADOOP_HOME = "${pkgs.hadoop}/lib/${pkgs.hadoop.untarDir}";
        SPARK_HOME = "${pkgs.spark}/lib/${pkgs.spark.untarDir}";
        OPENLINEAGE_URL = "http://localhost:5000";
        HADOOP_CONF_DIR = "/nix/store/77jir5wdg8asbpr4pd5gs73y49mjr8f2-hadoop-conf";
        SPARK_CONF_DIR = "/nix/store/4p8pg9hi52c2wq8r0hwi0b1643gi9xia-spark-conf";
        # SPARK_LOG_DIR = "/var/log/spark";
        # SPARK_MASTER_HOST = "127.0.0.1";


        # Run this command, only after creating the virtual environment
        postVenvCreation = ''
          unset SOURCE_DATE_EPOCH
          pip install -r requirements.txt
        '';

        # Now we can execute any commands within the virtual environment.
        # This is optional and can be left out to run pip manually.
        postShellHook = ''
          # allow pip to install wheels
          unset SOURCE_DATE_EPOCH
          export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native/
          export AIRFLOW_HOME=`pwd`
        '';
      };
    });
}
