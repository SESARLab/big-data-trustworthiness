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
          pythonFull

          # This execute some shell code to initialize a venv in $venvDir before
          # dropping into the shell
          pythonPackages.venvShellHook

          # Those are dependencies that we would like to use from nixpkgs, which will
          # add them to PYTHONPATH and thus make them accessible from within the venv.
          pythonPackages.virtualenv
          # pythonPackages.ipython
          (pythonPackages.matplotlib.override { enableQt = true; enableTk = true; })


          # pythonPackages.numpy
          # pythonPackages.pyspark
          # pythonPackages.apache-airflow # crash while running webserver

          # hadoop
          # spark
        ];

        JAVA_HOME = "${pkgs.jdk8}";
        HADOOP_HOME = "${pkgs.hadoop}/lib/${pkgs.hadoop.untarDir}";
        SPARK_HOME = "${pkgs.spark}/lib/${pkgs.spark.untarDir}";
        OPENLINEAGE_URL = "http://localhost:5000";
        HADOOP_CONF_DIR = "/etc/hadoop-conf/";
        SPARK_CONF_DIR = "/nix/store/0xxwyn6sa9vw7xsl3m1bv7z138qg899p-spark-conf";
        # PYSPARK_PYTHON = "./venv/bin/python3";
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
          export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native/:${pkgs.stdenv.cc.cc.lib}/lib
          export AIRFLOW_HOME=`pwd`
          export PYSPARK_PYTHON=`pwd`/venv/bin/python3
          export PYTHONPATH=`pwd`/venv/lib/python3.9/site-packages:$PYTHONPATH
        '';
      };
    });
}
