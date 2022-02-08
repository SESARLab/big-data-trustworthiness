{
  description = "A very basic flake";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }: flake-utils.lib.eachDefaultSystem (system:

    let
      pkgs = nixpkgs.legacyPackages.${system};
      python = "python39";
      pythonPackages = pkgs.${python}.pkgs;
    in
    {

      packages = {
        history-server = pkgs.writeShellScriptBin "compile_script" ''
          CLASS="org.apache.spark.deploy.history.HistoryServer"
          SPARK_CONF_DIR=/home/bertof/.config/nixpkgs/spark_conf/ ;
          SPARK_LOG_DIR=/var/log/spark ;
          SPARK_HOME="${pkgs.spark}/lib/${pkgs.spark.untarDir}" ;
          SPARK_MASTER_HOST=127.0.0.1
          sudo -E -u spark bash -c '. $SPARK_HOME/sbin/spark-config.sh; . $SPARK_HOME/bin/load-spark-env.sh; exec $SPARK_HOME/sbin/spark-daemon.sh start org.apache.spark.deploy.history.HistoryServer 1'
        '';

      };

      devShell = pkgs.mkShell {
        name = "impurePythonEnv";
        venvDir = "./venv";
        buildInputs = with pkgs;[
          # A Python interpreter including the 'venv' module is required to bootstrap
          # the environment.
          pythonPackages.python

          # This execute some shell code to initialize a venv in $venvDir before
          # dropping into the shell
          pythonPackages.venvShellHook

          # Those are dependencies that we would like to use from nixpkgs, which will
          # add them to PYTHONPATH and thus make them accessible from within the venv.
          pythonPackages.virtualenv
          pythonPackages.ipython

          hadoop
          spark
        ];

        JAVA_HOME = "${pkgs.jdk8}";
        HADOOP_HOME = "${pkgs.hadoop}/lib/${pkgs.hadoop.untarDir}";
        SPARK_HOME = "${pkgs.spark}/lib/${pkgs.spark.untarDir}";
        OPENLINEAGE_URL = "http://localhost:5000";
        HADOOP_CONF_DIR = "/nix/store/h6gjg7zklwzyrdsirj5vmy98qzpk9xnc-hadoop-conf/";
        SPARK_CONF_DIR = "/home/bertof/.config/nixpkgs/spark_conf/";
        SPARK_LOG_DIR = "/var/log/spark";
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
