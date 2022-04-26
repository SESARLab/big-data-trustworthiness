{ config, lib, pkgs, modulesPath, ... }:
let
  setup_scirpt = ''
    sudo mkdir -p /hdfs
    sudo chown -R hdfs:hadoop /hdfs

    for p in {nn,dn,jn,rm,nm,jhs,HTTP}; do
    	sudo kadmin.local -q "ank -randkey $p/my.engine";
    	sudo kadmin.local -q "xst -k /etc/hadoop.keytab $p/my.engine";
    	sudo kadmin.local -q "ktrem -k /etc/hadoop.keytab $p/my.engine old"
    done
    sudo chown hdfs:hadoop /etc/hadoop.keytab


    sudo kadmin.local -q "ank -randkey spark/my.engine";
    sudo kadmin.local -q "xst -k /etc/spark.keytab spark/my.engine";
    sudo kadmin.local -q "ktrem -k /etc/spark.keytab spark/my.engine old"
    sudo chown spark:spark /etc/spark.keytab
  '';
  hadoop_keytab_path = "/etc/hadoop.keytab";
  spark_keytab_path = "/etc/spark.keytab";
  pysparkPackageSelector = p: with p; [ numpy pyspark ];
  pysparkEnv = pkgs.python3.withPackages pysparkPackageSelector;
  hadoopConf = import (modulesPath + "/services/cluster/hadoop/conf.nix") {
    inherit pkgs lib;
    cfg = config.services.hadoop;
  };
  hadoopConfDir = "${hadoopConf}/";
  spark = pkgs.spark.override {
    extraPythonPackages = pysparkPackageSelector pkgs.python3.pkgs;
  };
  sparkConfDir = pkgs.stdenv.mkDerivation {
    name = "spark-conf";
    dontUnpack = true;
    installPhase = ''
      # source standard environment
      . $stdenv/setup

      # shorthands
      base_conf=${pkgs.spark}/lib/${pkgs.spark.untarDir}/conf/

      # create output dirs for new derivation
      mkdir -p $out/

      # link unchanged files from the original gnome-session
      for f in $base_conf/*.template ; do
         ln -sf $f $out/
      done

      # change selected files
      cp $out/log4j.properties{.template,}

      cat > $out/spark-env.sh <<- STOP
      export JAVA_HOME="${pkgs.jdk8}"
      export SPARK_HOME="${pkgs.spark}/lib/${pkgs.spark.untarDir}"
      export SPARK_DIST_CLASSPATH=$(${pkgs.hadoop}/bin/hadoop classpath)
      export PYSPARK_PYTHON="${pysparkEnv.outPath}/bin/${pysparkEnv.executable}"
      export PYSPARK_DRIVER_PYTHON="${pysparkEnv.outPath}/bin/${pysparkEnv.executable}"
      export PYTHONPATH="\$PYTHONPATH:$PYTHONPATH"
      export HADOOP_CONF_DIR="${hadoopConfDir}"
      export SPARKR_R_SHELL="${pkgs.R}/bin/R"
      export PATH="\$PATH:${pkgs.R}/bin"
      STOP

      cat > $out/spark-defaults.conf <<- STOP
      spark.eventLog.enabled									true
      spark.eventLog.dir											hdfs://localhost:/logs/spark
      spark.history.fs.logDirectory						hdfs://localhost:/logs/spark
      # spark.yarn.keytab											${spark_keytab_path}
      # spark.yarn.principal									spark/my.engine@MY.ENGINE
      spark.history.ui.acls.enable						true
      spark.history.kerberos.enabled					true
      spark.history.kerberos.keytab						${spark_keytab_path}
      spark.history.kerberos.principal				spark/my.engine@MY.ENGINE
      spark.yarn.appMasterEnv.PYSPARK_PYTHON	${pysparkEnv.outPath}/bin/${pysparkEnv.executable}
      spark.yarn.appMasterEnv.PYTHONPATH			${pysparkEnv.outPath}/lib/${pysparkEnv.executable}/site-packages
      spark.executorEnv.PYSPARK_PYTHON        ${pysparkEnv.outPath}/bin/${pysparkEnv.executable}
      STOP
    '';
  };
in
{

  networking = {
    hosts = {
      "127.0.0.1" = [
        "ds.my.engine"
        "kdc.my.engine"
        "my.engine"
      ];
    };

  };

  services = {
    spark = {
      package = spark;
      master = { enable = true; restartIfChanged = true; };
      worker = { enable = true; restartIfChanged = true; };
      confDir = sparkConfDir;
    };

    hadoop = {
      coreSite = {
        "fs.defaultFS" = "hdfs://my.engine:8020";

        # HDFS IMPERSONATION
        "hadoop.proxyuser.hdfs.hosts" = "*";
        "hadoop.proxyuser.hdfs.groups" = "*";

        # HIVE IMPERSONATION
        "hadoop.proxyuser.hive.hosts" = "*";
        "hadoop.proxyuser.hive.groups" = "*";

        # ENABLE AUTHENTICATION
        "hadoop.security.authentication" = "kerberos";
        "hadoop.security.authorization" = "true";
        "hadoop.rpc.protection" = "privacy";

        "hadoop.security.auth_to_local" = ''
          RULE:[2:$1/$2@$0]([ndj]n/.*@MY\.ENGINE)s/.*/hdfs/
          RULE:[2:$1/$2@$0]([rn]m/.*@MY\.ENGINE)s/.*/yarn/
          RULE:[2:$1/$2@$0](jhs/.*@MY\.ENGINE)s/.*/mapred/
          DEFAULT
        '';
      };
      hdfsSite = {
        # DATA
        "dfs.namenode.name.dir" = "/hdfs/dfs/name";
        "dfs.datanode.data.dir" = "/hdfs/dfs/data";
        "dfs.journalnode.edits.dir" = "/hdfs/dfs/edits";

        # HDFS SECURITY
        "dfs.block.access.token.enable" = "true";
        "dfs.cluster.administrators" = "hdfs,HTTP,bertof";

        # NAME NODE SECURITY
        "dfs.namenode.keytab.file" = hadoop_keytab_path;
        "dfs.namenode.kerberos.principal" = "nn/my.engine@MY.ENGINE";
        "dfs.namenode.kerberos.internal.spnego.principal" = "HTTP/my.engine@MY.ENGINE";

        # SECONDARY NAME NODE SECURITY
        "dfs.secondary.namenode.keytab.file" = hadoop_keytab_path;
        "dfs.secondary.namenode.kerberos.principal" = "nn/my.engine@MY.ENGINE";
        "dfs.secondary.namenode.kerberos.internal.spnego.principal" = "HTTP/my.engine@MY.ENGINE";

        # DATA NODE SECURITY
        "dfs.datanode.keytab.file" = hadoop_keytab_path;
        "dfs.datanode.kerberos.principal" = "dn/my.engine@MY.ENGINE";

        # JOURNAL NODE SECURITY
        "dfs.journalnode.keytab.file" = hadoop_keytab_path;
        "dfs.journalnode.kerberos.principal" = "jn/my.engine@MY.ENGINE";

        # WEBHDFS SECURITY
        "dfs.webhdfs.enabled" = "true";

        # WEB AUTHENTICATION CONFIG
        "dfs.web.authentication.kerberos.principal" = "HTTP/my.engine@MY.ENGINE";
        "dfs.web.authentication.kerberos.keytab" = hadoop_keytab_path;
        "ignore.secure.ports.for.testing" = "true";
        "dfs.http.policy" = "HTTP_ONLY";
        "dfs.data.transfer.protection" = "privacy";

        # ## MULTIHOMED
        # "dfs.namenode.rpc-bind-host" = "0.0.0.0";
        # "dfs.namenode.servicerpc-bind-host" = "0.0.0.0";
        # "dfs.namenode.http-bind-host" = "0.0.0.0";
        # "dfs.namenode.https-bind-host" = "0.0.0.0";
        # "dfs.client.use.datanode.hostname" = "true"; # force connection by hostname
        # "dfs.datanode.use.datanode.hostname" = "true"; # force connection by hostname
      };
      yarnSite = {
        "yarn.nodemanager.admin-env" = "PATH=$PATH";
        "yarn.nodemanager.aux-services" = "mapreduce_shuffle";
        "yarn.nodemanager.aux-services.mapreduce_shuffle.class" = "org.apache.hadoop.mapred.ShuffleHandler";
        "yarn.nodemanager.bind-host" = "0.0.0.0";
        "yarn.nodemanager.container-executor.class" = "org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor";
        "yarn.nodemanager.env-whitelist" = "JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,LANG,TZ";
        "yarn.nodemanager.linux-container-executor.group" = "hadoop";
        "yarn.nodemanager.linux-container-executor.path" = "/run/wrappers/yarn-nodemanager/bin/container-executor";
        "yarn.nodemanager.log-dirs" = "/var/log/hadoop/yarn/nodemanager";
        "yarn.resourcemanager.bind-host" = "0.0.0.0";
        "yarn.resourcemanager.scheduler.class" = "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler";

        "yarn.resourcemanager.keytab" = hadoop_keytab_path;
        "yarn.resourcemanager.principal" = "rm/my.engine@MY.ENGINE";
        "yarn.nodemanager.keytab" = hadoop_keytab_path;
        "yarn.nodemanager.principal" = "nm/my.engine@MY.ENGINE";

        # "yarn.nodemanager.container-executor.class" = "org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor";

        "yarn.scheduler.capacity.root.queues" = "default";
        "yarn.scheduler.capacity.root.default.capacity" = 100;
        # "yarn.scheduler.capacity.root.default.state" = "RUNNING";
        "yarn.scheduler.capacity.root.acl_submit_applications" = "hadoop,yarn,mapred,hdfs";
      };
      httpfsSite = {
        "kerberos.realm" = "MY.ENGINE";
        "httpfs.authentication.type" = "kerberos";
        "httpfs.authentication.kerberos.principal	" = "HTTP/my.engine@MY.ENGINE";
        "httpfs.authentication.kerberos.keytab" = hadoop_keytab_path;
        "httpfs.hadoop.kerberos.principal	" = "HTTP/my.engine@MY.ENGINE";
        "httpfs.hadoop.kerberos.keytab" = hadoop_keytab_path;
      };
      extraConfDirs = [ ];

      hdfs = {
        namenode = { enable = true; formatOnInit = true; restartIfChanged = true; };
        datanode = { enable = true; restartIfChanged = true; };
        journalnode = { enable = true; restartIfChanged = true; };
        zkfc = { enable = false; restartIfChanged = true; }; # ZOOKEEPER DISABLED, not using High Availability setup
        httpfs = { enable = true; restartIfChanged = true; };
      };
      yarn = {
        resourcemanager = { enable = true; restartIfChanged = true; };
        nodemanager = { enable = true; restartIfChanged = true; useCGroups = false; };
      };
    };

    kerberos_server = {
      enable = true;
      realms."MY.ENGINE".acl = [
        { principal = "*/admin"; access = "all"; }
        { principal = "*/my.engine"; access = "all"; }
      ];
    };
  };

  krb5 = {
    enable = true;
    realms = {
      "MY.ENGINE" = {
        admin_server = "kdc.my.engine";
        kdc = "kdc.my.engine";
        # default_domain = "my.engine";
        # kpasswd_server = "odin";
      };
    };
    domain_realm = {
      # ".my.engine" = "MY.ENGINE";
      "my.engine" = "MY.ENGINE";
    };
    libdefaults = {
      default_realm = "MY.ENGINE";
      dns_lookup_realm = true;
      dns_lookup_kdc = true;
      ticket_lifetime = "24h";
      renew_lifetime = "7d";
      forwardable = true;
    };
    extraConfig = ''
      [logging]
        default = FILE:/var/log/krb5libs.log
        kdc = FILE:/var/log/krb5kdc.log
        admin_server = FILE:/var/log/kadmind.log
    '';
  };

  users.users.bertof.extraGroups = [ "hadoop" ];

  systemd.services.spark-history = {
    path = with pkgs; [ procps openssh nettools ];
    description = "spark history service.";
    after = [ "network.target" ];
    wantedBy = [ "multi-user.target" ];
    restartIfChanged = true;
    environment = {
      SPARK_CONF_DIR = sparkConfDir;
      SPARK_LOG_DIR = "/var/log/spark";
    };
    serviceConfig = {
      Type = "forking";
      User = "spark";
      Group = "spark";
      WorkingDirectory = "${pkgs.spark}/lib/${pkgs.spark.untarDir}";
      ExecStart = "${pkgs.spark}/lib/${pkgs.spark.untarDir}/sbin/start-history-server.sh";
      ExecStop = "${pkgs.spark}/lib/${pkgs.spark.untarDir}/sbin/stop-history-server.sh";
      TimeoutSec = 300;
      StartLimitBurst = 10;
      Restart = "always";
    };
  };

}
