#coding:utf-8
'''
Created on Sep 8, 2017

@author: cloud
'''
import pprint,re,time,math,sys,urllib,urllib2
from configparser import ConfigParser
from subprocess import Popen,PIPE,STDOUT
from cm_api.api_client import ApiResource
from cm_api.endpoints.services import ApiService
from cm_api.endpoints.services import ApiServiceSetupInfo



def err_print(contents):
    print('\033[1;31m%s\033[0m'%contents)

def pre_print(contents):
    print('\033[1;32m%s\033[0m'%contents)

def warn_print(contents):
    print('\033[1;33m%s\033[0m'%contents)

def pretty_print(arg):
    PRETTY_PRINT = pprint.PrettyPrinter(indent=4)
    PRETTY_PRINT.pprint(arg)

#get host information from cluster_config.ini
class read_config(object):
    def __init__(self):
        cf=ConfigParser()
        cf.read("cluster_config.ini")
        cluster_hosts=[re.split(r"[\|\|]", host_info) for host_info in [hostname+"||"+cf.get("host_conf",hostname) for hostname in cf.options("host_conf")]]
        self.conf=map(lambda x:[y for y in x if y] , cluster_hosts)

def get_ele_index(target_list,target_element):
    try:
        path=[i for i, x in enumerate(target_list) if x == target_element][0]
        return path
    except:
        return None

def shell_command(command,info="",prin_t=True):
#     print(command)
    cm=Popen(command,stdout=PIPE,stderr=PIPE,shell=True)
    _,err=cm.communicate()
    if cm.returncode != 0:
        print(err.read())
        print("Non zero exit code:%s executing: %s" % (cm.returncode, info))
        raise StandardError

class cluster_config():
    CM_CONFIG = {
        'TSQUERY_STREAMS_LIMIT' : 1000,
    }
    CDH_VERSION = "CDH5"
    WORK_DIR = "/home/cloudera-cdh"
    
    API_VERSION = 16
    ADMIN_USER = "admin"
    ADMIN_PASSWD = "admin"
    
    CLUSTER_NAME = "cluster"
    CONFIG = ConfigParser()
    CONFIG.read("cluster_config.ini")
    CLUSTER_HOSTS = [host for host in re.split(r'[\|\|]',CONFIG.get("cluster_conf","cluster_hosts")) if host]
    CDH_PARCEL_VERSION = CONFIG.get("cluster_conf", "cdh.parcel.version")
    KAFKA_PARCEL_VERSION = CONFIG.get("cluster_conf", "kafka.parcel.version")
    PARCELS=[{ 'name' : "CDH", 'version' : CDH_PARCEL_VERSION },\
             { 'name' : "KAFKA", 'version' : KAFKA_PARCEL_VERSION }\
            ]
    # Do not change this sequence
    SUPPORT_SERVICES = ["ZOOKEEPER","HDFS","YARN","SPARK","HBASE","HIVE","IMPALA","KAFKA"]
    CM_MANAGER_INFO = [manager_info for manager_info in re.split(r'[\|\|]',[host for host in [opt+'||'+CONFIG.get("host_conf",opt) for opt in CONFIG.options("host_conf")] if re.findall(r'manager$', host)][0]) if manager_info]
    
    
    CM_HOST = CM_MANAGER_INFO[0]
    DB_HOST = CM_MANAGER_INFO[0]
    DB_PASSWD = "bigdata"
    
    INSTALL_SERVICES = [service for service in re.split(r'[\|\|]', CONFIG.get("cluster_conf","install_services")) if service]
    NAME_NODE_HOST = CONFIG.get("role_conf","hdfs.namenode.host")
    NAME_NODE_HOST_INFO = [info for info in re.split(r"[\|\|]",NAME_NODE_HOST+"||"+CONFIG.get("host_conf",NAME_NODE_HOST)) if info ]
    DATA_NODE_HOSTS = [host for host in re.split(r'[\|\|]', CONFIG.get("role_conf","hdfs.datanode.hosts")) if host]
    
    CMD_TIMEOUT = 180

#===========================================================
#      cloudera management service
#===========================================================
    MGMT_SERVICENAME = "MGMT"
    MGMT_SERVICE_CONFIG = {
       'zookeeper_datadir_autocreate': 'true',
    }
    MGMT_ROLE_CONFIG = {
       'quorumPort': 3181,
    }
    AMON_ROLENAME = "ACTIVITYMONITOR"
    AMON_ROLE_CONFIG = {
       'firehose_database_host': DB_HOST,
       'firehose_database_user': 'monitor',
       'firehose_database_password': DB_PASSWD,
       'firehose_database_type': 'mysql',
       'firehose_database_name': 'cdh_monitor',
       'mgmt_log_dir': WORK_DIR + '/var/log/cloudera-scm-firehose',
    }
    APUB_ROLENAME = "ALERTPUBLISHER"
    APUB_ROLE_CONFIG = { 
        'mgmt_log_dir': WORK_DIR + '/var/log/cloudera-scm-alertpublisher',
    }
    ESERV_ROLENAME = "EVENTSERVER"
    ESERV_ROLE_CONFIG = {
       'eventserver_index_dir': WORK_DIR + "/var/lib/cloudera-scm-eventserver",
       'mgmt_log_dir': WORK_DIR + '/var/log/cloudera-scm-eventserver',
    }
    HMON_ROLENAME = "HOSTMONITOR"
    HMON_ROLE_CONFIG = { 
        "firehose_storage_dir": WORK_DIR + "/var/lib/cloudera-host-monitor",
        'mgmt_log_dir': WORK_DIR + '/var/log/cloudera-scm-firehose',    
    }
    SMON_ROLENAME = "SERVICEMONITOR"
    SMON_ROLE_CONFIG = { 
        "firehose_storage_dir": WORK_DIR + "/var/lib/cloudera-service-monitor",
        'mgmt_log_dir': WORK_DIR + '/var/log/cloudera-scm-firehose',
    }

    RMAN_ROLENAME = "REPORTSMANAGER"
    RMAN_ROLE_CONFIG = {
       'headlamp_database_host': DB_HOST,
       'headlamp_database_user': 'reports',
       'headlamp_database_password': DB_PASSWD,
       'headlamp_database_type': 'mysql',
       'headlamp_database_name': 'cdh_reports',
       'headlamp_heapsize': '215964392',
       'mgmt_log_dir': WORK_DIR + '/var/log/cloudera-scm-headlamp',
       'headlamp_scratch_dir': WORK_DIR + '/var/lib/cloudera-scm-headlamp'
    }
#===========================================================
#      zookeeper
#===========================================================
    ZOOKEEPER_SERVICE_NAME = "ZOOKEEPER"
    ZOOKEEPER_SERVER_HOSTS = [host for host in re.split(r"[\|\|]",CONFIG.get("role_conf","zookeeper.server.hosts")) if host]
    ZOOKEEPER_SERVICE_CONFIG = {
       'zookeeper_datadir_autocreate': 'true',
    }
    ZOOKEEPER_ROLE_CONFIG = {
        'quorumPort': 3181,
        'electionPort': 4181,
        'dataLogDir': WORK_DIR+'/var/lib/zookeeper',
        'dataDir': WORK_DIR+'/var/lib/zookeeper',
        'zk_server_log_dir': WORK_DIR+'/var/log',
        'oom_heap_dump_dir': WORK_DIR+'/tmp',
        'maxClientCnxns': '1024',
    }
    
#===========================================================
#      hdfs
#===========================================================
    HDFS_SERVICE_NAME = "HDFS"
    HDFS_SERVICE_CONFIG = {
        'dfs_replication': 3,
        'dfs_permissions': 'false',
#         'service_health_suppression_hdfs_canary_health': 'true',
#         'role_health_suppression_data_node_block_count': 'true',
#         'smon_client_config_overrides': "<property><name>dfs.socket.timeout</name><value>30000</value></property><property><name>dfs.datanode.socket.write.timeout</name><value>30000</value></property><property><name>ipc.client.connect.max.retries</name><value>5</value></property><property><name>fs.permissions.umask-mode</name><value>000</value></property>",
        
    }
    HDFS_NAMENODE_SERVICE_NAME = "nn"
    HDFS_NAMENODE_HOST = NAME_NODE_HOST
    HDFS_NAMENODE_CONFIG = {
        'dfs_name_dir_list': WORK_DIR + '/hadoop/nn',
        'dfs_namenode_handler_count': 30 if 30>int(math.log(len(DATA_NODE_HOSTS))*20) else 30>int(math.log(len(DATA_NODE_HOSTS))*20),
        'dfs_namenode_service_handler_count': int(len(DATA_NODE_HOSTS)*10),
        'dfs_namenode_servicerpc_address': 8022,
    }
    HDFS_SECONDARY_NAMENODE_HOST = CONFIG.get("role_conf","hdfs.secondary.namenode.host")
    HDFS_SECONDARY_NAMENODE_CONFIG = {
        'fs_checkpoint_dir_list': WORK_DIR + '/hadoop/sn',
    }
    HDFS_DATANODE_HOSTS = DATA_NODE_HOSTS
    #dfs_datanode_du_reserved must be smaller than the amount of free space across the data dirs
    #Ideally each data directory will have at least 1TB capacity; they need at least 100GB at a minimum 
    #dfs_datanode_failed_volumes_tolerated must be less than the number of different data dirs (ie volumes) in dfs_data_dir_list
    HDFS_DATANODE_CONFIG = {
#         'dfs_data_dir_list': '/data01/hadoop/datanode,/data02/hadoop/datanode,/data03/hadoop/datanode,/data04/hadoop/datanode,/data05/hadoop/datanode,/data06/hadoop/datanode,/data07/hadoop/datanode,/data08/hadoop/datanode',
        'dfs_data_dir_list': WORK_DIR + '/hadoop/dn',
        'dfs_datanode_handler_count': int(len(DATA_NODE_HOSTS)*10),
        'dfs_datanode_max_xcievers': 8192,
        'dfs_datanode_du_reserved': 10737418240,
        'dfs_datanode_failed_volumes_tolerated': 0,
        'dfs_datanode_data_dir_perm': 755,
    }
    HDFS_GATEWAY_HOSTS = list(CLUSTER_HOSTS)
    if CM_HOST not in HDFS_GATEWAY_HOSTS:
        HDFS_GATEWAY_HOSTS.append(CM_HOST)
    HDFS_GATEWAY_CONFIG = {
        'dfs_client_use_trash' : 'true'
    }
    
#===========================================================
#      mapreduce
#===========================================================
    MAPRED_SERVICE_NAME = "MAPRED"
    MAPRED_SERVICE_CONFIG = {
      'hdfs_service': HDFS_SERVICE_NAME,
    }
    MAPRED_JT_HOST = CLUSTER_HOSTS[0]
    MAPRED_JT_CONFIG = {
      'mapred_jobtracker_restart_recover': 'true',
      'mapred_job_tracker_handler_count': 30, #int(ln(len(DATANODES))*20),
      'jobtracker_mapred_local_dir_list': WORK_DIR + '/var/lib/mapred',
      'mapreduce_jobtracker_staging_root_dir': WORK_DIR + '/var/lib/staging',
      'mapreduce_jobtracker_split_metainfo_maxsize': '100000000',
    }
    MAPRED_TT_HOSTS = list(CLUSTER_HOSTS)
    MAPRED_TT_CONFIG = {
      #'tasktracker_mapred_local_dir_list': '/data01/hadoop/mapred,/data02/hadoop/mapred,/data03/hadoop/mapred,/data04/hadoop/mapred,/data05/hadoop/mapred,/data06/hadoop/mapred,/data07/hadoop/mapred,/data08/hadoop/mapred',
      'tasktracker_mapred_local_dir_list': WORK_DIR + '/var/lib/mapred',
      'mapred_tasktracker_map_tasks_maximum': 6,
      'mapred_tasktracker_reduce_tasks_maximum': 3,
      'override_mapred_child_java_opts_base': '-Xmx4g -Djava.net.preferIPv4Stack=true',
      'override_mapred_child_ulimit': 8388608,
      'override_mapred_reduce_parallel_copies': 5,
      'tasktracker_http_threads': 40,
      'override_mapred_output_compress': 'true',
      'override_mapred_output_compression_type': 'BLOCK',
      'override_mapred_output_compression_codec': 'org.apache.hadoop.io.compress.SnappyCodec',
      'override_mapred_compress_map_output': 'true',
      'override_mapred_map_output_compression_codec': 'org.apache.hadoop.io.compress.SnappyCodec',
      'override_io_sort_record_percent': '0.15',
    }
    MAPRED_GW_HOSTS = list(CLUSTER_HOSTS)
    MAPRED_GW_CONFIG = {
      'mapred_reduce_tasks' : int(MAPRED_TT_CONFIG['mapred_tasktracker_reduce_tasks_maximum']*len(HDFS_DATANODE_HOSTS)/2),
      'mapred_submit_replication': 3,
    }
    
#===========================================================
#      yarn
#===========================================================
    YARN_SERVICE_NAME = "YARN"
    YARN_SERVICE_CONFIG = {
        'hdfs_service': HDFS_SERVICE_NAME,
        'zookeeper_service': ZOOKEEPER_SERVICE_NAME,
        'hdfs_user_home_dir': WORK_DIR+'/var/lib/hadoop-yarn'
    }
    YARN_RM_HOST = NAME_NODE_HOST
    YARN_RM_CONFIG = { 
        'yarn_rm_bind_wildcard':"true",
        'yarn_scheduler_maximum_allocation_vcores': 32,
        'yarn_scheduler_maximum_allocation_mb': 24576,
        'oom_heap_dump_dir': WORK_DIR+'/tmp',
        'resource_manager_log_dir': WORK_DIR + "/var/log/hadoop-yarn",
        }
    YARN_JHS_HOST = NAME_NODE_HOST
    YARN_JHS_CONFIG = { 
        'oom_heap_dump_dir': WORK_DIR+'/tmp',
        'mr2_jobhistory_log_dir': WORK_DIR + "/var/log/hadoop-mapreduce",
    }
    YARN_NM_HOSTS = DATA_NODE_HOSTS
    YARN_NM_CONFIG = {
        'yarn_nodemanager_resource_memory_mb': 24576,
        'yarn_nodemanager_resource_cpu_vcores': 4,
        'oom_heap_dump_dir': WORK_DIR+'/tmp',
        'yarn_nodemanager_local_dirs': WORK_DIR + '/yarn/nm',
        'node_manager_log_dir': WORK_DIR + '/var/log/hadoop-yarn',
        'yarn_nodemanager_log_dirs': WORK_DIR + '/yarn/container-logs',
        'yarn_nodemanager_recovery_dir': WORK_DIR + "/var/lib/hadoop-yarn/yarn-nm-recovery",
        'yarn_nodemanager_remote_app_log_dir': WORK_DIR + "/tmp/logs",
      
    }
    YARN_GW_HOSTS = list(CLUSTER_HOSTS)
    YARN_GW_CONFIG = {
      'mapred_submit_replication': min(3, len(YARN_GW_HOSTS)),
      'hadoop_job_history_dir': WORK_DIR + "/var/log/hadoop-mapreduce/jobhistory",
    }
    
#===========================================================
#     spark_on_yarn
#===========================================================
    SPARK_SERVICE_NAME = "SPARK"
    SPARK_SERVICE_CONFIG = {
#       'hdfs_service': HDFS_SERVICE_NAME,
        'yarn_service': YARN_SERVICE_NAME,
#       'hbase_service': "hbase",
    }
    SPARK_YARN_HISTORY_SERVER_HOST = NAME_NODE_HOST
    SPARK_YARN_HISTORY_SERVER_CONFIG = {
    #   'master_max_heapsize': 67108864,
#       'oom_heap_dump_dir': WORK_DIR+'/tmp',
      'log_dir': WORK_DIR + '/var/log/spark',
    }
    SPARK_GATEWAY_HOSTS = list(CLUSTER_HOSTS)
    SPARK_GATEWAY_CONFIG = {
    #   'executor_total_max_heapsize': 67108864,
    #   'worker_max_heapsize': 67108864,
    }
    
#===========================================================
#      hbase
#===========================================================
    HBASE_SERVICE_NAME = "HBASE"
    HBASE_SERVICE_CONFIG = {
      'hdfs_service': HDFS_SERVICE_NAME,
      'zookeeper_service': ZOOKEEPER_SERVICE_NAME,
#       'audit_event_log_dir': WORK_DIR + "/var/lib/hbase/audit",
    }
    HBASE_HM_HOST = NAME_NODE_HOST
    HBASE_HM_CONFIG = { 
        "hbase_master_log_dir": WORK_DIR + "/var/log/hbase"
    }
    HBASE_RS_HOSTS = DATA_NODE_HOSTS
    HBASE_RS_CONFIG = {
      'hbase_hregion_memstore_flush_size': 1073741824,
      'hbase_regionserver_handler_count': 10,
      'hbase_regionserver_java_heapsize': 2147483648,
#       'hbase_regionserver_java_opts': '',
      'hbase_regionserver_log_dir': WORK_DIR + "/var/log/hbase"
    }
#     HBASE_THRIFTSERVER_SERVICE_NAME = "HBASETHRIFTSERVER"
#     HBASE_THRIFTSERVER_HOST = CLUSTER_HOSTS[0]
#     HBASE_THRIFTSERVER_CONFIG = { }
#     HBASE_GW_HOSTS = list(CLUSTER_HOSTS)
#     HBASE_GW_CONFIG = { }
    
#===========================================================
#      hive
#===========================================================
    HIVE_SERVICE_NAME = "HIVE"
    HIVE_SERVICE_CONFIG = {
      'hive_metastore_database_host': CM_HOST,
      'hive_metastore_database_name': 'cdh_hive',
      'hive_metastore_database_password': DB_PASSWD,
      'hive_metastore_database_port': 3306,
      'hive_metastore_database_type': 'mysql',
      'mapreduce_yarn_service': YARN_SERVICE_NAME,
      'hbase_service': HBASE_SERVICE_NAME,
      'zookeeper_service': ZOOKEEPER_SERVICE_NAME,
      'spark_on_yarn_service': SPARK_SERVICE_NAME,
#       "audit_event_log_dir": WORK_DIR + "/var/log/hive/audit",
#       "linage_event_log_dir": WORK_DIR + "/var/log/hive/lineage",
    }
    HIVE_HMS_HOST = NAME_NODE_HOST
    HIVE_HMS_CONFIG = {
      'hive_metastore_java_heapsize': 4294967296,
      'hive_metastore_server_max_message_size': 536870912,
      "hive_log_dir": WORK_DIR + "/var/log/hive",
    }
    HIVE_HS2_HOST = NAME_NODE_HOST
    HIVE_HS2_CONFIG = { 
        "hive_log_dir": WORK_DIR + "/var/log/hive",
        "hiveserver2_spark_executor_cores": 5
        }
    HIVE_WHC_HOST = NAME_NODE_HOST
    HIVE_WHC_CONFIG = { 
        "hcatalog_log_dir": WORK_DIR + "/var/log/hcatalog",
        }
    HIVE_GW_HOSTS = list(CLUSTER_HOSTS)
    HIVE_GW_CONFIG = { }
    
#===========================================================
#      impala
#===========================================================
    IMPALA_SERVICE_NAME = "IMPALA"
    IMPALA_SERVICE_CONFIG = {
      'hdfs_service': HDFS_SERVICE_NAME,
      'hbase_service': HBASE_SERVICE_NAME,
      'hive_service': HIVE_SERVICE_NAME,
    }
    IMPALA_SS_HOST = NAME_NODE_HOST
    IMPALA_SS_CONFIG = { 
        'log_dir': WORK_DIR + "/var/log/catalogd",
        'core_dump_dir': WORK_DIR + "/var/log/catalogd",
    }
    IMPALA_CS_HOST = NAME_NODE_HOST
    IMPALA_CS_CONFIG = { 
        'log_dir': WORK_DIR + "/var/log/catalogd",
        'core_dump_dir': WORK_DIR + "/var/log/catalogd",
    }
    IMPALA_ID_HOSTS = DATA_NODE_HOSTS
    IMPALA_ID_CONFIG = { 
        'scratch_dirs': WORK_DIR + "/impala/impalad",
        'log_dir': WORK_DIR + "/var/log/impalad",
        'core_dump_dir': WORK_DIR + "/var/log/catalogd",
        'local_library_dir': WORK_DIR + "/var/lib/impala/udfs",
        'minidump_path': WORK_DIR + "/var/log/impala-minidumps",
        'impalad_memory_limit': 17179869184,
    }
    
#===========================================================
#      kafka
#===========================================================
    KAFKA_SERVICE_NAME = "KAFKA"
    KAFKA_SERVICE_CONFIG = {
        'zookeeper_service': ZOOKEEPER_SERVICE_NAME,
        'auto.create.topics.enable': 'false',
        'default.replication.factor': 3,
        'leader.imbalance.check.interval.seconds': 3600,
        'leader.imbalance.per.broker.percentage': 16,
        'replica.fetch.max.bytes': 1073741824,
        'num.replica.fetchers': 3,
        'log.cleaner.threads': 2,
        
    }
    KAFKA_BROKER_HOSTS = DATA_NODE_HOSTS
    KAFKA_BROKER_CONFIG = {
        'broker_max_heap_size': 4096,# int(2147483648*2),
        'log_dir': WORK_DIR + "/var/log/kafka",
        'log.dirs': WORK_DIR + "/var/local/kafka/data",
        'log.roll.hours': 240,
        'num.io.threads': 8,
        'broker_java_opts': "-server -XX:+UseParNewGC -XX:MaxDirectMemorySize=1G -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -XX:+CMSScavengeBeforeRemark -XX:+DisableExplicitGC -Djava.awt.headless=true",
        
    }
    
#==================================================================================================
#  cdh cluster deploy object
#==================================================================================================
class cdh_cluster_deploy():
    def __init__(self):
        self.cm_config=cluster_config()
        self.support_services=["HDFS","ZOOKEEPER","YARN","HIVE","HBASE","IMPALA","KAFKA"]
        self.CM_API = None
        self.CM_MANAGER = None
        self.CM_CLUSTER = None
    
    def __API(self):
        print("*"*60+"\n      Start connect to CM and update configuration\n"+"*"*60)
        self.CM_API = ApiResource(self.cm_config.CM_HOST, version=self.cm_config.API_VERSION, username=self.cm_config.ADMIN_USER, password=self.cm_config.ADMIN_PASSWD)
        return self.CM_API
    
    def __MANAGER(self):
        self.CM_MANAGER = self.CM_API.get_cloudera_manager()
        self.CM_MANAGER.update_config(self.cm_config.CM_CONFIG)
        print("Connected to CM host on " + self.cm_config.CM_HOST + " and updated CM configuration")
        return self.CM_MANAGER
    
    def __CLUSTER(self):
        print("*"*60+"\n      Start initialize cluster \n"+"*"*60)
        cluster = self.CM_API.create_cluster(name=self.cm_config.CLUSTER_NAME, version=self.cm_config.CDH_VERSION)
        # Add the CM host to the list of hosts to add in the cluster so it can run the management services
        all_hosts = list(self.cm_config.CLUSTER_HOSTS)
        if self.cm_config.CM_HOST not in all_hosts:
            all_hosts.append(self.cm_config.CM_HOST)
        cluster.add_hosts(all_hosts)
        self.CM_CLUSTER=cluster
        print("Initialized cluster " + self.cm_config.CLUSTER_NAME + " which uses CDH version " + self.cm_config.CDH_VERSION)
        return self.CM_CLUSTER
    
    def init_cdh(self):
        self.__API()
        self.__MANAGER()
        self.__CLUSTER()
    
    def parcel_deploy(self):
        print("*"*60+"\n      Start deploy parcels \n"+"*"*60)
        parcels=self.cm_config.PARCELS
        for parcel in parcels:
            p = self.CM_CLUSTER.get_parcel(parcel['name'], parcel['version'])
            p.start_download()
            while True:
                p = self.CM_CLUSTER.get_parcel(parcel['name'], parcel['version'])
                if p.stage == "DOWNLOADED":
                    sys.stdout.write("Downloading parcel: %s: %s / %s" % (parcel['name'], p.state.progress, p.state.totalProgress)+"\r")
                    sys.stdout.flush()
                    break
                if p.state.errors:
                    raise Exception(str(p.state.errors))
                sys.stdout.write("Downloading parcel: %s: %s / %s" % (parcel['name'], p.state.progress, p.state.totalProgress)+"\r")
                sys.stdout.flush()
                time.sleep(15)
            sys.stdout.write("\n")
            sys.stdout.flush()
            print("Downloaded parces: %s %s" % (parcel['name'],parcel['version']))
            p.start_distribution()
            while True:
                p = self.CM_CLUSTER.get_parcel(parcel['name'], parcel['version'])
                if p.stage == "DISTRIBUTED":
                    sys.stdout.write("Distributing parcel: %s: %s / %s" % (parcel['name'], p.state.progress, p.state.totalProgress)+"\r")
                    sys.stdout.flush()
                    break
                if p.state.errors:
                    raise Exception(str(p.state.errors))
                sys.stdout.write("Distributing parcel: %s: %s / %s" % (parcel['name'], p.state.progress, p.state.totalProgress)+"\r")
                sys.stdout.flush()
                time.sleep(15)
            sys.stdout.write("\n")
            sys.stdout.flush()
            print("Distributed parcel: %s %s" % (parcel['name'],parcel['version']))
            p.activate()
        print "Downloaded and distributed parcels: "
        pretty_print(self.cm_config.PARCELS)
    def management_deploy(self):
        print("*"*60+"\n      Start deploy cloudera management service \n"+"*"*60)
        mgmt = self.CM_MANAGER.create_mgmt_service(ApiServiceSetupInfo())
   
        # create roles. Note that host id may be different from host name (especially in CM 5). Look it it up in /api/v5/hosts
        mgmt.create_role(self.cm_config.AMON_ROLENAME + "-1", "ACTIVITYMONITOR", self.cm_config.CM_HOST)
        mgmt.create_role(self.cm_config.APUB_ROLENAME + "-1", "ALERTPUBLISHER", self.cm_config.CM_HOST)
        mgmt.create_role(self.cm_config.ESERV_ROLENAME + "-1", "EVENTSERVER", self.cm_config.CM_HOST)
        mgmt.create_role(self.cm_config.HMON_ROLENAME + "-1", "HOSTMONITOR", self.cm_config.CM_HOST)
        mgmt.create_role(self.cm_config.SMON_ROLENAME + "-1", "SERVICEMONITOR", self.cm_config.CM_HOST)
#        mgmt.create_role(nav_role_name + "-1", "NAVIGATOR", CM_HOST)
#        mgmt.create_role(navms_role_name + "-1", "NAVIGATORMETADATASERVER", CM_HOST)
#        mgmt.create_role(rman_role_name + "-1", "REPORTSMANAGER", cm_config.CM_HOST)
       
        # now configure each role   
        for group in mgmt.get_all_role_config_groups():
            if group.roleType == "ACTIVITYMONITOR":
                group.update_config(self.cm_config.AMON_ROLE_CONFIG)
            elif group.roleType == "ALERTPUBLISHER":
                group.update_config(self.cm_config.APUB_ROLE_CONFIG)
            elif group.roleType == "EVENTSERVER":
                group.update_config(self.cm_config.ESERV_ROLE_CONFIG)
            elif group.roleType == "HOSTMONITOR":
                group.update_config(self.cm_config.HMON_ROLE_CONFIG)
            elif group.roleType == "SERVICEMONITOR":
                group.update_config(self.cm_config.SMON_ROLE_CONFIG)
#            elif group.roleType == "NAVIGATOR":
#                group.update_config(nav_role_conf)
#            elif group.roleType == "NAVIGATORMETADATASERVER":
#                group.update_config(navms_role_conf)
#            elif group.roleType == "REPORTSMANAGER":
#                group.update_config(rman_role_conf)
       
        # now start the management service
        mgmt.start().wait()
        print("Deployed CM management service " + self.cm_config.MGMT_SERVICENAME + " to run on " + self.cm_config.CM_HOST)
        return mgmt
        
    def zookeeper_deploy(self):
        zk_service = self.CM_CLUSTER.create_service(self.cm_config.ZOOKEEPER_SERVICE_NAME, "ZOOKEEPER")
        zk_service.update_config(self.cm_config.ZOOKEEPER_SERVICE_CONFIG)
   
        zk_id = 0
        for zk_host in self.cm_config.ZOOKEEPER_SERVER_HOSTS:
            zk_id += 1
            self.cm_config.ZOOKEEPER_ROLE_CONFIG['serverId'] = zk_id
            role = zk_service.create_role(self.cm_config.ZOOKEEPER_SERVICE_NAME + "-" + str(zk_id), "SERVER", zk_host)
            role.update_config(self.cm_config.ZOOKEEPER_ROLE_CONFIG)
        
        print("Initialize zookeeper service")
        zk_service.init_zookeeper()
   
        return zk_service
    
    # Deploys HDFS - NN, DNs, SNN, gateways.
    # This does not yet support HA yet.
    def hdfs_deploy(self):
        hdfs_service = self.CM_CLUSTER.create_service(self.cm_config.HDFS_SERVICE_NAME, "HDFS")
        hdfs_service.update_config(self.cm_config.HDFS_SERVICE_CONFIG)
       
        nn_role_group = hdfs_service.get_role_config_group("{0}-NAMENODE-BASE".format(self.cm_config.HDFS_SERVICE_NAME))
        nn_role_group.update_config(self.cm_config.HDFS_NAMENODE_CONFIG)
        nn_service_pattern = "{0}-" + self.cm_config.HDFS_NAMENODE_SERVICE_NAME
        hdfs_service.create_role(nn_service_pattern.format(self.cm_config.HDFS_SERVICE_NAME), "NAMENODE", self.cm_config.HDFS_NAMENODE_HOST)
       
        snn_role_group = hdfs_service.get_role_config_group("{0}-SECONDARYNAMENODE-BASE".format(self.cm_config.HDFS_SERVICE_NAME))
        snn_role_group.update_config(self.cm_config.HDFS_SECONDARY_NAMENODE_CONFIG)
        hdfs_service.create_role("{0}-snn".format(self.cm_config.HDFS_SERVICE_NAME), "SECONDARYNAMENODE", self.cm_config.HDFS_SECONDARY_NAMENODE_HOST)
       
        dn_role_group = hdfs_service.get_role_config_group("{0}-DATANODE-BASE".format(self.cm_config.HDFS_SERVICE_NAME))
        dn_role_group.update_config(self.cm_config.HDFS_DATANODE_CONFIG)
       
        gw_role_group = hdfs_service.get_role_config_group("{0}-GATEWAY-BASE".format(self.cm_config.HDFS_SERVICE_NAME))
        gw_role_group.update_config(self.cm_config.HDFS_GATEWAY_CONFIG)
       
        datanode = 0
        for host in self.cm_config.HDFS_DATANODE_HOSTS:
            datanode += 1
            hdfs_service.create_role("{0}-dn-".format(self.cm_config.HDFS_SERVICE_NAME) + str(datanode), "DATANODE", host)
       
        gateway = 0
        for host in self.cm_config.HDFS_GATEWAY_HOSTS:
            gateway += 1
            hdfs_service.create_role("{0}-gw-".format(self.cm_config.HDFS_SERVICE_NAME) + str(gateway), "GATEWAY", host)
       
        return hdfs_service
    

    # Initializes HDFS - format the file system
    def hdfs_init(self):
        hdfs_service=self.CM_CLUSTER.get_service(self.cm_config.HDFS_SERVICE_NAME)
        cmd = hdfs_service.format_hdfs("{0}-nn".format(self.cm_config.HDFS_SERVICE_NAME))[0]
        if not cmd.wait(self.cm_config.CMD_TIMEOUT).success:
            print("WARNING: Failed to format HDFS, attempting to continue with the setup") 
    
    # Deploys MapReduce - JT, TTs, gateways.
    # This does not yet support HA yet.
    # This shouldn't be run if YARN is deployed.
    def mapreduce_deploy(self):
        mapred_service = self.CM_CLUSTER.create_service(self.cm_config.MAPRED_SERVICE_NAME, "MAPREDUCE")
        mapred_service.update_config(self.cm_config.MAPRED_SERVICE_CONFIG)
          
        jt = mapred_service.get_role_config_group("{0}-JOBTRACKER-BASE".format(self.cm_config.self.cm_config.MAPRED_SERVICE_NAME))
        jt.update_config(self.cm_config.MAPRED_JT_CONFIG)
        mapred_service.create_role("{0}-jt".format(self.cm_config.self.cm_config.MAPRED_SERVICE_NAME), "JOBTRACKER", self.cm_config.MAPRED_JT_HOST)
       
        tt = mapred_service.get_role_config_group("{0}-TASKTRACKER-BASE".format(self.cm_config.MAPRED_SERVICE_NAME))
        tt.update_config(self.cm_config.MAPRED_TT_CONFIG)
       
        gw = mapred_service.get_role_config_group("{0}-GATEWAY-BASE".format(self.cm_config.MAPRED_SERVICE_NAME))
        gw.update_config(self.cm_config.MAPRED_GW_CONFIG)
       
        tasktracker = 0
        for host in self.cm_config.MAPRED_TT_HOSTS:
            tasktracker += 1
            mapred_service.create_role("{0}-tt-".format(self.cm_config.MAPRED_SERVICE_NAME) + str(tasktracker), "TASKTRACKER", host)
       
        gateway = 0
        for host in self.cm_config.MAPRED_GW_HOSTS:
            gateway += 1
            mapred_service.create_role("{0}-gw-".format(self.cm_config.MAPRED_SERVICE_NAME) + str(gateway), "GATEWAY", host)
       
        return mapred_service
    
    # Deploys YARN - RM, JobHistoryServer, NMs, gateways
    # This shouldn't be run if MapReduce is deployed.
    def yarn_deploy(self):
        yarn_service = self.CM_CLUSTER.create_service(self.cm_config.YARN_SERVICE_NAME, "YARN")
        yarn_service.update_config(self.cm_config.YARN_SERVICE_CONFIG)
          
        rm = yarn_service.get_role_config_group("{0}-RESOURCEMANAGER-BASE".format(self.cm_config.YARN_SERVICE_NAME))
        rm.update_config(self.cm_config.YARN_RM_CONFIG)
        yarn_service.create_role("{0}-rm".format(self.cm_config.YARN_SERVICE_NAME), "RESOURCEMANAGER", self.cm_config.YARN_RM_HOST)
          
        jhs = yarn_service.get_role_config_group("{0}-JOBHISTORY-BASE".format(self.cm_config.YARN_SERVICE_NAME))
        jhs.update_config(self.cm_config.YARN_JHS_CONFIG)
        yarn_service.create_role("{0}-jhs".format(self.cm_config.YARN_SERVICE_NAME), "JOBHISTORY", self.cm_config.YARN_JHS_HOST)
       
        nm = yarn_service.get_role_config_group("{0}-NODEMANAGER-BASE".format(self.cm_config.YARN_SERVICE_NAME))
        nm.update_config(self.cm_config.YARN_NM_CONFIG)
       
        nodemanager = 0
        for host in self.cm_config.YARN_NM_HOSTS:
            nodemanager += 1
            yarn_service.create_role("{0}-nm-".format(self.cm_config.YARN_SERVICE_NAME) + str(nodemanager), "NODEMANAGER", host)
       
        gw = yarn_service.get_role_config_group("{0}-GATEWAY-BASE".format(self.cm_config.YARN_SERVICE_NAME))
        gw.update_config(self.cm_config.YARN_GW_CONFIG)
       
        gateway = 0
        for host in self.cm_config.YARN_GW_HOSTS:
            gateway += 1
            yarn_service.create_role("{0}-gw-".format(self.cm_config.YARN_SERVICE_NAME) + str(gateway), "GATEWAY", host)
       
        #TODO need api version 6 for these, but I think they are done automatically?
        #yarn_service.create_yarn_job_history_dir()
        #yarn_service.create_yarn_node_manager_remote_app_log_dir()
       
        return yarn_service
    
    # Deploys spark_on_yarn - history server, gateways
    def spark_deploy(self):
        spark_service = self.CM_CLUSTER.create_service(self.cm_config.SPARK_SERVICE_NAME, "SPARK_ON_YARN")
        spark_service.update_config(self.cm_config.SPARK_SERVICE_CONFIG)
        
        spark_hs=spark_service.get_role_config_group("{0}-SPARK_YARN_HISTORY_SERVER-BASE".format(self.cm_config.SPARK_SERVICE_NAME))
        spark_hs.update_config(self.cm_config.SPARK_YARN_HISTORY_SERVER_CONFIG)
        spark_service.create_role("{0}-SPARK_YARN_HISTORY_SERVER".format(self.cm_config.SPARK_SERVICE_NAME),"SPARK_YARN_HISTORY_SERVER",self.cm_config.SPARK_YARN_HISTORY_SERVER_HOST)
           
        gw = spark_service.get_role_config_group("{0}-GATEWAY-BASE".format(self.cm_config.SPARK_SERVICE_NAME))
        gw.update_config(self.cm_config.SPARK_GATEWAY_CONFIG)
       
        gateway = 0
        for host in self.cm_config.SPARK_GATEWAY_HOSTS:
            gateway += 1
            spark_service.create_role("{0}-gw-".format(self.cm_config.SPARK_SERVICE_NAME) + str(gateway), "GATEWAY", host)
       
        #TODO - CreateSparkUserDirCommand, SparkUploadJarServiceCommand???
        
        return spark_service
    
    # Deploys HBase - HMaster, RSes, HBase Thrift Server, gateways
    def hbase_deploy(self):
        hbase_service = self.CM_CLUSTER.create_service(self.cm_config.HBASE_SERVICE_NAME, "HBASE")
        hbase_service.update_config(self.cm_config.HBASE_SERVICE_CONFIG)
           
        hm = hbase_service.get_role_config_group("{0}-MASTER-BASE".format(self.cm_config.HBASE_SERVICE_NAME))
        hm.update_config(self.cm_config.HBASE_HM_CONFIG)
        hbase_service.create_role("{0}-hm".format(self.cm_config.HBASE_SERVICE_NAME), "MASTER", self.cm_config.HBASE_HM_HOST)
        
        rs = hbase_service.get_role_config_group("{0}-REGIONSERVER-BASE".format(self.cm_config.HBASE_SERVICE_NAME))
        rs.update_config(self.cm_config.HBASE_RS_CONFIG)
        
#         ts = hbase_service.get_role_config_group("{0}-HBASETHRIFTSERVER-BASE".format(hbase_service_name))
#         ts.update_config(hbase_thriftserver_config)
#         ts_name_pattern = "{0}-" + hbase_thriftserver_service_name
#         hbase_service.create_role(ts_name_pattern.format(hbase_service_name), "HBASETHRIFTSERVER", hbase_thriftserver_host)
#         
#         gw = hbase_service.get_role_config_group("{0}-GATEWAY-BASE".format(hbase_service_name))
#         gw.update_config(hbase_gw_config)
        
        regionserver = 0
        for host in self.cm_config.HBASE_RS_HOSTS:
            regionserver += 1
            hbase_service.create_role("{0}-rs-".format(self.cm_config.HBASE_SERVICE_NAME) + str(regionserver), "REGIONSERVER", host)
        
#         gateway = 0
#         for host in hbase_gw_hosts:
#             gateway += 1
#             hbase_service.create_role("{0}-gw-".format(hbase_service_name) + str(gateway), "GATEWAY", host)
        
        hbase_service.create_hbase_root()
        
        return hbase_service


    # Deploys Hive - hive metastore, hiveserver2, webhcat, gateways
    def hive_deploy(self):
        hive_service = self.CM_CLUSTER.create_service(self.cm_config.HIVE_SERVICE_NAME, "HIVE")
        hive_service.update_config(self.cm_config.HIVE_SERVICE_CONFIG)
        
        hms = hive_service.get_role_config_group("{0}-HIVEMETASTORE-BASE".format(self.cm_config.HIVE_SERVICE_NAME))
        hms.update_config(self.cm_config.HIVE_HMS_CONFIG)
        hive_service.create_role("{0}-hms".format(self.cm_config.HIVE_SERVICE_NAME), "HIVEMETASTORE", self.cm_config.HIVE_HMS_HOST)
        
        hs2 = hive_service.get_role_config_group("{0}-HIVESERVER2-BASE".format(self.cm_config.HIVE_SERVICE_NAME))
        hs2.update_config(self.cm_config.HIVE_HS2_CONFIG)
        hive_service.create_role("{0}-hs2".format(self.cm_config.HIVE_SERVICE_NAME), "HIVESERVER2", self.cm_config.HIVE_HS2_HOST)
        
        whc = hive_service.get_role_config_group("{0}-WEBHCAT-BASE".format(self.cm_config.HIVE_SERVICE_NAME))
        whc.update_config(self.cm_config.HIVE_WHC_CONFIG)
        hive_service.create_role("{0}-whc".format(self.cm_config.HIVE_SERVICE_NAME), "WEBHCAT", self.cm_config.HIVE_WHC_HOST)
        
        gw = hive_service.get_role_config_group("{0}-GATEWAY-BASE".format(self.cm_config.HIVE_SERVICE_NAME))
        gw.update_config(self.cm_config.HIVE_GW_CONFIG)
        
        gateway = 0
        for host in self.cm_config.HIVE_GW_HOSTS:
            gateway += 1
            hive_service.create_role("{0}-gw-".format(self.cm_config.HIVE_SERVICE_NAME) + str(gateway), "GATEWAY", host)
        
        return hive_service
    
    # Add hive mysql database jdbc driver
    def hive_mysqldb_deploy(self):
        host_conf=read_config()
        conf=host_conf.conf
        for node in conf:
            ip,sshport=node[1],node[3]
            shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s 'cp /opt/cm-5.11.1/share/cmf/lib/mysql-connector-java-5.1.42-bin.jar /opt/cloudera/parcels/CDH/jars/'"%(sshport,ip),"copy mysql-collector to cdh")
            shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s 'ln -s /opt/cloudera/parcels/CDH/jars/mysql-connector-java-5.1.42-bin.jar /opt/cloudera/parcels/CDH/lib/hive/lib/mysql-connector-java-bin.jar'"%(sshport,ip))
        
    # Initialized hive
    def hive_init(self):
        hive_service = self.CM_CLUSTER.get_service(self.cm_config.HIVE_SERVICE_NAME)
        hive_service.create_hive_metastore_database()
        hive_service.create_hive_metastore_tables()
        hive_service.create_hive_warehouse()
        #don't think that the create_hive_userdir call is needed as the create_hive_warehouse already creates it
        #hive_service.create_hive_userdir()
    
    # Deploys Impala - statestore, catalogserver, impalads
    def impala_deploy(self):
        impala_service = self.CM_CLUSTER.create_service(self.cm_config.IMPALA_SERVICE_NAME, "IMPALA")
        impala_service.update_config(self.cm_config.IMPALA_SERVICE_CONFIG)
        
        ss = impala_service.get_role_config_group("{0}-STATESTORE-BASE".format(self.cm_config.IMPALA_SERVICE_NAME))
        ss.update_config(self.cm_config.IMPALA_SS_CONFIG)
        impala_service.create_role("{0}-ss".format(self.cm_config.IMPALA_SERVICE_NAME), "STATESTORE", self.cm_config.IMPALA_SS_HOST)
        
        cs = impala_service.get_role_config_group("{0}-CATALOGSERVER-BASE".format(self.cm_config.IMPALA_SERVICE_NAME))
        cs.update_config(self.cm_config.IMPALA_CS_CONFIG)
        impala_service.create_role("{0}-cs".format(self.cm_config.IMPALA_SERVICE_NAME), "CATALOGSERVER", self.cm_config.IMPALA_CS_HOST)
        
        id = impala_service.get_role_config_group("{0}-IMPALAD-BASE".format(self.cm_config.IMPALA_SERVICE_NAME))
        id.update_config(self.cm_config.IMPALA_ID_CONFIG)
        
        impalad = 0
        for host in self.cm_config.IMPALA_ID_HOSTS:
            impalad += 1
            impala_service.create_role("{0}-id-".format(self.cm_config.IMPALA_SERVICE_NAME) + str(impalad), "IMPALAD", host)
    
        return impala_service
    
    # Don't think we need these at the end:
    def impala_init(self):
            impala_service=self.CM_CLUSTER.get_service(self.cm_config.IMPALA_SERVICE_NAME)
            impala_service.create_impala_catalog_database()
            impala_service.create_impala_catalog_database_tables()
            impala_service.create_impala_user_dir() 
    
    # Deploy kafka - kafka broker
    def kafka_deploy(self):
        kafka_service = self.CM_CLUSTER.create_service(self.cm_config.KAFKA_SERVICE_NAME, "KAFKA")
        kafka_service.update_config(self.cm_config.KAFKA_SERVICE_CONFIG)
#         kafka_service=cluster.get_service("KAFKA")
        
        kafka_broker = kafka_service.get_role_config_group("{0}-KAFKA_BROKER-BASE".format(self.cm_config.KAFKA_SERVICE_NAME))
        kafka_broker.update_config(self.cm_config.KAFKA_BROKER_CONFIG)
    
        broker_id = 0
        for host in self.cm_config.KAFKA_BROKER_HOSTS:
            broker_id +=1
            kafka_service.create_role("{0}-KAFKA_BROKER-".format(self.cm_config.KAFKA_SERVICE_NAME) + str(broker_id), "KAFKA_BROKER", host)
            
        return kafka_service
    
    def deploy_zookeeper(self):
        zookeeper_service = self.zookeeper_deploy()
        print "Deployed ZooKeeper " + self.cm_config.ZOOKEEPER_SERVICE_NAME + " to run on: "
        pretty_print(self.cm_config.ZOOKEEPER_SERVER_HOSTS)
        
        return zookeeper_service
        
    def deploy_hdfs(self):
        hdfs_service = self.hdfs_deploy()
        print("Deployed HDFS service " + self.cm_config.HDFS_SERVICE_NAME + " using NameNode on " + self.cm_config.HDFS_NAMENODE_HOST + ", SecondaryNameNode on " + self.cm_config.HDFS_SECONDARY_NAMENODE_HOST + ", and DataNodes running on: ")
        pretty_print(self.cm_config.HDFS_DATANODE_HOSTS)
        self.hdfs_init()
        print("Initialized HDFS service")
        
        return hdfs_service

    def deploy_yarn(self):
        yarn_service = self.yarn_deploy()
        print("Deployed YARN service " + self.cm_config.YARN_SERVICE_NAME + " using ResourceManager on " + self.cm_config.YARN_RM_HOST + ", JobHistoryServer on " + self.cm_config.YARN_JHS_HOST + ", and NodeManagers on ")
        pretty_print(self.cm_config.YARN_NM_HOSTS)
        
        return yarn_service
    
    def deploy_spark(self):
        spark_service = self.spark_deploy()
        print("Deployed SPARK service " + self.cm_config.SPARK_SERVICE_NAME + " using SparkHistoryServer on " + self.cm_config.SPARK_YARN_HISTORY_SERVER_HOST + " and Spark Gateway on ")
        pretty_print(self.cm_config.SPARK_GATEWAY_HOSTS)
        
        return spark_service
    
    def deploy_hbase(self):
        hbase_service = self.hbase_deploy()
        print("Deployed HBase service " + self.cm_config.HBASE_SERVICE_NAME + " using HMaster on " + self.cm_config.HBASE_HM_HOST + " and RegionServers on ")
        pretty_print(self.cm_config.HBASE_RS_HOSTS)
        
        return hbase_service
    
    def deploy_hive(self):
        hive_service = self.hive_deploy()
        print("Deployed Hive service " + self.cm_config.HIVE_SERVICE_NAME + " using HiveMetastoreServer on " + self.cm_config.HIVE_HMS_HOST + " and HiveServer2 on " + self.cm_config.HIVE_HS2_HOST)
        self.hive_mysqldb_deploy()
        print("Deployed hive mysql database jdbc driver")
        self.hive_init()
        print("Initialized Hive service")
        
        return hive_service
    
    def deploy_impala(self):
        impala_service = self.impala_deploy()
#         self.impala_init()
        print("Deployed Impala service " + self.cm_config.IMPALA_SERVICE_NAME + " using StateStore on " + self.cm_config.IMPALA_SS_HOST + ", CatalogServer on " + self.cm_config.IMPALA_CS_HOST + ", and ImpalaDaemons on ")
        pretty_print(self.cm_config.IMPALA_ID_HOSTS)
        
        return impala_service
        
    def deploy_kafka(self):
        kafka_service = self.kafka_deploy()
        print "Deployed Kafka service :" + self.cm_config.KAFKA_SERVICE_NAME + " using Broker on"
        pretty_print(self.cm_config.KAFKA_BROKER_HOSTS)
        
        return kafka_service
    
    def service_stats(self,cluster,service_name):
        service=cluster.get_service(service_name)
        return service.serviceState
    
    def install_service(self,service_name):
        install_scripts = {
            "HDFS":      self.deploy_hdfs,
            "YARN":      self.deploy_yarn,
            "ZOOKEEPER": self.deploy_zookeeper,
            "HIVE":      self.deploy_hive,
            "HBASE":     self.deploy_hbase,
            "IMPALA":    self.deploy_impala,
            "KAFKA":     self.deploy_kafka,
            "SPARK":     self.deploy_spark,
            }
        print("*"*60+"\n      Start to deploy %s \n"%(service_name,)+"*"*60)
        script = install_scripts[service_name.upper()]
        service = script()
        
        return service
    def healthcheck(self,service_name):
        service = self.CM_CLUSTER.get_service(service_name)
        ret = False
        helth = service.healthChecks
#        print(helth)
        role_helths = []
        role_names = []
        if helth:
            for i in helth:
#                 print("%s : %s %s"%(service.name,i[u"name"],i[u"summary"]))
                role_names.append(i[u"name"])
                role_helths.append(i[u"summary"])
                
            ret = u"GOOD" in role_helths and len(set(role_helths)) == 1
        else:
            ret = False
        return ret
# Executes steps that need to be done after the final startup once everything is deployed and running.
def post_startup(cm_config):
    # Create hive warehouse dir
#     import base64
#     header={"Content-Type": "application/json"}
#     para={
#           "serviceName":cm_config.HIVE_SERVICE_NAME,
#           "clusterName":cm_config.CLUSTER_NAME
#           }
#     url='http://' + cluster_config.CM_HOST + ':7180/api/v16/clusters/' + cm_config.CLUSTER_NAME + '/services/' + cm_config.HIVE_SERVICE_NAME + '/commands/hiveCreateHiveWarehouse'
#
#     req_data=urllib.urlencode(para)
#     req = urllib2.Request(url,req_data,header)
#     req.add_header("Authorization : Basic ", base64.b32encode("admin:admin"))
#     response=urllib2.urlopen(req)
#     print(response.read())
                # Create HDFS temp dir
    print("Create spark applicationHistory directory")
    command="ssh -tt -p %s root@%s 'sudo -u hdfs hadoop fs -mkdir -p /user/spark/applicationHistory && "\
            "sudo -u hdfs hadoop fs -chmod 777 /user/spark/applicationHistory && "\
            "sudo -u hdfs hadoop fs -chown -R  spark:spark /user/spark ' 2>&1 ;echo $?"%\
            (cm_config.NAME_NODE_HOST_INFO[3],cm_config.NAME_NODE_HOST_INFO[1])
    shell_command(command)
            
    # Noe change permissions on the /user dir so YARN will work
    command = "ssh -tt -p %s root@%s 'sudo -u hdfs hadoop fs -chmod 775 /user'"%(cm_config.NAME_NODE_HOST_INFO[3],cm_config.NAME_NODE_HOST_INFO[1])
#     shell_command(command)
    user_chmod_output = Popen(command, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True).stdout.read()
    print(user_chmod_output)
    # Create hive wharehose
    command = ['curl -i -H "Content-Type: application/json" -X POST -u "' + cm_config.ADMIN_USER + ':' + cm_config.ADMIN_PASSWD + '" -d "serviceName=' + cm_config.HIVE_SERVICE_NAME + ';clusterName=' + cm_config.CLUSTER_NAME + '" http://' + cm_config.CM_HOST + ':7180/api/v16/clusters/' + cm_config.CLUSTER_NAME + '/services/' + cm_config.HIVE_SERVICE_NAME + '/commands/hiveCreateHiveWarehouse']
    create_hive_warehouse_output = Popen(command, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True).stdout.read()
#     print(create_hive_warehouse_output)

### Main function ###
def main():
    pre_print("#"*100+"\n#"+" "*32+"Start deploy cloudera cdh cluster"+" "*33+"#\n"+"#"*100)
    cdh_cluster = cdh_cluster_deploy()
    # initialize API MANAGER CLUSTER
    cdh_cluster.init_cdh()
    # deploy cdh management
    cdh_cluster.management_deploy()
    # deploy cdh parcels
    cdh_cluster.parcel_deploy()
    
    cm_config = cdh_cluster.cm_config
    
    # deploy service
    print("*"*60+"\n      Start to deploy services\n"+"*"*60)
    install_services = cm_config.INSTALL_SERVICES
    support_services = cm_config.SUPPORT_SERVICES
    install_services_index = list(map(get_ele_index,[support_services for _ in install_services],[service_name.upper() for service_name in install_services]))
    seq_install_service=sorted([(install_services_index[i],install_services[i]) for i in range(len(install_services))])
    for service_name_index in seq_install_service:
        if service_name_index[0] != None:
            cdh_cluster.install_service(service_name_index[1])
        else:
            warn_print("Service: '%s' is unsupported"% service_name_index[1])
    
    # Need to start the cluster now as subsequent services need the cluster to be runnign
    # TODO can we just start ZK, and maybe HDFS, instead of everything? It's just needed for the search service
#     CLUSTER.first_run().wait()
    CLUSTER = cdh_cluster.CM_CLUSTER
    print("*"*60+"\nDeploy client config")
    cmd = CLUSTER.deploy_client_config()
    if not cmd.wait(cm_config.CMD_TIMEOUT).success:
        err_print("Failed to deploy client configs for {0}".format(CLUSTER.name))
    
    print("Start hdfs,zookeeper service")
    zookeeper_service = CLUSTER.get_service(cm_config.ZOOKEEPER_SERVICE_NAME)
    hdfs_service = CLUSTER.get_service(cm_config.HDFS_SERVICE_NAME)
    
    zookeeper_service.start().wait()
    hdfs_service.start().wait()
    
    print("Start check hdfs and zookeeper") 
    timeOut = 1200+time.time()
    service_health = {True:"Good",False:"Bad"}
    ret = True
    while time.time()<timeOut:
        hdfs_health = cdh_cluster.healthcheck(cm_config.HDFS_SERVICE_NAME)
        zk_health = cdh_cluster.healthcheck(cm_config.ZOOKEEPER_SERVICE_NAME)

        sys.stdout.write("Zookeeper health: %s HDFS health: %s "%(service_health[zk_health],service_health[hdfs_health])+"\r")
        sys.stdout.flush()        
        if  hdfs_health == True and  zk_health == True:
            hdfs_service.create_hdfs_tmp()
            
            post_startup(cm_config)
            ret = False
            break
        else:
            time.sleep(10)
    sys.stdout.write("\n")
    sys.stdout.flush()
    # if health check timeout run this step of last times
    if ret:
        hdfs_service.create_hdfs_tmp()
        post_startup(cm_config)
        
    print("About to restart cluster")
    CLUSTER.restart().wait()
#     CLUSTER.restart(redeploy_client_configuration=True).wait()
    print("Done restarting cluster")
    
    print("Start stoped service")
    service_list = CLUSTER.get_all_services()
    for service in service_list:
        if service:
            if service.serviceState == u'STOPPED':
                service.restart().wait()
    
    print("*"*80+"\nFinished deploying Cloudera cluster. Go to http://" + cm_config.CM_HOST + ":7180 to administer the cluster.")
    print("If there are any other services not running, restart them now.\n"+"*"*80)
if __name__ == "__main__":
    main()
