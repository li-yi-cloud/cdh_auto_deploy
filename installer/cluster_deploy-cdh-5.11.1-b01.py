#coding:utf-8
'''
Created on Sep 8, 2017

@author: cloud
'''
import pprint,re,time,math,urllib,urllib2
from configparser import ConfigParser
from subprocess import Popen,PIPE,STDOUT
from cm_api.api_client import ApiResource
from cm_api.endpoints.services import ApiService
from cm_api.endpoints.services import ApiServiceSetupInfo


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

def shell_command(command,info="",prin_t=True):
    print(command)
    cm=Popen(command,stdout=PIPE,stderr=PIPE,shell=True)
    _,err=cm.communicate()
    if cm.returncode != 0:
        print(err.read())
        print("Non zero exit code:%s executing: %s" % (cm.returncode, info))
        raise StandardError

class cm_config():
    CM_CONFIG = {
        'TSQUERY_STREAMS_LIMIT' : 1000,
    }
    CDH_VERSION = "CDH5"
    WORK_DIR = "/home/cloudera-cdh"
    
    ADMIN_USER = "admin"
    ADMIN_PASSWD = "admin"
    
    CLUSTER_NAME = "cluster"
    CONFIG = ConfigParser()
    CONFIG.read("cluster_config.ini")
    CLUSTER_HOSTS=[host for host in re.split(r'[\|\|]',CONFIG.get("cluster_conf","cluster_hosts")) if host]
    CDH_PARCEL_VERSION = CONFIG.get("cluster_conf", "cdh.parcel.version")
    KAFKA_PARCEL_VERSION = CONFIG.get("cluster_conf", "kafka.parcel.version")
    PARCELS=[{ 'name' : "CDH", 'version' : CDH_PARCEL_VERSION },\
             { 'name' : "KAFKA", 'version' : KAFKA_PARCEL_VERSION }\
            ]
    
    CM_MANAGER_INFO=[manager_info for manager_info in re.split(r'[\|\|]',[host for host in [opt+'||'+CONFIG.get("host_conf",opt) for opt in CONFIG.options("host_conf")] if re.findall(r'manager$', host)][0]) if manager_info]
    
    CM_HOST=CM_MANAGER_INFO[0]
    DB_HOST = CM_MANAGER_INFO[0]
    DB_PASSWD = "bigdata"
    
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
    SPARK_GATEWAY_HOST = list(CLUSTER_HOSTS)
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
### Deployment/Initialization Functions ###

# Creates the cluster and adds hosts
def init_cluster(api, cluster_name, cdh_version, hosts, cm_host):
#     api.delete_cluster(cluster_name)
#     cluster = api.get_cluster(cluster_name)
    cluster = api.create_cluster(cluster_name, cdh_version)
    # Add the CM host to the list of hosts to add in the cluster so it can run the management services
    all_hosts = list(cm_config.CLUSTER_HOSTS)
    if cm_config.CM_HOST not in all_hosts:
        all_hosts.append(cm_config.CM_HOST)
    cluster.add_hosts(all_hosts)
    return cluster

# Downloads and distributes parcels
def deploy_parcels(cluster, parcels):
    for parcel in parcels:
        p = cluster.get_parcel(parcel['name'], parcel['version'])
        p.start_download()
        while True:
            p = cluster.get_parcel(parcel['name'], parcel['version'])
            if p.stage == "DOWNLOADED":
                break
            if p.state.errors:
                raise Exception(str(p.state.errors))
            print "Downloading parcel: %s: %s / %s" % (parcel['name'], p.state.progress, p.state.totalProgress)
            time.sleep(15)
        print "Downloaded parces: %s %s" % (parcel['name'],parcel['version'])
        p.start_distribution()
        while True:
            p = cluster.get_parcel(parcel['name'], parcel['version'])
            if p.stage == "DISTRIBUTED":
                break
            if p.state.errors:
                raise Exception(str(p.state.errors))
            print "Distributing parcel: %s: %s / %s" % (parcel['name'], p.state.progress, p.state.totalProgress)
            time.sleep(15)
        print "Distributed parcel: %s %s" % (parcel['name'],parcel['version'])
        p.activate()
    
# Deploys management services. Not all of these are currently turned on because some require a license.
# This function also starts the services.
def deploy_management(manager, mgmt_servicename, mgmt_service_conf, mgmt_role_conf, amon_role_name, amon_role_conf, apub_role_name, apub_role_conf, eserv_role_name, eserv_role_conf, hmon_role_name, hmon_role_conf, smon_role_name, smon_role_conf, rman_role_name, rman_role_conf):
    mgmt = manager.create_mgmt_service(ApiServiceSetupInfo())
   
    # create roles. Note that host id may be different from host name (especially in CM 5). Look it it up in /api/v5/hosts
    mgmt.create_role(amon_role_name + "-1", "ACTIVITYMONITOR", cm_config.CM_HOST)
    mgmt.create_role(apub_role_name + "-1", "ALERTPUBLISHER", cm_config.CM_HOST)
    mgmt.create_role(eserv_role_name + "-1", "EVENTSERVER", cm_config.CM_HOST)
    mgmt.create_role(hmon_role_name + "-1", "HOSTMONITOR", cm_config.CM_HOST)
    mgmt.create_role(smon_role_name + "-1", "SERVICEMONITOR", cm_config.CM_HOST)
    #mgmt.create_role(nav_role_name + "-1", "NAVIGATOR", CM_HOST)
    #mgmt.create_role(navms_role_name + "-1", "NAVIGATORMETADATASERVER", CM_HOST)
#     mgmt.create_role(rman_role_name + "-1", "REPORTSMANAGER", cm_config.CM_HOST)
   
    # now configure each role   
    for group in mgmt.get_all_role_config_groups():
        if group.roleType == "ACTIVITYMONITOR":
            group.update_config(amon_role_conf)
        elif group.roleType == "ALERTPUBLISHER":
            group.update_config(apub_role_conf)
        elif group.roleType == "EVENTSERVER":
            group.update_config(eserv_role_conf)
        elif group.roleType == "HOSTMONITOR":
            group.update_config(hmon_role_conf)
        elif group.roleType == "SERVICEMONITOR":
            group.update_config(smon_role_conf)
    #   elif group.roleType == "NAVIGATOR":
    #       group.update_config(nav_role_conf)
    #   elif group.roleType == "NAVIGATORMETADATASERVER":
    #       group.update_config(navms_role_conf)
#         elif group.roleType == "REPORTSMANAGER":
#             group.update_config(rman_role_conf)
   
    # now start the management service
    mgmt.start().wait()
   
    return mgmt


# Deploys and initializes ZooKeeper
def deploy_zookeeper(cluster, zk_name, zk_hosts, zk_service_conf, zk_role_conf):
    zk = cluster.create_service(zk_name, "ZOOKEEPER")
    zk.update_config(zk_service_conf)
   
    zk_id = 0
    for zk_host in zk_hosts:
        zk_id += 1
        zk_role_conf['serverId'] = zk_id
        role = zk.create_role(zk_name + "-" + str(zk_id), "SERVER", zk_host)
        role.update_config(zk_role_conf)
   
    zk.init_zookeeper()
   
    return zk


# Deploys HDFS - NN, DNs, SNN, gateways.
# This does not yet support HA yet.
def deploy_hdfs(cluster, hdfs_service_name, hdfs_config, hdfs_nn_service_name, hdfs_nn_host, hdfs_nn_config, hdfs_snn_host, hdfs_snn_config, hdfs_dn_hosts, hdfs_dn_config, hdfs_gw_hosts, hdfs_gw_config):
    hdfs_service = cluster.create_service(hdfs_service_name, "HDFS")
    hdfs_service.update_config(hdfs_config)
   
    nn_role_group = hdfs_service.get_role_config_group("{0}-NAMENODE-BASE".format(hdfs_service_name))
    nn_role_group.update_config(hdfs_nn_config)
    nn_service_pattern = "{0}-" + hdfs_nn_service_name
    hdfs_service.create_role(nn_service_pattern.format(hdfs_service_name), "NAMENODE", hdfs_nn_host)
   
    snn_role_group = hdfs_service.get_role_config_group("{0}-SECONDARYNAMENODE-BASE".format(hdfs_service_name))
    snn_role_group.update_config(hdfs_snn_config)
    hdfs_service.create_role("{0}-snn".format(hdfs_service_name), "SECONDARYNAMENODE", hdfs_snn_host)
   
    dn_role_group = hdfs_service.get_role_config_group("{0}-DATANODE-BASE".format(hdfs_service_name))
    dn_role_group.update_config(hdfs_dn_config)
   
    gw_role_group = hdfs_service.get_role_config_group("{0}-GATEWAY-BASE".format(hdfs_service_name))
    gw_role_group.update_config(hdfs_gw_config)
   
    datanode = 0
    for host in hdfs_dn_hosts:
        datanode += 1
        hdfs_service.create_role("{0}-dn-".format(hdfs_service_name) + str(datanode), "DATANODE", host)
   
    gateway = 0
    for host in hdfs_gw_hosts:
        gateway += 1
        hdfs_service.create_role("{0}-gw-".format(hdfs_service_name) + str(gateway), "GATEWAY", host)
   
    return hdfs_service


# Initializes HDFS - format the file system
def init_hdfs(hdfs_service, hdfs_name, timeout):
    cmd = hdfs_service.format_hdfs("{0}-nn".format(hdfs_name))[0]
    if not cmd.wait(timeout).success:
        print "WARNING: Failed to format HDFS, attempting to continue with the setup" 


# Deploys MapReduce - JT, TTs, gateways.
# This does not yet support HA yet.
# This shouldn't be run if YARN is deployed.
def deploy_mapreduce(cluster, mapred_service_name, mapred_service_config, mapred_jt_host, mapred_jt_config, mapred_tt_hosts, mapred_tt_config, mapred_gw_hosts, mapred_gw_config ):
    mapred_service = cluster.create_service(mapred_service_name, "MAPREDUCE")
    mapred_service.update_config(mapred_service_config)
      
    jt = mapred_service.get_role_config_group("{0}-JOBTRACKER-BASE".format(mapred_service_name))
    jt.update_config(mapred_jt_config)
    mapred_service.create_role("{0}-jt".format(mapred_service_name), "JOBTRACKER", mapred_jt_host)
   
    tt = mapred_service.get_role_config_group("{0}-TASKTRACKER-BASE".format(mapred_service_name))
    tt.update_config(mapred_tt_config)
   
    gw = mapred_service.get_role_config_group("{0}-GATEWAY-BASE".format(mapred_service_name))
    gw.update_config(mapred_gw_config)
   
    tasktracker = 0
    for host in mapred_tt_hosts:
        tasktracker += 1
        mapred_service.create_role("{0}-tt-".format(mapred_service_name) + str(tasktracker), "TASKTRACKER", host)
   
    gateway = 0
    for host in mapred_gw_hosts:
        gateway += 1
        mapred_service.create_role("{0}-gw-".format(mapred_service_name) + str(gateway), "GATEWAY", host)
   
    return mapred_service


# Deploys YARN - RM, JobHistoryServer, NMs, gateways
# This shouldn't be run if MapReduce is deployed.
def deploy_yarn(cluster, yarn_service_name, yarn_service_config, yarn_rm_host, yarn_rm_config, yarn_jhs_host, yarn_jhs_config, yarn_nm_hosts, yarn_nm_config, yarn_gw_hosts, yarn_gw_config):
    yarn_service = cluster.create_service(yarn_service_name, "YARN")
    yarn_service.update_config(yarn_service_config)
      
    rm = yarn_service.get_role_config_group("{0}-RESOURCEMANAGER-BASE".format(yarn_service_name))
    rm.update_config(yarn_rm_config)
    yarn_service.create_role("{0}-rm".format(yarn_service_name), "RESOURCEMANAGER", yarn_rm_host)
      
    jhs = yarn_service.get_role_config_group("{0}-JOBHISTORY-BASE".format(yarn_service_name))
    jhs.update_config(yarn_jhs_config)
    yarn_service.create_role("{0}-jhs".format(yarn_service_name), "JOBHISTORY", yarn_jhs_host)
   
    nm = yarn_service.get_role_config_group("{0}-NODEMANAGER-BASE".format(yarn_service_name))
    nm.update_config(yarn_nm_config)
   
    nodemanager = 0
    for host in yarn_nm_hosts:
        nodemanager += 1
        yarn_service.create_role("{0}-nm-".format(yarn_service_name) + str(nodemanager), "NODEMANAGER", host)
   
    gw = yarn_service.get_role_config_group("{0}-GATEWAY-BASE".format(yarn_service_name))
    gw.update_config(yarn_gw_config)
   
    gateway = 0
    for host in yarn_gw_hosts:
        gateway += 1
        yarn_service.create_role("{0}-gw-".format(yarn_service_name) + str(gateway), "GATEWAY", host)
   
    #TODO need api version 6 for these, but I think they are done automatically?
    #yarn_service.create_yarn_job_history_dir()
    #yarn_service.create_yarn_node_manager_remote_app_log_dir()
   
    return yarn_service


# Deploys spark_on_yarn - history server, gateways
def deploy_spark(cluster, spark_service_name, spark_service_config ,spark_history_server_host,spark_history_server_config, spark_gw_hosts, spark_gw_config):
    spark_service = cluster.create_service(spark_service_name, "SPARK_ON_YARN")
    spark_service.update_config(spark_service_config)
    
    spark_hs=spark_service.get_role_config_group("{0}-SPARK_YARN_HISTORY_SERVER-BASE".format(spark_service_name))
    spark_hs.update_config(spark_history_server_config)
    spark_service.create_role("{0}-SPARK_YARN_HISTORY_SERVER".format(spark_service_name),"SPARK_YARN_HISTORY_SERVER", spark_history_server_host)
       
    gw = spark_service.get_role_config_group("{0}-GATEWAY-BASE".format(spark_service_name))
    gw.update_config(spark_gw_config)
   
    gateway = 0
    for host in spark_gw_hosts:
        gateway += 1
        spark_service.create_role("{0}-gw-".format(spark_service_name) + str(gateway), "GATEWAY", host)
   
    #TODO - CreateSparkUserDirCommand, SparkUploadJarServiceCommand???
   
    return spark_service


# Deploys HBase - HMaster, RSes, HBase Thrift Server, gateways
def deploy_hbase(cluster, hbase_service_name, hbase_service_config, hbase_hm_host, hbase_hm_config, hbase_rs_hosts, hbase_rs_config ):
    hbase_service = cluster.create_service(hbase_service_name, "HBASE")
    hbase_service.update_config(hbase_service_config)
       
    hm = hbase_service.get_role_config_group("{0}-MASTER-BASE".format(hbase_service_name))
    hm.update_config(hbase_hm_config)
    hbase_service.create_role("{0}-hm".format(hbase_service_name), "MASTER", hbase_hm_host)
    
    rs = hbase_service.get_role_config_group("{0}-REGIONSERVER-BASE".format(hbase_service_name))
    rs.update_config(hbase_rs_config)
    
#     ts = hbase_service.get_role_config_group("{0}-HBASETHRIFTSERVER-BASE".format(hbase_service_name))
#     ts.update_config(hbase_thriftserver_config)
#     ts_name_pattern = "{0}-" + hbase_thriftserver_service_name
#     hbase_service.create_role(ts_name_pattern.format(hbase_service_name), "HBASETHRIFTSERVER", hbase_thriftserver_host)
#     
#     gw = hbase_service.get_role_config_group("{0}-GATEWAY-BASE".format(hbase_service_name))
#     gw.update_config(hbase_gw_config)
    
    regionserver = 0
    for host in hbase_rs_hosts:
        regionserver += 1
        hbase_service.create_role("{0}-rs-".format(hbase_service_name) + str(regionserver), "REGIONSERVER", host)
    
#     gateway = 0
#     for host in hbase_gw_hosts:
#         gateway += 1
#         hbase_service.create_role("{0}-gw-".format(hbase_service_name) + str(gateway), "GATEWAY", host)
    
    hbase_service.create_hbase_root()
    
    return hbase_service


# Deploys Hive - hive metastore, hiveserver2, webhcat, gateways
def deploy_hive(cluster, hive_service_name, hive_service_config, hive_hms_host, hive_hms_config, hive_hs2_host, hive_hs2_config, hive_whc_host, hive_whc_config, hive_gw_hosts, hive_gw_config):
    hive_service = cluster.create_service(hive_service_name, "HIVE")
    hive_service.update_config(hive_service_config)
    
    hms = hive_service.get_role_config_group("{0}-HIVEMETASTORE-BASE".format(hive_service_name))
    hms.update_config(hive_hms_config)
    hive_service.create_role("{0}-hms".format(hive_service_name), "HIVEMETASTORE", hive_hms_host)
    
    hs2 = hive_service.get_role_config_group("{0}-HIVESERVER2-BASE".format(hive_service_name))
    hs2.update_config(hive_hs2_config)
    hive_service.create_role("{0}-hs2".format(hive_service_name), "HIVESERVER2", hive_hs2_host)
    
    whc = hive_service.get_role_config_group("{0}-WEBHCAT-BASE".format(hive_service_name))
    whc.update_config(hive_whc_config)
    hive_service.create_role("{0}-whc".format(hive_service_name), "WEBHCAT", hive_whc_host)
    
    gw = hive_service.get_role_config_group("{0}-GATEWAY-BASE".format(hive_service_name))
    gw.update_config(hive_gw_config)
    
    gateway = 0
    for host in hive_gw_hosts:
        gateway += 1
        hive_service.create_role("{0}-gw-".format(hive_service_name) + str(gateway), "GATEWAY", host)
    
    return hive_service


# Initialized hive
def init_hive(hive_service):
    hive_service.create_hive_metastore_database()
    hive_service.create_hive_metastore_tables()
    hive_service.create_hive_warehouse()
    #don't think that the create_hive_userdir call is needed as the create_hive_warehouse already creates it
    #hive_service.create_hive_userdir()

# Add hive mysql data base driver
def hive_mysqldb_deploy():
    host_conf=read_config()
    conf=host_conf.conf
    for node in conf:
        ip,sshport=node[1],node[3]
        shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s 'cp /opt/cm-5.11.1/share/cmf/lib/mysql-connector-java-5.1.42-bin.jar /opt/cloudera/parcels/CDH/jars/'"%(sshport,ip),"copy mysql-collector to cdh")
        shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s 'ln -s /opt/cloudera/parcels/CDH/jars/mysql-connector-java-5.1.42-bin.jar /opt/cloudera/parcels/CDH/lib/hive/lib/mysql-connector-java-bin.jar'"%(sshport,ip))


# Deploys Impala - statestore, catalogserver, impalads
def deploy_impala(cluster, impala_service_name, impala_service_config, impala_ss_host, impala_ss_config, impala_cs_host, impala_cs_config, impala_id_hosts, impala_id_config):
    impala_service = cluster.create_service(impala_service_name, "IMPALA")
    impala_service.update_config(impala_service_config)
    
    ss = impala_service.get_role_config_group("{0}-STATESTORE-BASE".format(impala_service_name))
    ss.update_config(impala_ss_config)
    impala_service.create_role("{0}-ss".format(impala_service_name), "STATESTORE", impala_ss_host)
    
    cs = impala_service.get_role_config_group("{0}-CATALOGSERVER-BASE".format(impala_service_name))
    cs.update_config(impala_cs_config)
    impala_service.create_role("{0}-cs".format(impala_service_name), "CATALOGSERVER", impala_cs_host)
    
    id = impala_service.get_role_config_group("{0}-IMPALAD-BASE".format(impala_service_name))
    id.update_config(impala_id_config)
    
    impalad = 0
    for host in impala_id_hosts:
        impalad += 1
        impala_service.create_role("{0}-id-".format(impala_service_name) + str(impalad), "IMPALAD", host)

    # Don't think we need these at the end:
    #impala_service.create_impala_catalog_database()
    #impala_service.create_impala_catalog_database_tables()
    #impala_service.create_impala_user_dir()
    
    return impala_service

# Deploy kafka - kafka broker
def deploy_kafka(cluster,kafka_service_name,kafka_service_config,kafka_broker_hosts,kafka_broker_config):
    kafka_service = cluster.create_service(kafka_service_name, "KAFKA")
    kafka_service.update_config(kafka_service_config)
#     kafka_service=cluster.get_service("KAFKA")
    
    kafka_broker = kafka_service.get_role_config_group("{0}-KAFKA_BROKER-BASE".format(kafka_service_name))
    kafka_broker.update_config(kafka_broker_config)

    broker_id = 0
    for host in kafka_broker_hosts:
        broker_id +=1
        kafka_service.create_role("{0}-KAFKA_BROKER-".format(kafka_service_name) + str(broker_id), "KAFKA_BROKER", host)
        


# Executes steps that need to be done after the final startup once everything is deployed and running.
def post_startup(cluster, hdfs_service):
    # Create HDFS temp dir
    hdfs_service.create_hdfs_tmp()
    
    # Create hive warehouse dir
    shell_command = ['curl -i -H "Content-Type: application/json" -X POST -u "' + cm_config.ADMIN_USER + ':' + cm_config.ADMIN_PASSWD + '" -d "serviceName=' + cm_config.HIVE_SERVICE_NAME + ';clusterName=' + cm_config.CLUSTER_NAME + '" http://' + cm_config.CM_HOST + ':7180/api/v16/clusters/' + cm_config.CLUSTER_NAME + '/services/' + cm_config.HIVE_SERVICE_NAME + '/commands/hiveCreateHiveWarehouse']
    create_hive_warehouse_output = Popen(shell_command, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True).stdout.read()
        
    # Deploy client configs to all necessary hosts
    cmd = cluster.deploy_client_config()
    if not cmd.wait(cm_config.CMD_TIMEOUT).success:
        print "Failed to deploy client configs for {0}".format(cluster.name)
    
    # Noe change permissions on the /user dir so YARN will work
    shell_command = ['sudo -u hdfs hadoop fs -chmod 775 /user']
    
    user_chmod_output = Popen(shell_command, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True).stdout.read()


### Main function ###
def main():
    API = ApiResource(cm_config.CM_HOST, version=16, username=cm_config.ADMIN_USER, password=cm_config.ADMIN_PASSWD)
    MANAGER = API.get_cloudera_manager()
    MANAGER.update_config(cm_config.CM_CONFIG)
    print "Connected to CM host on " + cm_config.CM_HOST + " and updated CM configuration"

    CLUSTER = init_cluster(API, cm_config.CLUSTER_NAME, cm_config.CDH_VERSION, cm_config.CLUSTER_HOSTS, cm_config.CM_HOST)
    print "Initialized cluster " + cm_config.CLUSTER_NAME + " which uses CDH version " + cm_config.CDH_VERSION

    deploy_management(MANAGER, cm_config.MGMT_SERVICENAME, cm_config.MGMT_SERVICE_CONFIG, cm_config.MGMT_ROLE_CONFIG, cm_config.AMON_ROLENAME, cm_config.AMON_ROLE_CONFIG, cm_config.APUB_ROLENAME, cm_config.APUB_ROLE_CONFIG, cm_config.ESERV_ROLENAME, cm_config.ESERV_ROLE_CONFIG, cm_config.HMON_ROLENAME, cm_config.HMON_ROLE_CONFIG, cm_config.SMON_ROLENAME, cm_config.SMON_ROLE_CONFIG, cm_config.RMAN_ROLENAME, cm_config.RMAN_ROLE_CONFIG)
    print "Deployed CM management service " + cm_config.MGMT_SERVICENAME + " to run on " + cm_config.CM_HOST
       
    deploy_parcels(CLUSTER, cm_config.PARCELS)
    print "Downloaded and distributed parcels: "
    pretty_print(cm_config.PARCELS)
 
    zookeeper_service = deploy_zookeeper(CLUSTER, cm_config.ZOOKEEPER_SERVICE_NAME, cm_config.ZOOKEEPER_SERVER_HOSTS, cm_config.ZOOKEEPER_SERVICE_CONFIG, cm_config.ZOOKEEPER_ROLE_CONFIG)
    print "Deployed ZooKeeper " + cm_config.ZOOKEEPER_SERVICE_NAME + " to run on: "
    pretty_print(cm_config.ZOOKEEPER_SERVER_HOSTS)
      
    hdfs_service = deploy_hdfs(CLUSTER, cm_config.HDFS_SERVICE_NAME, cm_config.HDFS_SERVICE_CONFIG, cm_config.HDFS_NAMENODE_SERVICE_NAME, cm_config.HDFS_NAMENODE_HOST, cm_config.HDFS_NAMENODE_CONFIG, cm_config.HDFS_SECONDARY_NAMENODE_HOST, cm_config.HDFS_SECONDARY_NAMENODE_CONFIG, cm_config.HDFS_DATANODE_HOSTS, cm_config.HDFS_DATANODE_CONFIG, cm_config.HDFS_GATEWAY_HOSTS, cm_config.HDFS_GATEWAY_CONFIG)
    print "Deployed HDFS service " + cm_config.HDFS_SERVICE_NAME + " using NameNode on " + cm_config.HDFS_NAMENODE_HOST + ", SecondaryNameNode on " + cm_config.HDFS_SECONDARY_NAMENODE_HOST + ", and DataNodes running on: "
    pretty_print(cm_config.HDFS_DATANODE_HOSTS)
    init_hdfs(hdfs_service, cm_config.HDFS_SERVICE_NAME, cm_config.CMD_TIMEOUT)
    print "Initialized HDFS service"
 
#     mapred and yarn are mutually exclusive; only deploy one of them
#     mapred_service = deploy_mapreduce(CLUSTER, MAPRED_SERVICE_NAME, MAPRED_SERVICE_CONFIG, MAPRED_JT_HOST, MAPRED_JT_CONFIG, MAPRED_TT_HOSTS, MAPRED_TT_CONFIG, MAPRED_GW_HOSTS, MAPRED_GW_CONFIG)
#     print "Deployed MapReduce service " + cm_config.MAPRED_SERVICE_NAME + " using JobTracker on " + cm_config.MAPRED_JT_HOST + " and TaskTrackers running on "
#     pretty_print(cm_config.MAPRED_TT_HOSTS)
     
    yarn_service = deploy_yarn(CLUSTER, cm_config.YARN_SERVICE_NAME, cm_config.YARN_SERVICE_CONFIG, cm_config.YARN_RM_HOST, cm_config.YARN_RM_CONFIG, cm_config.YARN_JHS_HOST, cm_config.YARN_JHS_CONFIG, cm_config.YARN_NM_HOSTS, cm_config.YARN_NM_CONFIG, cm_config.YARN_GW_HOSTS, cm_config.YARN_GW_CONFIG)
    print "Deployed YARN service " + cm_config.YARN_SERVICE_NAME + " using ResourceManager on " + cm_config.YARN_RM_HOST + ", JobHistoryServer on " + cm_config.YARN_JHS_HOST + ", and NodeManagers on "
    pretty_print(cm_config.YARN_NM_HOSTS)
      
    spark_service = deploy_spark(CLUSTER, cm_config.SPARK_SERVICE_NAME, cm_config.SPARK_SERVICE_CONFIG,cm_config.SPARK_YARN_HISTORY_SERVER_HOST,cm_config.SPARK_YARN_HISTORY_SERVER_CONFIG, cm_config.SPARK_GATEWAY_HOST, cm_config.SPARK_GATEWAY_CONFIG)
    print "Deployed SPARK service " + cm_config.SPARK_SERVICE_NAME + " using SparkHistoryServer on " + cm_config.SPARK_YARN_HISTORY_SERVER_HOST + " and Spark Gateway on "
    pretty_print(cm_config.SPARK_GATEWAY_HOST)
      
    deploy_hbase(CLUSTER, cm_config.HBASE_SERVICE_NAME, cm_config.HBASE_SERVICE_CONFIG, cm_config.HBASE_HM_HOST, cm_config.HBASE_HM_CONFIG, cm_config.HBASE_RS_HOSTS, cm_config.HBASE_RS_CONFIG)
    print "Deployed HBase service " + cm_config.HBASE_SERVICE_NAME + " using HMaster on " + cm_config.HBASE_HM_HOST + " and RegionServers on "
    pretty_print(cm_config.HBASE_RS_HOSTS)
     
    hive_service = deploy_hive(CLUSTER, cm_config.HIVE_SERVICE_NAME, cm_config.HIVE_SERVICE_CONFIG, cm_config.HIVE_HMS_HOST, cm_config.HIVE_HMS_CONFIG, cm_config.HIVE_HS2_HOST, cm_config.HIVE_HS2_CONFIG, cm_config.HIVE_WHC_HOST, cm_config.HIVE_WHC_CONFIG, cm_config.HIVE_GW_HOSTS, cm_config.HIVE_GW_CONFIG)
    print "Depoyed Hive service " + cm_config.HIVE_SERVICE_NAME + " using HiveMetastoreServer on " + cm_config.HIVE_HMS_HOST + " and HiveServer2 on " + cm_config.HIVE_HS2_HOST
    hive_service = CLUSTER.get_service("HIVE")
    hive_mysqldb_deploy()
    init_hive(hive_service)
    print "Initialized Hive service"
     
    impala_service = deploy_impala(CLUSTER, cm_config.IMPALA_SERVICE_NAME, cm_config.IMPALA_SERVICE_CONFIG, cm_config.IMPALA_SS_HOST, cm_config.IMPALA_SS_CONFIG, cm_config.IMPALA_CS_HOST, cm_config.IMPALA_CS_CONFIG, cm_config.IMPALA_ID_HOSTS, cm_config.IMPALA_ID_CONFIG)
    print "Deployed Impala service " + cm_config.IMPALA_SERVICE_NAME + " using StateStore on " + cm_config.IMPALA_SS_HOST + ", CatalogServer on " + cm_config.IMPALA_CS_HOST + ", and ImpalaDaemons on "
    pretty_print(cm_config.IMPALA_ID_HOSTS)
    
    kafka_service = deploy_kafka(CLUSTER, cm_config.KAFKA_SERVICE_NAME, cm_config.KAFKA_SERVICE_CONFIG, cm_config.KAFKA_BROKER_HOSTS, cm_config.KAFKA_BROKER_CONFIG)
    print "Deployed Kafka service :" + cm_config.KAFKA_SERVICE_NAME + " using Broker on"
    pretty_print(cm_config.KAFKA_BROKER_HOSTS)
    
    #Need to start the cluster now as subsequent services need the cluster to be runnign
    #TODO can we just start ZK, and maybe HDFS, instead of everything? It's just needed for the search service
#     CLUSTER.first_run().wait()
    print("Deploy client config")
    CLUSTER.deploy_client_config().wait()
    print("Start hdfs,zookeeper service")
    zookeeper_service.start().wait()
    hdfs_service.start().wait()
    time.sleep(20)
    print("Create spark applicationHistory directory")
    comand="ssh -p %s root@%s 'sudo -u hdfs hadoop fs -mkdir -p /user/spark/applicationHistory && "\
           "sudo -u hdfs hadoop fs -chmod 777 /user/spark/applicationHistory && "\
           "sudo -u hdfs hadoop fs -chown -R  spark:spark /user/spark ' >/dev/null 2>&1 ;echo $?"%\
           (cm_config.NAME_NODE_HOST_INFO[3],cm_config.NAME_NODE_HOST_INFO[1])
    shell_command(comand)
    print "About to restart cluster"
    CLUSTER.restart().wait()
#     CLUSTER.restart(redeploy_client_configuration=True).wait()
    print "Done restarting cluster"
    
      
    hdfs_service=CLUSTER.get_service("HDFS")
    post_startup(CLUSTER, hdfs_service)
    hive_service.restart().wait()
    impala_service.restart().wait()
    print "Finished deploying Cloudera cluster. Go to http://" + cm_config.CM_HOST + ":7180 to administer the cluster."
    print "If there are any other services not running, restart them now."
   
   
if __name__ == "__main__":
    main()



