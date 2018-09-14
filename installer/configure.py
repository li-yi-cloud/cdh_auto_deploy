#conding:utf-8
'''
Created By  
@LiJunlei
20170616
'''
import subprocess,re,argparse
from configparser import ConfigParser

#get host information from host.conf
class read_config(object):
    def __init__(self):
        cf=ConfigParser()
        cf.read("cluster_config.ini")
        self.cluster_hosts = list(map(lambda x:[y for y in x if y] , [re.split(r"[\|\|]", host_info) for host_info in [hostname+"||"+cf.get("host_conf",hostname) for hostname in cf.options("host_conf")]]))
        self.namenode_host = cf.get("role_conf", "hdfs.namenode.host")
        self.datanode_hosts = [hostname for hostname in re.split(r"[\|\|]",cf.get("role_conf", "hdfs.datanode.hosts")) if hostname]
        self.zookeeper_server_hosts = [hostname for hostname in re.split(r"[\|\|]",cf.get("role_conf", "zookeeper.server.hosts")) if hostname]
def get_para():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument("Configure_option", metavar='Configure_option',type=str,nargs='?',help='configure option:config_hive or config_quick_command')
    args = parser.parse_args()
    return vars(args)
def shell_command(command,info="",prin_t=True):
    print(command)
    cm=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
    _,err=cm.communicate()
    if cm.returncode != 0:
        print(err.read())
        print("Non zero exit code:%s executing: %s" % (cm.returncode, info))
        raise StandardError
def write_config(filename,contents):
    with open(filename,"w") as cdh_cluster_config:
        cdh_cluster_config.write(contents)
         
class Configure(object):
    def __init__(self,opt):
        conf=read_config()
        self.conf=conf.cluster_hosts
        self.NameNode = conf.namenode_host
        self.DataNode = conf.datanode_hosts
        self.ZooKeeperServer = conf.zookeeper_server_hosts
        self.ResourceManager = self.NameNode
        self.NodeManager = self.DataNode
        self.KafkaBroker = self.DataNode
        self.option=opt["Configure_option"]
        
    def cm_command_deploy(self):
        print("*"*60+"\n"+"   take quick command   \n"+"*"*60)
        shell_command("cp /opt/cm-5.11.1/etc/init.d/cloudera-scm-server . && cp /opt/cm-5.11.1/etc/init.d/cloudera-scm-agent . ")
        shell_command("sed -i '20a export JAVA_HOME=/usr/java/default\\n' ./cloudera-scm-agent")
        shell_command("sed -i '20a export JAVA_HOME=/usr/java/default\\n' ./cloudera-scm-server")
        shell_command("sed -i 's/CMF_DEFAULTS=${CMF_DEFAULTS.*/CMF_DEFAULTS=${CMF_DEFAULTS:-\\/opt\\/cm-5.11.1\\/etc\\/default}/g' ./cloudera-scm-agent")
        shell_command("sed -i 's/CMF_DEFAULTS=${CMF_DEFAULTS.*/CMF_DEFAULTS=${CMF_DEFAULTS:-\\/opt\\/cm-5.11.1\\/etc\\/default}/g' ./cloudera-scm-server")
        for node in self.conf:
            ip,sshport,role=node[1],node[3],node[4]
            shell_command("scp -P %s -o StrictHostKeyChecking=no -r ./cloudera-scm-agent root@%s:/usr/bin/ >/dev/null"%(sshport,ip))
            if role=="manager":
                shell_command("scp -P %s -o StrictHostKeyChecking=no -r ./cloudera-scm-server root@%s:/usr/bin/ >/dev/null"%(sshport,ip))
        shell_command("rm -rf ./cloudera-scm-server ./cloudera-scm-agent")
    def hive_mysqldb_deploy(self):
        for node in self.conf:
            ip,sshport=node[1],node[3]
            shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s 'cp /opt/cm-5.11.1/share/cmf/lib/mysql-connector-java-5.1.42-bin.jar /opt/cloudera/parcels/CDH/jars/'"%(sshport,ip),"copy mysql-collector to cdh")
            shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s 'ln -s /opt/cloudera/parcels/CDH/jars/mysql-connector-java-5.1.42-bin.jar /opt/cloudera/parcels/CDH/lib/hive/lib/mysql-connector-java-bin.jar'"%(sshport,ip))
    def write_cluster_info(self):
        cdh_conf="[cdh_config]\n"\
            "NameNode = %s\n"%self.NameNode+\
            "DataNode = %s\n"%reduce(lambda x,y:x+";"+y, self.DataNode)+\
            "ResourceManager = %s\n"%self.ResourceManager+\
            "NodeManager = %s\n"%reduce(lambda x,y:x+";"+y, self.NodeManager)+\
            "ZooKeeperServer = %s\n"%reduce(lambda x,y:x+";"+y, self.ZooKeeperServer)+\
            "KafkaBroker = %s\n"%reduce(lambda x,y:x+";"+y, self.KafkaBroker)
        
        write_config("/etc/cdh.conf", cdh_conf)
        
    def start(self):
        if self.option == "hive_mysqldb_deploy":
            self.hive_mysqldb_deploy()
        elif self.option == "cm_command_deploy":
            self.cm_command_deploy()
        elif self.option == "write_cluster_info":
            self.write_cluster_info()
        else:
            print("invalid option :%s"% self.option)
if __name__=="__main__":
    opt = get_para()
    configure=Configure(opt)
    configure.start()

