#conding:utf-8
'''
Created By  
@LiJunlei
20170706
'''
import subprocess,re,time,os,sys
import _socket as socket
from configparser import ConfigParser

#get host information from host.conf
class read_config(object):
    def __init__(self):
        cf=ConfigParser()
        cf.read("cluster_config.ini")
        cluster_hosts=[re.split(r"[\|\|]", host_info) for host_info in [hostname+"||"+cf.get("host_conf",hostname) for hostname in cf.options("host_conf")]]
        self.conf=map(lambda x:[y for y in x if y] , cluster_hosts)

def err_print(contents):
    print('\033[1;31m%s\033[0m'%contents)

def pre_print(contents):
    print('\033[1;32m%s\033[0m'%contents)

def warn_print(contents):
    print('\033[1;33m%s\033[0m'%contents)

def shell_command(command,info="",prin_t=True):
    print(command)
    cm=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
    _,err=cm.communicate()
    if cm.returncode != 0:
        if prin_t:
            err_print(err)
            err_print("Non zero exit code:%s executing: %s" % (cm.returncode, info))
        raise StandardError
    
#check cdh manager web service status
def cdhcheck(ip,port):  
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(3) 
    try:  
        s.connect((ip,int(port)))  
        s.shutdown(2)   
        return True  
    except:  
        return False
#step1  install sshpass
def step1():
    print("*"*60+"\n"+"   step1  install sshpass  \n"+"*"*60)
    mksshpass_cmd="if test ! -f /usr/bin/sshpass; then " \
        "cp -f ./bin/sshpass /usr/bin/" \
        "&& chmod +x /usr/bin/sshpass; fi; echo $?"
    shell_command(mksshpass_cmd,"install sshpass")
#step2 take off thp && take off swap
def step2(conf):
    print("*"*60+"\n"+"   step2 take off thp && take off swap  \n"+"*"*60)
    for node in conf:
        ip,passwd,sshport=node[1],node[2],node[3]
        shell_command("sshpass -p %s ssh -p %s -o StrictHostKeyChecking=no root@%s 'echo -e \"\\n* soft nofile 65535\\n* hard nofile 65535\">>/etc/security/limits.conf'"%(passwd,sshport,ip),"set /etc/security/limits.conf")
        shell_command("sshpass -p %s ssh -p %s -o StrictHostKeyChecking=no root@%s 'ulimit -n 65534'"%(passwd,sshport,ip),"set ulimit")
        
#         shell_command("sshpass -p %s ssh -p %s -o StrictHostKeyChecking=no root@%s sed -i 's/^SELINUX=[a-zA-Z]*/SELINUX=disabled/g' /etc/selinux/config"%(passwd,sshport,ip),"set /etc/selinux/config")
#         shell_command("sshpass -p %s ssh -p %s -o StrictHostKeyChecking=no root@%s service iptables stop"%(passwd,sshport,ip),"stop iptables")
#         shell_command("sshpass -p %s ssh -p %s -o StrictHostKeyChecking=no root@%s chkconfig iptables off"%(passwd,sshport,ip),"stop iptables")
        shell_command("sshpass -p %s ssh -p %s -o StrictHostKeyChecking=no root@%s 'echo never > /sys/kernel/mm/transparent_hugepage/defrag && echo never > /sys/kernel/mm/transparent_hugepage/enabled'"%(passwd,sshport,ip),"stop thp")
#         shell_command("sshpass -p %s ssh -p %s -o StrictHostKeyChecking=no root@%s set enforce 0"%(passwd,sshport,ip), "take off selinux")
#         shell_command("sshpass -p %s ssh -p %s -o StrictHostKeyChecking=no root@%s \"sed -i 's/^SELINUX=[a-zA-Z]*/SELINUX=disabled/g' /etc/selinux/config\""%(passwd,sshport,ip),"set /etc/selinux/config")
        shell_command("sshpass -p %s ssh -p %s -o StrictHostKeyChecking=no root@%s 'sysctl vm.swappiness=0'"%(passwd,sshport,ip),"take off swap")
        shell_command("sshpass -p %s ssh -p %s -o StrictHostKeyChecking=no root@%s \"sed -i 's/^vm.swappiness=0/#&/g' /etc/sysctl.conf && echo -e '\\nvm.swappiness=0\\n'>>/etc/sysctl.conf\""%(passwd,sshport,ip),"take off swap")
#step3 make ssh no-password && change hostname
def step3(conf):
    print("*"*60+"\n"+"   step3 make ssh no-password && change hostname  \n"+"*"*60)
    silent_log = "2>&1"
    keygen_cmd = "if test ! -f ~/.ssh/id_rsa.pub; then " \
        "ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa %s; fi; echo $?"%(silent_log,)
    shell_command(keygen_cmd,"make keygen")
    local_kfile="~/.ssh/id_rsa.pub"
    for node in conf:
        [hostname,ip,passwd,sshport,_]=node
        remote_authkeys = "~/.ssh/authorized_keys"
        nohostcheck = '-o StrictHostKeyChecking=no'
        nopassword = '-o PasswordAuthentication=no'
        bypass_command = "cat %s | " \
            "sshpass -p \'%s\' ssh -p %s %s %s " \
            "\"mkdir  -p  ~/.ssh/ ; cat >> %s \" %s; echo $?" \
            % (local_kfile, passwd,sshport, nohostcheck, ip, remote_authkeys, silent_log)
        shell_command(bypass_command, "cat id_rsa.pub>>authorized_keys")
        
        check_command = "ssh -p %s %s %s 'hostname;' %s; " \
            "echo $?" % (sshport, nopassword, ip, silent_log)
        shell_command(check_command,"check nopassword")
        #change hostname
        network_file = "/etc/sysconfig/network"
        sethostname_command = "ssh -p %s %s \"hostname %s && " \
                "sed -i 's/^HOSTNAME=/#&/g' %s && " \
                "echo \"HOSTNAME=%s\" >> %s \" 2>/dev/null;echo $?" \
                % (sshport, ip, hostname, network_file, hostname, network_file)
        shell_command(sethostname_command,"set hostname")
#step4 set /etc/hosts
def step4(conf):
    print("*"*60+"\n"+"   step4 set /etc/hosts  \n"+"*"*60)
    silent_log = "2>&1"
    local_hosts_file = "./installer/hosts.tmp"
    hosts_file = open(local_hosts_file, "w+")
    hosts_file.writelines(
        "127.0.0.1 localhost localhost.localdomain" \
        "localhost4 localhost4.localdomain4\n")
    for node in conf:
        [hostname, ip, role] = [node[0], node[1], node[4]]
        host_line = "%s %s\n" % (ip, hostname)
        if role=="manager":
            hosts_file.writelines("%s %s \n" % (ip, hostname))
        else:
            hosts_file.writelines(host_line)
    hosts_file.close()
    for node in conf:
        [hostname, ip, port] = [node[0], node[1], node[3]]
        # cat ./hosts.tmp | ssh -p 22222 10.100.123.48 'cat > /etc/hosts'
        syn_hosts_command = "cat %s | " \
            "ssh -p %s %s 'cat > /etc/hosts' %s;" \
            "echo $?" % (local_hosts_file, port, ip, silent_log)
        shell_command(syn_hosts_command, "set /etc/hosts")
    shell_command("rm -f ./installer/hosts.tmp")
#step5 set yum repo
def step5(conf):
    print("*"*60+"\n"+"   step5 set yum repo  \n"+"*"*60)
#    command="rpm -ivh bz_utils/apr-1.3.9-5.el6_2.x86_64.rpm &&"\
#        "rpm -ivh bz_utils/apr-util-1.3.9-3.el6_0.1.x86_64.rpm &&"\
#        "rpm -ivh bz_utils/apr-util-ldap-1.3.9-3.el6_0.1.x86_64.rpm && "\
#        "rpm -ivh bz_utils/httpd-tools-2.2.15-59.el6.centos.x86_64.rpm && "\
#        "rpm -ivh bz_utils/httpd-2.2.15-59.el6.centos.x86_64.rpm"
#    try:
#        shell_command(command, "install httpd service",False)
#    except:
#        pass
    manager = [host for host in conf if host[4]=='manager'][0] 
    repo='[bz_centos]\n'\
         'name=bz_centos\n'\
         'baseurl=http://%s/bz_centos\n'\
         'gpgcheck=1\n'\
         'enabled=1\n'\
         'gpgkey=http://%s/bz_centos/RPM-GPG-KEY-CentOS-6\n'\
         '[bz_utils]\n'\
         'name=bz_utils\n'\
         'baseurl=http://%s/bz_utils\n'\
         'gpgcheck=0\n'\
         'enabled=1\n'%(manager[0],manager[0],manager[0])
    with open("./installer/Centos_6.5_X64.repo",'w') as repo_file:
        repo_file.write(repo)
    shell_command("chkconfig httpd on && service httpd restart")
    if not os.path.exists("/mnt/bz_centos/Centos_6.5_X64"):
        shell_command("rm -rf /mnt/bz_centos 2>&1; echo $?")
        shell_command("mkdir /mnt/bz_centos 2>&1; echo $?")
        if os.path.exists("/mnt/Centos_6.5_x64.iso") and os.path.isfile("/mnt/Centos_6.5_x64.iso"):
            pass
        else:
            shell_command("rm -rf /mnt/Centos_6.5_x64.iso && cp ./env/Centos_6.5_x64.iso /mnt/ 2>/dev/null ;echo $?")
            shell_command("mount  -o loop /mnt/Centos_6.5_x64.iso /mnt/bz_centos")
    shell_command("rm -rf /var/www/html/bz_centos /var/www/html/bz_utils")
    shell_command("ln -s /mnt/bz_centos/Centos_6.5_X64 /var/www/html/bz_centos && cp -r bz_utils /var/www/html/")
    
    for node in conf:
        ip,sshport=node[1],node[3]
        
#         shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s rm -rf /etc/yum.repos.d/repo-back"%(sshport,ip))
        shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s 'if [ ! -e \"/etc/yum.repos.d/repo-back\" ]; then mkdir /etc/yum.repos.d/repo-back; fi;echo $? '"%(sshport,ip))
        shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s 'mv -f /etc/yum.repos.d/*.repo /etc/yum.repos.d/repo-back' >/dev/null 2>&1; echo $?"%(sshport,ip))
        shell_command("scp -P %s -o StrictHostKeyChecking=no ./installer/Centos_6.5_X64.repo root@%s:/etc/yum.repos.d/"%(sshport,ip))
        shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s yum clean all "%(sshport,ip))
        shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s yum makecache"%(sshport,ip))
    shell_command("rm -f ./installer/Centos_6.5_X64.repo")

#step6  configure ntp 
def step6(conf):
    print("*"*60+"\n"+"   step6  config ntp  \n"+"*"*60)
    try:
        shell_command(command="service ntpd restart",prin_t=False)
    except:
        shell_command("yum install ntp -y","install ntp service")
    shell_command("rm -f /etc/ntp.conf && cp installer/ntp.conf /etc/ && service ntpd restart","config ntp service")
    shell_command("chkconfig ntpd on", "check ntpd")
    time.sleep(2)
    manager = [host for host in conf if host[4]=='manager'][0] 
    for node in conf:
        ip,sshport,role=node[1],node[3],node[4]
        if role !="manager":
            try:
                shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s service ntpd restart"%(sshport,ip), "time update",False)
                shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s service ntpd stop"%(sshport,ip), "time update",False)
            except:
                shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s yum install ntp -y"%(sshport,ip),"uninstall ntp service")
            shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s 'sed -i \"s/^server/#&/g\" /etc/ntp.conf 2>&1 && echo -e \"\\nserver %s\\n\">>/etc/ntp.conf 2>&1;echo $?'"%(sshport,ip,manager[0]))
            shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s ntpdate %s"%(sshport,ip,manager[0]), "time update")
            shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s chkconfig ntpd on"%(sshport,ip), "chech ntpd")
            shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s service ntpd start"%(sshport,ip), "start ntpd service")
        
#step7 Configure jdk
def step7(conf):
    print("*"*60+"\n"+"     step7 install oracle jdk7    \n"+"*"*60)
    shell_command("rm -rf /usr/lib/cdh-jdk7-x64 /tmp/cdh-jdk7-x64")
    shell_command("tar -zxvf ./env/cdh-jdk7-x64.tar.gz -C /tmp/ >/dev/null","tar jdk")
    for node in conf:
        ip,sshport=node[1],node[3]
        cm=subprocess.Popen("ssh -p %s -o StrictHostKeyChecking=no root@%s rpm -qa|grep openjdk"%(sshport,ip),stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
        cm.wait()
        out=cm.stdout.readlines()
        for jdk in out:
            jdk_name=re.split(r'\n?$',jdk)[0]
            shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s rpm -e %s --nodeps"%(sshport,ip,jdk_name),"remove jdk")
        try:
            shell_command(command="ssh -p %s -o StrictHostKeyChecking=no root@%s mkdir /usr/java"%(sshport,ip),prin_t=False)
        except:
            pass
        shell_command("scp -P %s -o StrictHostKeyChecking=no -r /tmp/cdh-jdk7-x64 root@%s:/usr/lib"%(sshport,ip), "put jdk to /usr/lib/")
        shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s rm -rf /usr/java/default"%(sshport,ip))
        shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s ln -s /usr/lib/cdh-jdk7-x64 /usr/java/default"%(sshport,ip))
        shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s rm -f /etc/profile-back"%(sshport,ip))
        shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s mv /etc/profile /etc/profile-back"%(sshport,ip))
        shell_command("scp -P %s -o StrictHostKeyChecking=no installer/profile root@%s:/etc/"%(sshport,ip),"configure jdk envirment")
        shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s source /etc/profile"%(sshport,ip))
    shell_command("rm -rf /tmp/cdh-jdk7-x64 2>&1 ;echo $?")    
#step8 configure mysql db
def step8(conf):
    print("*"*60+"\n"+"   step8 configure mysql db  \n"+"*"*60)
    try:
        shell_command("service mysqld restart","start mysql",False)
    except:
        shell_command("yum install mysql mysql-server mysql-devel -y","install mysql")
        shell_command("chkconfig mysqld on","config mysql")
        shell_command("service mysqld start","start mysql")
    mysql_command="mysql --execute "
    manager_hname=[node[0] for node in conf if node[4]=="manager"][0]
    shell_command(mysql_command+"\"create database cdh_hive DEFAULT CHARSET utf8 COLLATE utf8_general_ci;\"","create database cdh_hive")
    shell_command(mysql_command+"\"create database cdh_monitor DEFAULT CHARSET utf8 COLLATE utf8_general_ci;\"","create database cdh_monitor")
    shell_command(mysql_command+"\"create database cdh_reports DEFAULT CHARSET utf8 COLLATE utf8_general_ci;\"","create database cdh_reports")
    shell_command(mysql_command+"\"grant all privileges on *.* to 'admin'@'localhost' identified by 'aaaaaa' with grant option;\"","create admin user")
    shell_command(mysql_command+"\"grant all privileges on cdh_hive.* to 'hive'@'%' identified by 'bigdata' with grant option;\"","create hive user")
    shell_command(mysql_command+"\"grant all privileges on cdh_monitor.* to 'monitor'@'%' identified by 'bigdata' with grant option;\"","create monitor user")
    shell_command(mysql_command+"\"grant all privileges on cdh_reports.* to 'reports'@'%' identified by 'bigdata' with grant option;\"","create reports user")
    shell_command(mysql_command+"\"grant all on cdh_hive.* to hive@'%s' identified by 'bigdata';\""% manager_hname,"create hive user")
    shell_command(mysql_command+"\"grant all on cdh_monitor.* to monitor@'%s' identified by 'bigdata';\""% manager_hname,"create monitor user")
    shell_command(mysql_command+"\"grant all on cdh_reports.* to reports@'%s' identified by 'bigdata';\""% manager_hname,"create reports user")
    shell_command(mysql_command+"\"flush privileges;\"")
#    shell_command("mysql <installer/mysql_cdh_env.sql","crate cdh clouster database")
#step9 configure cloudera manager
def step9(conf):
    print("*"*60+"\n"+"   step9 install cloudera cdh manager  \n"+"*"*60)
    shell_command("tar -zxvf env/cloudera-manager-el6-cm5.11.1_x86_64.tar.gz -C . >/dev/null", "tar cloudera manager")
    shell_command("cp ./env/mysql-connector-java-5.1.42-bin.jar ./cm-5.11.1/share/cmf/lib/")
    server_hostname=[node[0] for node in conf if node[4]=="manager"][0]
    shell_command("sed -i 's/^server_host=[a-zA-Z]*/server_host=%s/g' ./cm-5.11.1/etc/cloudera-scm-agent/config.ini"%(server_hostname,))
    for node in conf:
        ip,sshport,role=node[1],node[3],node[4]
        shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s 'rm -rf /home/cloudera-cdh /opt/cloudera /opt/cm-5.11.1'"%(sshport,ip))
        shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s mkdir -p /home/cloudera-cdh/var/lib"%(sshport,ip))
        shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s mkdir -p /home/cloudera-cdh/var/local"%(sshport,ip))
        shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s mkdir -p /home/cloudera-cdh/var/log"%(sshport,ip))
        shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s mkdir -p /home/cloudera-cdh/var/run"%(sshport,ip))
        shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s 'mkdir -p /home/cloudera-cdh/tmp && chmod 777 /home/cloudera-cdh/tmp && chmod 777 /tmp'"%(sshport,ip))
        shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s mkdir -p /home/cloudera-cdh/dfs"%(sshport,ip))
        shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s mkdir -p /home/cloudera-cdh/yarn"%(sshport,ip))
        shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s mkdir -p /home/cloudera-cdh/opt/cloudera/parcels"%(sshport,ip))
        shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s ln -s /home/cloudera-cdh/opt/cloudera /opt/cloudera"%(sshport,ip))
        shell_command("scp -P %s -o StrictHostKeyChecking=no -r ./cm-5.11.1 root@%s:/opt/ >/dev/null"%(sshport,ip))
        try:
            shell_command(command="ssh -p %s -o StrictHostKeyChecking=no root@%s 'userdel cloudera-scm'"%(sshport,ip),prin_t=False)
        except:
            pass
        shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s 'useradd --system --home=/opt/cm-5.11.1/run/cloudera-scm-server/ --no-create-home --shell=/bin/false --comment \"Cloudera SCM User\" cloudera-scm'"%(sshport,ip),"create cloudera-scm user")
        if role=="manager":
            shell_command("mkdir /home/cloudera-cdh/opt/cloudera/csd","make csd dir")
            shell_command("mkdir /home/cloudera-cdh/opt/cloudera/parcel-repo","make parcel-repo dir")
            shell_command("cp package/* /home/cloudera-cdh/opt/cloudera/parcel-repo/","make parcel-repo")
            shell_command("/opt/cm-5.11.1/share/cmf/schema/scm_prepare_database.sh mysql cm -hlocalhost -uadmin -paaaaaa --scm-host localhost scm scm scm")
            shell_command("/opt/cm-5.11.1/etc/init.d/cloudera-scm-server start","start cloudera-scm-server")
        shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s /opt/cm-5.11.1/etc/init.d/cloudera-scm-agent start"%(sshport,ip),"start cloudera-scm-agent")
    shell_command("rm -rf ./cm-5.11.1 ./cloudera")

#step10 take cloudera manager on boot
def step10(conf):
    print("*"*60+"\n"+"   step10 take cloudera manager on boot   \n"+"*"*60)
    shell_command("cp /opt/cm-5.11.1/etc/init.d/cloudera-scm-server . && cp /opt/cm-5.11.1/etc/init.d/cloudera-scm-agent . ")
    shell_command("sed -i '20a export JAVA_HOME=/usr/java/default\\n' ./cloudera-scm-agent")
    shell_command("sed -i '20a export JAVA_HOME=/usr/java/default\\nif [ ! -d \"/mnt/bz_centos/Centos_6.5_x64\" ] ; then \\n  mount -o loop /mnt/Centos_6.5_x64.iso /mnt/bz_centos\\nfi\\n' ./cloudera-scm-server")
    shell_command("sed -i 's/CMF_DEFAULTS=${CMF_DEFAULTS.*/CMF_DEFAULTS=${CMF_DEFAULTS:-\\/opt\\/cm-5.11.1\\/etc\\/default}/g' ./cloudera-scm-agent")
    shell_command("sed -i 's/CMF_DEFAULTS=${CMF_DEFAULTS.*/CMF_DEFAULTS=${CMF_DEFAULTS:-\\/opt\\/cm-5.11.1\\/etc\\/default}/g' ./cloudera-scm-server")
    for node in conf:
        ip,sshport,role=node[1],node[3],node[4]
        shell_command("scp -P %s -o StrictHostKeyChecking=no -r ./cloudera-scm-agent root@%s:/etc/init.d/ >/dev/null"%(sshport,ip))
        shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s chkconfig --add cloudera-scm-agent && chkconfig cloudera-scm-agent on"%(sshport,ip))
        if role=="manager":
            shell_command("scp -P %s -o StrictHostKeyChecking=no -r ./cloudera-scm-server root@%s:/etc/init.d/ >/dev/null"%(sshport,ip))
            shell_command("ssh -p %s -o StrictHostKeyChecking=no root@%s chkconfig --add cloudera-scm-server && chkconfig cloudera-scm-server on"%(sshport,ip))
    shell_command("rm -rf ./cloudera-scm-server ./cloudera-scm-agent")
class install(object):
    def __init__(self):
        conf=read_config()
        self.conf=conf.conf
    def start(self):
        pre_print("#"*100+"\n#"+" "*28+"Start install clouera cdh manager && agent"+" "*28+"#\n"+"#"*100)
#        step1()
        step2(self.conf)
        step3(self.conf)
        step4(self.conf)
        step5(self.conf)
        step6(self.conf)
        step7(self.conf)
        step8(self.conf)
        step9(self.conf)
        
        manager_ip=[node[1] for node in self.conf if node[4]=="manager"][0]
        endTime=time.time()+600
        ret = False
        while time.time()<endTime:
            progress_bar=int((endTime-time.time())/6)
            sys.stdout.write("Wait for cm server start:["+(100-progress_bar)*"#"+progress_bar*" "+"]"+"\r")
            sys.stdout.flush()
            if cdhcheck(manager_ip, 7180):
                sys.stdout.write("Wait for cm server start:["+100*"#"+"]"+"\r")
                sys.stdout.flush()
                ret = True
                break
            else:
                time.sleep(3)
        sys.stdout.write("\n")
        sys.stdout.flush()
        if ret:
            step10(self.conf)
            print("*"*80+"\n"+"Done:Please Visit cloudera manager page:http://%s:7180 \n" % manager_ip +"*"*80)
        else:
            warn_print("Wait for cloudera manager start time out,please check %s:/opt/cm-5.11.1/log/cloudera-scm-server/cloudera-scm-server.log"% manager_ip)
            
if __name__=="__main__":
    install=install()
    try:
        install.start()
        os._exit(0)
    except:
        err_print("$"*10+" Install cloudera manager error "+"$"*10)
        os._exit(1)

