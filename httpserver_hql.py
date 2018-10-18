#!/usr/bin/env python
#coding=utf-8
from SimpleHTTPServer import SimpleHTTPRequestHandler
import SocketServer
import urllib
import os,sys
import logging
import subprocess 

class HiveHttpTask:

    def __init__(self):
        logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',datefmt='%a, %d %b %Y %H:%M:%S')
        self.returncode = None
    
    def check_sql(self, sql):
        hive_sql = []
        hql = sql.strip().split('\n')
        for q in hql:
            if q.startswith('--'):
                continue
            hive_sql.append(q)
        return '\n'.join(hive_sql)

    def run_shell_cmd(self, shellcmd, encoding='utf8'):
        sql = self.check_sql(shellcmd)
        flag = True
        for f in ['DESC FORMATTED', 'SHOW PARTITIONS']:
            if f in sql.upper():
                flag = False
        if flag:
            logging.info(sql)
        res = subprocess.Popen(sql, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        results = []
        while True:
            line = res.stdout.readline().decode(encoding).strip()
            if line == '' and res.poll() is not None:
                break
            else:
                results.append(line)
        ReturnCode = res.returncode
        if ReturnCode != 0:
            raise Exception('\n'.join(results))
        return [ReturnCode, '\n'.join(results)]

    def fetch_hive_data(self,hql):
        final_result = []
        find = 0
        return_val = self.run_shell_cmd(hql)
        #print(return_val)
        res_tmp = return_val[1]
        for res in res_tmp.split('\n'):
            #print('=====   '+res)
            if res == 'OK':
                find=find+1
            if find == 2 and res !='OK' and res.find('Time taken:')<0:
                if res !='' and res != None:
                    final_result.append(res)
        return final_result


class HiveHttpHandler(SimpleHTTPRequestHandler):
    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        
    def do_GET(self):
        logging.basicConfig(level=logging.INFO,format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',datefmt='%a, %d %b %Y %H:%M:%S')
        logging.info("got get request "+str(self.path))
        #data0 = urllib.splitquery(self.path)[0]
        hql = urllib.splitquery(self.path)[1]
        logging.info("hql==="+str(hql))
        exe_cmd = "hive -e '"+str(hql)+"'"
        logging.info('cmd==='+str(exe_cmd))
        self.wfile.write('ssss')
          
    #curl -d "use default;select * from students" http://127.0.0.1:8090     
    def do_POST(self):
        logging.basicConfig(level=logging.INFO,format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',datefmt='%a, %d %b %Y %H:%M:%S')
        logging.info("got post!!")
        content_len = int(self.headers.getheader('content-length', 0))
        post_data = self.rfile.read(content_len)
        logging.info("post-data==="+str(post_data))
        #set hive.cli.print.header=true; 设置这个可以获取scheam
        hql = 'hive -e "set hive.cli.print.header=true;'+str(post_data)+';"'
        logging.info("hql==="+str(hql))
        results = HiveHttpTask().fetch_hive_data(hql)
        # exe_cmd = "hive -e '"+str(hql)+"'"
        # print 'cmd===%s' %(exe_cmd)
        # lines_d = os.popen(exe_cmd).readlines()
        # lines = ''
        # for row in lines_d:
        #     lines+=str(row)
        self.send_response(200)
        self.end_headers()
        self.wfile.write(results)


def start_server():
    server_host = '127.0.0.1'
    server_port = 8090
    httpd = SocketServer.TCPServer((server_host,server_port), HiveHttpHandler)
    print '\nStart server success ... \nserver_host:'+server_host+'   server_port:'+str(server_port)
    httpd.serve_forever()


if __name__ == "__main__":
    #curl -d "use default;select * from students" http://127.0.0.1:8090
    start_server()
