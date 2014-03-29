#!/user/bin/python
#encoding:utf-8

import sys
import re

class pig:
    def __init__(self):
        self.my_id = 0
        self.my_name = ""
        self.parent_id = []
        self.my_filter = ""
        self.my_generate = ""
        self.my_streaming = ""

    def about(self, alls):
        print "++++++++++ a pig coming ++++++"
        print "my id %s" % self.my_id
        print "my name is %s" % self.my_name
        print "my father is %s" % getFatherNameByID(self.parent_id, alls)
        print "++++++++++ welcome another pig ++++++"

def getFatherNameByID(parent_ids, alls):
        father_name = []
        if len(parent_ids) == 0:
            father_name.append("I'm the father")
        else:
            for parent_id in parent_ids:
                for apig in alls:
                    if alls[apig].my_id == parent_id:
                        father_name.append(alls[parent_id].my_name)
        return ','.join(father_name)

def getParentIDSByNames(parent_names, alls):
        parent_ids = []
        if len(parent_names) == 0:
            father_name = "I'm the father"
        else:
            for pname in parent_names:
                for apig in alls:
                    if alls[apig].my_name == pname:
                        parent_ids.append(alls[apig].my_id)
        return parent_ids

def getList(regex,text):
    '''
    输入：正则表达式，文本
    输出：匹配到的数组(默认使用findall匹配)
    '''
    res = re.findall(regex, text)
    if res:
        for r in res:
            return r

def getOpts():
    if len(sys.argv) != 2:
        print "use pigsty.py <filepath>"
        exit(0)
    else:
        filepath = sys.argv[1]
        return filepath

def main():
    filepath = getOpts()
    id_counter = 1
    pigs = {}
    #import pdb
    #pdb.set_trace()
    for line in open(filepath):
        if line.find('LOAD') != -1:
            load_list = line.strip(' ').split('=')
            pigs[id_counter] = pig()
            pigs[id_counter].my_id = id_counter
            pigs[id_counter].my_name = load_list[0].strip(' ')
            id_counter += 1
        elif line.find('FILTER') != -1:
            filter_list = getList(u"^([\S]+).*=.*FILTER\s([\S]+).*GENERATE\s([^;)]+)", line )
            pigs[id_counter] = pig()
            pigs[id_counter].my_id = id_counter
            pigs[id_counter].my_name = filter_list[0]
            pigs[id_counter].my_generate = filter_list[-1]
            pigs[id_counter].parent_id = getParentIDSByNames(filter_list[1:-1], pigs) 
            id_counter += 1
        elif line.find('JOIN ') != -1:
            join_list = getList(u"^([\S]+).*=.*JOIN\s([\S]+).*,[\s]+([\S]+)[\s]+BY.*GENERATE\s([^;)]+)", line )
            pigs[id_counter] = pig()
            pigs[id_counter].my_id = id_counter
            pigs[id_counter].my_name = join_list[0]
            pigs[id_counter].my_generate = join_list[-1]
            pigs[id_counter].parent_id = getParentIDSByNames(join_list[1:-1], pigs) 
            id_counter += 1
        else :
            pass

    for apig in pigs:
        pigs[apig].about(pigs)

if __name__ == '__main__':
    main()
