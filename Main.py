import re
import numpy as np
import os
import pandas as pd
from pandas import DataFrame

if len(sys.argv) < 3:
    print("--------------\n")
    print("Number of arguments expected - 2\n")
    print("Examples\n")
    print("Path where all job log files are present: Source\n")
    print("Path where all extracted table name get loaded\n")
    sys.exit(1)

source_path = sys.argv[1]
target_path = sys.argv[2]

list_all = []  # to get the position of SELECT BLOCK keywords
rlist_all = []
select_all = []  # to get the block of all join statements
table_name = []  # to get all the table names
join_all = []  # to get the block of all join statements
list_all_1 = []
rlist_all_1 = []
cmnt_list = []
cmnt_list_loc = []
slashN_loc= []
SQL_RM_CMNTS =[]
path = source_path
# C:\Users\\ABC\Desktop\NewFolder
file_name = []

for filename in os.listdir(path):
    file_name.append(filename)
total_files = len(file_name)
#print(file_name)
# print(total_files)
for q in range(0, total_files):

    path_ = path

    path2 = path + '\\' + file_name[q]

    with open(path2, "r") as log_file:
        contents = log_file.read()
        select_all.append(contents)

        # to remove single line comments from SQL statement
        for Tmp in range(0, len(select_all)):
            if "--" in select_all[Tmp]:
                temp_string = select_all[Tmp]

                for match in re.finditer('--', temp_string):
                    cmnt_list_loc.append(match.start())
                len_cmnt = len(cmnt_list_loc)
                for sj in range(0, len_cmnt):
                    cmnt_loc = temp_string.find('--')
                    slashN_loc1 = temp_string.find('\n', cmnt_loc)
                    temp_string = temp_string.replace(temp_string[cmnt_loc:slashN_loc1], '')
                    len_cmnt =len_cmnt -1
                    if cmnt_loc == -1:
                        break
        cmnt_list_loc.clear()
        select_all.clear()
        select_all.append(temp_string)
        temp_string = ''
        # to remove block of comments from SQL statement
        for Tmp1 in range(0, len(select_all)):
            if "/*" in select_all[Tmp1]:
                temp_string1 = select_all[Tmp1]
                #print(temp_string1)
                for match in re.finditer('\/\*', temp_string1):
                    cmnt_list.append(match.start())
                len_cmnt1 = len(cmnt_list)
                for sj in range(0, len_cmnt1):
                    cmnt_loc1 = temp_string1.find('/*')
                    End_loc = temp_string1.find('*/', cmnt_loc1)
                    temp_string1 = temp_string1.replace(temp_string1[cmnt_loc1:End_loc+2], '')
                    len_cmnt1 =len_cmnt1 -1
                    if cmnt_loc1 == -1:
                        break
                #print(temp_string1)
        cmnt_list.clear()
        select_all.clear()
        select_all.append(temp_string1)
        #print(temp_string1)


    # --------------------------------
    #print(select_all)
    list2 = []
    list2 = [u.replace('\t', '') for u in select_all]
    list2 = [u.replace('\n', ' ') for u in list2]
    list2 = [x.upper() for x in list2]  # FOR UPPER CASE

    #print(list2)

    # FROM block to get the tables name
    def FROM_TABLE_NAME(s_list2=[]):

        if len(s_list2) != 0:
            # print(s_list2)
            s_list2_len = len(s_list2)
            # print(s_list2)
            # print(s_list2_len)
            from_all = []
            SRC_keyword = ['WHERE', 'where', 'RIGHT', '(', ')', 'LEFT', 'INNER', 'FULL', 'EVENT ID']
            src_keyword_len = len(SRC_keyword)
            from_all.clear()
            for c in range(0, s_list2_len):

                for match in re.finditer(' FROM ', s_list2[c]):  # to get all FROM positions
                    from_all.append(match.end())
                s = []  # to take the positions of matched SRC_KEYWORD from "SELECT-FROM" block
                s.clear()
                # print(from_all)
                f_len = len(from_all)
                for a in range(0, f_len):

                    for i in range(0, src_keyword_len):  # to get the table name last position just after FROM
                        s.append(s_list2[c].find(SRC_keyword[i], from_all[a]))
                    # print(s)
                    count_rep = {i: s.count(i) for i in s}  # count of all duplicate position entries
                    count_minus_1 = count_rep[-1]  # to get count of only -1 from dict
                    for i in range(0, count_minus_1):  # process to remove -1 from list
                        for l in s:
                            if l == -1:
                                s.remove(l)

                    # print(s)
                    if len(s) != 0:
                        mini_pos_src = min(s)  # to get minimum position value
                    temp = s_list2[c]
                    table_name.append(temp[from_all[a]:mini_pos_src])
                    s.clear()
                from_all.clear()
        # s_list2.clear()


    # --------------------------------------------------------------------------------------------------------
    # list_all

    def JOIN_TABLE_NAME(s_list_all=[]):

        s = []
        lop_len = len(s_list_all)
        join_pos_x = []
        join_src_keywords = ['.', '(']
        k = len(join_src_keywords)
        for z in range(0, lop_len):
            if s_list_all[z].find('JOIN') != -1:  # Checking whether JOIN is present or not

                for join_pos_1 in re.finditer('JOIN', s_list_all[z]):  # searching for all JOIN keyword in block
                    join_pos_x.append(join_pos_1.end())  # listing all end position of JOIN keywords

                t_len = len(join_pos_x)
                for t in range(0, t_len):
                    # for i in range(0, k):
                    #   s.append(s_list_all[z].find(join_src_keywords[i], join_pos_x[t]))

                    space_j = s_list_all[z].find('ON', join_pos_x[t])
                    table_name.append(s_list_all[z][join_pos_x[t]:space_j])
                    s.clear()

                join_pos_x.clear()
        s_list_all.clear()


    # -------------------------------------------------------------------------------------
    # FUNCTION CALLING
    FROM_TABLE_NAME(list2)
    JOIN_TABLE_NAME(list2)

    # UPDATE BLOCK
    updt_list = []
    set_list = []
    Update_string = 'UPDATE'
    set = 'SET'
    str = temp_string1.upper()

    for match in re.finditer(Update_string, str):
        updt_list.append(match.end())
    updt_lp_cnt = len(updt_list)

    for x in range(0, updt_lp_cnt):
        set_list.append(str.find(set, updt_list[x]))
        table_name.append(str[updt_list[x]:set_list[x]])

    # -----------------------------------------------------------------------------
    # -----------------------------------------------------------------------------
    # Insert BLOCK
    insert_string = 'INSERT INTO'
    inst_list = []
    r_event_list = []
    s_inst_list = []
    s_list = []
    ins_src_keyword = ['SELECT', '(', 'VALUES']
    ins_src_keyword_len = 3
    for match in re.finditer(insert_string, str):
        inst_list.append(match.end())
    #print(inst_list)
    len_inst_list = len(inst_list)
    for x in range(0, len_inst_list):
        r_event_list.append(temp_string1.find(';', inst_list[x]))
        # r_event_list = [x + 8 for x in r_event_list]
        s_inst_list.append(temp_string1[inst_list[x]:r_event_list[x]])
    #print(r_event_list)
    s_inst_list = [u.replace('\t', '') for u in s_inst_list]
    s_inst_list = [u.replace('\n', ' ') for u in s_inst_list]
    s_inst_list = [x.upper() for x in s_inst_list]
    #print(s_inst_list)
    s_inst_list_len = len(s_inst_list)
    #print(len(s_inst_list))
    # inst_lp_cnt = len(inst_list)
    for c in range(0, s_inst_list_len):
        for i in range(0, ins_src_keyword_len):
            s_list.append(s_inst_list[c].find(ins_src_keyword[i], 1))

        count_rep_1 = {i: s_list.count(i) for i in s_list}  # count of all duplicate position entries
        count_minus_1 = count_rep_1[-1]  # to get count of only -1 from dict
        for i in range(0, count_minus_1):  # process to remove -1 from list
            for l in s_list:
                if l == -1:
                    s_list.remove(l)

        mini_pos_src = min(s_list)
        temp_1 = s_inst_list[c]
        table_name.append(temp_1[0:mini_pos_src])
        s_list.clear()

    # ----------------------------------------------------------------------------

    # cleaning of table name list
    #table_name.append(filename)
    table_len = len(table_name)
    table_name = [elem for elem in table_name if elem.lstrip()]
    table_name = [elem for elem in table_name if elem.rstrip()]
    table_name = list(dict.fromkeys(table_name))

    #print(len(table_name))

    list_all.clear()



#print(table_name)
mmm = pd.DataFrame({'Table_name': table_name})
mmm.replace('^\s+', '', regex=True, inplace=True)
mmm.replace('\s+$', '', regex=True, inplace=True)
#print(mmm)
if len(table_name) > 1:
    mmm[['Table_name', 'Aliase_name']] = mmm["Table_name"].str.split(" ", 1, expand=True)
    mmm.drop("Aliase_name",axis=1,inplace=True)
    kkk = mmm.drop_duplicates(subset='Table_name', keep='first')
    kkk = kkk.replace('(SELECT', np.nan).dropna()
    kkk.to_csv(target_path)
else:
    kkk = mmm
    kkk.to_csv(target_path)
#provide path for the target file 
#For example: C:\Users\ABC\Desktop\Target_File.txt
print(kkk)




