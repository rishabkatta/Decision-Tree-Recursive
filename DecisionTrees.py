'''
@author-name: Rishab Katta

Decision Tree Program for binary classification on waittable.csv file.

NOTE: This program needs a database and a user with SUPERUSER priviliges. SQL scripts are provided in sql_scripts.txt
'''
import psycopg2
import time
import math
from functools import reduce
import operator


class DatabaseConnection:

    def __init__(self,h,db,username,pwd):
        '''
        Constructor is used to connect to the database
        :param h: hostname
        :param db: database name
        :param username: Username
        :param pwd: password
        '''
        try:
            self.connection = psycopg2.connect(host=str(h), database=str(db), user=str(username), password=str(pwd))
            # self.connection = psycopg2.connect(host='localhost', database='DecisionTrees', user='user1', password='abcde')
            self.connection.autocommit=True
            self.cursor=self.connection.cursor()
            self.attr_set = list()
        except Exception as e:
            print(getattr(e, 'message', repr(e)))
            print(getattr(e, 'message', str(e)))

    def create_table(self):
        '''
        Create table waittable in the database DecisionTree
        :return: None
        '''
        self.cursor.execute("create table waittable(num int primary key, alt char(1), bar char(1), fri char(1), hun char(1), pat varchar, price varchar, "
					        " rain char(1), res char(1), type varchar, est varchar, wait char(1))")

    def insert_table(self,path):
        '''
        Insert into table waittable the data from .csv file
        :param path: Path of the .csv file
        :return: None
        '''

        file = str(path)+'WaitTable.csv'
        self.cursor.execute("COPY waittable(num, alt, bar, fri, hun, pat, price, rain, res, type, est, wait )"
                            " FROM %s DELIMITER ',' CSV HEADER", (file,))

    def group_values(self):
        '''
        Groups records for a given target attribute and each of the non-target attributes based on values
        :return: Dictionary with grouped values.
        '''
        col_list=['alt', 'bar', 'fri', 'hun', 'pat', 'price', 'rain', 'res', 'type', 'est' ]
        final_dict={}
        for col in col_list:
            self.cursor.execute("select " +str(col)+ ", array_agg(num), 'T' as decision from waittable where wait='T' group by " + str(col) +
                                " union " 
                                "select " +str(col)+ ", array_agg(num), 'F' as decision from waittable where wait='F' group by " + str(col) )
            rows = self.cursor.fetchall()
            group_dict = {}
            for row in rows:
                if row[0] not in group_dict.keys():
                    group_dict[row[0]] = dict()
                group_dict[row[0]][row[2]] = row[1]
            final_dict[col] = group_dict

        return final_dict

    def calculate_entropy(self,final_dict):
        '''
        Calculate Entropy for all the non-target attributes
        :param final_dict: Dictionary with gropued values from the above function
        :return: Dictionary with non attribute names as keys and entropies as values
        '''
        tl_dict = {}
        for keyoncol, splitoncol in final_dict.items():
            tl_dict[keyoncol] = dict()
            for keyoncv, splitoncv in splitoncol.items():
                total_length = 0
                for keyonbin, splitonbin in splitoncv.items():
                    total_length += len(splitonbin)
                tl_dict[keyoncol][keyoncv] = total_length

        ent_dict = {}
        for keyoncol, splitoncol in final_dict.items():
            ent = 0
            for keyoncv, splitoncv in splitoncol.items():
                total_length = tl_dict[keyoncol][keyoncv]
                for keyonbin, splitonbin in splitoncv.items():
                    ent += -(len(splitonbin) / total_length) * math.log(len(splitonbin) / total_length, 10)
            ent_dict[keyoncol] = ent

        return ent_dict


    def decision_tree(self, final_dict, ent_dict):
        '''
        Recursively split the examples until all the subset have the same value
        :param final_dict: Dictionary with grouped values from group_values function
        :param ent_dict: Entropy Dictionary from calculate entropy function
        :return:
        '''

        num_list = [0,1,2,3,4,5,6,7,8,9,10,11]
        min_ent=1000
        attribute=""     #attribute to split on
        for attr, entropy in ent_dict.items():
            if entropy<min_ent and attr not in self.attr_set:
                min_ent = entropy
                attribute = attr

        split = final_dict[attribute]
        self.attr_set.append(attribute)
        for attrk, attrv in split.items():
            if len(attrv.keys())>1:
                examples_list = reduce(operator.concat, attrv.values())
                not_el = list(set(num_list) - set(examples_list))
                for num in not_el:
                    self.cursor.execute("delete from waittable where num = " + str(num))
                fin_dict = self.group_values()
                en_dict = self.calculate_entropy(fin_dict)
                self.decision_tree(fin_dict,en_dict)

    def drop_tables(self):
        '''
        Drop table waittable if it exists in DecisionTree database
        :return: None
        '''
        self.cursor.execute("drop table if exists waittable")




if __name__ == '__main__':
    h = str(input("Enter host name"))
    db = str(input("Enter Database Name"))
    username = str(input("Enter username"))
    pwd = str(input("Enter password"))
    path = str(input("Enter Path except the file name - example- C:/users/files/"))
    db_con = DatabaseConnection(h,db,username,pwd)
    start_time = time.time()
    db_con.drop_tables()
    db_con.create_table()
    db_con.insert_table(path)
    final_dict=db_con.group_values()
    print("Initial dictionary made by grouping values for all non target attr for a given target attr: ")
    print(final_dict)
    print(" ")
    ent_dict=db_con.calculate_entropy(final_dict)
    print("Initial Entropy calculated for all non target attr: ")
    print(ent_dict)
    print(" ")
    db_con.decision_tree(final_dict,ent_dict)
    print("Attribute Decision Tree split on, in order:", end=' ')
    print(db_con.attr_set)
    print("\n")
    print("--- %s seconds for program to run ---" % (time.time() - start_time))

