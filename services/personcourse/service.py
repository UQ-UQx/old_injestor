import pymongo
import baseservice
import MySQLdb
import json
import urllib2
import math
from models import PCModel
import time
import csv
import os.path


class PersonCourse(baseservice.BaseService):
    inst = None

    def __init__(self, course_id):
        self.course_id = course_id

        if course_id == "all":
            self.courses = self.get_all_courses()
        else:
            course = self.get_course(course_id)
            if course is None:
                self.log("error", "Unknown course code")
                return
            self.courses = {course_id : course}
        print self.courses

        PersonCourse.inst = self
        super(PersonCourse, self).__init__()

        self.status['name'] = "Person Course"
        self.mongo_enabled = True
        self.sql_enabled = True
        self.sql_pc_conn = None
        self.sql_course_conn = None
        self.initialize()
        pass


    # The class need it own initialize
    def initialize(self):
        self.set_service_name()
        self.setup_logging()
        self.log("info", "Starting service")
        if self.mongo_enabled:
            self.connect_to_mongo()
            self.log("info", "MongoDB Conn: %s" % self.mongo_client)
        if self.sql_enabled:
            # Create a sql connect for the database Person_Course
            self.sql_pc_conn = self.connect_to_sql(self.sql_pc_conn, "Person_Course", True)
            self.log("info", "MySQL Conn: %s" % self.sql_pc_conn)
            # Create a sql connect for the course database
            self.sql_course_conn = self.connect_to_sql(self.sql_course_conn, "", True)
        self.log("info", "Setup")
        self.setaction('loading')
        self.setup()
        # We only want to run once
        self.log("info", "Running...")
        self.setaction('running')
        self.run()
        self.log("info", "The end.")
        pass

    def connect_to_sql(self, sql_connect, db_name="", force_reconnect=False, create_db=True):
        if sql_connect is None or force_reconnect:
            try:
                sql_connect = MySQLdb.connect(host=self.sql_host, user=self.sql_user, passwd=self.sql_pass, db=db_name)
                return sql_connect
            except Exception, e:
                # Create the database
                if e[0] and create_db and db_name != "":
                    if sql_connect is None:
                        sql_connect = MySQLdb.connect(host=self.sql_host, user=self.sql_user, passwd=self.sql_pass)
                    self.log("info", "Creating database " + db_name)
                    self.sql_query(sql_connect, "CREATE DATABASE " + db_name, True)
                    sql_connect.select_db(db_name)
                    return sql_connect
                else:
                    self.log("error", "Could not connect to MySQL: %s" % e)
                    return False
        return False

    def sql_query(self, sql_conn, query, commit=False):
        cur = sql_conn.cursor()
        cur.execute(query)
        for row in cur.fetchall():
            print row[0]
        if commit:
            sql_conn.commit()

    # The function to create the table "personcourse".
    def create_pc_table(self):
        tablename = "personcourse"
        columns = [
            {"col_name": "id", "col_type": "int NOT NULL AUTO_INCREMENT PRIMARY KEY"},
            {"col_name": "course_id", "col_type": "varchar(255)"},
            {"col_name": "user_id", "col_type": "varchar(255)"},
            {"col_name": "registered", "col_type": "TINYINT(1) default 1"},
            {"col_name": "viewed", "col_type": "TINYINT(1)"},
            {"col_name": "explored", "col_type": "TINYINT(1)"},
            {"col_name": "certified", "col_type": "TINYINT(1)"},
            {"col_name": "final_cc_cname", "col_type": "varchar(255)"},
            {"col_name": "LoE", "col_type": "varchar(255)"},
            {"col_name": "YoB", "col_type": "year"},
            {"col_name": "gender", "col_type": "varchar(255)"},
            {"col_name": "grade", "col_type": "float"},
            {"col_name": "start_time", "col_type": "date"},
            {"col_name": "last_event", "col_type": "date"},
            {"col_name": "nevents", "col_type": "int"},
            {"col_name": "ndays_act", "col_type": "int"},
            {"col_name": "nplay_video", "col_type": "int"},
            {"col_name": "nchapters", "col_type": "int"},
            {"col_name": "nforum_posts", "col_type": "int"},
            {"col_name": "roles", "col_type": "varchar(255)"},
            {"col_name": "attempted_problems", "col_type": "int"},
            #{"col_name": "inconsistent_flag", "col_type": "TINYINT(1)"}
        ]
        query = "CREATE TABLE IF NOT EXISTS " + tablename
        query += " ("
        for column in columns:
            query += column["col_name"] + " " + column["col_type"] + ", "
        query += " inconsistent_flag TINYINT(1)"
        query += " );"
        print query
        cursor = self.sql_pc_conn.cursor()
        cursor.execute(query)

    def setup(self):
        pass

    def run(self):
        # Drop the table 'personcourse' if exists
        self.remove_pc_table()

        # Create the table "personcourse"
        self.create_pc_table()

        for course_id, course in self.courses.items():
            self.log('info', 'Preparing data for %s ...' % course_id)

            # Dict of items of personcourse, key is the user id
            pc_dict = {}

            # Get chapters from course info
            json_file = course['dbname'].replace("_", "-") + '.json'
            courseinfo = self.loadcourseinfo(json_file)
            if courseinfo is None:
                self.log("error", "Can not find course info.")
                return
            chapters = []
            chapters = self.get_chapter(courseinfo, chapters)
            #for chapter in chapters:
            #    print chapter['display_name']
            half_chapters = math.ceil(float(len(chapters)) / 2)

            # Select the database
            self.sql_course_conn.select_db(course['dbname'])
            course_cursor = self.sql_course_conn.cursor()

            # course_id for PCModel
            pc_course_id = course['mongoname']

            # find all user_id
            self.log("info", "{auth_user}")
            query = "SELECT id, is_staff FROM auth_user"
            course_cursor.execute(query)
            result = course_cursor.fetchall()
            for record in result:
                pc_dict[record[0]] = PCModel(pc_course_id, record[0])
                pc_dict[record[0]].set_roles(record[1])

            # The list of user_id
            user_id_list = pc_dict.keys()
            user_id_list.sort()
            #print user_id_list

            # Set LoE, YoB, gender based on the data in {auth_userprofile}
            self.log("info", "{auth_userprofile}")
            query = "SELECT user_id, year_of_birth, level_of_education, gender FROM auth_userprofile WHERE user_id in (" + ",".join(["%s"] * len(user_id_list)) + ")"
            query = query % tuple(user_id_list)
            course_cursor.execute(query)
            result = course_cursor.fetchall()
            for record in result:
                user_id = int(record[0])
                pc_dict[user_id].set_YoB(record[1])
                pc_dict[user_id].set_LoE(record[2])
                pc_dict[user_id].set_gender(record[3])

            # Set certified based on the data in {certificates_generatedcertificate}
            self.log("info", "{certificates_generatedcertificate}")
            query = "SELECT user_id, grade, status FROM certificates_generatedcertificate WHERE user_id in (" + ",".join(["%s"] * len(user_id_list)) + ")"
            query = query % tuple(user_id_list)
            course_cursor.execute(query)
            result = course_cursor.fetchall()
            for record in result:
                user_id = int(record[0])
                pc_dict[user_id].set_grade(float(record[1]))
                pc_dict[user_id].set_certified(record[2])

            # Set start_time based on the data in {student_courseenrollment}
            self.log("info", "{student_courseenrollment}")
            query = "SELECT user_id, created FROM student_courseenrollment WHERE user_id in (" + ",".join(["%s"] * len(user_id_list)) + ")"
            query = query % tuple(user_id_list)
            course_cursor.execute(query)
            result = course_cursor.fetchall()
            for record in result:
                user_id = int(record[0])
                pc_dict[user_id].set_start_time(record[1])

            # Set ndays_act and viewed based on the data in {courseware_studentmodule}
            self.log("info", "{ndays_act: courseware_studentmodule}")
            query = "SELECT student_id, COUNT(DISTINCT SUBSTRING(created, 1, 10)) FROM courseware_studentmodule GROUP BY student_id"
            course_cursor.execute(query)
            result = course_cursor.fetchall()
            for record in result:
                user_id = int(record[0])
                if user_id in pc_dict:
                    pc_dict[user_id].set_ndays_act(record[1])
                    if record[1] > 0:
                        pc_dict[user_id].set_viewed(1)
                else:
                    self.log("error", "Student id: %s does not exist in {auth_user}." % user_id)

            # Set attempted problems
            self.log("info", "{attempted_problems: courseware_studentmodule}")
            query = "SELECT student_id, COUNT(state) FROM courseware_studentmodule WHERE state LIKE '%correct_map%' GROUP BY student_id"
            course_cursor.execute(query)
            result = course_cursor.fetchall()
            for record in result:
                user_id = int(record[0])
                if user_id in pc_dict:
                    pc_dict[user_id].set_attempted_problems(record[1])
                else:
                    self.log("error", "Student id: %s does not exist in {auth_user}." % user_id)

            # Set nplay_video based on the data in {courseware_studentmodule}
            self.log("info", "{nplay_video: courseware_studentmodule}")
            query = "SELECT student_id, COUNT(*) FROM courseware_studentmodule WHERE module_type = 'video' GROUP BY student_id"
            course_cursor.execute(query)
            result = course_cursor.fetchall()
            for record in result:
                user_id = int(record[0])
                if user_id in pc_dict:
                    pc_dict[user_id].set_nplay_video(record[1])

            # Set nchapters and explored based on the data in {courseware_studentmodule}
            self.log("info", "{nchapters: courseware_studentmodule}")
            query = "SELECT student_id, COUNT(DISTINCT module_id) FROM courseware_studentmodule WHERE module_type = 'chapter' GROUP BY student_id"
            course_cursor.execute(query)
            result = course_cursor.fetchall()
            for record in result:
                user_id = int(record[0])
                if user_id in pc_dict:
                    pc_dict[user_id].set_nchapters(record[1])
                    if record[1] >= half_chapters:
                        pc_dict[user_id].set_explored(1)
                    else:
                        pc_dict[user_id].set_explored(0)

            # Mongo
            # Discussion forum
            self.log("info", "{discussion_forum}")
            self.mongo_dbname = "discussion_forum"
            self.mongo_collectionname = course['discussiontable']
            self.connect_to_mongo(self.mongo_dbname, self.mongo_collectionname)

            user_posts = self.mongo_collection.aggregate([
                #{"$match": {"author_id": {"$in": user_id_list}}},
                {"$group": {"_id": "$author_id", "postSum": {"$sum": 1}}}
            ])['result']

            for item in user_posts:
                user_id = int(item["_id"])
                if user_id in pc_dict:
                    pc_dict[user_id].set_nforum_posts(item['postSum'])
                else:
                    self.log("error", "Author id: %s does not exist in {auth_user}." % user_id)

            # Tracking logs
            self.log("info", "{logs}")
            self.mongo_dbname = "logs"
            self.mongo_collectionname = "clickstream"
            #self.mongo_collectionname = "clickstream_hypers_301x_sample"
            self.connect_to_mongo(self.mongo_dbname, self.mongo_collectionname)

            user_events = self.mongo_collection.aggregate([
                {"$match": {"context.course_id": pc_course_id}},
                {"$sort": {"time": 1}},
                {"$group": {"_id": "$context.user_id", "countrySet": {"$addToSet": "$country"}, "eventSum": {"$sum": 1}, "last_event": {"$last": "$time"}}}
            ], allowDiskUse=True)['result']

            for item in user_events:
                user_id = item["_id"]
                if user_id in pc_dict:
                    pc_dict[user_id].set_last_event(item["last_event"])
                    pc_dict[user_id].set_nevents(item["eventSum"])
                    pc_dict[user_id].set_final_cc_cname(item["countrySet"])
                else:
                    self.log("error", "Context.user_id: %s does not exist in {auth_user}." % user_id)

            #print pc_dict[1000]

            # Till now, data preparation has been finished. Check consistent then write them into the database.
            self.log("info", "save to {personcourse}")
            tablename = "personcourse"
            pc_cursor = self.sql_pc_conn.cursor()
            for user_id, user_data in pc_dict.items():
                pc_dict[user_id].set_inconsistent_flag()
                pc_dict[user_id].save2db(pc_cursor, tablename)
            self.sql_pc_conn.commit()

        # Datadumping
        self.datadump2csv()


    def loadcourseinfo(self, json_file):
        courseurl = 'https://tools.ceit.uq.edu.au/datasources/course_structure/'+json_file
        courseinfofile = urllib2.urlopen(courseurl)
        if courseinfofile:
            courseinfo = json.load(courseinfofile)
            return courseinfo
        return None

    def get_chapter(self, obj, found=[]):
        if obj['tag'] == 'chapter':
            found.append(obj)
        for child in obj['children']:
            found = self.get_chapter(child, found)
        return found

    def datadump2csv(self, tablename = "personcourse"):
        backup_path = self.get_backup_path()
        current_time = time.strftime('%m%d%Y-%H%M%S')
        backup_prefix = "PersonCourse_" + self.course_id + "_" + current_time
        backup_file = os.path.join(backup_path, backup_prefix + ".csv")

        if self.sql_pc_conn is None:
            self.sql_pc_conn = self.connect_to_sql(self.sql_pc_conn, "Person_Course", True)

        pc_cursor = self.sql_pc_conn.cursor()
        query = "SELECT * FROM %s" % tablename
        pc_cursor.execute(query)
        result = pc_cursor.fetchall()

        with open(backup_file, "w") as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow([i[0] for i in pc_cursor.description]) # write headers
            for record in result:
                csv_writer.writerow(record)
        self.log("info", "The table %s exported to csv file %s" % (tablename, backup_file))

    def remove_pc_table(self):
        tablename = "personcourse"
        query = "DROP TABLE IF EXISTS %s" % tablename
        pc_cursor = self.sql_pc_conn.cursor()
        pc_cursor.execute(query)
        self.sql_pc_conn.commit()
        self.log('info', query)


def name():
    return str("personcourse")


def status():
    return PersonCourse.inst.status


def runservice(course_id):
    return PersonCourse(course_id)