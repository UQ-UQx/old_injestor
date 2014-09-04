import pymongo
import baseservice
import MySQLdb
import json
import urllib2
import math
from models import PCModel
from models import CFModel
import time
import csv
import os.path
import dateutil.parser
import datetime
import math


class PersonCourse(baseservice.BaseService):
    inst = None
    pc_db = 'Person_Course'
    pc_table = 'personcourse'
    cf_table = 'courseprofile'

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
        cursor = self.sql_pc_conn.cursor()
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
            {"col_name": "mode", "col_type": "varchar(255)"},
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
        for course_id, course in self.courses.items():
            pc_tablename = self.pc_table + "_" + course_id
            query = "CREATE TABLE IF NOT EXISTS " + pc_tablename
            query += " ("
            for column in columns:
                query += column["col_name"] + " " + column["col_type"] + ", "
            query += " inconsistent_flag TINYINT(1)"
            query += " );"
            #print query
            cursor.execute(query)

    # The function to create the table "courseprofile"
    def create_cf_table(self):
        tablename = "courseprofile"
        columns = [
            {"col_name": "id", "col_type": "int NOT NULL AUTO_INCREMENT PRIMARY KEY"},
            {"col_name": "course", "col_type": "varchar(255)"},
            {"col_name": "dbname", "col_type": "varchar(255)"},
            {"col_name": "mongoname", "col_type": "varchar(255)"},
            {"col_name": "discussiontable", "col_type": "varchar(255)"},
            {"col_name": "registration_open_date", "col_type": "date"},
            {"col_name": "course_launch_date", "col_type": "date"},
            {"col_name": "course_close_date", "col_type": "date"},
            {"col_name": "nregistered_students", "col_type": "int"},
            {"col_name": "nviewed_students", "col_type": "int"},
            {"col_name": "nexplored_students", "col_type": "int"},
            {"col_name": "ncertified_students", "col_type": "int"},
            {"col_name": "nhonor_students", "col_type": "int"},
            {"col_name": "naudit_students", "col_type": "int"},
            {"col_name": "nvertified_students", "col_type": "int"},
            {"col_name": "nhonor_before", "col_type": "int"},
            {"col_name": "naudit_before", "col_type": "int"},
            {"col_name": "nvertified_before", "col_type": "int"},
            {"col_name": "nhonor_during", "col_type": "int"},
            {"col_name": "naudit_during", "col_type": "int"},
            {"col_name": "nvertified_during", "col_type": "int"},
            {"col_name": "nhonor_after", "col_type": "int"},
            {"col_name": "naudit_after", "col_type": "int"},
            {"col_name": "nvertified_after", "col_type": "int"},
            {"col_name": "course_effort", "col_type": "float"},
            {"col_name": "course_length", "col_type": "int"},
            {"col_name": "nchapters", "col_type": "int"},
            {"col_name": "nvideos", "col_type": "int"},
            {"col_name": "nhtmls", "col_type": "int"},
            {"col_name": "nassessments", "col_type": "int"},
            {"col_name": "nsummative_assessments", "col_type": "int"},
            {"col_name": "nformative_assessments", "col_type": "int"},
            {"col_name": "nincontent_discussions", "col_type": "int"},
            {"col_name": "nactivities", "col_type": "int"},
            {"col_name": "best_assessment", "col_type": "varchar(255)"},
            #{"col_name": "worst_assessment", "col_type": "varchar(255)"},
        ]
        query = "CREATE TABLE IF NOT EXISTS " + tablename
        query += "("
        for column in columns:
            query += column['col_name'] + " " + column['col_type'] + ', '
        query += " worst_assessment varchar(255)"
        query += " );"
        #print query
        cursor = self.sql_pc_conn.cursor()
        cursor.execute(query)

    def setup(self):
        pass

    def run(self):
        # Create 'cf_table'
        self.create_cf_table()
        # Clean 'pc_table'
        self.clean_pc_db()

        for course_id, course in self.courses.items():
            self.log('info', 'Preparing data for %s ...' % course_id)

            # Get chapters from course info
            json_file = course['dbname'].replace("_", "-") + '.json'
            courseinfo = self.loadcourseinfo(json_file)
            if courseinfo is None:
                self.log("error", "Can not find course info.")
                return

            cf_item = CFModel(course_id, course['dbname'], course['mongoname'], course['discussiontable'])
            # Set cf_item course_launch_date
            if 'start' in courseinfo:
                course_launch_time = dateutil.parser.parse(courseinfo['start']).replace(tzinfo=None)
                course_launch_date = course_launch_time.date()
                cf_item.set_course_launch_date(course_launch_date)
            # Set cf_item course_close_date
            if 'end' in courseinfo:
                course_close_time = dateutil.parser.parse(courseinfo['end']).replace(tzinfo=None)
                course_close_date = course_close_time.date()
                cf_item.set_course_close_date(course_close_date)
            # Set cf_item course_length
            if course_launch_date and course_close_date:
                date_delta = course_close_date - course_launch_date
                cf_item.set_course_length(math.ceil(date_delta.days/7.0))

            # Set cf_item nchapters
            chapters = []
            chapters = self.get_chapter(courseinfo, chapters)
            nchapters = len(chapters)
            cf_item.set_nchapters(nchapters)
            half_chapters = math.ceil(float(nchapters) / 2)

            # Set cf_item nvideos, nhtmls, nassessments, nsummative_assessments, nformative_assessments, nincontent_discussions, nactivities
            content = self.analysis_chapters(chapters)
            cf_item.set_nvideos(content['nvideos'])
            cf_item.set_nhtmls(content['nhtmls'])
            cf_item.set_nassessments(content['nassessments'])
            cf_item.set_nsummative_assessments(content['nsummative_assessments'])
            cf_item.set_nformative_assessments(content['nformative_assessments'])
            cf_item.set_nincontent_discussions(content['nincontent_discussions'])
            cf_item.set_nactivities(content['nactivities'])

            # Create 'pc_table'
            self.create_pc_table()

            # Dict of items of personcourse, key is the user id
            pc_dict = {}

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
            query = "SELECT user_id, created, mode FROM student_courseenrollment WHERE user_id in (" + ",".join(["%s"] * len(user_id_list)) + ")"
            query = query % tuple(user_id_list)
            course_cursor.execute(query)
            result = course_cursor.fetchall()
            nhonor = 0
            naudit = 0
            nvertified = 0
            nhonor_before = 0
            naudit_before = 0
            nvertified_before = 0
            nhonor_during = 0
            naudit_during = 0
            nvertified_during = 0
            nhonor_after = 0
            naudit_after = 0
            nvertified_after = 0
            registration_open_date = datetime.date.today()
            for record in result:
                user_id = int(record[0])
                start_time = dateutil.parser.parse(record[1])
                start_date = start_time.date()
                pc_dict[user_id].set_start_time(start_date)
                pc_dict[user_id].set_mode(record[2])
                if record[2] == 'honor':
                    nhonor += 1
                    if course_launch_time and course_close_time:
                        if start_time < course_launch_time:
                            nhonor_before += 1
                        elif start_time > course_close_time:
                            nhonor_after += 1
                        else:
                            nhonor_during += 1
                if record[2] == 'audit':
                    naudit += 1
                    if course_launch_time and course_close_time:
                        if start_time < course_launch_time:
                            naudit_before += 1
                        elif start_time > course_close_time:
                            naudit_after += 1
                        else:
                            naudit_during += 1
                if record[2] == 'verified':
                    nvertified += 1
                    if course_launch_time and course_close_time:
                        if start_time < course_launch_time:
                            nvertified_before += 1
                        elif start_time > course_close_time:
                            nvertified_after += 1
                        else:
                            nvertified_during += 1
                if start_date < registration_open_date:
                    registration_open_date = start_date
            # Set cf_item nhonor_students, naudit_students, nvertified_students, registration_open_date
            cf_item.set_nhonor_students(nhonor)
            cf_item.set_naudit_students(naudit)
            cf_item.set_nvertified_students(nvertified)
            cf_item.set_nhonor_before(nhonor_before)
            cf_item.set_naudit_before(naudit_before)
            cf_item.set_nvertified_before(nvertified_before)
            cf_item.set_nhonor_during(nhonor_during)
            cf_item.set_naudit_during(naudit_during)
            cf_item.set_nvertified_during(nvertified_during)
            cf_item.set_nhonor_after(nhonor_after)
            cf_item.set_naudit_after(naudit_after)
            cf_item.set_nvertified_after(nvertified_after)
            cf_item.set_registration_open_date(registration_open_date)

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

            # Set cf_item nregistered_students, nviewed_students, nexplored_students, ncertified_students
            nregistered_students = sum(pc_item.registered for pc_item in pc_dict.values())
            nviewed_students = sum(pc_item.viewed for pc_item in pc_dict.values())
            nexplored_students = sum(pc_item.explored for pc_item in pc_dict.values())
            ncertified_students = sum(pc_item.certified for pc_item in pc_dict.values())
            cf_item.set_nregistered_students(nregistered_students)
            cf_item.set_nviewed_students(nviewed_students)
            cf_item.set_nexplored_students(nexplored_students)
            cf_item.set_ncertified_students(ncertified_students)

            pc_cursor = self.sql_pc_conn.cursor()
            #print cf_item
            cf_item.save2db(pc_cursor, self.cf_table)

            # Till now, data preparation for pc_tablex has been finished.
            # Check consistent then write them into the database.
            self.log("info", "save to {personcourse}")
            tablename = self.pc_table + "_" + course_id
            for user_id, user_data in pc_dict.items():
                pc_dict[user_id].set_inconsistent_flag()
                pc_dict[user_id].save2db(pc_cursor, tablename)

            self.sql_pc_conn.commit()

        #Datadumping
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
        else:
            for child in obj['children']:
                found = self.get_chapter(child, found)
        return found

    def analysis_chapters(self, chapters):
        nvideos = 0
        nhtmls = 0
        nassessments = 0
        nsummative_assessments = 0
        nformative_assessments = 0
        nincontent_discussions = 0
        nactivities = 0

        for chapter in chapters:
            for sequential in chapter['children']:
                if sequential['tag'] == 'sequential' and 'children' in sequential:
                    for vertical in sequential['children']:
                        if vertical['tag'] == 'vertical' and 'children' in vertical:
                            for child in vertical['children']:
                                if child['tag'] == 'video':
                                    nvideos += 1
                                elif child['tag'] == 'html':
                                    nhtmls += 1
                                elif child['tag'] == 'problem':
                                    nassessments += 1
                                    if 'weight' in child and float(child['weight']) > 0:
                                        nsummative_assessments += 1
                                    else:
                                        nformative_assessments += 1
                                elif child['tag'] == 'discussion':
                                    nincontent_discussions += 1
                                else:
                                    nactivities += 1

        return {"nvideos": nvideos, "nhtmls": nhtmls, "nassessments": nassessments,
                "nsummative_assessments": nsummative_assessments, "nformative_assessments": nformative_assessments,
                "nincontent_discussions": nincontent_discussions, "nactivities": nactivities}

    def datadump2csv(self, tablename = "personcourse"):
        if self.sql_pc_conn is None:
            self.sql_pc_conn = self.connect_to_sql(self.sql_pc_conn, "Person_Course", True)
        pc_cursor = self.sql_pc_conn.cursor()

        backup_path = self.get_backup_path()
        current_time = time.strftime('%m%d%Y-%H%M%S')

        # export the {personcourse}x tables
        for course_id, course in self.courses.items():

            pc_tablename = self.pc_table + "_" + course_id

            backup_prefix = pc_tablename +  "_" + current_time
            backup_file = os.path.join(backup_path, backup_prefix + ".csv")

            query = "SELECT * FROM %s" % pc_tablename
            pc_cursor.execute(query)
            result = pc_cursor.fetchall()

            with open(backup_file, "w") as csv_file:
                csv_writer = csv.writer(csv_file)
                csv_writer.writerow([i[0] for i in pc_cursor.description]) # write headers
                for record in result:
                    csv_writer.writerow(record)
            self.log("info", "The personcourse table: %s exported to csv file %s" % (pc_tablename, backup_file))

        # export the cf_table
        backup_prefix = self.cf_table + "_" + current_time
        backup_file = os.path.join(backup_path, backup_prefix + ".csv")

        query = "SELECT * FROM %s" % self.cf_table
        pc_cursor.execute(query)
        result = pc_cursor.fetchall()

        with open(backup_file, "w") as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow([i[0] for i in pc_cursor.description]) # write headers
            for record in result:
                csv_writer.writerow(record)
        self.log("info", "The courseprofile table: %s exported to csv file %s" % (self.cf_table, backup_file))

    def clean_pc_db(self):
        pc_cursor = self.sql_pc_conn.cursor()

        for course_id, course in self.courses.items():
            pc_tablename = self.pc_table + "_" + course_id
            query = "DROP TABLE IF EXISTS %s" % pc_tablename
            pc_cursor.execute(query)

            query = "DELETE FROM %s WHERE course = '%s'" % (self.cf_table, course_id)
            pc_cursor.execute(query)

        self.sql_pc_conn.commit()
        self.log('info', self.pc_db + " has been cleaned.")


def name():
    return str("personcourse")


def status():
    return PersonCourse.inst.status


def runservice(course_id):
    return PersonCourse(course_id)