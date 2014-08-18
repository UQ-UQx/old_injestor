import pymongo
import baseservice
import os
import subprocess
import time
import datetime

class ExtractSample(baseservice.BaseService):
    inst = None
    
    def __init__(self, course_id, sample_size, pass_ratio):
        self.course_id = course_id
        self.sample_size = sample_size
        self.pass_ratio = pass_ratio
        self.course = self.get_course(course_id)
        #print course
        
        ExtractSample.inst = self
        super(ExtractSample, self).__init__()
        
        self.status['name'] = "Extract Sample"
        self.mongo_enabled = True
        self.sql_enabled = True
        self.sql_dbname = self.course['dbname']
        self.initialize()

    def setup(self):
        pass
        
        
    def run(self):
        if self.sql_db is None:
            self.connect_to_sql(self.sql_dbname)
        self.cursor = self.sql_db.cursor()

        # Step 1. randomly choose sample students
        subset_table = self.sql_select_students()
         
        # Step 2. extract samples
        if subset_table:
            selected_user_ids = self.sql_user_id(subset_table)
            int_selected_user_ids = map(int, selected_user_ids)
            
            # MySQL
            tables = ["auth_user", "auth_userprofile", "courseware_studentmodule", "student_courseenrollment", "user_api_usercoursetag", "user_id_map", "wiki_article", "wiki_articlerevision"]
            for table in tables:
                # Create new table
                new_tablename = table + "_sample"
                self.sql_create_subset_table(new_tablename, table)
                
                # Insert rows of selected user ids into new table
                query = "INSERT INTO %s SELECT * FROM %s WHERE %s in (" + ",".join(["'%s'"] * len(selected_user_ids)) + ")"                
                if table in ["auth_user", "user_id_map"]:
                    colname = "id"
                    query = "INSERT INTO %s SELECT * FROM %s WHERE %s in (" + ",".join(["%d"] * len(int_selected_user_ids)) + ")"
                    arguments = (new_tablename, table, colname) + tuple(int_selected_user_ids)
                elif table in ["auth_userprofile", "student_courseenrollment", "user_api_usercoursetag", "wiki_articlerevision"]:
                    colname = "user_id"
                    arguments = (new_tablename, table, colname) + tuple(selected_user_ids)              
                elif table in ["courseware_studentmodule"]:
                    colname = "student_id"
                    arguments = (new_tablename, table, colname) + tuple(selected_user_ids)                           
                elif table in ["wiki_article"]:
                    colname = "owner_id"
                    arguments = (new_tablename, table, colname) + tuple(selected_user_ids)                             
                query = query % arguments
                self.cursor.execute(query)
                
            # Mongo            
            # Discussion forum
            self.mongo_dbname = "discussion_forum"
            self.connect_to_mongo(self.mongo_dbname, "")
            self.mongo_collectionname = self.course['discussiontable']
            new_collectionname = self.mongo_collectionname + "_sample"
            self.log("info", "Extract samples for " + self.mongo_dbname + " and put into " + new_collectionname + " ...")
            
            for item in self.mongo_db[self.mongo_collectionname].find({"author_id": {"$in": selected_user_ids}}):
                item_id = item.pop('_id')
                self.mongo_db[new_collectionname].update({"_id": item_id}, {"$set": item}, upsert=True)
                
            # logs    
            self.mongo_dbname = "logs"
            self.connect_to_mongo(self.mongo_dbname, "")
            self.mongo_collectionname = "clickstream"
            new_collectionname = self.mongo_collectionname + "_" + self.course_id + "_sample"
            self.log("info", "Extract samples for " + self.mongo_dbname + " and put into " + new_collectionname + " ...")
            
            for item in self.mongo_db[self.mongo_collectionname].find({"context.course_id": self.course['mongoname'], "context.user_id": {"$in": int_selected_user_ids}}):
                item_id = item.pop("_id")
                self.mongo_db[new_collectionname].update({"_id": item_id}, {"$set": item}, upsert=True)

        self.sql_db.commit()
        
        self.log("info", "Backup extracted samples.")
        self.subset_backup()
        # Just want to run once
        self.loop = False
        self.log("info", "Extractsample should/had only run once.")
        
    def subset_backup(self):
        backup_path = self.get_backup_path()
        current_time = time.strftime('%m%d%Y-%H%M%S')
        backup_prefix = self.course_id + current_time
        
        # Backup MySql        
        tables = ["certificates_generatedcertificate_sample", "auth_user_sample", "auth_userprofile_sample", "courseware_studentmodule_sample", "student_courseenrollment_sample", "user_api_usercoursetag_sample", "user_id_map_sample", "wiki_article_sample", "wiki_articlerevision_sample"]
        backup_file = os.path.join(backup_path, backup_prefix+".sql")
        cmd = "mysqldump -u %s -p%s -h %s %s " + " ".join(["%s"] * len(tables)) + " > %s" 
        arguments = [self.sql_user, self.sql_pass, self.sql_host, self.sql_dbname] + tables + [backup_file]  
        cmd = cmd % tuple(arguments)
        os.popen(cmd)
        sedcmd = "sed -i 's/_sample//g' " + backup_file
        os.system(sedcmd)
                
        # Backup Mongo
        # Discussion forum
        backup_file = os.path.join(backup_path, backup_prefix)
        cmd_str = "mongodump --db %s --collection %s --out %s"
        arguments = ["discussion_forum", self.course['discussiontable'] + "_sample", backup_file]
        cmd = cmd_str % tuple(arguments)
        os.popen(cmd)
        # Clickstream
        arguments = ["logs", "clickstream_" + self.course_id + "_sample", backup_file]
        cmd = cmd_str % tuple(arguments)
        os.popen(cmd)
        
    
    def sql_user_id(self, table):
        user_ids = []
        query = "SELECT user_id FROM " + table
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        for record in result:
            user_ids.append(record[0])
        return user_ids
    
    def sql_select_students(self, tablename=None, columns = []):
        if tablename == None:
            tablename = 'certificates_generatedcertificate'

        # Counting the rows of the table
        query0 = "SELECT COUNT(*) FROM " + tablename
        table_size = self.sql_query_size(query0)
        self.log("meta data", "Row count of %s: %s" % (tablename,  table_size))

        # Counting how many students passed
        query0 = "SELECT COUNT(*) FROM %s WHERE status = '%s'" % (tablename, "downloadable")
        pass_size = self.sql_query_size(query0) 
        self.log("meta data", "  Count of %s: %s" % ("downloadedable", pass_size))
        
        # Counting how many students only registered   
        query0 = "SELECT COUNT(*) FROM %s WHERE grade = '%s'" % (tablename, "0.0")
        only_registered = self.sql_query_size(query0)
        self.log("meta data", "  Count of %s: %s" % ("only registered", only_registered))
        
        # Counting how many students viewed but not passed
        query0 = "SELECT COUNT(*) FROM %s WHERE grade != '%s' AND status = '%s'" % (tablename, "0.0", "notpassing")
        view_notpass = self.sql_query_size(query0)
        self.log("meta data", "  Count of %s: %s" % ("Viewed but Notpassing", view_notpass))
        
        pass_samplesize = int(float(self.sample_size) * float(self.pass_ratio))
        view_notpass_samplesize = int(self.sample_size) - pass_samplesize
        
        if table_size <= int(self.sample_size):
            self.log("info", "Row count of %s is equal or less than sample size. No more work to be done." % tablename)
            return False
        
        # Create subset table if not exist
        new_tablename = tablename + "_sample"
        self.sql_create_subset_table(new_tablename, tablename)
        
        # Do nothing if the table already has records
        query0 = "SELECT COUNT(*) FROM " + new_tablename
        newtable_size = self.sql_query_size(query0)
        if newtable_size > 0:
            self.log("info", "%s already has %d records. No more work to be done." % (new_tablename, newtable_size))
            return False
        
        # Insert records of students passed
        query1 = "INSERT INTO %s SELECT * FROM %s WHERE status = '%s' ORDER BY RAND() LIMIT %d" % (new_tablename, tablename, "downloadable", pass_samplesize)
        # Insert records of students viewed but not passed
        query2 = "INSERT INTO %s SELECT * FROM %s WHERE status = '%s' AND grade <> '%s' ORDER BY RAND() LIMIT %d" % (new_tablename, tablename, "notpassing", "0.0", view_notpass_samplesize)
        self.cursor.execute(query1)
        self.cursor.execute(query2)
        return new_tablename
        
        
    def sql_create_subset_table(self, new_tablename, tablename):
        query = "CREATE TABLE IF NOT EXISTS " + new_tablename 
        query += " AS SELECT * FROM " + tablename + " WHERE 0"
        self.log("info", "%s exists now." % new_tablename)
        self.cursor.execute(query)
               
    def sql_create_table(self, tablename, columns):        
        query = "CREATE TABLE IF NOT EXISTS " + tablename + "_sample"
        query += " ("
        for column in columns:
            coltype = "varchar(255)"
            if column == "id":
                coltype = "int NOT NULL UNIQUE"
            if column == "key":
                column = "_key"
            if column == "state" or column == "content" or column == "meta":
                coltype = "longtext"
            if column == "goals" or column == "mailing_address":
                coltype = "text"
            query += column.replace("\n", "") + " " + coltype + ", "
        query += " xhash varchar(200) "
        query += ", UNIQUE (xhash)"
        query += " );"
        self.cursor.execute(query)            
                
    def sql_query_size(self, query):
        try:
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            return result[0]
        except:
            print "Error: unable to fetch data"


def name():
    return str("extractsample")


def status():
    return ExtractSample.inst.status


def runservice(course_id, sample_size, pass_ratio):
    return ExtractSample(course_id, sample_size, pass_ratio)

