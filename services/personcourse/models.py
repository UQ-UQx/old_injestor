import MySQLdb

class PCModel(object):

    def __init__(self, course_id, user_id, registered=1, viewed=0, explored=0, certified=0, final_cc_cname="",
                 LoE="", YoB=None, gender="", grade=0, start_time="", last_event="", nevents=0, ndays_act=0,
                 nplay_video=0, nchapters=0, nforum_posts=0, roles="", inconsistent_flag=0):
        self.course_id = course_id
        # todo
        self.user_id = user_id
        self.registered = registered
        self.viewed = viewed
        self.explored = explored
        self.certified = certified
        self.final_cc_cname = final_cc_cname
        self.LoE = LoE
        self.YoB = YoB
        self.gender = gender
        self.grade = grade
        self.start_time = start_time
        self.last_event = last_event
        self.nevents = nevents
        self.ndays_act = ndays_act
        self.nplay_video = nplay_video
        self.nchapters = nchapters
        self.nforum_posts = nforum_posts
        self.roles = roles
        self.inconsistent_flag = inconsistent_flag
        pass

    def set_viewed(self, viewed):
        self.viewed = viewed

    def set_explored(self, explored):
        self.explored = explored

    def set_certified(self, status):
        if status == 'downloadable':
            self.certified = 1
        else:
            self.certified = 0

    def set_final_cc_cname(self, final_cc_cname):
        countries = [x for x in final_cc_cname if x is not None]
        self.final_cc_cname = ",".join(countries)

    def set_LoE(self, LoE):
        if LoE == "NULL":
            self.LoE = ""
        else:
            self.LoE = LoE

    def set_YoB(self, YoB):
        if YoB == "NULL":
            self.YoB = ""
        else:
            self.YoB = YoB

    def set_gender(self, gender):
        if gender == "NULL":
            self.gender = ""
        else:
            self.gender = gender

    def set_grade(self, grade):
        self.grade = grade

    def set_start_time(self, start_time):
        self.start_time = start_time

    def set_last_event(self, last_event):
        self.last_event = last_event

    def set_nevents(self, nevents):
        self.nevents = nevents

    def set_ndays_act(self, ndays_act):
        self.ndays_act = ndays_act

    def set_nplay_video(self, nplay_video):
        self.nplay_video = nplay_video

    def set_nchapters(self, nchapters):
        self.nchapters = nchapters

    def set_nforum_posts(self, nforum_posts):
        self.nforum_posts = nforum_posts

    def set_roles(self, roles):
        self.roles = roles

    def set_inconsistent_flag(self):
        if self.nevents == 0 and (self.ndays_act + self.nplay_video + self.nchapters + self.nforum_posts) > 0:
            self.inconsistent_flag = 1
        else:
            self.inconsistent_flag = 0

    def save2db(self, cursor, table):
        parameters = table, self.course_id, self.user_id, self.registered, self.viewed, self.explored, self.certified, self.final_cc_cname, self.LoE, self.YoB, self.gender, self.grade, self.start_time, self.last_event, self.nevents, self.ndays_act, self.nplay_video, self.nchapters, self.nforum_posts, self.roles, self.inconsistent_flag
        #print parameters

        query = "INSERT INTO %s (course_id, user_id, registered, viewed, explored, certified, final_cc_cname, LoE, YoB, gender, grade, start_time, last_event, nevents, ndays_act, nplay_video, nchapters, nforum_posts, roles, inconsistent_flag) VALUES ('%s', '%s', %d, %d, %d, %d, '%s', '%s', '%s', '%s', %f, '%s', '%s', %d, %d, %d, %d, %d, '%s', %d)" % parameters
        cursor.execute(query)

    def __repr__(self):
        result = ""
        result += "course_id: " + str(self.course_id) + ", "
        result += "user_id: " + str(self.user_id) + ", "
        result += "registered " + str(self.registered) + ", "
        result += "viewed " + str(self.viewed) + ", "
        result += "explored " + str(self.explored) + ", "
        result += "certified " + str(self.certified) + ", "
        result += "final_cc_cname " + str(self.final_cc_cname) + ", "
        result += "LoE " + str(self.LoE) + ", "
        result += "YoB " + str(self.YoB) + ", "
        result += "gender " + str(self.gender) + ", "
        result += "grade " + str(self.grade) + ", "
        result += "start_time " + str(self.start_time) + ", "
        result += "last_event " + str(self.last_event) + ", "
        result += "nevents " + str(self.nevents) + ", "
        result += "ndays_act " + str(self.ndays_act) + ", "
        result += "nplay_video " + str(self.nplay_video) + ", "
        result += "nchapters " + str(self.nchapters) + ", "
        result += "nforum_posts " + str(self.nforum_posts) + ", "
        result += "roles " + str(self.roles) + ", "
        result += "inconsistent_flag " + str(self.inconsistent_flag) + ", "
        #result += " " + str(self.) + ", "
        return result




