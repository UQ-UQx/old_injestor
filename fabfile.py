from fabric.api import *
from fabric.contrib.console import confirm

verbose = True
env.projectname = 'injestor'

env.local_base = '../..'
env.remote_base = '/var/uqxparser'
env.remote_code_dir = env.remote_base+'/src/'+env.projectname
env.gitname = "injestor"

env.hosts = ['tools']

def prepare():
    func_gitadd()
    func_gitpush()

def deploy():
    with hide('output', 'running', 'warnings'), settings(warn_only=True):
        remote_vc("%s/injestor.py stop" % env.remote_code_dir,"Stopping Injestor")

    with hide('output', 'running', 'warnings'), settings(warn_only=True):
        if run("test -d %s" % env.remote_code_dir).failed:
            print "ERROR: NOT CLONED YET"
            sudo("mkdir -p "+env.remote_code_dir, "Creating Directory",True)
            sudo("git clone https://uqxtechnical@github.com/UQ-UQx/"+env.gitname+".git %s" % env.remote_code_dir)
    with cd(env.remote_code_dir):
        remote_vc("git stash", "Resetting changes to remote",True)
        remote_vc("git reset --hard", "Resetting changes to remote",True)
        remote_vc("git pull", "Pulling from git",True)
        remote_vc("%s/injestor.py start" % env.remote_code_dir,"Starting Injestor")


def create():
    with hide('output', 'running', 'warnings'), settings(warn_only=True):
        if run("test -d %s" % env.remote_code_dir).failed:
            print "Source Directory does not exist: "+env.remote_code_dir

#Internal


def func_gitadd():
    git_message = prompt("[Local] Enter Git Commit Message: ","Hotfix")
    if git_message == "":
        git_message = "Anonymous Hotfix"
    local_ve("git add . && git commit -a -m \"" + git_message + "\"", "Git adding", True)

def func_gitpush():
    local_ve("git push", "Pushing to github")


#Helpers

def local_ve(cmd, message, ignoreerror=False):
    if verbose:
        print "[Local] Command: " + message
    with hide('running'), settings(warn_only=True):  #'output', 'running', 'warnings'
        result = local(cmd, capture=True)
        if not ignoreerror and result.failed and not confirm("+ Error: " + message + " failed. Continue anyway?"):
            abort("Aborting at user request.")

def remote_vc(cmd, message, showout=False):
    if verbose:
        print "["+env.host_string+"] Command: " + message
    with hide('running', 'warnings', 'output'), settings(warn_only=True):
        if showout == True:
            with show('output'):
                result = sudo(cmd)
                if result.failed and not confirm("+ Error: " + message + " failed. Continue anyway?"):
                    abort("Aborting at user request.")
        else:
            result = sudo(cmd)
            if result.failed and not confirm("+ Error: " + message + " failed. Continue anyway?"):
                abort("Aborting at user request.")
