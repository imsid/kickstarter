#!/usr/bin/py


from dependency import Dependency


class Table(object):
    """ A table definition to run in kickstarter.
        Table properties include:
        - One or more data dependencies.
        - Chronos command to execute
        - Status
           0  : Yet to run
           1  : Ready to run
           2  : Completed
           -1 : Suspended
    """


    def __init__(self, name, job, location=None):
        self.name = name
        self.dependencies = []
        self.job = job
        self.location = location
        self.next_partition = ''
        self.start_time = None
        self.status = 0


    def add_dependency(self, dependency):
        self.dependencies.append(dependency)
