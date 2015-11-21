#!/usr/bin/py


class Dependency(object):
    """ Data dependency for tables """

    def __init__(self, table, partition, location=None):
        self.table = table
        self.partition = partition
        self.location = location
        self.status = 0
