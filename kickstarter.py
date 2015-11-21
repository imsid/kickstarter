#!/usr/bin/env python

from classes.table import Table
from classes.dependency import Dependency
from classes.hdfsstat import HDFSStat

import multiprocessing
import json
import sys
import time
import logging
import datetime

TABLE_CONFIG_JSON='conf/table_configs.json'
SLEEP_INTERVAL = 5
SUSPEND_LIMIT = 20

h_stat = HDFSStat()
table_chain = []
running_chain = []
suspend_chain = []

def read_config():

  """ Read config file and create a chain of table objects with their dependencies """
  
  with open(TABLE_CONFIG_JSON,"r") as query:
    try:
      table_list = json.loads(query.read())
      for table in table_list['tables']:
        job = table_list['tables'][table]['chronos_job']
        location = table_list['tables'][table]['location'] if 'location' in table_list['tables'][table] else None
        t = Table(table, job, location)
        for dependency in table_list['tables'][table]['dependencies']:
          dependency_location = dependency['location'] if 'location' in dependency else None
          d = Dependency(dependency['name'], dependency['partition'], dependency_location)
          t.add_dependency(d)
        table_chain.append(t)
    except ValueError as err:
      sys.stderr.write("[JSON Error] : " +str(err) + "\n")
      sys.exit(1)

def display_status():

  """ Print the current status of all tables within the table chain """
  
  for t in table_chain:
    print t.__dict__


def check_status():
  
  """ Check and display status of running jobs """
  
  for idx, table in enumerate(running_chain[:]):
    print "Verifying: " +table.name
    latest_partition = h_stat.latest_partition(table.name,table.location)
    table.status = 2 if latest_partition == table.next_partition else 1
  display_status()

# Iterate through jobs in the table_chain and send to executor as the dependencies are satisfied
# Runs until the table chain is empty sleeping every n seconds
# Moves tables to suspend state if th elapsed time reaches the suspend limit
# Verify and display status of all tables periodically
def kickstart():
  
  """ Schedule and manage tables specified in config """
  
  table_op = list(table_chain)
  while table_op:
    check_status()
    print 'Number of tables yet to run: {0}'.format(str(len(table_op)))
    for idx,table in enumerate(table_op[:]):
      print 'Attempting to run: {table}'.format(table=table.name)
      total_dependencies = len(table.dependencies)
      latest_partition = h_stat.latest_partition(table.name,table.location)
      next_partition = (datetime.datetime.strptime(latest_partition, "%Y-%m-%d") + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
      
      #Set few more table properties
      table.next_partition = next_partition
      if table.start_time is None:
        table.start_time = time.time()
       
      print "Latest partition available for {table} : {partition} ".format(table=table.name, partition=latest_partition)
      print "Number of dependencies for {table} : {dep}".format(table=table.name, dep=total_dependencies)
      met_dependencies = 0
      for ix, dependency in enumerate(table.dependencies):
        print "Checking Dependency {n} : {table} for {next_partition}". format(n=str(ix + 1), table=dependency.table, next_partition=next_partition)
        if h_stat.poke_partition(dependency.table, dependency.partition, next_partition, dependency.location):
          print "Dependency {n} : {table} satisified for {next_partition}".format(n=str(ix + 1), table=dependency.table, next_partition=next_partition)
          dependency.status = 1
          met_dependencies += 1
        print "Total number of dependencies satisifed for {table} : {d}".format(table=table.name, d=str(met_dependencies))
      
      # Move table to running chain if dependencies are satsified. Set status to 1
      if total_dependencies == met_dependencies:
        table_op.remove(table)
        running_chain.append(table)
        table.status = 1
        p = multiprocessing.Process(name=table.name, target=execute, args=(table,))
        p.start()
        p.join()

      # Move table to suspend chain if suspend limit is reached. Set status to -1
      if time.time() - table.start_time > SUSPEND_LIMIT:
        print "Table {table} reached suspend limit".format(table=table.name)
        table_op.remove(table)
        suspend_chain.append(table)
        table.status = -1
    print "Sleeping " +str(SLEEP_INTERVAL)+ " secs before checking again.."
    time.sleep(SLEEP_INTERVAL)
  check_status()


def execute(t):
  
  """ Executor method that runs the job """
  
  print '\nStarting: ', multiprocessing.current_process().name
  print 'Executing job: ' +t.job

if __name__ == '__main__':
  read_config()
  kickstart()
