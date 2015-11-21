# kickstarter
* What?
  * Light weight dependency mangement tool for scheduling jobs that create immutable tables with immutable partitions

* Why?
  * In a ideal data warehouse where data is stored in immutable partitions every table has a bunch of dependencies on other immutable tables. Kickstarter enables such tables to automatically get triggered as these dependencies are satisfied.
  
* Requirements
  * https://github.com/spotify/snakebite 

