mapreduceTemplate
=================

Template for running mapreduce jobs against Cloudera CDH4.4

Examples:
  WordCount - Classic example of counting occurrence of words within multiple files.
  Hive2Text - An example of outputting the content of rows in an external Hive table into a text file.


1) How to use/build locally:
  - Clone the repository
  - Import the directory as a maven project into Eclipse 
  - Hack the examples into using local paths relative to the project root (as opposed to argument paths)

2) How to use/build for running on a hadoop cluster:
  - 1)
  - run: mvn assembly:assembly
  - copy (if necessary) the target/*-jar-with-dependencies.jar file to your hadoop box (where you'll be launching your job from)
  - as described in the examples, run hadoop jar <...-jar-with-dependencies.jar> <example class, eg: org.mapreduce.examples.WordCount> <input directory> <output directory>
