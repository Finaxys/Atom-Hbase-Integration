# Atom-Hbase-Integration

## Build

- atom.jar (v13) -> http://atom.univ-lille1.fr/ in an atom folder in the base of the project
- Do mvn install:install-file -Dfile=atom/atom.jar -DgroupId=atom -DartifactId=atom -Dversion=1.0 -Dpackaging=jar to install atom dependencies.
- Maven configuration :

  > mvn clean compile assembly:single

## Launch

- Go to folder containing : atom.jar / Atom-HBase-Integration-1.0-SNAPSHOT-jar-with-dependencies.jar / properties.xml
- Modify properties.txt depending on your need
  - Set the path of the config files of hbase : hbase-site.xml | hdfs-site.xml | core-site.xml
  - Orderbooks and agents are set by symbol. See DOW30 example to create your own list
- Launch application with this command:

  > java -jar Atom-HBase-Integration-1.0-SNAPSHOT-jar-with-dependencies.jar

- Or with this one if you only want to create the table :

  > java -jar Atom-HBase-Integration-1.0-SNAPSHOT-jar-with-dependencies.jar -t
  
- You can also do this command if you want to replay an order file :

  > java -jar Atom-HBase-Integration-1.0-SNAPSHOT-jar-with-dependencies.jar -r pathtoyourfile

The system will then output the time it took to complete and the some basic informations about HBase.

You cannot have both an outputfile and a standard output. (Yet you can with HBase and an other output)
