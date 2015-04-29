# Atom-Hbase-Integration

## Dependencies

atom.jar (v13) -> http://atom.univ-lille1.fr/ at the base of the project
Maven will take care of the rest

## Launch

- Go to folder containing : atom.jar / Atom-HBase-Integration-1.0-SNAPSHOT-jar-with-dependencies.jar / properties.xml
- Modify properties.txt depending on your need
  - Set the path of the config files of hbase : hbase-site.xml | hdfs-site.xml | core-site.xml
  - Orderbooks and agents are set by symbol. See DOW30 example to create your own liste
- Launch application with this command:

  > java -cp "atom.jar:Atom-HBase-Integration-1.0-SNAPSHOT-jar-with-dependencies.jar" AtomHBaseIntegration

The system will then output the time it took to complete and the some basic informations about HBase.

You cannot have both an outputfile and a standard output. (Yet you can with HBase and an other output)
