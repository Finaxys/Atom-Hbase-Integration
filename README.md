# Atom-Hbase-Integration

## Launch

- Go to folder containing : atom.jar / Atom-HBase-Integration-1.0-SNAPSHOT-jar-with-dependencies.jar / properties.xml
- Modify properties.xml depending on your need
  - Set the path of the config files of hbase : hbase-site.xml | hdfs-site.xml | core-site.xml
- Launch application with this command:

  > java -cp "atom.jar:Atom-HBase-Integration-1.0-SNAPSHOT-jar-with-dependencies.jar" AtomHBaseIntegration

The system will then output the time it took to complete and the some basic informations about HBase.

You cannot have both an outputfile and a standard output. (Yet you can with HBase and an other output)