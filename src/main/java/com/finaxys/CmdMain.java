package com.finaxys;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnectionManager;

/**
 *
 */
public class CmdMain
{

    public static void main(String args[]) throws Exception
    {
        AtomHBaseIntegration.getConfiguration();

        try
        {
            createTable();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public static void createTable() throws Exception
    {
        Configuration conf = HBaseLogger.createConfiguration();
        HBaseLogger.createTable("trace", "cf", HConnectionManager.createConnection(conf));
    }


}
