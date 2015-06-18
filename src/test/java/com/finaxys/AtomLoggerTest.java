package com.finaxys;

import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.testng.Assert;

@Test
public class AtomLoggerTest
{
  private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(AtomLogger.class.getName());

  @Test
  public void testReturnOfCoreSite() throws Exception
  {
    FileInputStream propFile = new FileInputStream("properties.txt");
    Properties p = new Properties(System.getProperties());
    p.load(propFile);
    System.setProperties(p);
    String coreSitePath = System.getProperty("hbase.conf.core", "core-site.xml");
    String test = new File(System.getProperty("hbase.conf.core", "core-site.xml")).getAbsoluteFile().toURI().toURL().toString();
    LOGGER.info("test = " + test);
    LOGGER.info(coreSitePath);
    Assert.assertEquals(coreSitePath, "/home/sr-readonly/cluster/conf/core-site.xml");
  }
}
