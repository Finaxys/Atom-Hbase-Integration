package com.finaxys;

import v13.Day;
import v13.MonothreadedSimulation;
import v13.Replay;
import v13.Simulation;
import v13.agents.ZIT;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;


public class AtomHBaseIntegration {
  private static final Logger LOGGER = Logger.getLogger(AtomHBaseIntegration.class.getName());
  // Static infos
  static public final String[] DOW2 = {"MMM", "AXP"};
  static public final String[] DOW30 = {"MMM", "AXP", "AAPL", "BA", "CAT", "CVX", "CSCO", "KO", "DIS", "DD", "XOM", "GE", "GS", "HD", "IBM", "INTC", "JNJ", "JPM", "MCD", "MRK", "MSFT", "NKE", "PFE", "PG", "TRV", "UTX", "UNH", "VZ", "V", "WMT"};
  static private List<String> orderBooks;
  static private List<String> agents;

  private static String tableName;
  private static String cfName;
  private static boolean outHbase;
  private static String outFile;
  private static boolean outSystem;
  private static int dayGap;
  private static long startTime;
  private static Simulation sim;
  private static HBaseLogger logger = null;

  public static Simulation getSim() {
    return sim;
  }

  // Main config for Atom
  public static void main(String args[]) {
    String[] str = {"-r", "/home/heraul_m/outputAtom.out"};
    //args = str;

    if (args.length > 0) {
      if ("-t".equals(args[0].toString()) || "-table".equals(args[0].toString())) {
        try {
          initTableAndCfName();
          logger = new HBaseLogger(tableName, cfName);
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          return;
        }
      } else if ("-r".equals(args[0].toString()) || "-replay".equals(args[0].toString())) {
        if (args.length <= 1) {
          LOGGER.log(Level.INFO, "You have to specify the path of the file you want to replay.");
          return;
        } else {
          try {
            replay(args[1]);
            //LOGGER.info("args1 = " + args[1]);
          } catch (Exception e) {
            e.printStackTrace();
          }
          return;
        }
      } else {
        LOGGER.log(Level.SEVERE, "Arguments not recognized");
        return;
      }
    }
    run();
  }

  public static void replay(String pathFile) throws Exception {
    File file = new File(pathFile);
    if (file.exists()) {
      Replay replay = new Replay(pathFile);

      //outSystem must be true, is value false
      initSim();
      replay.sim.setLogger(logger);
      //replay.sim.setLogger(new HBaseLogger(OutputType.Both, System.out, tableName, cfName, dayGap));
      //LOGGER.info("curentdayreplay = " + replay.sim.currentDay);
      //replay.sim = sim;

      replay.go();
      //closeSim();
      logger.close();
    } else {
      LOGGER.severe("File does not exist");
    }
  }

  public static void getConfiguration() throws Exception {
    FileInputStream propFile = new FileInputStream("properties.txt");
    Properties p = new Properties(System.getProperties());
    p.load(propFile);
    System.setProperties(p);

    // Get agents & orderbooks
    String obsym = System.getProperty("atom.orderbooks", "");
    LOGGER.info("obsym = " + obsym);
    assert obsym != null;
    String agsym = System.getProperty("atom.agents", "");
    assert agsym != null;

    agents = Arrays.asList(System.getProperty("symbols.agents." + agsym, "").split("\\s*,\\s*"));
    orderBooks = Arrays.asList(System.getProperty("symbols.orderbooks." + obsym, "").split("\\s*,\\s*"));

    if (agents.isEmpty() || orderBooks.isEmpty()) {
      LOGGER.log(Level.SEVERE, "Agents/Orderbooks not set");
      throw new Exception("agents/orderbooks");
    }

    tableName = System.getProperty("hbase.table", "trace");
    assert tableName != null;
    cfName = System.getProperty("hbase.cf", "cf");
    assert cfName != null;
    outHbase = System.getProperty("simul.output.hbase", "true").equals("true");
    outFile = System.getProperty("simul.output.file", "");
    assert outFile != null;
    outSystem = System.getProperty("simul.output.standard", "false").equals("false");
    dayGap = Integer.parseInt(System.getProperty("simul.day.startDay", "1")) - 1;

    // How long
    startTime = System.currentTimeMillis();
  }

  private static void initTableAndCfName() throws Exception {
    FileInputStream propFile = new FileInputStream("properties.txt");
    Properties p = new Properties(System.getProperties());
    p.load(propFile);
    System.setProperties(p);
    tableName = System.getProperty("hbase.table", "trace");
    assert tableName != null;
    cfName = System.getProperty("hbase.cf", "cf");
    assert cfName != null;
  }

  public static void initHbaseLogger() {
    try {
      if (outHbase) {
        if (outSystem)
          logger = new HBaseLogger(OutputType.Both, System.out, tableName, cfName, dayGap);
        else if (!outFile.equals(""))
          logger = new HBaseLogger(OutputType.Both, outFile, tableName, cfName, dayGap);
        else
          logger = new HBaseLogger(tableName, cfName, dayGap);
      } else if (outSystem)
        logger = new HBaseLogger(OutputType.Other, System.out, tableName, cfName, dayGap);
      else if (!outFile.equals(""))
        logger = new HBaseLogger(OutputType.Other, outFile, tableName, cfName, dayGap);
      else {
        LOGGER.log(Level.SEVERE, "Config file must have at least one output");
        return;
      }
    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, "Could not init logger", e);
      return;
    }
  }

  public static void run() {
    initSim();

    sim.run(Day.createEuroNEXT(Integer.parseInt(System.getProperty("simul.tick.opening", "0")),
            Integer.parseInt(System.getProperty("simul.tick.continuous", "10")),
            Integer.parseInt(System.getProperty("simul.tick.closing", "0"))),
        Integer.parseInt(System.getProperty("simul.days", "1")));

    closeSim();
  }

  public static void initSim() {
    // Loading properties
    try {
      getConfiguration();
    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, "Could not load properties", e);
      return;
    }

    // Create simulator with custom logger
    // Simulation sim = new MultithreadedSimulation();
    sim = new MonothreadedSimulation();

    initHbaseLogger();
    sim.setLogger(logger);

    LOGGER.log(Level.INFO, "Setting up agents and orderbooks");

    // Create Agents and Order book to MarketMaker depending properties
    boolean marketmaker = System.getProperty("atom.marketmaker", "true").equals("true");

    for (String agent : agents)
      sim.addNewAgent(new ZIT(agent, Integer.parseInt(System.getProperty("simul. .cash", "0")),
          Integer.parseInt(System.getProperty("simul.agent.minprice", "10000")),
          Integer.parseInt(System.getProperty("simul.agent.maxprice", "20000")),
          Integer.parseInt(System.getProperty("simul.agent.minquantity", "10")),
          Integer.parseInt(System.getProperty("simul.agent.maxquantity", "50"))));

    List<AgentReferentialLine> agLines = new ArrayList<AgentReferentialLine>();
    int idCount = 0;

    for (String agent : agents) {
      agLines.add(new AgentReferentialLine(++idCount, agent));
    }
    if (marketmaker) {
      agLines.add(new AgentReferentialLine(++idCount, "mm"));
    }
    for (String orderBook : orderBooks) {
      if (marketmaker) {
        sim.addNewMarketMaker(orderBook);
      } else {
        sim.addNewOrderBook(orderBook);
      }
    }
    LOGGER.log(Level.INFO, Arrays.toString(agLines.toArray(new AgentReferentialLine[agLines.size()])));
    LOGGER.log(Level.INFO, "Is sending agent referential...");
    if (outHbase) {
      //Send agent referential
      try {
        logger.agentReferential(agLines);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    LOGGER.log(Level.INFO, "Launching simulation");
    sim.currentDay = Integer.parseInt(System.getProperty("simul.startDay", "1"));
  }

  public static void closeSim() {
    LOGGER.log(Level.INFO, "Closing up");

    sim.market.close();

    try {
      logger.close();
    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, "Could not close logger", e);
      return;
    }

    long estimatedTime = System.currentTimeMillis() - startTime;
    LOGGER.info("Elapsed time: " + estimatedTime / 1000 + "s");
  }
}