package com.finaxys;

import v13.Day;
import v13.MonothreadedSimulation;
import v13.Replay;
import v13.Simulation;
import v13.agents.ZIT;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


public class AtomHBaseIntegration {
  private static final Logger LOGGER = Logger.getLogger(AtomHBaseIntegration.class.getName());

  private static Simulation sim;
  private static AtomLogger logger = null;
  private static AtomHBConfiguration atomConf;

  private static long startTime = System.currentTimeMillis();

  public static void main(String args[]) throws Exception {

    atomConf = new AtomHBConfiguration();

    if (args.length > 0) {
      if ("-t".equals(args[0].toString()) || "-table".equals(args[0].toString())) {
        try {
          HBaseInjector injector = new HBaseInjector(atomConf);
          injector.createOutput();
          injector.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else if ("-r".equals(args[0].toString()) || atomConf.isReplay()) {
        if (atomConf.getReplaySource() == null || atomConf.getReplaySource().isEmpty()) {
          LOGGER.log(Level.INFO, "You have to specify the path of the file you want to replay.");
          return;
        } else {
          File f = new File(atomConf.getReplaySource());
          if (!f.exists()) {
            LOGGER.severe("Replay file source not found : " + atomConf.getReplaySource());
          }
          try {
            replay(f);
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
    } else {

      run();
    }


  }

  public static void replay(File file) throws Exception {
    Replay replay = new Replay(file.getAbsolutePath());

    //outSystem must be true, is value false
    initSim();
    replay.sim.setLogger(logger);
    replay.go();
    logger.close();
  }

  public static void run() throws Exception {
    initSim();
    startTime = System.currentTimeMillis();
    sim.run(Day.createEuroNEXT(atomConf.getTickOpening(), atomConf.getTickContinuous(), atomConf.getTickClosing())
        , atomConf.getDays());
    closeSim();
  }

  public static void initSim() throws Exception {
    // Create simulator with custom logger
    // Simulation sim = new MultithreadedSimulation();
    sim = new MonothreadedSimulation();
    boolean trace = atomConf.isOutSystem();
    if (atomConf.isOutHbase()) {
      logger = new AtomLogger(atomConf, new HBaseInjector(atomConf));
    } else {
      logger = new AtomLogger(atomConf);
    }

    sim.setLogger(logger);

    LOGGER.log(Level.INFO, "Setting up agents and orderbooks");

    // Create Agents and Order book to MarketMaker depending properties
    boolean marketmaker = System.getProperty("atom.marketmaker", "true").equals("true");

    for (String agent : atomConf.getAgents())
      sim.addNewAgent(new ZIT(agent, Integer.parseInt(System.getProperty("simul. .cash", "0")),
          Integer.parseInt(System.getProperty("simul.agent.minprice", "10000")),
          Integer.parseInt(System.getProperty("simul.agent.maxprice", "20000")),
          Integer.parseInt(System.getProperty("simul.agent.minquantity", "10")),
          Integer.parseInt(System.getProperty("simul.agent.maxquantity", "50"))));

    List<AgentReferentialLine> agLines = new ArrayList<AgentReferentialLine>();
    int idCount = 0;

    for (String agent : atomConf.getAgents()) {
      agLines.add(new AgentReferentialLine(++idCount, agent));
    }
    if (marketmaker) {
      agLines.add(new AgentReferentialLine(++idCount, "mm"));
    }
    for (String orderBook : atomConf.getOrderBooks()) {
      if (marketmaker) {
        sim.addNewMarketMaker(orderBook);
      } else {
        sim.addNewOrderBook(orderBook);
      }
    }
    LOGGER.log(Level.INFO, Arrays.toString(agLines.toArray(new AgentReferentialLine[agLines.size()])));
    LOGGER.log(Level.INFO, "Is sending agent referential...");
    if (atomConf.isOutHbase()) {
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