package com.finaxys;

import com.sun.istack.NotNull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import v13.Day;
import v13.LimitOrder;
import v13.Order;
import v13.OrderBook;
import v13.PriceRecord;
import v13.agents.Agent;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 *
 */
public class HBaseInjector implements AtomDataInjector {
  private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(HBaseInjector.class.getName());

  AtomicLong idGen = new AtomicLong(1_000_000);
  //Confs
  private final AtomHBConfiguration atomConf;
  private final Configuration hbConf;
  //MThreads
  private ExecutorService eService;

  private final ArrayBlockingQueue<Put> queue;

  final HBaseDataTypeEncoder hbEncoder = new HBaseDataTypeEncoder();
  private TimeStampBuilder tsb;

  private final AtomicBoolean isClosing = new AtomicBoolean(false);
  //Props
  private final byte[] cfall;

  private final AtomicLong globalCount = new AtomicLong(0L);

  public HBaseInjector(@NotNull AtomHBConfiguration conf) throws Exception {
    this.atomConf = conf;
    this.queue = new ArrayBlockingQueue<>(atomConf.getBufferSize());
    this.cfall = conf.getColumnFamily();
    this.hbConf = createHbaseConfiguration();

    this.tsb = new TimeStampBuilder();
    this.tsb.loadConfig(atomConf);
    this.tsb.init();
    initWorkers();
  }

  private void initWorkers() throws IOException {
    int worker = atomConf.getWorker();
    eService = Executors.newFixedThreadPool(worker);
    for (int i = 1; i <= worker; i++) {
      eService.submit(new HBWorker(queue, createHTableConnexion(atomConf.getTableName(), this.hbConf),
          atomConf.getFlushRatio(), i, isClosing, globalCount));
    }
  }

  private HTable createHTableConnexion(String tableName, Configuration hbConf) throws IOException {
    HTable table = new HTable(hbConf, tableName);
    // AutoFlushing
    table.setAutoFlushTo(atomConf.isAutoFlush());
    return table;
  }

  @Override
  public void createOutput() throws Exception {
    assert !atomConf.getTableName().isEmpty();
    String port = hbConf.get("hbase.zookeeper.property.clientPort");
    String host = hbConf.get("hbase.zookeeper.quorum");

    LOGGER.log(Level.INFO, "Try to connect to " + host + ":" + port);

    LOGGER.log(Level.INFO, "Configuration completed");
    HConnection connection = HConnectionManager.createConnection(hbConf);
    try {
      createTable(connection);
    } catch (IOException e) {
      LOGGER.log(Level.SEVERE, "Could not create Connection", e);
      throw new Exception("hbase connection", e);
    } finally {
      connection.close();
    }
  }

  private void createTable(HConnection connection) throws
      Exception {
    HBaseAdmin admin = null;
    try {
      admin = new HBaseAdmin(connection);
    } catch (MasterNotRunningException e) {
      LOGGER.log(Level.SEVERE, "Master server not running", e);
      throw new Exception("hbase master server", e);
    } catch (ZooKeeperConnectionException e) {
      LOGGER.log(Level.SEVERE, "Could not connect to ZooKeeper", e);
      throw new Exception("zookeeper", e);
    }

    if (admin.tableExists(atomConf.getTableName())) {
      LOGGER.log(Level.INFO, atomConf.getTableName() + " already exists");
      return;
    }

    HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(atomConf.getTableName()));
    try {
      LOGGER.log(Level.INFO, "Creating table");
      LOGGER.log(Level.INFO, admin.getClusterStatus().toString());

      tableDescriptor.addFamily(new HColumnDescriptor(atomConf.getColumnFamily()));
      admin.createTable(tableDescriptor);
      LOGGER.log(Level.INFO, "Table Created");
    } catch (IOException e) //ajouter exception spécique à la non création de table
    {
      LOGGER.log(Level.FINEST, "Table already created");
    }
  }

  @Override
  public void sendPriceRecord(PriceRecord pr, long bestAskPrice, long bestBidPrice) {
    long ts = System.currentTimeMillis() + 2L; //hack for update on scaledrisk (does not manage put then update with same ts)
    pr.timestamp = tsb.nextTimeStamp();

    Put p = new Put(Bytes.toBytes(createRequired("P")), ts);
    p.add(cfall, Bytes.toBytes("obName"), ts, hbEncoder.encodeString(pr.obName));
    p.add(cfall, Bytes.toBytes("price"), ts, hbEncoder.encodeLong(pr.price));
    p.add(cfall, Bytes.toBytes("executedQuty"), ts, hbEncoder.encodeInt(pr.quantity));
    p.add(cfall, Bytes.toBytes("dir"), ts, hbEncoder.encodeChar(pr.dir));
    p.add(cfall, Bytes.toBytes("order1"), ts, hbEncoder.encodeString(pr.extId1));
    p.add(cfall, Bytes.toBytes("order2"), ts, hbEncoder.encodeString(pr.extId2));
    p.add(cfall, Bytes.toBytes("bestask"), ts, hbEncoder.encodeLong(bestAskPrice));
    p.add(cfall, Bytes.toBytes("bestbid"), ts, hbEncoder.encodeLong(bestBidPrice));
    p.add(cfall, Bytes.toBytes("timestamp"), ts, hbEncoder.encodeLong((pr.timestamp > 0 ? pr.timestamp : ts))); // tsb.nextTimeStamp()

    putTable(p);
  }

  @Override
  public void sendAgent(Agent a, Order o, PriceRecord pr) {
    Put p = new Put(Bytes.toBytes(createRequired("A")));
    p.add(cfall, Bytes.toBytes("agentName"), hbEncoder.encodeString(a.name));
    p.add(cfall, Bytes.toBytes("orderBookName"), hbEncoder.encodeString(o.obName));
    p.add(cfall, Bytes.toBytes("cash"), hbEncoder.encodeLong(a.cash));
    p.add(cfall, Bytes.toBytes("executed"), hbEncoder.encodeInt(pr.quantity));
    p.add(cfall, Bytes.toBytes("price"), hbEncoder.encodeLong(pr.price));
    if (o.getClass().equals(LimitOrder.class)) {
      p.add(cfall, Bytes.toBytes("direction"), hbEncoder.encodeChar(((LimitOrder) o).direction));
      p.add(cfall, Bytes.toBytes("timestamp"), hbEncoder.encodeLong(pr.timestamp)); //pr.timestamp
      p.add(cfall, Bytes.toBytes("orderExtId"), hbEncoder.encodeString(o.extId));
    }
    putTable(p);
  }

  @Override
  public void sendOrder(Order o) {
    o.timestamp = tsb.nextTimeStamp();
    long ts = System.currentTimeMillis(); //hack for update on scaledrisk (does not manage put then update with same ts)
    Put p = new Put(Bytes.toBytes(createRequired("O")), ts);
    p.add(cfall, Bytes.toBytes("orderBookName"), hbEncoder.encodeString(o.obName));
    p.add(cfall, Bytes.toBytes("sender"), hbEncoder.encodeString(o.sender.name));
    p.add(cfall, Bytes.toBytes("extId"), hbEncoder.encodeString(o.extId));
    p.add(cfall, Bytes.toBytes("type"), hbEncoder.encodeChar(o.type));
    p.add(cfall, Bytes.toBytes("id"), hbEncoder.encodeLong(o.id));
    p.add(cfall, Bytes.toBytes("timestamp"), hbEncoder.encodeLong(o.timestamp)); //o.timestamp

    Date d = new Date(tsb.getTimeStamp());
    if (o.getClass().equals(LimitOrder.class)) {
      LimitOrder lo = (LimitOrder) o;
      p.add(cfall, Bytes.toBytes("quantity"), hbEncoder.encodeInt(lo.quantity));
      p.add(cfall, Bytes.toBytes("direction"), hbEncoder.encodeChar(lo.direction));
      p.add(cfall, Bytes.toBytes("price"), hbEncoder.encodeLong(lo.price));
      p.add(cfall, Bytes.toBytes("validity"), hbEncoder.encodeLong(lo.validity));
    }
    putTable(p);
  }

  @Override
  public void sendTick(Day day, Collection<OrderBook> orderbooks) {
    for (OrderBook ob : orderbooks) {

      tsb.setCurrentTick(day.currentTick());
      tsb.setTimeStamp(tsb.baseTimeStampForCurrentTick());

      Put p = new Put(Bytes.toBytes(createRequired("T")));
      p.add(cfall, Bytes.toBytes("numTick"), hbEncoder.encodeInt(day.currentTick()));
      p.add(cfall, Bytes.toBytes("numDay"), hbEncoder.encodeInt(day.number + atomConf.getDayGap()));
      p.add(cfall, Bytes.toBytes("orderBookName"), hbEncoder.encodeString(ob.obName));
      if (!ob.ask.isEmpty())
        p.add(cfall, Bytes.toBytes("bestAsk"), hbEncoder.encodeLong(ob.ask.last().price));

      if (!ob.bid.isEmpty())
        p.add(cfall, Bytes.toBytes("bestBid"), hbEncoder.encodeLong(ob.bid.last().price));

      if (ob.lastFixedPrice != null)
        p.add(cfall, Bytes.toBytes("lastFixedPrice"), hbEncoder.encodeLong(ob.lastFixedPrice.price));

      putTable(p);

    }
  }

  @Override
  public void sendExec(Order o) {
    Put p = new Put(Bytes.toBytes(createRequired("E")));
    p.add(cfall, Bytes.toBytes("sender"), hbEncoder.encodeString(o.sender.name));
    p.add(cfall, Bytes.toBytes("extId"), hbEncoder.encodeString(o.extId));
    putTable(p);
  }

  @Override
  public void sendDay(int nbDays, Collection<OrderBook> orderbooks) {
    tsb.setCurrentDay(nbDays);
    for (OrderBook ob : orderbooks) {
      Put p = new Put(Bytes.toBytes(createRequired("D")));
      p.add(cfall, Bytes.toBytes("NumDay"), hbEncoder.encodeInt(nbDays + atomConf.getDayGap()));
      p.add(cfall, Bytes.toBytes("orderBookName"), hbEncoder.encodeString(ob.obName));
      p.add(cfall, Bytes.toBytes("FirstFixedPrice"), hbEncoder.encodeLong(ob.firstPriceOfDay));
      p.add(cfall, Bytes.toBytes("LowestPrice"), hbEncoder.encodeLong(ob.lowestPriceOfDay));
      p.add(cfall, Bytes.toBytes("HighestPrice"), hbEncoder.encodeLong(ob.highestPriceOfDay));
      long price = 0;
      if (ob.lastFixedPrice != null)
        price = ob.lastFixedPrice.price;
      p.add(cfall, Bytes.toBytes("LastFixedPrice"), hbEncoder.encodeLong(price));
      p.add(cfall, Bytes.toBytes("nbPricesFixed"), hbEncoder.encodeLong(ob.numberOfPricesFixed));
      putTable(p);
    }
  }

  @Override
  public void sendAgentReferential(List<AgentReferentialLine> referencial) {
    HTable table = null;
    try {
      table = createHTableConnexion(atomConf.getTableName(), hbConf);
      for (AgentReferentialLine agent : referencial) {
        Put p = agent.toPut(hbEncoder, cfall, System.currentTimeMillis());
        table.put(p);
      }
      table.flushCommits();
      table.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void putTable(@NotNull Put p) {
    try {
      if (queue.size() > 0 && queue.size() % 1000 == 0) {
        LOGGER.info("Pending data size : " + queue.size());
      }
      queue.put(p);
    } catch (InterruptedException e) {
      LOGGER.severe("Faild to push data into queue : " + e.getMessage());
    }
  }

  @NotNull
  private String createRequired(@NotNull String name) {
    long rowKey = Long.reverseBytes(idGen.incrementAndGet());
    return String.valueOf(rowKey) + name;
  }

  private Configuration createHbaseConfiguration() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    try {
      String miniCluster = System.getProperty("hbase.conf.minicluster", "");
      if (!miniCluster.isEmpty())
        conf.addResource(new FileInputStream(miniCluster));
      else {
        conf.addResource(new File(System.getProperty("hbase.conf.core", "core-site.xml")).getAbsoluteFile().toURI().toURL());
        conf.addResource(new File(System.getProperty("hbase.conf.hbase", "hbase-site.xml")).getAbsoluteFile().toURI().toURL());
        conf.addResource(new File(System.getProperty("hbase.conf.hdfs", "hdfs-site.xml")).getAbsoluteFile().toURI().toURL());
      }
    } catch (MalformedURLException e) {
      LOGGER.log(Level.SEVERE, "Could not get hbase configuration files", e);
      throw new Exception("hbase", e);
    }
    conf.reloadConfiguration();
    return conf;
  }

  @Override
  public void close() {
    eService.shutdown();
    isClosing.set(true);
    try {
      while (!eService.awaitTermination(10L, TimeUnit.SECONDS)) {
        LOGGER.info("Await pool termination. Still " + queue.size() + " puts to proceed.");
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      LOGGER.info("Total put sent : " + globalCount.get());
    }
  }
}
