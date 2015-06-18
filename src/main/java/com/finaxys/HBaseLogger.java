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
import v13.Logger;
import v13.Order;
import v13.OrderBook;
import v13.PriceRecord;
import v13.agents.Agent;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

class HBaseLogger extends Logger {
  private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(HBaseLogger.class.getName());
  private Configuration conf;
  private AtomicLong idTrace = new AtomicLong(0);

  private int countOrder;
  private int countExec;

  final HBaseDataTypeEncoder hbEncoder = new HBaseDataTypeEncoder();

  private OutputType output;

  private byte[] cfall;

  private final int dayGap;
  private long stackedPuts = 0;
  private long flushedPuts = 0;
  private boolean autoflush;
  private long stackPuts;

  private final String tableName;
  //TODO params
  int bufferSize = 10_000;
  int flushRatio = 10_000;
  int worker = 10;
  private ExecutorService eService;

  private final ArrayBlockingQueue<Put> queue = new ArrayBlockingQueue<>(bufferSize);
  private TimeStampBuilder tsb;
  private int count = 0;
  private int countPrice = 0;

  public HBaseLogger(@NotNull OutputType output, @NotNull String filename, @NotNull String tableName,
                     @NotNull String cfName, int dayGap) throws Exception {
    super(filename);
    LOGGER.info("filename = " + filename);
    this.dayGap = dayGap;
    this.tableName = tableName;
    init(output, cfName, false);
  }

  public HBaseLogger(@NotNull OutputType output, @NotNull String filename, @NotNull String tableName,
                     @NotNull String cfName, int dayGap, boolean createConnectionOnly) throws Exception {
    super(filename);
    LOGGER.info("filename = " + filename);
    this.dayGap = dayGap;
    this.tableName = tableName;

    init(output, cfName, createConnectionOnly);
  }

  public HBaseLogger(@NotNull OutputType output, @NotNull PrintStream o, @NotNull String tableName,
                     @NotNull String cfName, int dayGap) throws Exception {
    super(o);
    this.dayGap = dayGap;
    this.tableName = tableName;

    init(output, cfName, false);
  }

  public HBaseLogger(@NotNull String tableName, @NotNull String cfName, int dayGap) throws Exception {
    this.dayGap = dayGap;
    this.tableName = tableName;

    init(OutputType.HBase, cfName, false);
  }

  public HBaseLogger(@NotNull String tableName, @NotNull String cfName) throws Exception {
    this.dayGap = 0;
    this.tableName = tableName;

    createTableOnly(cfName);
  }

  private void initWorkers() throws IOException {
    eService = Executors.newFixedThreadPool(this.worker);
    for (int i = 1; i <= worker; i++) {
      eService.submit(new Worker(queue, createHTableConnexion(tableName), flushRatio, i));
    }
  }

  public void createTableOnly(@NotNull String cfName) throws Exception {
    assert !tableName.isEmpty();
    assert !cfName.isEmpty();
    cfall = Bytes.toBytes(cfName);
    Configuration conf = createConfiguration();
    LOGGER.log(Level.INFO, conf.get("hbase.zookeeper.property.clientPort"));
    LOGGER.log(Level.INFO, conf.get("hbase.zookeeper.quorum"));
    conf.reloadConfiguration();
    LOGGER.log(Level.INFO, "Configuration completed");
    try {
      HConnection connection = HConnectionManager.createConnection(conf);
      createTable(tableName, cfName, connection);
    } catch (IOException e) {
      LOGGER.log(Level.SEVERE, "Could not create Connection", e);
      throw new Exception("hbase connection", e);
    }
  }

  public void init(@NotNull OutputType output, @NotNull String cfName, boolean createTableOnly) throws
      Exception {
    tsb = new TimeStampBuilder();
    tsb.loadConfig();
    tsb.init();

    assert !tableName.isEmpty();
    assert !cfName.isEmpty();
    cfall = Bytes.toBytes(cfName);
    this.output = output;

    this.worker = Integer.parseInt(System.getProperty("simul.worker", "10"));
    this.flushRatio = Integer.parseInt(System.getProperty("simul.flushRatio", "1000"));
    this.bufferSize = Integer.parseInt(System.getProperty("simul.bufferSize", "10000"));

    if (output == OutputType.Other)
      return;

    autoflush = Boolean.parseBoolean(System.getProperty("hbase.autoflush", "false"));
    stackPuts = Integer.parseInt(System.getProperty("hbase.stackputs", "1000"));

    conf = createConfiguration();

    LOGGER.log(Level.INFO, conf.get("hbase.zookeeper.property.clientPort"));
    LOGGER.log(Level.INFO, conf.get("hbase.zookeeper.quorum"));

    conf.reloadConfiguration();

    try {
      HConnection connection = HConnectionManager.createConnection(conf);
      createTable(tableName, cfName, connection);
    } catch (IOException e) {
      LOGGER.log(Level.SEVERE, "Could not create Connection", e);
      throw new Exception("hbase connection", e);
    }

    if (createTableOnly) {
      return;
    }
    initWorkers();
    LOGGER.log(Level.INFO, "Configuration completed");
  }

  private HTable createHTableConnexion(String tableName) throws IOException {
    HTable table = new HTable(conf, tableName);
    // AutoFlushing
    table.setAutoFlushTo(autoflush);
    return table;
  }

  public static Configuration createConfiguration() throws Exception {
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
    return conf;
  }

  public static void createTable(String tableName, String cfName, HConnection connection) throws
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

    if (admin.tableExists(tableName)) {
      LOGGER.log(Level.INFO, tableName + " already exists");
      return;
    }

    HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

    try {
      LOGGER.log(Level.INFO, "Creating table");
      LOGGER.log(Level.INFO, admin.getClusterStatus().toString());

      tableDescriptor.addFamily(new HColumnDescriptor(cfName));
      admin.createTable(tableDescriptor);
      LOGGER.log(Level.INFO, "Table Created");
    } catch (IOException e) //ajouter exception spécique à la non création de table
    {
      LOGGER.log(Level.FINEST, "Table already created");
    }
  }

  public void agentReferential(@NotNull List<AgentReferentialLine> referencial) throws
      IOException {
    assert !referencial.isEmpty();
    HTable table = createHTableConnexion(tableName);
    for (AgentReferentialLine agent : referencial) {
      Put p = agent.toPut(hbEncoder, cfall, System.currentTimeMillis());
      table.put(p);
    }
    table.flushCommits();
    table.close();
  }

  @Override
  public void agent(Agent a, Order o, PriceRecord pr) {
    super.agent(a, o, pr);
    if (output == OutputType.Other)
      return;

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
  public void exec(Order o) {
    super.exec(o);
    if (output == OutputType.Other)
      return;
    Put p = new Put(Bytes.toBytes(createRequired("E")));
    p.add(cfall, Bytes.toBytes("sender"), hbEncoder.encodeString(o.sender.name));
    p.add(cfall, Bytes.toBytes("extId"), hbEncoder.encodeString(o.extId));
    countExec++;
    putTable(p);
  }

  @Override
  public void order(Order o) {
    super.order(o);
    if (output == OutputType.Other)
      return;
    long ts = System.currentTimeMillis(); //hack for update on scaledrisk (does not manage put then update with same ts)
    o.timestamp = tsb.nextTimeStamp();
    Put p = new Put(Bytes.toBytes(createRequired("O")), ts);
    p.add(cfall, Bytes.toBytes("orderBookName"), hbEncoder.encodeString(o.obName));
    p.add(cfall, Bytes.toBytes("sender"), hbEncoder.encodeString(o.sender.name));
    p.add(cfall, Bytes.toBytes("extId"), hbEncoder.encodeString(o.extId));
    p.add(cfall, Bytes.toBytes("type"), hbEncoder.encodeChar(o.type));
    p.add(cfall, Bytes.toBytes("id"), hbEncoder.encodeLong(o.id));
    p.add(cfall, Bytes.toBytes("timestamp"), hbEncoder.encodeLong(o.timestamp)); //o.timestamp

    Date d = new Date(tsb.getTimeStamp());
    LOGGER.info("timestamp equal " + tsb.getTimeStamp());
    //LOGGER.info("timestamp encoder = " + hbEncoder.encodeLong(o.timestamp));
    LOGGER.info("timestamp order date = " + d + " current tick = " + tsb.getCurrentTick() + " current day = " + tsb.getCurrentDay());

    if (o.getClass().equals(LimitOrder.class)) {
      LimitOrder lo = (LimitOrder) o;
      p.add(cfall, Bytes.toBytes("quantity"), hbEncoder.encodeInt(lo.quantity));
      p.add(cfall, Bytes.toBytes("direction"), hbEncoder.encodeChar(lo.direction));
      p.add(cfall, Bytes.toBytes("price"), hbEncoder.encodeLong(lo.price));
      p.add(cfall, Bytes.toBytes("validity"), hbEncoder.encodeLong(lo.validity));
    }
    countOrder++;
    putTable(p);

    //count++;
    //LOGGER.info("count = " + count);
  }

  @Override
  public void price(PriceRecord pr, long bestAskPrice, long bestBidPrice) {
    super.price(pr, bestAskPrice, bestBidPrice);
    if (output == OutputType.Other)
      return;
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
  public void day(int nbDays, java.util.Collection<OrderBook> orderbooks) {
    super.day(nbDays, orderbooks);
    if (output == OutputType.Other)
      return;

    for (OrderBook ob : orderbooks) {
      LOGGER.info("day en cours = " + nbDays);
      //LOGGER.info("cureent day + dayGap = " + nbDays + dayGap);

      tsb.setCurrentDay(nbDays);

      Put p = new Put(Bytes.toBytes(createRequired("D")));
      p.add(cfall, Bytes.toBytes("NumDay"), hbEncoder.encodeInt(nbDays + dayGap));
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
  public void tick(Day day, java.util.Collection<OrderBook> orderbooks) {
    super.tick(day, orderbooks);
    if (output == OutputType.Other)
      return;

    for (OrderBook ob : orderbooks) {
      //LOGGER.info("day current tick = " + day.currentTick());
      //LOGGER.info("day number + dayGap = " + day.number + dayGap);
      tsb.setCurrentTick(day.currentTick());
      tsb.setTimeStamp(tsb.baseTimeStampForCurrentTick());

      Put p = new Put(Bytes.toBytes(createRequired("T")));
      p.add(cfall, Bytes.toBytes("numTick"), hbEncoder.encodeInt(day.currentTick()));
      p.add(cfall, Bytes.toBytes("numDay"), hbEncoder.encodeInt(day.number + dayGap));
      p.add(cfall, Bytes.toBytes("orderBookName"), hbEncoder.encodeString(ob.obName));
      if (!ob.ask.isEmpty())
        p.add(cfall, Bytes.toBytes("bestAsk"), hbEncoder.encodeLong(ob.ask.last().price));

      if (!ob.bid.isEmpty())
        p.add(cfall, Bytes.toBytes("bestBid"), hbEncoder.encodeLong(ob.bid.last().price));

      if (ob.lastFixedPrice != null)
        p.add(cfall, Bytes.toBytes("lastFixedPrice"), hbEncoder.encodeLong(ob.lastFixedPrice.price));

      putTable(p);

      count = 0;
    }
  }

  private void putTable(@NotNull Put p) {
    try {
      if (queue.size() % 500 == 0) {
        LOGGER.info("Pending data size : " + queue.size());
      }
      queue.put(p);
    } catch (InterruptedException e) {
      LOGGER.severe("Faild to push data into queue : " + e.getMessage());
    }
  }

  public void close() throws Exception {
    if (output == OutputType.Other)
      return;
    LOGGER.info("Shutting down workers");
    eService.shutdown();

    while (!eService.awaitTermination(10L, TimeUnit.SECONDS)) {
      LOGGER.info("Await pool termination. Still " + queue.size() + " Puts to proceed.");
    }
    LOGGER.log(Level.INFO, "number of orders = " + countOrder);
    LOGGER.log(Level.INFO, "number of prices = " + countPrice);
    LOGGER.log(Level.INFO, "number of puts = " + (flushedPuts + stackedPuts) + " " + stackedPuts);

  }

  @NotNull
  private String createRequired(@NotNull String name) {
    long rowKey = Long.reverseBytes(idTrace.incrementAndGet());
    return name + String.valueOf(rowKey);
  }

  public static class Worker implements Runnable {
    private static final java.util.logging.Logger LOGGER =
        java.util.logging.Logger.getLogger(Worker.class.getName());
    private final ArrayBlockingQueue<Put> queue;
    private final HTable table;
    private final int id;
    private final int flushRate;
    private int stackedPuts = 0;

    public Worker(@NotNull ArrayBlockingQueue<Put> putQueue, @NotNull HTable htbl,
                  int flushRate,
                  int id) {
      this.queue = putQueue;
      this.table = htbl;
      this.id = id;
      this.flushRate = flushRate;
    }

    @Override
    public void run() {
      LOGGER.info("Worker #" + id + " started.");
      try {
        while (!Thread.currentThread().isInterrupted()) {
          try {
            table.put(queue.take());
            ++stackedPuts;
            manageFlush();
          } catch (InterruptedIOException e) {
            LOGGER.severe("Error Worker #" + id + " has been interrupted...");
            return;
          } catch (Throwable t) {
            LOGGER.severe("Error Worker #" + id + " encountered an error : " + t.getMessage() + " - " + t.toString());
          }
        }
      } finally {
        try {
          table.flushCommits();
          table.close();
        } catch (IOException e) {
          LOGGER.severe("Error Worker #" + id + " failed to close table..." + e.getMessage());
        }
      }
      LOGGER.info("Worker #" + id + " ended");
    }

    private void manageFlush() {

      // Flushing every X
      if (stackedPuts > flushRate) {
        try {
          table.flushCommits();
          stackedPuts = 0;
        } catch (Throwable t) {
          LOGGER.severe("Error Worker #" + id + " encountered an error while flushing : " + t.getMessage() + " - " + t.toString());
        }
      }
    }
  }

}