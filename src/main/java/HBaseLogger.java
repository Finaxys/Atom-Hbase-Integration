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
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

class HBaseLogger extends Logger {
  private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(HBaseLogger.class.getName());
  private Configuration conf;

  private HConnection connection;
  private HBaseAdmin admin;
  private HTableDescriptor tableDescriptor;
  private HTable table;
  private Day lastTickDay;
  private AtomicLong idTrace = new AtomicLong(0);

  private int countOrder;
  private int countExec;

  final HBaseDataTypeEncoder hbEncoder = new HBaseDataTypeEncoder();

  private Output output;

  private byte[] cfall;

  private long stackedPuts = 0;
  private long flushedPuts = 0;
  private boolean autoflush;
  private long stackPuts;

  public HBaseLogger(Output output, String filename, String tableName, String cfName) throws Exception {
    super(filename);

    init(output, tableName, cfName);
  }

  public HBaseLogger(Output output, PrintStream o, String tableName, String cfName) throws Exception {
    super(o);

    init(output, tableName, cfName);
  }

  public HBaseLogger(String tableName, String cfName) throws Exception {
    init(Output.HBase, tableName, cfName);
  }

  public void init(Output output, String tableName, String cfName) throws Exception {
    cfall = Bytes.toBytes(cfName);
    this.output = output;

    if (output == Output.Other)
      return;

    autoflush = Boolean.parseBoolean(System.getProperty("hbase.autoflush", "false"));
    stackPuts = Integer.parseInt(System.getProperty("hbase.stackputs", "1000"));

    Configuration conf = HBaseConfiguration.create();
    try {
      String minicluster = System.getProperty("hbase.conf.minicluster", "");
      if (!minicluster.isEmpty())
        conf.addResource(new FileInputStream(minicluster));
      else {
        conf.addResource(new File(System.getProperty("hbase.conf.core", "core-site.xml")).getAbsoluteFile().toURI().toURL());
        conf.addResource(new File(System.getProperty("hbase.conf.hbase", "hbase-site.xml")).getAbsoluteFile().toURI().toURL());
        conf.addResource(new File(System.getProperty("hbase.conf.hdfs", "hdfs-site.xml")).getAbsoluteFile().toURI().toURL());
      }
    } catch (MalformedURLException e) {
      LOGGER.log(Level.SEVERE, "Could not get hbase configuration files", e);
      throw new Exception("hbase", e);
    }

    LOGGER.log(Level.INFO, conf.get("hbase.zookeeper.property.clientPort"));
    LOGGER.log(Level.INFO, conf.get("hbase.zookeeper.quorum"));

    conf.reloadConfiguration();

    LOGGER.log(Level.INFO, "Configuration completed");

    try {
      connection = HConnectionManager.createConnection(conf);
    } catch (IOException e) {
      LOGGER.log(Level.SEVERE, "Could not create Connection", e);
      throw new Exception("hbase connection", e);
    }

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

    tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
    try {
      LOGGER.log(Level.INFO, "Creating table");
      LOGGER.log(Level.INFO, admin.getClusterStatus().toString());

      tableDescriptor.addFamily(new HColumnDescriptor(cfName));
      admin.createTable(tableDescriptor);

      LOGGER.log(Level.INFO, "Table Created");
    } catch (IOException e) {
      LOGGER.log(Level.FINEST, "Table already created");
    }

    try {
      LOGGER.log(Level.INFO, "Getting table information");
      table = new HTable(conf, tableName);
//            AutoFlushing
      table.setAutoFlushTo(autoflush);
    } catch (IOException e) {
      LOGGER.log(Level.SEVERE, "Could not get table " + tableName, e);
      throw new Exception("Table", e);
    }

    LOGGER.log(Level.INFO, "Configuration completed");
  }

  @Override
  public void agent(Agent a, Order o, PriceRecord pr) {
    super.agent(a, o, pr);
    if (output == Output.Other)
      return;

    Put p = new Put(Bytes.toBytes(createRequired("A")));
    p.add(cfall, Bytes.toBytes("agentName"), hbEncoder.encodeString(a.name));
    p.add(cfall, Bytes.toBytes("orderBookName"), hbEncoder.encodeString(o.obName));
    p.add(cfall, Bytes.toBytes("cash"), hbEncoder.encodeLong(a.cash));
    p.add(cfall, Bytes.toBytes("executed"), hbEncoder.encodeInt(pr.quantity));
    p.add(cfall, Bytes.toBytes("price"), hbEncoder.encodeLong(pr.price));
    if (o.getClass().equals(LimitOrder.class)) {
      p.add(cfall, Bytes.toBytes("direction"), hbEncoder.encodeChar(((LimitOrder)o).direction));
      p.add(cfall, Bytes.toBytes("tenor"), hbEncoder.encodeLong(pr.timestamp));
    }
    putTable(p);
  }

  @Override
  public void exec(Order o) {
    super.exec(o);
    if (output == Output.Other)
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
    if (output == Output.Other)
      return;
    long ts = System.currentTimeMillis(); //hack for update on scaledrisk (does not manage put then update with same ts)
    Put p = new Put(Bytes.toBytes(createRequired("O")), ts);
    p.add(cfall, Bytes.toBytes("orderBookName"), hbEncoder.encodeString(o.obName));
    p.add(cfall, Bytes.toBytes("sender"), hbEncoder.encodeString(o.sender.name));
    p.add(cfall, Bytes.toBytes("extId"), hbEncoder.encodeString(o.extId));
    p.add(cfall, Bytes.toBytes("type"), hbEncoder.encodeChar(o.type));
    p.add(cfall, Bytes.toBytes("id"), hbEncoder.encodeLong(o.id));
    p.add(cfall, Bytes.toBytes("timestamp"), hbEncoder.encodeLong(o.timestamp));

    if (o.getClass().equals(LimitOrder.class)) {
      LimitOrder lo = (LimitOrder) o;
      p.add(cfall, Bytes.toBytes("quantity"), hbEncoder.encodeInt(lo.quantity));
      p.add(cfall, Bytes.toBytes("direction"), hbEncoder.encodeChar(lo.direction));
      p.add(cfall, Bytes.toBytes("price"), hbEncoder.encodeLong(lo.price));
      p.add(cfall, Bytes.toBytes("validity"), hbEncoder.encodeLong(lo.validity));
    }
    countOrder++;
//    System.out.println("OrderBook : " + o.obName + " sender : " + o.sender.name + " extId " + o.extId + " id : " + o.id);

    putTable(p);
  }

  @Override
  public void price(PriceRecord pr, long bestAskPrice, long bestBidPrice) {
    super.price(pr, bestAskPrice, bestBidPrice);
    if (output == Output.Other)
      return;
    long ts = System.currentTimeMillis() + 2L; //hack for update on scaledrisk (does not manage put then update with same ts)
    Put p = new Put(Bytes.toBytes(createRequired("P")), ts);
    p.add(cfall, Bytes.toBytes("obName"), ts, hbEncoder.encodeString(pr.obName));
    p.add(cfall, Bytes.toBytes("price"), ts, hbEncoder.encodeLong(pr.price));
    p.add(cfall, Bytes.toBytes("executedQuty"), ts, hbEncoder.encodeInt(pr.quantity));
    p.add(cfall, Bytes.toBytes("dir"), ts, hbEncoder.encodeChar(pr.dir));
    p.add(cfall, Bytes.toBytes("order1"), ts, hbEncoder.encodeString(pr.extId1));
    p.add(cfall, Bytes.toBytes("order2"), ts, hbEncoder.encodeString(pr.extId2));
    p.add(cfall, Bytes.toBytes("bestask"), ts, hbEncoder.encodeLong(bestAskPrice));
    p.add(cfall, Bytes.toBytes("bestbid"), ts, hbEncoder.encodeLong(bestBidPrice));
    p.add(cfall, Bytes.toBytes("timestamp"), ts, hbEncoder.encodeLong((pr.timestamp > 0 ? pr.timestamp : ts)));

    putTable(p);
  }

  @Override
  public void day(int nbDays, java.util.Collection<OrderBook> orderbooks) {
    super.day(nbDays, orderbooks);
    if (output == Output.Other)
      return;

    for (OrderBook ob : orderbooks) {
      Put p = new Put(Bytes.toBytes(createRequired("D")));

      p.add(cfall, Bytes.toBytes("NumDay"), hbEncoder.encodeInt(nbDays));
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
    if (output == Output.Other)
      return;

    lastTickDay = day;
    for (OrderBook ob : orderbooks) {
      Put p = new Put(Bytes.toBytes(createRequired("T")));
      p.add(cfall, Bytes.toBytes("numTick"), hbEncoder.encodeInt(day.currentPeriod));
      p.add(cfall, Bytes.toBytes("orderBookName"), hbEncoder.encodeString(ob.obName));

      if (!ob.ask.isEmpty())
        p.add(cfall, Bytes.toBytes("bestask"), hbEncoder.encodeLong(ob.ask.last().price));

      if (!ob.bid.isEmpty())
        p.add(cfall, Bytes.toBytes("bestbid"), hbEncoder.encodeLong(ob.bid.last().price));

      if (ob.lastFixedPrice != null)
        p.add(cfall, Bytes.toBytes("lastPrice"), hbEncoder.encodeLong(ob.lastFixedPrice.price));

      putTable(p);
    }
  }

  private void putTable(Put p) {
    try {
      table.put(p);
      ++stackedPuts;

      // Flushing every X
      if (!autoflush && stackedPuts > stackPuts)
        flushPuts();

    } catch (InterruptedIOException e) {
      e.printStackTrace();
    } catch (RetriesExhaustedWithDetailsException e) {
      e.printStackTrace();
    }
  }

  private void flushPuts() {
    try {
      LOGGER.log(Level.INFO, "Flushing... total order sent : " + countOrder +
          " - total exec sent : " + countExec + " - sum = " + (countOrder + countExec));
      table.flushCommits();
    } catch (InterruptedIOException e) {
      LOGGER.log(Level.SEVERE, "Could not flush table", e);
    } catch (RetriesExhaustedWithDetailsException e) {
      LOGGER.log(Level.SEVERE, "Could not flush table ", e);
    }

    flushedPuts += stackedPuts;
    stackedPuts = 0;
  }

  public void close() throws Exception {
    if (output == Output.Other)
      return;

    if (!autoflush)
      flushPuts();

    try {
      table.close();
    } catch (IOException e) {
      LOGGER.log(Level.SEVERE, "Could not close table", e);
      throw new Exception("Closing", e);
    }

    LOGGER.log(Level.INFO, "Closing table with " + (flushedPuts + stackedPuts) + " puts");
  }

  private String createRequired(String name) {
    String required = "";
    required += String.format("%010d", idTrace.incrementAndGet()) + name;
    return required;
  }
}