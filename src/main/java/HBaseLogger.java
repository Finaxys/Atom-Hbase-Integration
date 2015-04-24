import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import v13.*;
import v13.Logger;
import v13.agents.Agent;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.util.logging.*;

class HBaseLogger extends Logger
{
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(HBaseLogger.class.getName());

    private Configuration conf;
    private HConnection connection;
    private HBaseAdmin admin;
    private HTableDescriptor tableDescriptor;
    private HTable table;
    private Day lastTickDay;

    private long idOrder = 0;
    private long idPrice = 0;
    private long idAgent = 0;
    private long idExec = 0;
    private long idTick = 0;
    private long idDay = 0;

    private Output output;

    private byte[] cfall;

    private long stackedPuts = 0;
    private long flushedPuts = 0;

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

        Configuration conf = HBaseConfiguration.create() ;
        try {
            conf.addResource(new File("core-site.xml").getAbsoluteFile().toURI().toURL());
            conf.addResource(new File("hbase-site.xml").getAbsoluteFile().toURI().toURL());
            conf.addResource(new File("hdfs-site.xml").getAbsoluteFile().toURI().toURL());
        } catch (MalformedURLException e) {
            LOGGER.log(Level.SEVERE, "Could not get hbase configuration files", e);
            throw new Exception("hbase", e);
        }

        LOGGER.log(Level.INFO, conf.get("hbase.zookeeper.property.clientPort"));
        LOGGER.log(Level.INFO, conf.get("hbase.zookeeper.quorum"));

        conf.reloadConfiguration();

        LOGGER.log(Level.INFO, "Configuration completed");

        try {
            connection =  HConnectionManager.createConnection(conf);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Could not create Connection", e);
        }
        HBaseAdmin admin = null;
        try {
            admin = new HBaseAdmin(connection);
        } catch (MasterNotRunningException e) {
            LOGGER.log(Level.SEVERE, "Master server not running", e);
        } catch (ZooKeeperConnectionException e) {
            LOGGER.log(Level.SEVERE, "Could not connect to ZooKeeper", e);
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
            table.setAutoFlushTo(false);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Could not get table " + tableName, e);
            throw new Exception("Table", e);
        }
    }

    @Override
    public void    agent(Agent a, Order o, PriceRecord pr)
    {
        super.agent(a, o, pr);
        if (output == Output.Other)
            return;

        Put p = new Put(Bytes.toBytes(createRequired("A", idAgent++)));
        p.add(cfall, Bytes.toBytes("name"), Bytes.toBytes(a.name));
        p.add(cfall, Bytes.toBytes("cash"), Bytes.toBytes(a.cash));
        p.add(cfall, Bytes.toBytes("obName"), Bytes.toBytes(o.obName));
        p.add(cfall, Bytes.toBytes("nbInvest"), Bytes.toBytes(pr.quantity));
        p.add(cfall, Bytes.toBytes("lastFixedPrice"), Bytes.toBytes(pr.price));

        putTable(p);
    }
    @Override
    public void    exec(Order o)
    {
        super.exec(o);
        if (output == Output.Other)
            return;

        Put p = new Put(Bytes.toBytes(createRequired("E", idExec++)));
        p.add(cfall, Bytes.toBytes("sender"), Bytes.toBytes(o.sender.name));
        p.add(cfall, Bytes.toBytes("extId"), Bytes.toBytes(o.extId));

        putTable(p);
    }
    @Override
    public void    order(Order o)
    {
        super.order(o);
        if (output == Output.Other)
            return;

        Put p = new Put(Bytes.toBytes(createRequired("O", idOrder++)));
        p.add(cfall, Bytes.toBytes(o.type), Bytes.toBytes(o.type));
        p.add(cfall, Bytes.toBytes("obName"), Bytes.toBytes(o.obName));
        p.add(cfall, Bytes.toBytes("sender"), Bytes.toBytes(o.sender.name));
        p.add(cfall, Bytes.toBytes("extId"), Bytes.toBytes(o.extId));
        p.add(cfall, Bytes.toBytes("type"), Bytes.toBytes(o.type));
        p.add(cfall, Bytes.toBytes("id"), Bytes.toBytes(o.id));
        p.add(cfall, Bytes.toBytes("timestamp"), Bytes.toBytes(o.timestamp));

        if (o.getClass().equals(LimitOrder.class))
        {
            LimitOrder lo = (LimitOrder) o;
            p.add(cfall, Bytes.toBytes("quty"), Bytes.toBytes(lo.quantity));
            p.add(cfall, Bytes.toBytes("dir"), Bytes.toBytes(lo.direction));
            p.add(cfall, Bytes.toBytes("price"), Bytes.toBytes(lo.price));
            p.add(cfall, Bytes.toBytes("valid"), Bytes.toBytes(lo.validity));
        }

        putTable(p);
    }
    @Override
    public void    price(PriceRecord pr, long bestAskPrice, long bestBidPrice)
    {
        super.price(pr, bestAskPrice, bestBidPrice);
        if (output == Output.Other)
            return;

        Put p = new Put(Bytes.toBytes(createRequired("P", idPrice++)));
        p.add(cfall, Bytes.toBytes("obName"), Bytes.toBytes(pr.obName));
        p.add(cfall, Bytes.toBytes("price"), Bytes.toBytes(pr.price));
        p.add(cfall, Bytes.toBytes("executedQuty"), Bytes.toBytes(pr.quantity));
        p.add(cfall, Bytes.toBytes("dir"), Bytes.toBytes(pr.dir));
        p.add(cfall, Bytes.toBytes("order1"), Bytes.toBytes(pr.extId1));
        p.add(cfall, Bytes.toBytes("order2"), Bytes.toBytes(pr.extId2));
        p.add(cfall, Bytes.toBytes("bestask"), Bytes.toBytes(bestAskPrice));
        p.add(cfall, Bytes.toBytes("bestbid"), Bytes.toBytes(bestBidPrice));

        putTable(p);
    }

    @Override
    public void    day(int nbDays, java.util.Collection<OrderBook> orderbooks)
    {
        super.day(nbDays, orderbooks);
        if (output == Output.Other)
            return;

        for (OrderBook ob : orderbooks)
        {
            Put p = new Put(Bytes.toBytes(createRequired("D", idDay++)));

            p.add(cfall, Bytes.toBytes("NumDay"), Bytes.toBytes(nbDays));
            p.add(cfall, Bytes.toBytes("obName"), Bytes.toBytes(nbDays));
            p.add(cfall, Bytes.toBytes("FirstFixedPrice"), Bytes.toBytes(ob.firstPriceOfDay));
            p.add(cfall, Bytes.toBytes("LowestPrice"), Bytes.toBytes(ob.lowestPriceOfDay));
            p.add(cfall, Bytes.toBytes("HighestPrice"), Bytes.toBytes(ob.highestPriceOfDay));
            long price = 0;
            if (ob.lastFixedPrice != null)
                price = ob.lastFixedPrice.price;
            p.add(cfall, Bytes.toBytes("LastFixedPrice"), Bytes.toBytes(price));
            p.add(cfall, Bytes.toBytes("nbPricesFixed"), Bytes.toBytes(ob.numberOfPricesFixed));

            putTable(p);
        }
    }

    @Override
    public void    tick(Day day, java.util.Collection<OrderBook> orderbooks)
    {
        super.tick(day, orderbooks);
        if (output == Output.Other)
            return;

        lastTickDay = day;
        for (OrderBook ob : orderbooks)
        {
            Put p = new Put(Bytes.toBytes(createRequired("T", idTick++)));

            p.add(cfall, Bytes.toBytes("numTick"), Bytes.toBytes(day.currentPeriod));
            p.add(cfall, Bytes.toBytes("obName"), Bytes.toBytes(ob.obName));

            long price = 0;
            if (!ob.ask.isEmpty())
                price = ob.ask.last().price;
            p.add(cfall, Bytes.toBytes("bestask"), Bytes.toBytes(price));

            price = 0;
            if (!ob.bid.isEmpty())
                price = ob.bid.last().price;
            p.add(cfall, Bytes.toBytes("bestbid"), Bytes.toBytes(price));

            price = 0;
            if (ob.lastFixedPrice != null)
                price = ob.lastFixedPrice.price;
            p.add(cfall, Bytes.toBytes("lastPrice"), Bytes.toBytes(price));

            putTable(p);
        }
    }

    private void putTable(Put p)
    {
        try {
            table.put(p);
            ++stackedPuts;

            // Flushing every X
            if (stackedPuts > 1000)
                flushPuts();

        } catch (InterruptedIOException e) {
            e.printStackTrace();
        } catch (RetriesExhaustedWithDetailsException e) {
            e.printStackTrace();
        }
    }

    private void flushPuts(){
        try {
            table.flushCommits();
        } catch (InterruptedIOException e) {
            LOGGER.log(Level.SEVERE, "Could not flush table", e);
        } catch (RetriesExhaustedWithDetailsException e) {
            LOGGER.log(Level.SEVERE, "Could not flush table ", e);
        }

        flushedPuts += stackedPuts;
        stackedPuts = 0;

        if (flushedPuts % 100000 < 1000)
            System.out.println("Flushed " + flushedPuts + " puts");
    }

    public void close() throws Exception {
        if (output == Output.Other)
            return;

        flushPuts();
        try {
            table.close();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Could not close table", e);
            throw new Exception("Closing", e);
        }

        System.out.println("Closing table with " + flushedPuts + " puts");
    }

    private String createRequired(String name, long id)
    {
        String required = "";
        required += String.format("%010d", id) + name;
        return required;
    }
}