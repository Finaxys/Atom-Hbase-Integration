import v13.Day;
import v13.MonothreadedSimulation;
import v13.Simulation;
import v13.agents.ZIT;

import java.io.*;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

enum Output
{
    SystemOut,
    HBase,
    All
};

public class AtomHBaseIntegration
{
    // Static infos
    static public final String[] DOW2 = { "MMM", "AXP"};
    static public final String[] DOW30 = { "MMM", "AXP", "AAPL", "BA", "CAT", "CVX", "CSCO", "KO", "DIS", "DD", "XOM", "GE", "GS", "HD", "IBM", "INTC", "JNJ", "JPM", "MCD", "MRK", "MSFT", "NKE", "PFE", "PG", "TRV", "UTX", "UNH", "VZ", "V", "WMT"};
    private static final Logger LOGGER = Logger.getLogger( AtomHBaseIntegration.class.getName() );

    // Main config for Atom
    public static void main(String args[]) throws IOException {
        // Loading properties
        try {
            FileInputStream propFile = new FileInputStream("properties.txt");
            Properties p = new Properties(System.getProperties());
            p.load(propFile);
            System.setProperties(p);
            // display new properties
            System.getProperties().list(System.out);
        }
        catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Could not load properties", e);
        }


        // Pre config arguments
        Output output;
        String tableName = "atom";
        String cfName = "cf";

        // Config Arguments
        if (args[0].equals("1"))
            output = Output.HBase;
        else if (args[0].equals("2"))
            output = Output.SystemOut;
        else
            output = Output.All;

        if (args.length > 1)
        {
            tableName = args[1];
            if (args.length > 2)
                cfName = args[2];
        }

        // How long
        long startTime = System.currentTimeMillis();

        // Create simulator with custom logger
        Simulation sim = new MonothreadedSimulation();
        HBaseLogger logger = new HBaseLogger(output, tableName, cfName);
        sim.setLogger(logger);

        // Create Agents and Order book to MarketMaker with static infos
        for (int i = 1; i < 10; ++i)
            sim.addNewAgent(new ZIT("ZIT" + i,0,10000,20000,10,50));
        for (String ob : DOW30)
            sim.addNewMarketMaker(ob);

        sim.run(Day.createEuroNEXT(1, 500, 0), 1);

        sim.market.close();
        logger.close();


        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println("Elapsed time: " + estimatedTime / 1000 + "s");
    }
}