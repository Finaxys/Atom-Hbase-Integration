import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class TimeStampBuilder
{
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(HBaseLogger.class.getName());
    private int nbTickMax;
    private int currentTick = 1;
    private int currentDay = 0;
    private long dateToSeconds = 0L;
    private long openHoursToSeconds;
    private long ratio;
    private long timeStamp;
    private long restRatio = 0;
    private long nbMaxOrderPerTick;
    private long timePerOrder;
    private static final long nbMilliSecDay = 86400000;
    private static final long nbMilliSecHour = 3600000;

    public long getNbMaxOrderPerTick()
    {
        return (this.nbMaxOrderPerTick);
    }

    public int getCurrentTick()
    {
        return currentTick;
    }

    public void setCurrentTick(int currentTick)
    {
        this.currentTick = currentTick;
    }

    public int getCurrentDay()
    {
        return currentDay;
    }

    public void setCurrentDay(int currentDay)
    {
        this.currentDay = currentDay;
    }

    public long getRatio()
    {
        return ratio;
    }

    public long getRestRatio()
    {
        return restRatio;
    }

    public void setRestRatio(long restRatio)
    {
        this.restRatio = restRatio;
    }

    public long getTimeStamp()
    {
        return (this.timeStamp);
    }

    public TimeStampBuilder() throws Exception
    {
        loadConfig();
    }

    public long nextTimeStamp()
    {
        //restRatio = restRatio / 2;
        restRatio += timePerOrder;

        LOGGER.info("restRatio = " + restRatio);

        //last tick is made current day + 1
        if (currentTick == nbTickMax)
        {
            timeStamp = nbMilliSecHour + dateToSeconds + (currentDay - 1) * nbMilliSecDay + openHoursToSeconds + currentTick * ratio + restRatio;
        }
        else
        {
            timeStamp = nbMilliSecHour + dateToSeconds + currentDay * nbMilliSecDay + openHoursToSeconds + currentTick * ratio + restRatio;
        }
        return (timeStamp);

        //on prend le ratio et à chaque nouveau timestamp on ajoute la moitié du ratio restant
        // ratio restant = ratioRestant / 2
        //on reset le ratio restant quand current tick change

        //on divise le ratio par nb max order et on ajoute a chaque fois
    }

    private void loadConfig() throws Exception
    {
        //take the date
        String dateBegin = System.getProperty("simul.time.startdate");
        assert dateBegin != null;

        SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
        Date date = null;

        date = formatter.parse(dateBegin);
        //LOGGER.info("date = " + date);
        dateToSeconds = date.getTime();
        //LOGGER.info("timestamp à partir du fichier de conf : " + dateToSeconds);

        //take the hours
        String openHourStr = System.getProperty("simul.time.openhour");
        String closeHourStr = System.getProperty("simul.time.closehour");

        DateFormat dateFormatter = new SimpleDateFormat("h:mm");
        Date openHour = null;
        Date closeHour = null;
        openHour = (Date) dateFormatter.parse(openHourStr);
        assert openHour != null;
        closeHour = (Date) dateFormatter.parse(closeHourStr);
        assert closeHour != null;
        openHoursToSeconds = openHour.getTime();
        long closeHoursToSeconds = closeHour.getTime();

        //Take the period
        String nbTickMaxStr = System.getProperty("simul.tick.continuous");
        assert nbTickMaxStr != null;
        //LOGGER.info("simul.tick.continuous = " + nbTickMaxStr);
        nbTickMax = Integer.parseInt(nbTickMaxStr);

        ratio = (closeHoursToSeconds - openHoursToSeconds) / (nbTickMax + 1); // +1 to not reach the closehour on the last tick

        //restRatio = ratio;
        LOGGER.info("ratio = " + ratio);

        //calc nb max order between 2 ticks
        nbMaxOrderPerTick = getNbAgents() * getNbOrderBooks() * 2;
        LOGGER.info("nbmaxorderpertick = " + nbMaxOrderPerTick);
        timePerOrder = (ratio / nbMaxOrderPerTick);
        LOGGER.info("timePerOrder is = " + timePerOrder);
    }

    private int getNbAgents()
    {
        String nbAgentsStr = System.getProperty("symbols.agents.basic");
        assert nbAgentsStr != null;
        //LOGGER.info("nbAgentsStr = " + nbAgentsStr);
        List<String> Str = Arrays.asList(nbAgentsStr.split(","));
        //LOGGER.info("str size = " + Str.size());
        int nbAgents = Str.size();
        return (nbAgents);
    }

    private int getNbOrderBooks()
    {
        String obsym = System.getProperty("atom.orderbooks", "");
        assert obsym != null;

        String nbOrderBookStr = System.getProperty("symbols.orderbooks." + obsym);
        //LOGGER.info("nbOderBookStr = " + nbOrderBookStr);
        assert nbOrderBookStr != null;
        List<String> Str = Arrays.asList(nbOrderBookStr.split(","));
        //LOGGER.info("str order book size = " + Str.size());
        int nbOrderBook = Str.size();
        return (nbOrderBook);
    }

}
