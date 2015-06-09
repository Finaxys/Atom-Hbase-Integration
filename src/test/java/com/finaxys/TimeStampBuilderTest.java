package com.finaxys;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.junit.Assert.*;

@Test
public class TimeStampBuilderTest
{
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(HBaseLogger.class.getName());

    private TimeStampBuilder tsb = new TimeStampBuilder();

    @BeforeMethod
    public void setUp() throws Exception
    {
    }

    @AfterMethod
    public void tearDown()
    {
    }

    public long callNextTimeStampXTime(long nb)
    {
        while (nb > 0)
        {
         tsb.nextTimeStamp();
            nb--;
        }
        return tsb.getTimeStamp();
    }

    @DataProvider
    public Object[][] myDataProvider()
    {
        return new Object[][]{
                {
                        tsb.baseTimeStampForCurrentTick() , tsb.getTimeStamp(),true
                },
                {
                        tsb.baseTimeStampForCurrentTick() + tsb.getCumulTimePerOrder(), tsb.nextTimeStamp(), true
                },
                {
                        callNextTimeStampXTime(tsb.getNbMaxOrderPerTick()), tsb.baseTimeStampForNextTick(), false
                        //le callnectimestampxtime ne doit pas etre supérieur au basetimestampnextick
                }
        };
    }

//    @Test(dataProvider = "myDataProvider", enabled = false)
//    public void testSimple(String val1, String val2, boolean match){
//        assertEquals(match, val1.equals(val2));
//    }

    @Test(dataProvider = "myDataProvider")
    public void timestampFor20Ticks(long expected, long received, boolean match) throws Exception
    {
        //loadConfig
        String dateBegin = "09/13/1986";
        String openHourStr = "9:00";
        String closeHourStr = "17:30";
        String nbTickMaxStr = "20";
        tsb.convertFromString(dateBegin, openHourStr, closeHourStr, nbTickMaxStr);
        tsb.init();
        tsb.baseTimeStampForCurrentTick();
        assertEquals(match, expected == received);




    }

    @Test(dataProvider = "myDataProvider")
    public void timestampFor30kTicks(long expected, long received, boolean match) throws Exception
    {
        //loadConfig
        String dateBegin = "09/13/1986";
        String openHourStr = "9:00";
        String closeHourStr = "17:30";
        String nbTickMaxStr = "30000";
        tsb.convertFromString(dateBegin, openHourStr, closeHourStr, nbTickMaxStr);
        tsb.init();
        tsb.baseTimeStampForCurrentTick();
        assertEquals(match, expected == received);



    }
}