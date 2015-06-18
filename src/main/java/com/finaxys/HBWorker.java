package com.finaxys;

import com.sun.istack.NotNull;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Worker consumer used to send put in table.
 * Put are provided by an ArrayBlockingQueue.
 */
public class HBWorker implements Runnable {
  private static final java.util.logging.Logger LOGGER =
      java.util.logging.Logger.getLogger(HBWorker.class.getName());
  private final ArrayBlockingQueue<Put> queue;
  private final HTable table;
  private final int id;
  private final int flushRate;
  private int stackedPuts = 0;

  private final AtomicBoolean isClosing;

  public HBWorker(@NotNull ArrayBlockingQueue<Put> putQueue, @NotNull HTable htbl,
                  int flushRate,
                  int id, AtomicBoolean isClosing) {
    this.queue = putQueue;
    this.table = htbl;
    this.id = id;
    this.flushRate = flushRate;
    this.isClosing = isClosing;
  }

  @Override
  public void run() {
    LOGGER.info("Worker #" + id + " started.");
    try {
      while (!queue.isEmpty() || !isClosing.get()) {
        try {
          Put p = queue.poll(500, TimeUnit.MILLISECONDS);
          if (p != null) {
            table.put(p);
            ++stackedPuts;
            manageFlush();
          }
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

