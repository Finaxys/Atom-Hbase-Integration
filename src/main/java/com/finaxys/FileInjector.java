package com.finaxys;

import v13.Day;
import v13.Order;
import v13.OrderBook;
import v13.PriceRecord;
import v13.agents.Agent;

import java.util.Collection;
import java.util.List;

/**
 *
 */
public class FileInjector implements AtomDataInjector {


  @Override
  public void createOutput() throws Exception {

  }

  @Override
  public void sendAgent(Agent a, Order o, PriceRecord pr) {

  }

  @Override
  public void sendPriceRecord(PriceRecord pr, long bestAskPrice, long bestBidPrice) {

  }

  @Override
  public void sendAgentReferential(List<AgentReferentialLine> referencial) {

  }

  @Override
  public void sendOrder(Order o) {

  }

  @Override
  public void sendTick(Day day, Collection<OrderBook> orderbooks) {

  }

  @Override
  public void sendDay(int nbDays, Collection<OrderBook> orderbooks) {

  }

  @Override
  public void sendExec(Order o) {

  }

  @Override
  public void close() {

  }
}
