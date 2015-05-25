import com.sun.istack.NotNull;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class AgentReferentialLine {

  private final int agentRefId;
  private final String orderBookName;
  private final String agentName;
  private final boolean isMarketMaker;
  private final String details = "No details available";

  public AgentReferentialLine(int agentRefId,@NotNull String orderBookName,@NotNull String agentName) {
    this.agentRefId = agentRefId;
    this.orderBookName = orderBookName;
    this.agentName = agentName;
    this.isMarketMaker = agentName.equals("mm");
  }

  @NotNull
  public Put toPut(@NotNull HBaseDataTypeEncoder encoder, @NotNull byte[] columnF, long ts) {
    Put p = new Put(Bytes.toBytes(agentRefId + "R"), ts);
    p.add(columnF, Bytes.toBytes("agentRefId"), encoder.encodeInt(agentRefId));
    p.add(columnF, Bytes.toBytes("orderBookName"), encoder.encodeString(orderBookName));
    p.add(columnF, Bytes.toBytes("agentName"), encoder.encodeString(agentName));
    p.add(columnF, Bytes.toBytes("isMarketMaker"), encoder.encodeBoolean(isMarketMaker));
    p.add(columnF, Bytes.toBytes("details"), encoder.encodeString(details));
    return p;
  }

  @Override
  public String toString() {
    return "AgentReferentialLine{" +
        "agentRefId=" + agentRefId +
        ", orderBookName='" + orderBookName + '\'' +
        ", agentName='" + agentName + '\'' +
        ", isMarketMaker=" + isMarketMaker +
        ", details='" + details + '\'' +
        '}';
  }
}
