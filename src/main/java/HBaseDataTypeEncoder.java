import com.sun.istack.NotNull;
import org.apache.hadoop.hbase.types.DataType;
import org.apache.hadoop.hbase.types.OrderedBlobVar;
import org.apache.hadoop.hbase.types.RawInteger;
import org.apache.hadoop.hbase.types.RawLong;
import org.apache.hadoop.hbase.types.RawStringTerminated;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;

public class HBaseDataTypeEncoder {

  private final DataType<String> strDataType = new RawStringTerminated("\0");
  private final DataType<Integer> intDataType = new RawInteger();
  private final DataType<Long> longDataType = new RawLong();
  private final DataType<byte[]> charType = OrderedBlobVar.ASCENDING;

  public byte[] encodeString(@NotNull String value) {
    return encode(strDataType, value);
  }

  public byte[] encodeInt(int value) {
    return encode(intDataType, value);
  }

  public byte[] encodeLong(long value) {
    return encode(longDataType, value);

  }

  public byte[] encodeChar(@NotNull char value) {
    return encode(charType, charToBytes(value));
  }

  /**
   * Return an array of 2 bytes
   *
   * @param c ascii or not (example : 'é', '^o', 'ç'...)
   * @return encoded char as byte array
   */
  @NotNull
  private static byte[] charToBytes(@NotNull Character c) {
    byte[] b = new byte[2];
    b[0] = (byte) ((c & 0xFF00) >> 8);
    b[1] = (byte) (c & 0x00FF);
    return b;
  }

  private <T> byte[] encode(@NotNull DataType<T> dt, @NotNull T value) {
    SimplePositionedByteRange sbpr = new SimplePositionedByteRange(dt.encodedLength(value));
    dt.encode(sbpr, value);
    return sbpr.getBytes();
  }
}