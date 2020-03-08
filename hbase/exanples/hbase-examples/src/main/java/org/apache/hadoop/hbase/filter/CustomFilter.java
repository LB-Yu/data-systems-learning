package org.apache.hadoop.hbase.filter;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author Yu Liebing
 */
public class CustomFilter extends FilterBase {

  private static Logger logger = LoggerFactory.getLogger(CustomFilter.class);

  private boolean filterRow = true;
  private boolean passedPrefix = false;

  private byte[] prefix;
  private byte[] cf;
  private byte[] cq;
  private byte[] value;

  public CustomFilter(
          final byte[] prefix,
          final byte[] cf,
          final byte[] cq,
          final byte[] value) {
    logger.info("Constructor method is called, row prefix: " + new String(prefix) + ", column family: "
            + new String(cf) + ", column qualifier: " + new String(cq) + ", value " + new String(value));
    this.prefix = prefix;
    this.cf = cf;
    this.cq = cq;
    this.value = value;
  }

  public byte[] getPrefix() {
    return prefix;
  }

  public byte[] getValue() {
    return value;
  }

  public byte[] getCf() {
    return cf;
  }

  public byte[] getCq() {
    return cq;
  }

  @Override
  public void reset() {
    logger.info("reset() method is called");
    filterRow = true;
  }

  @Override
  public boolean filterRowKey(byte[] buffer, int offset, int length) {
    logger.info("filterRowKey() method is called");
    if (buffer == null || this.prefix == null)
      return true;
    if (length < prefix.length)
      return true;
    // if they are equal, return false => pass row
    // else return true, filter row
    int cmp = Bytes.compareTo(buffer, offset, this.prefix.length, this.prefix, 0,
            this.prefix.length);
    // if we passed the prefix, set flag
    if ((!isReversed() && cmp > 0) || (isReversed() && cmp < 0)) {
      passedPrefix = true;
    }
    filterRow = (cmp != 0);
    return filterRow;
  }

  @Override
  public boolean filterAllRemaining() {
    logger.info("filterAllRemaining() method is called, passed prefix: " + passedPrefix);
    return passedPrefix;
  }

  @Override
  public ReturnCode filterKeyValue(Cell cell) {
    logger.info("filterKeyValue() method is called, cell: " + cellToString(cell));
    if (filterRow) return ReturnCode.NEXT_ROW;
    if (!(Bytes.equals(CellUtil.cloneFamily(cell), cf))) {
      return ReturnCode.NEXT_COL;
    }
    if (!(Bytes.equals(CellUtil.cloneQualifier(cell), cq))) {
      return ReturnCode.NEXT_COL;
    }
    if (!(Bytes.equals(CellUtil.cloneValue(cell), value))) {
      return ReturnCode.NEXT_COL;
    }
    return ReturnCode.INCLUDE;
  }

  @Override
  public Cell transformCell(Cell cell) {
    logger.info("transformCell() method is called, cell: " + cellToString(cell));
    return cell;
  }

  @Override
  @Deprecated
  public KeyValue transform(KeyValue keyValue) {
    return keyValue;
  }

  @Override
  public void filterRowCells(List<Cell> list) {
    logger.info("filterRowCells() method is called, size: " + list.size());
    for (Cell cell : list) {
      logger.info("filterRowCells() " + cellToString(cell));
    }
  }

  @Override
  public boolean hasFilterRow() {
    logger.info("hasFilterRow() method is called");
    return false;
  }

  @Override
  public boolean filterRow() {
    logger.info("filterRow() method is called, filterRow: " + filterRow);
    return filterRow;
  }

  @Override
  @Deprecated
  public KeyValue getNextKeyHint(KeyValue keyValue) {
    return null;
  }

  @Override
  public Cell getNextCellHint(Cell cell) {
    logger.info("getNextCellHint() method is called, cell: " + cellToString(cell));
    return null;
  }

  @Override
  public boolean isFamilyEssential(byte[] bytes) {
    logger.info("isFamilyEssential() method is called, param: " + new String(bytes));
    return true;
  }

  @Override
  public byte[] toByteArray() {
    logger.info("toByteArray() method is called");
    int bufferLength = prefix.length + cf.length + cq.length + value.length + 16;
    ByteBuffer buf = ByteBuffer.allocate(bufferLength);
    buf.putInt(prefix.length);
    buf.put(prefix);
    buf.putInt(cf.length);
    buf.put(cf);
    buf.putInt(cq.length);
    buf.put(cq);
    buf.putInt(value.length);
    buf.put(value);
    return buf.array();
  }

  public static CustomFilter parseFrom(final byte[] bytes) {
    logger.info("parseFrom() method is called");
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    byte[] prefix = new byte[buf.getInt()];
    buf.get(prefix, 0, prefix.length);
    byte[] cf = new byte[buf.getInt()];
    buf.get(cf, 0, cf.length);
    byte[] cq = new byte[buf.getInt()];
    buf.get(cq, 0, cq.length);
    byte[] value = new byte[buf.getInt()];
    buf.get(value, 0, value.length);
    return new CustomFilter(prefix, cf, cq, value);
  }

  @Override
  boolean areSerializedFieldsEqual(Filter o) {
    logger.info("areSerializedFieldsEqual() method is called");
    if (o == this) return true;
    if (!(o instanceof CustomFilter)) return false;

    CustomFilter other = (CustomFilter)o;

    return (Bytes.equals(this.prefix, other.prefix) && Bytes.equals(this.cf, other.cf) &&
            Bytes.equals(this.cq, other.cq) && Bytes.equals(this.value, other.value));
  }

  private String cellToString(Cell cell) {
    StringBuilder sb = new StringBuilder();
    sb.append(new String(CellUtil.cloneRow(cell))).append(", ")
            .append(new String(CellUtil.cloneFamily(cell))).append(", ")
            .append(new String(CellUtil.cloneQualifier(cell))).append(", ")
            .append(new String(CellUtil.cloneValue(cell))).append(", ");
    return sb.toString();
  }
}
