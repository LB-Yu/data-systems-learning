package org.apache.hbase.learning.filter;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

import static org.apache.hbase.learning.filter.Util.cellToString;

/**
 * Custom filter example.
 * */
public class CustomFilter extends FilterBase {

  private static final Logger logger = LoggerFactory.getLogger(CustomFilter.class);

  private boolean filterRow = true;
  private boolean passedPrefix = false;
  private final byte[] prefix;
  private final byte[] cf;
  private final byte[] cq;
  private final byte[] value;

  public CustomFilter(final byte[] prefix, final byte[] cf, final byte[] cq,
                      final byte[] value) {
    logger.info("Constructor method is called, row prefix: " + new String(prefix) +
            ", column family: " + new String(cf) + ", column qualifier: " +
            new String(cq) + ", value " + new String(value));
    this.prefix = prefix;
    this.cf = cf;
    this.cq = cq;
    this.value = value;
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
    return ReturnCode.INCLUDE_AND_NEXT_COL;
  }

  @Override
  public Cell transformCell(Cell cell) {
    logger.info("transformCell() method is called, cell: " + cellToString(cell));
    return cell;
  }

  @Override
  public void filterRowCells(List<Cell> list) {
    logger.info("filterRowCells() method is called, size: " + list.size());
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
    int len = prefix.length + cf.length + cq.length + value.length + 4 * 4;
    ByteBuffer bf = ByteBuffer.allocate(len);
    bf.putInt(prefix.length);
    bf.put(prefix);
    bf.putInt(cf.length);
    bf.put(cf);
    bf.putInt(cq.length);
    bf.put(cq);
    bf.putInt(value.length);
    bf.put(value);
    return bf.array();
  }

  public static Filter parseFrom(final byte[] bytes) {
    logger.info("parseFrom() method is called");
    int len;
    ByteBuffer bf = ByteBuffer.wrap(bytes);
    len = bf.getInt();
    byte[] prefix = new byte[len];
    bf.get(prefix);
    len = bf.getInt();
    byte[] cf = new byte[len];
    bf.get(cf);
    len = bf.getInt();
    byte[] cq = new byte[len];
    bf.get(cq);
    len = bf.getInt();
    byte[] value = new byte[len];
    bf.get(value);
    return new CustomFilter(prefix, cf, cq, value);
  }
}
