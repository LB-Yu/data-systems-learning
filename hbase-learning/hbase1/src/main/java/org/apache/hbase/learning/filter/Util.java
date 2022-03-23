package org.apache.hbase.learning.filter;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;

public class Util {

  public static String cellToString(Cell cell) {
    String row = new String(CellUtil.cloneRow(cell));
    String family = new String(CellUtil.cloneFamily(cell));
    String qualify = new String(CellUtil.cloneQualifier(cell));
    String value = new String(CellUtil.cloneValue(cell));
    return String.format("row key: %s, family: %s, qualify: %s, value: %s",
            row, family, qualify, value);
  }
}
