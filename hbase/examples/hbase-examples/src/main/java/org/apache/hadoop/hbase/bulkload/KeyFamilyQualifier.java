package org.apache.hadoop.hbase.bulkload;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Yu Liebing
 */
public class KeyFamilyQualifier implements WritableComparable<KeyFamilyQualifier> {

  private byte[] rowKey;
  private byte[] family;
  private byte[] qualifier;

  public KeyFamilyQualifier(byte[] rowKey, byte[] family, byte[] qualifier) {
    this.rowKey = rowKey;
    this.family = family;
    this.qualifier = qualifier;
  }

  @Override
  public int compareTo(@Nonnull KeyFamilyQualifier other) {
    int rowCmp = Bytes.compareTo(this.rowKey, other.rowKey);
    if (rowCmp == 0) {
      int familyCmp = Bytes.compareTo(this.family, other.family);
      if (familyCmp == 0) {
        return Bytes.compareTo(this.family, other.family);
      }
      return rowCmp;
    }
    return rowCmp;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(rowKey.length); dataOutput.write(rowKey);
    dataOutput.writeInt(family.length); dataOutput.write(family);
    dataOutput.writeInt(qualifier.length); dataOutput.write(qualifier);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    int rowKeyLength = dataInput.readInt();
    this.rowKey = new byte[rowKeyLength];
    dataInput.readFully(this.rowKey, 0, rowKeyLength);

    int familyLength = dataInput.readInt();
    this.family = new byte[familyLength];
    dataInput.readFully(this.family, 0, familyLength);

    int qualifierLength = dataInput.readInt();
    this.qualifier = new byte[qualifierLength];
    dataInput.readFully(this.qualifier, 0, qualifierLength);
  }

  public byte[] getRowKey() {
    return rowKey;
  }

  public byte[] getFamily() {
    return family;
  }

  public byte[] getQualifier() {
    return qualifier;
  }
}
