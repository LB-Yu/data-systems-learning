package org.apache.hbase.learning.filter;

import java.io.File;

public class Test {

  public static void main(String[] args) {
    File file = new File("/home/liebing/Code/data_systems_learning/hbase-learning/hbase1/src/main/java/org/apache/hbase/learning/filter/FilterExample.java");
    System.out.println(file.getParent());
    System.out.println(file.getName());

  }
}
