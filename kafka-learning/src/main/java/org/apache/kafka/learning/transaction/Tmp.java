package org.apache.kafka.learning.transaction;

import org.apache.kafka.common.utils.Utils;

public class Tmp {

    public static void main(String[] args) {
        System.out.println(Utils.murmur2("A".getBytes()) % 3);
        System.out.println(Utils.murmur2("B".getBytes()) % 3);
        System.out.println(Utils.murmur2("C".getBytes()) % 3);
    }
}
