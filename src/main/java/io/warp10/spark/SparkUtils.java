package io.warp10.spark;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Row;

import scala.Product;
import scala.collection.Iterator;
import scala.collection.Iterable;

public class SparkUtils {
  public static Object fromSpark(Object o) {
    if (null == o) {
      return null;
    } else if (o instanceof String) {
      return o;
    } else if (o instanceof byte[]) {
      return o;
    } else if (o instanceof BigInteger || o instanceof Long || o instanceof Integer || o instanceof Byte) {
      return ((Number) o).longValue();
    } else if (o instanceof BigDecimal || o instanceof Double || o instanceof Float) {
      return ((Number) o).doubleValue();
    } else if (o instanceof Product) {
      Product prod = (Product) o;
      
      List<Object> list = new ArrayList<Object>();
      scala.collection.Iterator<Object> iter = prod.productIterator();
      
      while(iter.hasNext()) {
        list.add(fromSpark(iter.next()));
      }
      
      return list;
    } else if (o instanceof List) {
      List<Object> l = new ArrayList<Object>();
      for (Object elt: (List) o) {
        l.add(fromSpark(elt));
      }
      return l;
    } else if (o instanceof Iterator || o instanceof Iterable) {
      final Iterator<Object> siter = o instanceof Iterator ? (Iterator<Object>) o : ((Iterable<Object>) o).iterator();
      return new java.util.Iterator<Object>() {
        @Override
        public boolean hasNext() {
          return siter.hasNext();
        }
        @Override
        public Object next() {
          return siter.next();
        }
      };
    } else if (o instanceof Row) {
      List<Object> l = new ArrayList<Object>(((Row) o).size());      
      Row row = (Row) o;

      for (int i = 0; i < row.size(); i++) {
        l.add(fromSpark(row.get(i)));
      }
      return l;
    } else {
      return o;
      //throw new RuntimeException("Encountered yet unsupported type: " + o.getClass());
    }
  }
  
  public static Object toSpark(Object o) {
    if (null == o) {
      return null;
    } else if (o instanceof String) {
      return o;
    } else if (o instanceof byte[]) {
      return o;
    } else if (o instanceof Number) {
      return o;
    } else if (o instanceof List) {
      ArrayList<Object> l = new ArrayList<Object>();
      
      for (Object elt: (List) o) {
        l.add(toSpark(elt));
      }
      
      return l;
    } else if (o instanceof Iterator) {
      return null;
    } else {
      return o;
      //throw new RuntimeException("Encountered yet unsupported type: " + o.getClass());
    }
  }

}
