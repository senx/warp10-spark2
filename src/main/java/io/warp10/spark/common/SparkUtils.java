package io.warp10.spark.common;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Charsets;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.script.WarpScriptException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.spark.SparkFiles;
import org.apache.spark.sql.Row;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import scala.Product;
import scala.collection.Iterator;

public class SparkUtils {
  public static Object fromSpark(Object o) {
    if (null == o) {
      return null;
    } else if (o instanceof String) {
      return o;
    } else if (o instanceof byte[]) {
      return o;
    } else if (o instanceof BytesWritable) {
      return ((BytesWritable) o).getBytes();
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
      for (Object elt : (List) o) {
        l.add(fromSpark(elt));
      }
      return l;
    } else if (o instanceof Row) {
      List<Object> l = new ArrayList<Object>(((Row) o).size());      
      Row row = (Row) o;
      for (int i = 0; i < row.size(); i++) {
        l.add(fromSpark(row.get(i)));
      }
      return l;
    } else if (o instanceof Iterator || o instanceof scala.collection.Iterable) {
      final Iterator<Object> siter = o instanceof Iterator ? (Iterator<Object>) o : ((scala.collection.Iterable<Object>) o).iterator();
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
    } else {
      return o;
      //throw new RuntimeException("Encountered yet unsupported type: " + o.getClass());
    }
  }

  /**
   * Parse Warpscript file and return its content as String
   * @param warpscriptFile name of the script to parse
   * @return String
   */
  public static String parseScript(String warpscriptFile) throws IOException, WarpScriptException {
    //
    // Load the WarpsScript file
    // Warning: provide target directory when file has been copied on each node
    //
    StringBuffer scriptSB = new StringBuffer();
    InputStream fis = null;
    BufferedReader br = null;
    try {      
      File f = new File(SparkFiles.get(warpscriptFile));
      if (!f.exists()) {
        fis = SparkUtils.class.getClassLoader().getResourceAsStream(warpscriptFile);
      } else {
        fis = new FileInputStream(f);
      }
      br = new BufferedReader(new InputStreamReader(fis, Charsets.UTF_8));

      while (true) {
        String line = br.readLine();
        if (null == line) {
          break;
        }
        scriptSB.append(line).append("\n");
      }
    } catch (IOException ioe) {
      throw new IOException("WarpScript file could not be loaded", ioe);
    } finally {
      if (null == br) { try { br.close(); } catch (Exception e) {} }
      if (null == fis) { try { fis.close(); } catch (Exception e) {} }
    }

    return scriptSB.toString();

  }

  /**
   * Dump GTSWrapper (String representation of a GTSWrapper)
   * @param wrapper
   * @param optimize
   * @result String representation of a GTSWrapper
   */
  public static String GTSDump(byte[] wrapper, boolean optimize) throws IOException {
    TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());

    GTSWrapper gtsWrapper = new GTSWrapper();

    try {
      deserializer.deserialize(gtsWrapper, (wrapper));
    } catch (TException te) {
      throw new IOException(te);
    }

    Metadata metadataChunk = new Metadata(gtsWrapper.getMetadata());

    GTSDecoder decoder = GTSWrapperHelper.fromGTSWrapperToGTSDecoder(gtsWrapper);

    //
    // Metadata as String
    //
    StringBuilder metasb = new StringBuilder();
    GTSHelper.metadataToString(metasb, metadataChunk.getName(), metadataChunk.getLabels());

    StringBuilder sb = new StringBuilder();

    boolean first = true;

    while(decoder.next()) {
      if (optimize && !first) {
        sb.append("=");
        sb.append(GTSHelper.tickToString(null, decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), decoder.getValue()));
      } else {
        sb.append(GTSHelper.tickToString(metasb, decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), decoder.getValue()));
        first = false;
      }
      sb.append("\n");
    }
    return sb.toString();
  }


}
