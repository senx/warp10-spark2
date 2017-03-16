package io.warp10.spark.common;

import io.warp10.continuum.Configuration;
import io.warp10.crypto.SipHashInline;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptExecutor;
import io.warp10.script.WarpScriptExecutor.StackSemantics;
import org.apache.spark.SparkFiles;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

public abstract class WarpScriptAbstractFunction implements Serializable {

  //
  // variable to register Warpscript filename
  //
  public static final String WARPSCRIPT_FILE_VARIABLE = "warpscript.file";

  //
  // variable to register hash computed onto Warpscript commands
  //
  public static final String WARPSCRIPT_ID_VARIABLE = "warpscript.id";
  
  private StackSemantics semantics;

  //
  // WarpScriptExecutor
  //

  protected WarpScriptExecutor executor = null;

  //
  // Default timeunits
  // ns means nanoseconds
  // us means microseconds
  // ms means milliseconds
  //

  public static final String DEFAULT_TIME_UNITS_PER_MS = "us";

  /**
   * For keys above 1024 characters, we'll use the hash instead
   */
  private static final int EXECUTOR_MAX_KEY_SIZE = 1024;
  private static final int EXECUTOR_CACHE_SIZE = 128;

  //
  // Hash key to compute Sip64 hash on MC2 script
  //

  protected static long[] SIPKEY_SCRIPT =  new long[] {0xF117F9642AF54BAEL, 0x80D1E8A854D22E42L};

  //
  // Hash key to compute Sip64 hash on data
  //

  protected static long[] SIPKEY_UUID =  new long[] {0xF102F5622CF54CAEL, 0x1217A4C4BC129A21L};

  private static final Map<Object,WarpScriptExecutor> executors = new LinkedHashMap<Object, WarpScriptExecutor>(100, 0.75F, true) {
    @Override
    protected boolean removeEldestEntry(java.util.Map.Entry<Object, WarpScriptExecutor> eldest) {
      return this.size() > EXECUTOR_CACHE_SIZE;
    }
  };

  public WarpScriptAbstractFunction() {
      this.semantics = StackSemantics.PERTHREAD;
  }

  public WarpScriptAbstractFunction(String... args) throws WarpScriptException {
    if (0 == args.length) {
      throw new IllegalArgumentException("At least one parameter is required: Warpscript code");
    } else if ( 1 == args.length) {
      this.semantics = StackSemantics.PERTHREAD;
      setCode(args[0]);
    } else {
      setCode(args[0]);
      this.semantics = StackSemantics.valueOf(args[1]);
      if (args.length > 2) {
        for (int i = 1; i < args.length; i++) {
          String[] tokens = args[i].split("=");
          System.setProperty(tokens[0], tokens[1]);
        }
      } else {
        System.setProperty(Configuration.WARP_TIME_UNITS, DEFAULT_TIME_UNITS_PER_MS);
      }
    }
  }

  public void setCode(String code) throws WarpScriptException {
    init(code);
  }

  private void writeObject(java.io.ObjectOutputStream out) throws IOException {
    out.writeUTF(this.semantics.toString());
    out.writeObject(this.executor);
  }
  
  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    this.semantics = StackSemantics.valueOf(in.readUTF());
    this.executor = (WarpScriptExecutor)in.readObject();
  }

  private void init(String code) throws WarpScriptException {

    //
    // Compute hash of mc2
    //

    Object key = code;

    if (code.length() > EXECUTOR_MAX_KEY_SIZE) {
      byte[] keyHash = code.getBytes(StandardCharsets.UTF_8);
      key = SipHashInline.hash24(SIPKEY_SCRIPT[0], SIPKEY_SCRIPT[1], keyHash, 0, keyHash.length);
    }

    //
    // Check if we have an executor for this hash
    //

    WarpScriptExecutor executor = executors.get(key);

    try {
      if (null == executor) {
        byte[] keyHash = code.getBytes(StandardCharsets.UTF_8);
        long hash = SipHashInline
            .hash24(SIPKEY_SCRIPT[0], SIPKEY_SCRIPT[1], keyHash, 0,
                keyHash.length);

        synchronized (executors) {
          if (code.startsWith("@")) {

            //
            // delete the @ character
            //

            String originalfilePath = code.substring(1);

            //
            // addFile has to be done on the local fs (on the Driver side) to propagate the file.
            // Then we have to retrieve the real path with SparkFiles.get()
            //

            //
            // Keep only the filename (filepath is related to the original FS)
            // Target directory on each one is dynamic
            //
            String filename = Paths.get(originalfilePath).getFileName().toString();

            //
            // Compute (dynamic) filepath
            //
            String filePath = SparkFiles.get(filename);

            String mc2FileContent = "'" + filePath + "' '" + WARPSCRIPT_FILE_VARIABLE + "' STORE " + SparkUtils.parseScript(filePath);

            executor = new WarpScriptExecutor(this.semantics, mc2FileContent, null, null);
          } else {

            //
            // String with Warpscript commands
            //

            //
            // Compute the hash against String content to identify this run
            //

            String mc2Content = "'" + String.valueOf(hash) + "' '" + WARPSCRIPT_ID_VARIABLE + "' STORE " + code;

            executor = new WarpScriptExecutor(this.semantics, mc2Content, null, null);
          }

          executors.put(key, executor);
          this.executor = executor;
        }

      }
    } catch (IOException ioe) {
      throw new WarpScriptException(ioe);
    }
  }

}
