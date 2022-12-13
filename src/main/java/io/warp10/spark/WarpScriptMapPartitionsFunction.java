//
//   Copyright 2022  SenX S.A.S.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

package io.warp10.spark;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;

import io.warp10.hadoop.WritableUtils;
import io.warp10.script.WarpScriptException;
import io.warp10.spark.common.SparkUtils;
import io.warp10.spark.common.WarpScriptAbstractFunction;
import scala.collection.convert.Wrappers;
import scala.collection.convert.Wrappers.IteratorWrapper;

public class WarpScriptMapPartitionsFunction<T,U> extends WarpScriptAbstractFunction implements MapPartitionsFunction<T, U>, FlatMapFunction<T, U> {

  public WarpScriptMapPartitionsFunction(String code) throws WarpScriptException {
    super(code);
  }

  @Override
  public Iterator<U> call(T t) throws Exception {
    Iterator<T> iter = (Iterator<T>) t;
    return call(iter);
  }

  @Override
  public Iterator<U> call(Iterator<T> t) throws Exception {
    synchronized(this) {
      return new Iterator<U>() {
        final List<Object> records = new ArrayList<Object>();
        int recordidx = 0;
        boolean done = false;

        final Iterator<T> iter = t;

        @Override
        public boolean hasNext() {
          // There is at least one element in 'buffer'
          if (records.size() != recordidx) {
            return true;
          }

          if (done) {
            return false;
          }

          fillBuffer();

          return hasNext();
        }

        @Override
        public U next() {
          if (records.size() != recordidx) {
            U record = (U) records.get(recordidx++);
            if (records.size() == recordidx) {
              records.clear();
              recordidx = 0;
            }
            return record;
          }

          if (done) {
            throw new NoSuchElementException();
          }

          fillBuffer();

          return next();
        }

        private void fillBuffer() throws RuntimeException {
          //
          // Request the next record from the input iterator
          // and pass it to the WarpScript code until
          // the code actually returns records or we are done.
          //

          while(!done) {
            boolean hasInput = iter.hasNext();

            if (hasInput) {
              List<Object> input = new ArrayList<Object>();

              // This is not the last K/V we feed to the executor
              input.add(done);
              input.add(SparkUtils.fromSpark(iter.next()));

              try {
                List<Object> results = executor.exec(input);

                // If there are no results on the stack, continue
                // calling the input iterator

                if (results.isEmpty()) {
                  continue;
                }

                // push the records onto 'records', the deepest first,
                // ensuring each is a [ key value ] pair
                for (int i = results.size() - 1; i >= 0; i--) {
                  Object result = results.get(i);
                  records.add(SparkUtils.toSpark(result));
                }
              } catch (WarpScriptException wse) {
                throw new RuntimeException(wse);
              }
            } else {
              done = true;
              // Call the WarpScript with true on top of the stack, meaning
              // we reached the end of the input

              List<Object> input = new ArrayList<Object>();

              // This is the last call for this partition
              input.add(true);

              try {
                List<Object> results = executor.exec(input);

                // If there are no results on the stack, return
                if (results.isEmpty()) {
                  return;
                }

                // push the records onto 'records', the deepest first,
                // ensuring each is a [ key value ] pair
                for (int i = results.size() - 1; i >= 0; i--) {
                  Object result = results.get(i);
                  records.add(SparkUtils.toSpark(result));
                }
              } catch (WarpScriptException wse) {
                throw new RuntimeException(wse);
              }
            }
          }
        }
      };
    }
  }

  public static WarpScriptMapPartitionsFunction getInstance(String code) throws WarpScriptException {
    return new WarpScriptMapPartitionsFunction<Row,Row>(code);
  }
}
