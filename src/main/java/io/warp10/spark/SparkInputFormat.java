//
//   Copyright 2023  SenX S.A.S.
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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class SparkInputFormat extends InputFormat<Object, Object> {

  private InputFormat wrappedInputFormat = null;

  private static final String SPARK_INPUTFORMAT_CLASS = "spark.inputformat.class";

  static {
    Warp10Spark.init();
  }

  @Override
  public RecordReader<Object, Object> createRecordReader(InputSplit split, TaskAttemptContext attempt) throws IOException, InterruptedException {
    return getWrappedInputFormat(attempt.getConfiguration()).createRecordReader(split, attempt);
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException, InterruptedException {
    return getWrappedInputFormat(job.getConfiguration()).getSplits(job);
  }

  private InputFormat getWrappedInputFormat(Configuration conf) throws IOException {
    if (null == this.wrappedInputFormat) {
      try {
        String cls = conf.get(SPARK_INPUTFORMAT_CLASS);
        Class innerClass = Class.forName(cls);
        this.wrappedInputFormat = (InputFormat) innerClass.newInstance();
      } catch (Throwable t) {
        throw new IOException(t);
      }
    }
    return this.wrappedInputFormat;
  }
}
