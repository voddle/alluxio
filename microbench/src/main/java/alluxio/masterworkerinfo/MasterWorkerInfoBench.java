/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.masterworkerinfo;

import alluxio.master.block.meta.MasterWorkerInfo;
import alluxio.master.block.meta.MasterWorkerInfoHashSet;
import alluxio.master.block.meta.MasterWorkerInfoLongHashSet;
import alluxio.master.block.meta.MasterWorkerInfoUnifiedSet;
import alluxio.wire.WorkerNetAddress;

import org.apache.lucene.util.RamUsageEstimator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.CommandLineOptionException;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

// @Fork(value = 5)
// @Warmup(iterations = 0)
// @Measurement(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
public class MasterWorkerInfoBench {

//     public class HashSetMasterWorkerInfo extends MasterWorkerInfo {
//
//
//
//         /**
//          * Creates a new instance of {@link MasterWorkerInfo}.
//          *
//          * @param id      the worker id to use
//          * @param address the worker address to use
//          */
//         public HashSetMasterWorkerInfo(long id, WorkerNetAddress address) {
//             super(id, address);
//         }
//     }

  @State(Scope.Benchmark)
  public static class AddBenchState {
    @Param({"100000", "500000", "1000000", "5000000", "10000000"})
    long mTest;
    public MasterWorkerInfo mMasterWorkerInfo;

    long mCounter = 0;

    // public long mTest = 0;

    @Setup
    public void before() {
      mMasterWorkerInfo = new MasterWorkerInfo(1, new WorkerNetAddress());
    }

    @TearDown
    public void after() {
      System.out.println(" ");
      System.out.println("Counter: " + mCounter);
      System.out.println("shallow: " + RamUsageEstimator.shallowSizeOf(mMasterWorkerInfo.mBlocks));
      System.out.println("Object: " + RamUsageEstimator.sizeOfObject(mMasterWorkerInfo.mBlocks));
      System.out.print("Throughput: ");
      mMasterWorkerInfo = null;
    }
  }

  @State(Scope.Benchmark)
  public static class AddHashBenchState {
    @Param({"100000", "500000", "1000000", "5000000", "10000000"})
    long mTest;
    public MasterWorkerInfoHashSet mMasterWorkerInfo;

    long mCounter = 0;

    // public long mTest = 0;

    @Setup
    public void before() {
      mMasterWorkerInfo = new MasterWorkerInfoHashSet(1, new WorkerNetAddress());
    }

    @TearDown
    public void after() {
      System.out.println(" ");
      System.out.println("Counter: " + mCounter);
      System.out.println("shallow: " + RamUsageEstimator.shallowSizeOf(mMasterWorkerInfo.mBlocks));
      System.out.println("Object: " + RamUsageEstimator.sizeOfObject(mMasterWorkerInfo.mBlocks));
      System.out.print("Throughput: ");
      mMasterWorkerInfo = null;
    }
  }

  @State(Scope.Benchmark)
  public static class AddUnifiedBenchState {
    @Param({"100000", "500000", "1000000", "5000000", "10000000"})
    long mTest;
    public MasterWorkerInfoUnifiedSet mMasterWorkerInfo;

    long mCounter = 0;

    // public long mTest = 0;

    @Setup
    public void before() {
      mMasterWorkerInfo = new MasterWorkerInfoUnifiedSet(1, new WorkerNetAddress());
    }

    @TearDown
    public void after() {
      System.out.println(" ");
      System.out.println("Counter: " + mCounter);
      System.out.println("shallow: " + RamUsageEstimator.shallowSizeOf(mMasterWorkerInfo.mBlocks));
      System.out.println("Object: " + RamUsageEstimator.sizeOfObject(mMasterWorkerInfo.mBlocks));
      System.out.print("Throughput: ");
      mMasterWorkerInfo = null;
    }
  }

  @State(Scope.Benchmark)
  public static class AddLongHashSetBenchState {
    @Param({"100000", "500000", "1000000", "5000000", "10000000"})
    long mTest;
    public MasterWorkerInfoLongHashSet mMasterWorkerInfo;

    long mCounter = 0;

    // public long mTest = 0;

    @Setup
    public void before() {
      mMasterWorkerInfo = new MasterWorkerInfoLongHashSet(1, new WorkerNetAddress());
    }

    @TearDown
    public void after() {
      System.out.println(" ");
      System.out.println("Counter: " + mCounter);
      System.out.println("shallow: " + RamUsageEstimator.shallowSizeOf(mMasterWorkerInfo.mBlocks));
      System.out.println("Object: " + RamUsageEstimator.sizeOfObject(mMasterWorkerInfo.mBlocks));
      System.out.print("Throughput: ");
      mMasterWorkerInfo = null;
    }
  }

  @State(Scope.Benchmark)
  public static class RemoveBenchState {
    @Param({"100000", "500000", "1000000", "5000000", "10000000"})
    long mTest;
    public MasterWorkerInfo mMasterWorkerInfo;

    long mCounter = 0;

    // public long mTest = 0;

    @Setup
    public void before() {
      mMasterWorkerInfo = new MasterWorkerInfo(1, new WorkerNetAddress());
      for (long i = mTest; i > 0; i--) {
        mMasterWorkerInfo.addBlock(i);
      }
    }

    @TearDown
    public void after() {
      System.out.println(" ");
      System.out.println("Counter: " + mCounter);
      System.out.println("shallow: " + RamUsageEstimator.shallowSizeOf(mMasterWorkerInfo.mBlocks));
      System.out.println("Object: " + RamUsageEstimator.sizeOfObject(mMasterWorkerInfo.mBlocks));
      System.out.print("Throughput: ");
      mMasterWorkerInfo = null;
    }
  }

  @State(Scope.Benchmark)
  public static class RemoveHashBenchState {
    @Param({"100000", "500000", "1000000", "5000000", "10000000"})
    long mTest;
    public MasterWorkerInfoHashSet mMasterWorkerInfo;

    long mCounter = 0;

    // public long mTest = 0;

    @Setup
    public void before() {
      mMasterWorkerInfo = new MasterWorkerInfoHashSet(1, new WorkerNetAddress());
      for (long i = mTest; i > 0; i--) {
        mMasterWorkerInfo.addBlock(i);
      }
    }

    @TearDown
    public void after() {
      System.out.println(" ");
      System.out.println("Counter: " + mCounter);
      System.out.println("shallow: " + RamUsageEstimator.shallowSizeOf(mMasterWorkerInfo.mBlocks));
      System.out.println("Object: " + RamUsageEstimator.sizeOfObject(mMasterWorkerInfo.mBlocks));
      System.out.print("Throughput: ");
      mMasterWorkerInfo = null;
    }
  }

  @State(Scope.Benchmark)
  public static class RemoveUnifiedBenchState {
    @Param({"100000", "500000", "1000000", "5000000", "10000000"})
    long mTest;
    public MasterWorkerInfoUnifiedSet mMasterWorkerInfo;

    long mCounter = 0;

    // public long mTest = 0;

    @Setup
    public void before() {
      mMasterWorkerInfo = new MasterWorkerInfoUnifiedSet(1, new WorkerNetAddress());
      for (long i = mTest; i > 0; i--) {
        mMasterWorkerInfo.addBlock(i);
      }
    }

    @TearDown
    public void after() {
      System.out.println(" ");
      System.out.println("Counter: " + mCounter);
      System.out.println("shallow: " + RamUsageEstimator.shallowSizeOf(mMasterWorkerInfo.mBlocks));
      System.out.println("Object: " + RamUsageEstimator.sizeOfObject(mMasterWorkerInfo.mBlocks));
      System.out.print("Throughput: ");
      mMasterWorkerInfo = null;
    }
  }

  @State(Scope.Benchmark)
  public static class RemoveLongHashSetBenchState {
    @Param({"100000", "500000", "1000000", "5000000", "10000000"})
    long mTest;
    public MasterWorkerInfoLongHashSet mMasterWorkerInfo;

    long mCounter = 0;

    // public long mTest = 0;

    @Setup
    public void before() {
      mMasterWorkerInfo = new MasterWorkerInfoLongHashSet(1, new WorkerNetAddress());
      for (long i = mTest; i > 0; i--) {
        mMasterWorkerInfo.addBlock(i);
      }
    }

    @TearDown
    public void after() {
      System.out.println(" ");
      System.out.println("Counter: " + mCounter);
      System.out.println("shallow: " + RamUsageEstimator.shallowSizeOf(mMasterWorkerInfo.mBlocks));
      System.out.println("Object: " + RamUsageEstimator.sizeOfObject(mMasterWorkerInfo.mBlocks));
      System.out.print("Throughput: ");
      mMasterWorkerInfo = null;
    }
  }

  @Fork(value = 3)
  @Warmup(iterations = 0)
  @Measurement(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Benchmark
  public long LongOpenHashSetAddBlockTest(AddBenchState bs) {
    // System.out.println("hello world");
    long i = bs.mTest;
    for (; i > 0; i--) {
      bs.mMasterWorkerInfo.addBlock(bs.mCounter);
      bs.mCounter += 1;
    }
    return bs.mCounter;
  }

  @Fork(value = 3)
  @Warmup(iterations = 0)
  @Measurement(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Benchmark
  public long LongOpenHashSetAddBlockHashTest(AddHashBenchState bs) {
    long i = bs.mTest;
    for (; i > 0; i--) {
      bs.mMasterWorkerInfo.addBlock(bs.mCounter);
      bs.mCounter += 1;
    }
    return bs.mCounter;
  }

  @Fork(value = 3)
  @Warmup(iterations = 0)
  @Measurement(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Benchmark
  public long LongOpenUnifiedSetAddBlockHashTest(AddUnifiedBenchState bs) {
    long i = bs.mTest;
    for (; i > 0; i--) {
      bs.mMasterWorkerInfo.addBlock(bs.mCounter);
      bs.mCounter += 1;
    }
    return bs.mCounter;
  }

  @Fork(value = 3)
  @Warmup(iterations = 0)
  @Measurement(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Benchmark
  public long LongHashSetAddBlockTest(AddLongHashSetBenchState bs) {
    // System.out.println("hello world");
    long i = bs.mTest;
    for (; i > 0; i--) {
      bs.mMasterWorkerInfo.addBlock(bs.mCounter);
      bs.mCounter += 1;
    }
    return bs.mCounter;
  }

  @Fork(value = 3)
  @Warmup(iterations = 0)
  @Measurement(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
  @Benchmark
  public long LongOpenHashSetAddBlockZeroTest(AddBenchState bs) {
    // System.out.println("hello world");
    bs.mMasterWorkerInfo.addBlock(0);
    bs.mCounter += 1;
    return bs.mCounter;
  }

  @Fork(value = 3)
  @Warmup(iterations = 0)
  @Measurement(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Benchmark
  public long LongOpenHashSetRemoveBlockTest(RemoveBenchState bs) {
    long i = bs.mTest;
    for (; i > 0; i--) {
      bs.mMasterWorkerInfo.removeBlockFromWorkerMeta(i);
    }
    return i;
  }

  @Fork(value = 3)
  @Warmup(iterations = 0)
  @Measurement(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Benchmark
  public long LongOpenHashSetRemoveHashBlockTest(RemoveHashBenchState bs) {
    long i = bs.mTest;
    for (; i > 0; i--) {
      bs.mMasterWorkerInfo.removeBlockFromWorkerMeta(i);
    }
    return i;
  }

  @Fork(value = 3)
  @Warmup(iterations = 0)
  @Measurement(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Benchmark
  public long LongOpenUnifiedSetRemoveHashBlockTest(RemoveUnifiedBenchState bs) {
    long i = bs.mTest;
    for (; i > 0; i--) {
      bs.mMasterWorkerInfo.removeBlockFromWorkerMeta(i);
    }
    return i;
  }

  @Fork(value = 3)
  @Warmup(iterations = 0)
  @Measurement(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Benchmark
  public long LongHashSetRemoveBlockTest(RemoveLongHashSetBenchState bs) {
    long i = bs.mTest;
    for (; i > 0; i--) {
      bs.mMasterWorkerInfo.removeBlockFromWorkerMeta(i);
    }
    return i;
  }

  @Fork(value = 3)
  @Warmup(iterations = 0)
  @Measurement(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Benchmark
  public long LongOpenEnumerationTest(RemoveBenchState bs) {
    long t = 0;
    long i = bs.mTest;
    for (long x: bs.mMasterWorkerInfo.getBlocksNoCopy()) {
      t = x;
    }
    return t;
  }

  @Fork(value = 3)
  @Warmup(iterations = 0)
  @Measurement(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Benchmark
  public long LongOpenHashSetEnumerationTest(RemoveHashBenchState bs) {
    long t = 0;
    long i = bs.mTest;
    for (long x: bs.mMasterWorkerInfo.getBlocksNoCopy()) {
      t = x;
    }
    return t;
  }

  @Fork(value = 3)
  @Warmup(iterations = 0)
  @Measurement(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Benchmark
  public long LongOpenUnifiedSetEnumerationTest(RemoveUnifiedBenchState bs) {
    long t = 0;
    long i = bs.mTest;
    for (long x: bs.mMasterWorkerInfo.getBlocksNoCopy()) {
      t = x;
    }
    return t;
  }

  @Fork(value = 3)
  @Warmup(iterations = 0)
  @Measurement(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Benchmark
  public long LongHashSetEnumerationTest(RemoveLongHashSetBenchState bs) {
    long t = 0;
    long i = bs.mTest;
    // for (long x: bs.mMasterWorkerInfo.getBlocksNoCopy()) {
    //   t = x;
    // }
    bs.mMasterWorkerInfo.goThrough();
    return t;
  }

  public static void main(String[] args) throws RunnerException, CommandLineOptionException {
    Options argsCli = new CommandLineOptions(args);
    Options opts = new OptionsBuilder()
            .parent(argsCli)
            .include(MasterWorkerInfoBench.class.getName())
            .result("results.json")
            .resultFormat(ResultFormatType.JSON)
            .build();
    new Runner(opts).run();
  }
}
