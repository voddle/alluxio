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

package alluxio.master.metastore.rocks;

import alluxio.master.metastore.ReadOption;

import org.junit.rules.TemporaryFolder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@Fork(value = 3)
@Warmup(iterations = 3)
public class RocksInodeStoreBench {
  @State(Scope.Benchmark)
  public static class RockState {
    protected RocksInodeStore mRock;

    @Param("10000")
    protected long mInodeCount;

    @Setup
    public void before() {
      TemporaryFolder sFolder = new TemporaryFolder();
      try {
        sFolder.create();
        String sBaseDir = sFolder.newFolder().getAbsolutePath();
        System.out.println("sBaseDir: " + sBaseDir);
        mRock = new RocksInodeStore(sBaseDir);
      } catch (Exception e) {
        System.out.println("error when getting base dir: " + e);
      }
      for (long i = 1; i <= mInodeCount; i++) {
        mRock.addChild(0, "test" + i, i);
      }
    }

    @TearDown
    public void after() {
      mRock.close();
    }
  }

  @State(Scope.Benchmark)
  public static class RockAddChildState {
    protected RocksInodeStore mRock;

    @Param("10000")
    protected long mInodeCount;

    @Setup
    public void before() {
      TemporaryFolder sFolder = new TemporaryFolder();
      try {
        sFolder.create();
        String sBaseDir = sFolder.newFolder().getAbsolutePath();
        System.out.println("sBaseDir: " + sBaseDir);
        mRock = new RocksInodeStore(sBaseDir);
      } catch (Exception e) {
        System.out.println("error when getting base dir: " + e);
      }
    }

    @TearDown
    public void after() {
      mRock.close();
    }
  }

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Benchmark
  public long RockGetMutableBench(RockState rs) {
    long counter = 0;
    for (long i = rs.mInodeCount; i > 0; i--) {
      rs.mRock.getMutable(i);
      counter += 1;
    }
    return counter;
  }

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Benchmark
  public long RockGetChildIdBench(RockState rs) {
    long counter = 0;
    for (long i = rs.mInodeCount; i > 0; i--) {
      rs.mRock.getChildId(i, "test" + i, ReadOption.defaults());
      counter += 1;
    }
    return counter;
  }

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Benchmark
  public long RockAddChildIdBench(RockAddChildState rs) {
    long counter = 0;
    long limit = rs.mInodeCount;
    for (long i = 1; i <= limit; i++) {
      rs.mRock.addChild(0, "test" + i, i);
      counter += 1;
    }
    return counter;
  }
}
