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

package alluxio.stress.cli.journaldisruptor;

import alluxio.master.journal.JournalEntryStreamReader;
import alluxio.master.journal.raft.RaftJournalSystem;
import alluxio.master.journal.raft.RaftJournalUtils;
import alluxio.proto.journal.Journal;

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.raftlog.segmented.LogSegmentPath;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogInputStream;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.storage.StorageImplUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * the raft version of the EntryStream.
 */
public class RaftJournalEntryStream extends EntryStream {

  private List<LogSegmentPath> mPaths;
  private int mCurrentPathIndex = 0;
  private SegmentedRaftLogInputStream mIn = null;
  private RaftProtos.LogEntryProto mProto = null;
  private InputStream mStream;
  private JournalEntryStreamReader mReader;
  private byte[] mBuffer = new byte[4096];

  private RaftProtos.LogEntryProto mNextProto;
  private Journal.JournalEntry mEntry;
  private List<Journal.JournalEntry> mList = new ArrayList<>();
  private int mIndex = 0;

  /**
   * init RaftJournalEntryStream.
   * @param master
   * @param start
   * @param end
   * @param inputDir
   */
  public RaftJournalEntryStream(String master, long start, long end, String inputDir) {
    super(master, start, end, inputDir);
    try (
        RaftStorage storage = StorageImplUtils.newRaftStorage(getJournalDir(),
            RaftServerConfigKeys.Log.CorruptionPolicy.getDefault(),
            RaftStorage.StartupOption.RECOVER,
            RaftServerConfigKeys.STORAGE_FREE_SPACE_MIN_DEFAULT.getSize())
      ) {
      storage.initialize();
      System.out.printf("storage is: %s", storage);
      mPaths = LogSegmentPath.getLogSegmentPaths(storage);
    } catch (Exception e) {
      // do sth
      System.out.println(e);
    }
  }

  /**
   * the nextEntry function of EntryStream
   * Because in Raft Journal one proto may contain several Alluxio entries, we
   * declare a List to record current entry list.
   * And every time when trying to return next entry the Stream will
   * check whether current entry list has been finished.
   * If not read one entry,
   * if yes read next proto till met the proto that contains the Alluxio entry.
   *
   * @returny next JournalEntry
   */
  @Override
  public Journal.JournalEntry nextEntry() {
    if (mIndex < mList.size()) {
      return mList.get(mIndex++);
    }
    while ((mNextProto = nextProto()) != null && !mNextProto.hasStateMachineLogEntry()) {}
    if (mNextProto == null) {
      // no proto, stream comes to the end
      return null;
    }
    try {
      mEntry = Journal.JournalEntry.parseFrom(
          mNextProto.getStateMachineLogEntry().getLogData().asReadOnlyByteBuffer());
    } catch (Exception e) {
      System.out.println("failed to parse proto to entry: " + e);
    }
    mList = mEntry.getJournalEntriesList();
    mIndex = 0;
    return nextEntry();
  }

  /**
   * since the non-log entry proto needs to be process/print by the journal tool,
   * here cannot integrate the process proto.
   *
   * non Alluxio entry was generated and wrote by RaftJournalWriter, no need to worry about
   *
   * Here check SegmentedRaftLogInputStream first,
   * if null read the path of next journal file and open the stream of it, till all paths were used
   *
   * the type of the proto was judged by the caller
   * @return all LogEntryProto
   */
  public RaftProtos.LogEntryProto nextProto() {
    // check current stream is ok, or open next stream. need a condition to stop and return null.
    if (mIn == null) {
      if (mCurrentPathIndex == mPaths.size()) {
        return null;
      }
      LogSegmentPath path = mPaths.get(mCurrentPathIndex);
      try {
        mIn = new SegmentedRaftLogInputStream(path.getPath().toFile(),
            path.getStartEnd().getStartIndex(),
            path.getStartEnd().getEndIndex(), path.getStartEnd().isOpen());
        mCurrentPathIndex += 1;
      } catch (Exception e) {
        // LOG sth, but how to init log with destination...
      }
    }

    try {
      mProto = mIn.nextEntry();
      if (mProto != null) {
        return mProto;
      } else {
        mIn = null;
        return nextProto();
      }
    } catch (Exception e) {
      System.out.println(e);
      throw new RuntimeException(e);
    }
  }

  /**
   * @return Journal Directory
   */
  private File getJournalDir() {
    return new File(RaftJournalUtils.getRaftJournalDir(new File(mInputDir)),
        RaftJournalSystem.RAFT_GROUP_UUID.toString());
  }

  @Override
  public void close() throws IOException {
    mReader.close();
  }
}
