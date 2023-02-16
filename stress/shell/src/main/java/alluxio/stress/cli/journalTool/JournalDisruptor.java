package alluxio.stress.cli.journalTool;

import alluxio.master.block.BlockId;
import alluxio.proto.journal.File;
import alluxio.proto.journal.Journal;

/**
 * Disruptor should be like a gun, the EntryStream like the magazine, and the entry like bullet
 */
public class JournalDisruptor {
  long mDisruptStep;
  JournalReader mReader;
  long mStepCounter;
  int mEntryType;
  Journal.JournalEntry mEntry;
  Journal.JournalEntry mHoldEntry;
  boolean mHoldFlag;
  long mHoldEntrySequenceNumber;

  public JournalDisruptor(JournalReader reader, long step, int type) {
    mReader = reader;
    mDisruptStep = step;
    mEntryType = type;
    mHoldFlag = false;
  }

  /**
   * the Disrupt() do every thing.
   * The journal tool should lay down after call the Disrupt()
   * Here will read, disrupt, and write
   *
   * TODO: add writer to Disruptor inorder to complete the Disrupt()
   */
  public void Disrupt() {
    Journal.JournalEntry entry;
    while ((entry = nextEntry()) != null) {
      System.out.println("entry: " + entry);
      writeEntry(entry.toBuilder().clearSequenceNumber().build());
    }
  }

  /**
   * Here ought to do the order disruption
   *
   * Hold entry if the entry is target type, and return next type
   * If it comes to the end of the Stream, return the hold entry (could be null)
   * @return
   */
  public Journal.JournalEntry nextEntry() {
    if (mHoldFlag && mStepCounter == 0) {
      mEntry = mHoldEntry;
      mHoldEntry = null;
      mHoldFlag = false;
      mStepCounter = mDisruptStep;
      return mEntry;
    }

    if ((mEntry = mReader.nextEntry()) != null) {
      if (mHoldFlag) {
        mStepCounter -= 1;
        return mEntry;
      }
      if (targetEntry(mEntry)) {
        mHoldFlag = true;
        // writer will set SN, no need to worry
        // mHoldEntrySequenceNumber = mEntry.getSequenceNumber();
        mHoldEntry = mEntry;
        return nextEntry();
      }
      return mEntry;
    }
    return mHoldEntry;
  }

  /**
   * Temporary nextEntry(), inside define the disrupt action
   * @return JournalEntry in target order
   */
  public Journal.JournalEntry test() {
    if ((mEntry = mReader.nextEntry()) != null) {
      if (targetEntry(mEntry)) {
        // return mEntry.toBuilder().setUpdateInode(File.UpdateInodeEntry.newBuilder().setId(BlockId.createBlockId(mEntry.getUpdateInode().getId()+100, BlockId.getMaxSequenceNumber())).build()).build();
        return mEntry.toBuilder().setUpdateInode(mEntry.getUpdateInode().toBuilder().setId(mEntry.getUpdateInode().getId()+100).build()).build();
      }
    }
    return mEntry;
  }

  /**
   * the targetEntry can only handle the hard coded entry type below
   * @param entry
   * @return true if the given entry has the target entry type
   */
  private boolean targetEntry(Journal.JournalEntry entry) {
    int type = -1;
    if (entry.hasDeleteFile()) {
      type = 0;
    } else if (entry.hasInodeDirectory()) {
      type = 1;
    } else if (entry.hasInodeFile()) {
      type = 2;
    } else if (entry.hasNewBlock()) {
      type = 3;
    } else if (entry.hasRename()) {
      type = 4;
    } else if (entry.hasSetAcl()) {
      type = 5;
    } else if (entry.hasUpdateInode()) {
      type = 6;
    } else if (entry.hasUpdateInodeDirectory()) {
      type = 7;
    } else if (entry.hasUpdateInodeFile()) {
      type = 8;
      // Deprecated entries
    } else if (entry.hasAsyncPersistRequest()) {
      type = 9;
    } else if (entry.hasCompleteFile()) {
      type = 10;
    } else if (entry.hasInodeLastModificationTime()) {
      type = 11;
    } else if (entry.hasPersistDirectory()) {
      type = 12;
    } else if (entry.hasSetAttribute()) {
      type = 13;
    } else if (entry.hasInodeDirectoryIdGenerator()) {
      type = 14;
    } else if (entry.hasDeleteFile()) {
      type = 15;
    } else {
      type = -1;
    }
    return type == mEntryType;
  }

  /**
   * TODO: need writer
   * @param entry
   */
  private void writeEntry(Journal.JournalEntry entry) {

  }
}
