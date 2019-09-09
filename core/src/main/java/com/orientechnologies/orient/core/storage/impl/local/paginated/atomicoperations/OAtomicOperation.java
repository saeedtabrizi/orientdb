package com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.OComponentOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;

import java.io.IOException;
import java.util.Set;

public interface OAtomicOperation {
  OOperationUnitId getOperationUnitId();

  OCacheEntry loadPageForWrite(long fileId, long pageIndex, boolean checkPinnedPages, int pageCount, boolean verifyChecksum)
      throws IOException;

  void addComponentOperation(OComponentOperationRecord componentOperation) throws IOException;

  OCacheEntry loadPageForRead(long fileId, long pageIndex, boolean checkPinnedPages, int pageCount) throws IOException;

  void addMetadata(OAtomicOperationMetadata<?> metadata);

  OAtomicOperationMetadata<?> getMetadata(String key);

  void addDeletedRidBag(OBonsaiBucketPointer rootPointer);

  Set<OBonsaiBucketPointer> getDeletedBonsaiPointers();

  OCacheEntry addPage(long fileId) throws IOException;

  void releasePageFromRead(OCacheEntry cacheEntry);

  void releasePageFromWrite(OCacheEntry cacheEntry) throws IOException;

  long filledUpTo(long fileId);

  long addFile(String fileName) throws IOException;

  long loadFile(String fileName) throws IOException;

  void deleteFile(long fileId) throws IOException;

  boolean isFileExists(String fileName);

  String fileNameById(long fileId);

  void truncateFile(long fileId) throws IOException;

  int getCounter();

  void incrementCounter();

  void decrementCounter();

  boolean containsInLockedObjects(String lockName);

  void addLockedObject(String lockName);

  void rollbackInProgress();

  boolean isRollbackInProgress();

  OLogSequenceNumber commitChanges(OWriteAheadLog writeAheadLog) throws IOException;

  Iterable<String> lockedObjects();

  void addDeletedRecordPosition(final int clusterId, final int pageIndex, final int recordPosition);

  Set<Integer> getBookedRecordPositions(final int clusterId, final int pageIndex);
}
