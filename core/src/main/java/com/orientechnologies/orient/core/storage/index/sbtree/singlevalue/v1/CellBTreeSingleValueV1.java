/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v1;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.exception.NotEmptyComponentCanNotBeRemovedException;
import com.orientechnologies.orient.core.exception.OTooBigIndexKeyException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OAlwaysGreaterKey;
import com.orientechnologies.orient.core.index.OAlwaysLessKey;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.engine.OBaseIndexEngine;
import com.orientechnologies.orient.core.iterator.OEmptyIterator;
import com.orientechnologies.orient.core.iterator.OEmptyMapEntryIterator;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.cellbtreesinglevalue.OCellBTreeSingleValuePutCO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.cellbtreesinglevalue.OCellBTreeSingleValueRemoveCO;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.OCellBTreeSingleValue;

import java.io.IOException;
import java.util.*;

/**
 * This is implementation which is based on B+-tree implementation threaded tree. The main differences are:
 * <ol>
 * <li>Buckets are not compacted/removed if they are empty after deletion of item. They reused later when new items are added.</li>
 * <li>All non-leaf buckets have links to neighbor buckets which contain keys which are less/more than keys contained in current
 * bucket</li> <ol/> There is support of null values for keys, but values itself cannot be null. Null keys support is switched off
 * by default if null keys are supported value which is related to null key will be stored in separate file which has only one page.
 * Buckets/pages for usual (non-null) key-value entries can be considered as sorted array. The first bytes of page contains such
 * auxiliary information as size of entries contained in bucket, links to neighbors which contain entries with keys less/more than
 * keys in current bucket. The next bytes contain sorted array of entries. Array itself is split on two parts. First part is growing
 * from start to end, and second part is growing from end to start. First part is array of offsets to real key-value entries which
 * are stored in second part of array which grows from end to start. This array of offsets is sorted by accessing order according to
 * key value. So we can use binary search to find requested key. When new key-value pair is added we append binary presentation of
 * this pair to the second part of array which grows from end of page to start, remember value of offset for this pair, and find
 * proper position of this offset inside of first part of array. Such approach allows to minimize amount of memory involved in
 * performing of operations and as result speed up data processing.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 8/7/13
 */
public final class CellBTreeSingleValueV1<K> extends ODurableComponent implements OCellBTreeSingleValue<K> {
  private static final int               MAX_KEY_SIZE       = OGlobalConfiguration.SBTREE_MAX_KEY_SIZE.getValueAsInteger();
  private static final OAlwaysLessKey    ALWAYS_LESS_KEY    = new OAlwaysLessKey();
  private static final OAlwaysGreaterKey ALWAYS_GREATER_KEY = new OAlwaysGreaterKey();

  private static final int MAX_PATH_LENGTH = OGlobalConfiguration.SBTREE_MAX_DEPTH.getValueAsInteger();

  private static final int                   ENTRY_POINT_INDEX = 0;
  private static final long                  ROOT_INDEX        = 1;
  private final        Comparator<? super K> comparator        = ODefaultComparator.INSTANCE;

  private final String               nullFileExtension;
  private       long                 fileId;
  private       long                 nullBucketFileId = -1;
  private       int                  keySize;
  private       OBinarySerializer<K> keySerializer;
  private       OType[]              keyTypes;
  private       OEncryption          encryption;

  private final int indexId;

  public CellBTreeSingleValueV1(final String name, int indexId, final String dataFileExtension, final String nullFileExtension,
      final OAbstractPaginatedStorage storage) {
    super(storage, name, dataFileExtension, name + dataFileExtension);
    this.indexId = indexId;
    acquireExclusiveLock();
    try {
      this.nullFileExtension = nullFileExtension;
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void create(final OBinarySerializer<K> keySerializer, final OType[] keyTypes, final int keySize,
      final OEncryption encryption) throws IOException {
    assert keySerializer != null;
    boolean rollback = false;
    final OAtomicOperation atomicOperation = startAtomicOperation(false);
    try {
      acquireExclusiveLock();
      try {

        this.keySize = keySize;
        if (keyTypes != null) {
          this.keyTypes = Arrays.copyOf(keyTypes, keyTypes.length);
        } else {
          this.keyTypes = null;
        }

        this.encryption = encryption;
        this.keySerializer = keySerializer;

        fileId = addFile(atomicOperation, getFullName());
        nullBucketFileId = addFile(atomicOperation, getName() + nullFileExtension);

        final OCacheEntry entryPointCacheEntry = addPage(atomicOperation, fileId);
        try {
          final CellBTreeSingleValueEntryPointV1 entryPoint = new CellBTreeSingleValueEntryPointV1(entryPointCacheEntry);
          entryPoint.init();
        } finally {
          releasePageFromWrite(atomicOperation, entryPointCacheEntry);
        }

        final OCacheEntry rootCacheEntry = addPage(atomicOperation, fileId);
        try {
          @SuppressWarnings("unused")
          final CellBTreeBucketSingleValueV1<K> rootBucket = new CellBTreeBucketSingleValueV1<>(rootCacheEntry);
          rootBucket.init(true);
        } finally {
          releasePageFromWrite(atomicOperation, rootCacheEntry);
        }

        final OCacheEntry nullCacheEntry = addPage(atomicOperation, nullBucketFileId);
        try {
          @SuppressWarnings("unused")
          final CellBTreeNullBucketSingleValueV1 nullBucket = new CellBTreeNullBucketSingleValueV1(nullCacheEntry);
          nullBucket.init();
        } finally {
          releasePageFromWrite(atomicOperation, nullCacheEntry);
        }

      } finally {
        releaseExclusiveLock();
      }
    } catch (final Exception e) {
      rollback = true;
      throw e;
    } finally {
      endAtomicOperation(rollback);
    }

  }

  @Override
  public ORID get(K key) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
        if (key != null) {
          key = keySerializer.preprocess(key, (Object[]) keyTypes);

          final BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);
          if (bucketSearchResult.itemIndex < 0) {
            return null;
          }

          final long pageIndex = bucketSearchResult.pageIndex;
          final OCacheEntry keyBucketCacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
          try {
            final CellBTreeBucketSingleValueV1<K> keyBucket = new CellBTreeBucketSingleValueV1<>(keyBucketCacheEntry);
            return keyBucket.getValue(bucketSearchResult.itemIndex, encryption, keySerializer);
          } finally {
            releasePageFromRead(atomicOperation, keyBucketCacheEntry);
          }
        } else {
          final OCacheEntry nullBucketCacheEntry = loadPageForRead(atomicOperation, nullBucketFileId, 0, false);
          try {
            final CellBTreeNullBucketSingleValueV1 nullBucket = new CellBTreeNullBucketSingleValueV1(nullBucketCacheEntry);
            return nullBucket.getValue();
          } finally {
            releasePageFromRead(atomicOperation, nullBucketCacheEntry);
          }
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException
          .wrapException(new OCellBTreeSingleValueV1Exception("Error during retrieving  of sbtree with name " + getName(), this), e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public void put(final K key, final ORID value) throws IOException {
    update(key, value, null);
  }

  @Override
  public boolean validatedPut(final K key, final ORID value, final OBaseIndexEngine.Validator<K, ORID> validator)
      throws IOException {
    return update(key, value, validator);
  }

  private boolean update(K key, ORID value, final OBaseIndexEngine.Validator<K, ORID> validator) throws IOException {
    boolean rollback = false;
    final OAtomicOperation atomicOperation = startAtomicOperation(true);
    try {
      acquireExclusiveLock();
      try {
        if (key != null) {

          key = keySerializer.preprocess(key, (Object[]) keyTypes);

          if (keySize > MAX_KEY_SIZE) {
            throw new OTooBigIndexKeyException(
                "Key size is more than allowed, operation was canceled. Current key size " + keySize + ", allowed  " + MAX_KEY_SIZE,
                getName());
          }

          UpdateBucketSearchResult bucketSearchResult = findBucketForUpdate(key, atomicOperation);

          OCacheEntry keyBucketCacheEntry = loadPageForWrite(atomicOperation, fileId, bucketSearchResult.getLastPathItem(), false,
              true);
          CellBTreeBucketSingleValueV1<K> keyBucket = new CellBTreeBucketSingleValueV1<>(keyBucketCacheEntry);

          final byte[] oldRawValue = bucketSearchResult.itemIndex > -1
              ? keyBucket.getRawValue(bucketSearchResult.itemIndex, encryption, keySerializer)
              : null;
          final ORID oldValue;
          if (oldRawValue == null) {
            oldValue = null;
          } else {
            final int clusterId = OShortSerializer.INSTANCE.deserializeNative(oldRawValue, 0);
            final long clusterPosition = OLongSerializer.INSTANCE.deserializeNative(oldRawValue, OShortSerializer.SHORT_SIZE);
            oldValue = new ORecordId(clusterId, clusterPosition);
          }

          if (validator != null) {
            boolean failure = true; // assuming validation throws by default
            boolean ignored = false;

            try {

              final Object result = validator.validate(key, oldValue, value);
              if (result == OBaseIndexEngine.Validator.IGNORE) {
                ignored = true;
                failure = false;
                return false;
              }

              value = (ORID) result;
              failure = false;
            } finally {
              if (failure || ignored) {
                releasePageFromWrite(atomicOperation, keyBucketCacheEntry);
              }
            }
          }

          final byte[] serializedValue = new byte[OShortSerializer.SHORT_SIZE + OLongSerializer.LONG_SIZE];
          OShortSerializer.INSTANCE.serializeNative((short) value.getClusterId(), serializedValue, 0);
          OLongSerializer.INSTANCE.serializeNative(value.getClusterPosition(), serializedValue, OShortSerializer.SHORT_SIZE);

          final byte[] rawKey = serializeKey(key);
          int insertionIndex;
          final int sizeDiff;
          if (bucketSearchResult.itemIndex >= 0) {
            assert oldRawValue != null;

            if (oldRawValue.length == serializedValue.length) {
              keyBucket.updateValue(bucketSearchResult.itemIndex, serializedValue, rawKey.length);
              releasePageFromWrite(atomicOperation, keyBucketCacheEntry);
              return true;
            } else {
              keyBucket.removeLeafEntry(bucketSearchResult.itemIndex, rawKey, oldRawValue);
              insertionIndex = bucketSearchResult.itemIndex;
              sizeDiff = 0;
            }
          } else {
            insertionIndex = -bucketSearchResult.itemIndex - 1;
            sizeDiff = 1;
          }

          while (!keyBucket.addLeafEntry(insertionIndex, rawKey, serializedValue)) {
            bucketSearchResult = splitBucket(keyBucket, keyBucketCacheEntry, bucketSearchResult.path,
                bucketSearchResult.insertionIndexes, insertionIndex, atomicOperation);

            insertionIndex = bucketSearchResult.itemIndex;

            final long pageIndex = bucketSearchResult.getLastPathItem();

            if (pageIndex != keyBucketCacheEntry.getPageIndex()) {
              releasePageFromWrite(atomicOperation, keyBucketCacheEntry);

              keyBucketCacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, false, true);
            }

            keyBucket = new CellBTreeBucketSingleValueV1<>(keyBucketCacheEntry);
          }

          releasePageFromWrite(atomicOperation, keyBucketCacheEntry);

          if (sizeDiff != 0) {
            updateSize(sizeDiff, atomicOperation);
          }

          atomicOperation.addComponentOperation(
              new OCellBTreeSingleValuePutCO(rawKey, value, oldValue, keySerializer.getId(), indexId,
                  encryption != null ? encryption.name() : null));

        } else {
          final OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, nullBucketFileId, 0, false, true);

          int sizeDiff = 0;

          final ORID oldValue;
          try {
            final CellBTreeNullBucketSingleValueV1 nullBucket = new CellBTreeNullBucketSingleValueV1(cacheEntry);
            oldValue = nullBucket.getValue();

            if (validator != null) {
              final Object result = validator.validate(null, oldValue, value);
              if (result == OBaseIndexEngine.Validator.IGNORE) {
                return false;
              }
            }

            if (oldValue != null) {
              sizeDiff = -1;
            }

            nullBucket.setValue(value);

          } finally {
            releasePageFromWrite(atomicOperation, cacheEntry);
          }

          sizeDiff++;
          updateSize(sizeDiff, atomicOperation);

          final byte[] serializedValue = new byte[OShortSerializer.SHORT_SIZE + OLongSerializer.LONG_SIZE];
          OShortSerializer.INSTANCE.serializeNative((short) value.getClusterId(), serializedValue, 0);
          OLongSerializer.INSTANCE.serializeNative(value.getClusterPosition(), serializedValue, OShortSerializer.SHORT_SIZE);

          atomicOperation.addComponentOperation(
              new OCellBTreeSingleValuePutCO(null, value, oldValue, keySerializer.getId(), indexId,
                  encryption != null ? encryption.name() : null));

        }
        return true;
      } finally {
        releaseExclusiveLock();
      }
    } catch (final Exception e) {
      rollback = true;
      throw e;
    } finally {
      endAtomicOperation(rollback);
    }
  }

  @Override
  public void close() {
    acquireExclusiveLock();
    try {
      readCache.closeFile(fileId, true, writeCache);
      readCache.closeFile(nullBucketFileId, true, writeCache);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void delete() throws IOException {
    boolean rollback = false;
    final OAtomicOperation atomicOperation = startAtomicOperation(false);

    try {
      acquireExclusiveLock();
      try {
        final long size = size();
        if (size > 0) {
          throw new NotEmptyComponentCanNotBeRemovedException(
              getName() + " : Not empty index can not be deleted. Index has " + size + " records");
        }

        deleteFile(atomicOperation, fileId);
        deleteFile(atomicOperation, nullBucketFileId);
      } finally {
        releaseExclusiveLock();
      }
    } catch (final Exception e) {
      rollback = true;
      throw e;
    } finally {
      endAtomicOperation(rollback);
    }
  }

  @Override
  public void load(final String name, final int keySize, final OType[] keyTypes, final OBinarySerializer<K> keySerializer,
      final OEncryption encryption) {
    acquireExclusiveLock();
    try {
      final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();

      fileId = openFile(atomicOperation, getFullName());
      nullBucketFileId = openFile(atomicOperation, name + nullFileExtension);

      this.keySize = keySize;
      this.keyTypes = keyTypes;
      this.keySerializer = keySerializer;
      this.encryption = encryption;
    } catch (final IOException e) {
      throw OException.wrapException(new OCellBTreeSingleValueV1Exception("Exception during loading of sbtree " + name, this), e);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public long size() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();

        final OCacheEntry entryPointCacheEntry = loadPageForRead(atomicOperation, fileId, ENTRY_POINT_INDEX, false);
        try {
          final CellBTreeSingleValueEntryPointV1 entryPoint = new CellBTreeSingleValueEntryPointV1(entryPointCacheEntry);
          return entryPoint.getTreeSize();
        } finally {
          releasePageFromRead(atomicOperation, entryPointCacheEntry);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException
          .wrapException(new OCellBTreeSingleValueV1Exception("Error during retrieving of size of index " + getName(), this), e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public ORID remove(K key) throws IOException {
    boolean rollback = false;
    final OAtomicOperation atomicOperation = startAtomicOperation(true);
    try {
      acquireExclusiveLock();
      try {
        final ORID removedValue;

        if (key != null) {
          key = keySerializer.preprocess(key, (Object[]) keyTypes);

          final BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);
          if (bucketSearchResult.itemIndex < 0) {
            return null;
          }

          final byte[] rawKey = serializeKey(key);
          final OCacheEntry keyBucketCacheEntry = loadPageForWrite(atomicOperation, fileId, bucketSearchResult.pageIndex, false,
              true);
          try {
            final CellBTreeBucketSingleValueV1<K> keyBucket = new CellBTreeBucketSingleValueV1<>(keyBucketCacheEntry);
            final byte[] rawRemovedValue = keyBucket.getRawValue(bucketSearchResult.itemIndex, encryption, keySerializer);

            final int clusterId = OShortSerializer.INSTANCE.deserializeNative(rawRemovedValue, 0);
            final long clusterPosition = OLongSerializer.INSTANCE.deserializeNative(rawRemovedValue, OShortSerializer.SHORT_SIZE);

            removedValue = new ORecordId(clusterId, clusterPosition);

            keyBucket.removeLeafEntry(bucketSearchResult.itemIndex, rawKey, rawRemovedValue);
            updateSize(-1, atomicOperation);
          } finally {
            releasePageFromWrite(atomicOperation, keyBucketCacheEntry);
          }

          atomicOperation.addComponentOperation(
              new OCellBTreeSingleValueRemoveCO(keySerializer.getId(), indexId, encryption != null ? encryption.name() : null,
                  rawKey, removedValue));
        } else {
          if (getFilledUpTo(atomicOperation, nullBucketFileId) == 0) {
            return null;
          }

          removedValue = removeNullBucket(atomicOperation);

          if (removedValue != null) {
            atomicOperation.addComponentOperation(
                new OCellBTreeSingleValueRemoveCO(keySerializer.getId(), indexId, encryption != null ? encryption.name() : null,
                    null, removedValue));
          }
        }
        return removedValue;
      } finally {
        releaseExclusiveLock();
      }
    } catch (final Exception e) {
      rollback = true;
      throw e;
    } finally {
      endAtomicOperation(rollback);
    }
  }

  private ORID removeNullBucket(final OAtomicOperation atomicOperation) throws IOException {
    ORID removedValue;
    final OCacheEntry nullCacheEntry = loadPageForWrite(atomicOperation, nullBucketFileId, 0, false, true);
    try {
      final CellBTreeNullBucketSingleValueV1 nullBucket = new CellBTreeNullBucketSingleValueV1(nullCacheEntry);
      removedValue = nullBucket.getValue();

      if (removedValue != null) {
        nullBucket.removeValue();
      }
    } finally {
      releasePageFromWrite(atomicOperation, nullCacheEntry);
    }

    if (removedValue != null) {
      updateSize(-1, atomicOperation);
    }

    return removedValue;
  }

  @Override
  public OCellBTreeCursor<K, ORID> iterateEntriesMinor(final K key, final boolean inclusive, final boolean ascSortOrder) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        if (!ascSortOrder) {
          return iterateEntriesMinorDesc(key, inclusive);
        }

        return iterateEntriesMinorAsc(key, inclusive);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public OCellBTreeCursor<K, ORID> iterateEntriesMajor(final K key, final boolean inclusive, final boolean ascSortOrder) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        if (ascSortOrder) {
          return iterateEntriesMajorAsc(key, inclusive);
        }

        return iterateEntriesMajorDesc(key, inclusive);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public K firstKey() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();

        final BucketSearchResult searchResult = firstItem(atomicOperation);
        if (searchResult == null) {
          return null;
        }

        final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, searchResult.pageIndex, false);
        try {
          final CellBTreeBucketSingleValueV1<K> bucket = new CellBTreeBucketSingleValueV1<>(cacheEntry);
          return bucket.getKey(searchResult.itemIndex, encryption, keySerializer);
        } finally {
          releasePageFromRead(atomicOperation, cacheEntry);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException
          .wrapException(new OCellBTreeSingleValueV1Exception("Error during finding first key in sbtree [" + getName() + "]", this),
              e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public K lastKey() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();

        final BucketSearchResult searchResult = lastItem(atomicOperation);
        if (searchResult == null) {
          return null;
        }

        final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, searchResult.pageIndex, false);
        try {
          final CellBTreeBucketSingleValueV1<K> bucket = new CellBTreeBucketSingleValueV1<>(cacheEntry);
          return bucket.getKey(searchResult.itemIndex, encryption, keySerializer);
        } finally {
          releasePageFromRead(atomicOperation, cacheEntry);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException
          .wrapException(new OCellBTreeSingleValueV1Exception("Error during finding last key in sbtree [" + getName() + "]", this),
              e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public OCellBTreeKeyCursor<K> keyCursor() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
        final BucketSearchResult searchResult = firstItem(atomicOperation);
        if (searchResult == null) {
          return prefetchSize -> null;
        }

        return new OSBTreeFullKeyCursor(searchResult.pageIndex);
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException
          .wrapException(new OCellBTreeSingleValueV1Exception("Error during finding first key in sbtree [" + getName() + "]", this),
              e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public OCellBTreeCursor<K, ORID> iterateEntriesBetween(final K keyFrom, final boolean fromInclusive, final K keyTo,
      final boolean toInclusive, final boolean ascSortOrder) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        if (ascSortOrder) {
          return iterateEntriesBetweenAscOrder(keyFrom, fromInclusive, keyTo, toInclusive);
        } else {
          return iterateEntriesBetweenDescOrder(keyFrom, fromInclusive, keyTo, toInclusive);
        }
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  /**
   * Acquires exclusive lock in the active atomic operation running on the current thread for this SB-tree.
   */
  @Override
  public void acquireAtomicExclusiveLock() {
    atomicOperationsManager.acquireExclusiveLockTillOperationComplete(this);
  }

  private void updateSize(final long diffSize, final OAtomicOperation atomicOperation) throws IOException {
    final OCacheEntry entryPointCacheEntry = loadPageForWrite(atomicOperation, fileId, ENTRY_POINT_INDEX, false, true);
    try {
      final CellBTreeSingleValueEntryPointV1 entryPoint = new CellBTreeSingleValueEntryPointV1(entryPointCacheEntry);
      entryPoint.setTreeSize(entryPoint.getTreeSize() + diffSize);
    } finally {
      releasePageFromWrite(atomicOperation, entryPointCacheEntry);
    }
  }

  private OCellBTreeCursor<K, ORID> iterateEntriesMinorDesc(K key, final boolean inclusive) {
    key = keySerializer.preprocess(key, (Object[]) keyTypes);
    key = enhanceCompositeKeyMinorDesc(key, inclusive);

    return new OCellBTreeCursorBackward(null, key, false, inclusive);
  }

  private OCellBTreeCursor<K, ORID> iterateEntriesMinorAsc(K key, final boolean inclusive) {
    key = keySerializer.preprocess(key, (Object[]) keyTypes);
    key = enhanceCompositeKeyMinorAsc(key, inclusive);

    return new OCellBTreeCursorForward(null, key, false, inclusive);
  }

  private K enhanceCompositeKeyMinorDesc(K key, final boolean inclusive) {
    final PartialSearchMode partialSearchMode;
    if (inclusive) {
      partialSearchMode = PartialSearchMode.HIGHEST_BOUNDARY;
    } else {
      partialSearchMode = PartialSearchMode.LOWEST_BOUNDARY;
    }

    key = enhanceCompositeKey(key, partialSearchMode);
    return key;
  }

  private K enhanceCompositeKeyMinorAsc(K key, final boolean inclusive) {
    final PartialSearchMode partialSearchMode;
    if (inclusive) {
      partialSearchMode = PartialSearchMode.HIGHEST_BOUNDARY;
    } else {
      partialSearchMode = PartialSearchMode.LOWEST_BOUNDARY;
    }

    key = enhanceCompositeKey(key, partialSearchMode);
    return key;
  }

  private OCellBTreeCursor<K, ORID> iterateEntriesMajorAsc(K key, final boolean inclusive) {
    key = keySerializer.preprocess(key, (Object[]) keyTypes);
    key = enhanceCompositeKeyMajorAsc(key, inclusive);

    return new OCellBTreeCursorForward(key, null, inclusive, false);
  }

  private OCellBTreeCursor<K, ORID> iterateEntriesMajorDesc(K key, final boolean inclusive) {
    acquireSharedLock();
    try {
      key = keySerializer.preprocess(key, (Object[]) keyTypes);
      key = enhanceCompositeKeyMajorDesc(key, inclusive);

      return new OCellBTreeCursorBackward(key, null, inclusive, false);

    } finally {
      releaseSharedLock();
    }

  }

  private K enhanceCompositeKeyMajorAsc(K key, final boolean inclusive) {
    final PartialSearchMode partialSearchMode;
    if (inclusive) {
      partialSearchMode = PartialSearchMode.LOWEST_BOUNDARY;
    } else {
      partialSearchMode = PartialSearchMode.HIGHEST_BOUNDARY;
    }

    key = enhanceCompositeKey(key, partialSearchMode);
    return key;
  }

  private K enhanceCompositeKeyMajorDesc(K key, final boolean inclusive) {
    final PartialSearchMode partialSearchMode;
    if (inclusive) {
      partialSearchMode = PartialSearchMode.LOWEST_BOUNDARY;
    } else {
      partialSearchMode = PartialSearchMode.HIGHEST_BOUNDARY;
    }

    key = enhanceCompositeKey(key, partialSearchMode);
    return key;
  }

  private BucketSearchResult firstItem(final OAtomicOperation atomicOperation) throws IOException {
    final LinkedList<PagePathItemUnit> path = new LinkedList<>();

    long bucketIndex = ROOT_INDEX;

    OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, bucketIndex, false);
    int itemIndex = 0;
    try {
      CellBTreeBucketSingleValueV1<K> bucket = new CellBTreeBucketSingleValueV1<>(cacheEntry);

      while (true) {
        if (!bucket.isLeaf()) {
          if (bucket.isEmpty() || itemIndex > bucket.size()) {
            if (!path.isEmpty()) {
              final PagePathItemUnit pagePathItemUnit = path.removeLast();

              bucketIndex = pagePathItemUnit.pageIndex;
              itemIndex = pagePathItemUnit.itemIndex + 1;
            } else {
              return null;
            }
          } else {
            path.add(new PagePathItemUnit(bucketIndex, itemIndex));

            if (itemIndex < bucket.size()) {
              bucketIndex = bucket.getLeft(itemIndex);
            } else {
              bucketIndex = bucket.getRight(itemIndex - 1);
            }

            itemIndex = 0;
          }
        } else {
          if (bucket.isEmpty()) {
            if (!path.isEmpty()) {
              final PagePathItemUnit pagePathItemUnit = path.removeLast();

              bucketIndex = pagePathItemUnit.pageIndex;
              itemIndex = pagePathItemUnit.itemIndex + 1;
            } else {
              return null;
            }
          } else {
            return new BucketSearchResult(0, bucketIndex);
          }
        }

        releasePageFromRead(atomicOperation, cacheEntry);

        cacheEntry = loadPageForRead(atomicOperation, fileId, bucketIndex, false);

        bucket = new CellBTreeBucketSingleValueV1<>(cacheEntry);
      }
    } finally {
      releasePageFromRead(atomicOperation, cacheEntry);
    }
  }

  private BucketSearchResult lastItem(final OAtomicOperation atomicOperation) throws IOException {
    final LinkedList<PagePathItemUnit> path = new LinkedList<>();

    long bucketIndex = ROOT_INDEX;

    OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, bucketIndex, false);

    CellBTreeBucketSingleValueV1<K> bucket = new CellBTreeBucketSingleValueV1<>(cacheEntry);

    int itemIndex = bucket.size() - 1;
    try {
      while (true) {
        if (!bucket.isLeaf()) {
          if (itemIndex < -1) {
            if (!path.isEmpty()) {
              final PagePathItemUnit pagePathItemUnit = path.removeLast();

              bucketIndex = pagePathItemUnit.pageIndex;
              itemIndex = pagePathItemUnit.itemIndex - 1;
            } else {
              return null;
            }
          } else {
            path.add(new PagePathItemUnit(bucketIndex, itemIndex));

            if (itemIndex > -1) {
              bucketIndex = bucket.getRight(itemIndex);
            } else {
              bucketIndex = bucket.getLeft(0);
            }

            itemIndex = CellBTreeBucketSingleValueV1.MAX_PAGE_SIZE_BYTES + 1;
          }
        } else {
          if (bucket.isEmpty()) {
            if (!path.isEmpty()) {
              final PagePathItemUnit pagePathItemUnit = path.removeLast();

              bucketIndex = pagePathItemUnit.pageIndex;
              itemIndex = pagePathItemUnit.itemIndex - 1;
            } else {
              return null;
            }
          } else {
            return new BucketSearchResult(bucket.size() - 1, bucketIndex);
          }
        }

        releasePageFromRead(atomicOperation, cacheEntry);

        cacheEntry = loadPageForRead(atomicOperation, fileId, bucketIndex, false);

        bucket = new CellBTreeBucketSingleValueV1<>(cacheEntry);
        if (itemIndex == CellBTreeBucketSingleValueV1.MAX_PAGE_SIZE_BYTES + 1) {
          itemIndex = bucket.size() - 1;
        }
      }
    } finally {
      releasePageFromRead(atomicOperation, cacheEntry);
    }
  }

  private OCellBTreeCursor<K, ORID> iterateEntriesBetweenAscOrder(K keyFrom, final boolean fromInclusive, K keyTo,
      final boolean toInclusive) {
    keyFrom = keySerializer.preprocess(keyFrom, (Object[]) keyTypes);
    keyTo = keySerializer.preprocess(keyTo, (Object[]) keyTypes);

    keyFrom = enhanceFromCompositeKeyBetweenAsc(keyFrom, fromInclusive);
    keyTo = enhanceToCompositeKeyBetweenAsc(keyTo, toInclusive);

    return new OCellBTreeCursorForward(keyFrom, keyTo, fromInclusive, toInclusive);
  }

  private OCellBTreeCursor<K, ORID> iterateEntriesBetweenDescOrder(K keyFrom, final boolean fromInclusive, K keyTo,
      final boolean toInclusive) {
    keyFrom = keySerializer.preprocess(keyFrom, (Object[]) keyTypes);
    keyTo = keySerializer.preprocess(keyTo, (Object[]) keyTypes);

    keyFrom = enhanceFromCompositeKeyBetweenDesc(keyFrom, fromInclusive);
    keyTo = enhanceToCompositeKeyBetweenDesc(keyTo, toInclusive);

    return new OCellBTreeCursorBackward(keyFrom, keyTo, fromInclusive, toInclusive);
  }

  private K enhanceToCompositeKeyBetweenAsc(K keyTo, final boolean toInclusive) {
    final PartialSearchMode partialSearchModeTo;
    if (toInclusive) {
      partialSearchModeTo = PartialSearchMode.HIGHEST_BOUNDARY;
    } else {
      partialSearchModeTo = PartialSearchMode.LOWEST_BOUNDARY;
    }

    keyTo = enhanceCompositeKey(keyTo, partialSearchModeTo);
    return keyTo;
  }

  private K enhanceFromCompositeKeyBetweenAsc(K keyFrom, final boolean fromInclusive) {
    final PartialSearchMode partialSearchModeFrom;
    if (fromInclusive) {
      partialSearchModeFrom = PartialSearchMode.LOWEST_BOUNDARY;
    } else {
      partialSearchModeFrom = PartialSearchMode.HIGHEST_BOUNDARY;
    }

    keyFrom = enhanceCompositeKey(keyFrom, partialSearchModeFrom);
    return keyFrom;
  }

  private K enhanceToCompositeKeyBetweenDesc(K keyTo, final boolean toInclusive) {
    final PartialSearchMode partialSearchModeTo;
    if (toInclusive) {
      partialSearchModeTo = PartialSearchMode.HIGHEST_BOUNDARY;
    } else {
      partialSearchModeTo = PartialSearchMode.LOWEST_BOUNDARY;
    }

    keyTo = enhanceCompositeKey(keyTo, partialSearchModeTo);
    return keyTo;
  }

  private K enhanceFromCompositeKeyBetweenDesc(K keyFrom, final boolean fromInclusive) {
    final PartialSearchMode partialSearchModeFrom;
    if (fromInclusive) {
      partialSearchModeFrom = PartialSearchMode.LOWEST_BOUNDARY;
    } else {
      partialSearchModeFrom = PartialSearchMode.HIGHEST_BOUNDARY;
    }

    keyFrom = enhanceCompositeKey(keyFrom, partialSearchModeFrom);
    return keyFrom;
  }

  private UpdateBucketSearchResult splitBucket(final CellBTreeBucketSingleValueV1<K> bucketToSplit, final OCacheEntry entryToSplit,
      final List<Long> path, final List<Integer> itemPointers, final int keyIndex, final OAtomicOperation atomicOperation)
      throws IOException {
    final boolean splitLeaf = bucketToSplit.isLeaf();
    final int bucketSize = bucketToSplit.size();

    final int indexToSplit = bucketSize >>> 1;
    final K separationKey = bucketToSplit.getKey(indexToSplit, encryption, keySerializer);
    final List<byte[]> rightEntries = new ArrayList<>(indexToSplit);

    final int startRightIndex = splitLeaf ? indexToSplit : indexToSplit + 1;

    for (int i = startRightIndex; i < bucketSize; i++) {
      rightEntries.add(bucketToSplit.getRawEntry(i, encryption != null, keySerializer));
    }

    if (entryToSplit.getPageIndex() != ROOT_INDEX) {
      return splitNonRootBucket(path, itemPointers, keyIndex, entryToSplit.getPageIndex(), bucketToSplit, splitLeaf, indexToSplit,
          separationKey, rightEntries, atomicOperation);
    } else {
      return splitRootBucket(keyIndex, entryToSplit, bucketToSplit, splitLeaf, indexToSplit, separationKey, rightEntries,
          atomicOperation);
    }
  }

  private UpdateBucketSearchResult splitNonRootBucket(final List<Long> path, final List<Integer> itemPointers, final int keyIndex,
      final long pageIndex, final CellBTreeBucketSingleValueV1<K> bucketToSplit, final boolean splitLeaf, final int indexToSplit,
      final K separationKey, final List<byte[]> rightEntries, final OAtomicOperation atomicOperation) throws IOException {

    final OCacheEntry rightBucketEntry;
    final OCacheEntry entryPointCacheEntry = loadPageForWrite(atomicOperation, fileId, ENTRY_POINT_INDEX, false, true);
    try {
      final CellBTreeSingleValueEntryPointV1 entryPoint = new CellBTreeSingleValueEntryPointV1(entryPointCacheEntry);
      int pageSize = entryPoint.getPagesSize();

      if (pageSize < getFilledUpTo(atomicOperation, fileId) - 1) {
        pageSize++;
        rightBucketEntry = loadPageForWrite(atomicOperation, fileId, pageSize, false, false);
        entryPoint.setPagesSize(pageSize);
      } else {
        assert pageSize == getFilledUpTo(atomicOperation, fileId) - 1;

        rightBucketEntry = addPage(atomicOperation, fileId);
        entryPoint.setPagesSize((int) rightBucketEntry.getPageIndex());
      }
    } finally {
      releasePageFromWrite(atomicOperation, entryPointCacheEntry);
    }

    try {
      final CellBTreeBucketSingleValueV1<K> newRightBucket = new CellBTreeBucketSingleValueV1<>(rightBucketEntry);
      newRightBucket.init(splitLeaf);

      newRightBucket.addAll(rightEntries, encryption != null, keySerializer);

      bucketToSplit.shrink(indexToSplit, encryption != null, keySerializer);

      if (splitLeaf) {
        final long rightSiblingPageIndex = bucketToSplit.getRightSibling();

        newRightBucket.setRightSibling(rightSiblingPageIndex);
        newRightBucket.setLeftSibling(pageIndex);

        bucketToSplit.setRightSibling(rightBucketEntry.getPageIndex());

        if (rightSiblingPageIndex >= 0) {
          final OCacheEntry rightSiblingBucketEntry = loadPageForWrite(atomicOperation, fileId, rightSiblingPageIndex, false, true);
          final CellBTreeBucketSingleValueV1<K> rightSiblingBucket = new CellBTreeBucketSingleValueV1<>(rightSiblingBucketEntry);
          try {
            rightSiblingBucket.setLeftSibling(rightBucketEntry.getPageIndex());
          } finally {
            releasePageFromWrite(atomicOperation, rightSiblingBucketEntry);
          }
        }
      }

      long parentIndex = path.get(path.size() - 2);
      OCacheEntry parentCacheEntry = loadPageForWrite(atomicOperation, fileId, parentIndex, false, true);
      try {
        CellBTreeBucketSingleValueV1<K> parentBucket = new CellBTreeBucketSingleValueV1<>(parentCacheEntry);
        int insertionIndex = itemPointers.get(itemPointers.size() - 2);
        while (!parentBucket
            .addNonLeafEntry(insertionIndex, (int) pageIndex, rightBucketEntry.getPageIndex(), serializeKey(separationKey), true)) {
          final UpdateBucketSearchResult bucketSearchResult = splitBucket(parentBucket, parentCacheEntry,
              path.subList(0, path.size() - 1), itemPointers.subList(0, itemPointers.size() - 1), insertionIndex, atomicOperation);

          parentIndex = bucketSearchResult.getLastPathItem();
          insertionIndex = bucketSearchResult.itemIndex;

          if (parentIndex != parentCacheEntry.getPageIndex()) {
            releasePageFromWrite(atomicOperation, parentCacheEntry);

            parentCacheEntry = loadPageForWrite(atomicOperation, fileId, parentIndex, false, true);
          }

          parentBucket = new CellBTreeBucketSingleValueV1<>(parentCacheEntry);
        }

      } finally {
        releasePageFromWrite(atomicOperation, parentCacheEntry);
      }

    } finally {
      releasePageFromWrite(atomicOperation, rightBucketEntry);
    }

    final ArrayList<Long> resultPath = new ArrayList<>(path.subList(0, path.size() - 1));
    final ArrayList<Integer> resultItemPointers = new ArrayList<>(itemPointers.subList(0, itemPointers.size() - 1));

    if (keyIndex <= indexToSplit) {
      resultPath.add(pageIndex);
      resultItemPointers.add(keyIndex);

      return new UpdateBucketSearchResult(resultItemPointers, resultPath, keyIndex);
    }

    final int parentIndex = resultItemPointers.size() - 1;
    resultItemPointers.set(parentIndex, resultItemPointers.get(parentIndex) + 1);
    resultPath.add((long) rightBucketEntry.getPageIndex());

    if (splitLeaf) {
      resultItemPointers.add(keyIndex - indexToSplit);
      return new UpdateBucketSearchResult(resultItemPointers, resultPath, keyIndex - indexToSplit);
    }

    resultItemPointers.add(keyIndex - indexToSplit - 1);
    return new UpdateBucketSearchResult(resultItemPointers, resultPath, keyIndex - indexToSplit - 1);
  }

  private byte[] serializeKey(K separationKey) {
    final byte[] serializedKey = keySerializer.serializeNativeAsWhole(separationKey, (Object[]) keyTypes);
    final byte[] rawKey;
    if (encryption == null) {
      rawKey = serializedKey;
    } else {
      final byte[] encryptedKey = encryption.encrypt(serializedKey);
      rawKey = new byte[encryptedKey.length + OIntegerSerializer.INT_SIZE];
      OIntegerSerializer.INSTANCE.serializeNative(encryptedKey.length, rawKey, 0);
      System.arraycopy(encryptedKey, 0, rawKey, OIntegerSerializer.INT_SIZE, encryptedKey.length);
    }
    return rawKey;
  }

  private UpdateBucketSearchResult splitRootBucket(final int keyIndex, final OCacheEntry bucketEntry,
      CellBTreeBucketSingleValueV1<K> bucketToSplit, final boolean splitLeaf, final int indexToSplit, final K separationKey,
      final List<byte[]> rightEntries, final OAtomicOperation atomicOperation) throws IOException {
    final List<byte[]> leftEntries = new ArrayList<>(indexToSplit);

    for (int i = 0; i < indexToSplit; i++) {
      leftEntries.add(bucketToSplit.getRawEntry(i, encryption != null, keySerializer));
    }

    final OCacheEntry leftBucketEntry;
    final OCacheEntry rightBucketEntry;

    final OCacheEntry entryPointCacheEntry = loadPageForWrite(atomicOperation, fileId, ENTRY_POINT_INDEX, false, true);
    try {
      final CellBTreeSingleValueEntryPointV1 entryPoint = new CellBTreeSingleValueEntryPointV1(entryPointCacheEntry);
      int pagesSize = entryPoint.getPagesSize();

      final int filledUpTo = (int) getFilledUpTo(atomicOperation, fileId);

      if (pagesSize < filledUpTo - 1) {
        pagesSize++;
        leftBucketEntry = loadPageForWrite(atomicOperation, fileId, pagesSize, false, false);
      } else {
        assert pagesSize == filledUpTo - 1;
        leftBucketEntry = addPage(atomicOperation, fileId);
        pagesSize = (int) leftBucketEntry.getPageIndex();
      }

      if (pagesSize < filledUpTo) {
        pagesSize++;
        rightBucketEntry = loadPageForWrite(atomicOperation, fileId, pagesSize, false, false);
      } else {
        assert pagesSize == filledUpTo;
        rightBucketEntry = addPage(atomicOperation, fileId);
        pagesSize = (int) rightBucketEntry.getPageIndex();
      }

      entryPoint.setPagesSize(pagesSize);
    } finally {
      releasePageFromWrite(atomicOperation, entryPointCacheEntry);
    }

    try {
      final CellBTreeBucketSingleValueV1<K> newLeftBucket = new CellBTreeBucketSingleValueV1<>(leftBucketEntry);
      newLeftBucket.init(splitLeaf);

      newLeftBucket.addAll(leftEntries, encryption != null, keySerializer);

      if (splitLeaf) {
        newLeftBucket.setRightSibling(rightBucketEntry.getPageIndex());
      }

    } finally {
      releasePageFromWrite(atomicOperation, leftBucketEntry);
    }

    try {
      final CellBTreeBucketSingleValueV1<K> newRightBucket = new CellBTreeBucketSingleValueV1<>(rightBucketEntry);
      newRightBucket.init(splitLeaf);

      newRightBucket.addAll(rightEntries, encryption != null, keySerializer);

      if (splitLeaf) {
        newRightBucket.setLeftSibling(leftBucketEntry.getPageIndex());
      }
    } finally {
      releasePageFromWrite(atomicOperation, rightBucketEntry);
    }

    bucketToSplit = new CellBTreeBucketSingleValueV1<>(bucketEntry);
    bucketToSplit.shrink(0, encryption != null, keySerializer);
    if (splitLeaf) {
      bucketToSplit.switchBucketType();
    }

    bucketToSplit
        .addNonLeafEntry(0, leftBucketEntry.getPageIndex(), rightBucketEntry.getPageIndex(), serializeKey(separationKey), true);

    final ArrayList<Long> resultPath = new ArrayList<>(8);
    resultPath.add(ROOT_INDEX);

    final ArrayList<Integer> itemPointers = new ArrayList<>(8);

    if (keyIndex <= indexToSplit) {
      itemPointers.add(-1);
      itemPointers.add(keyIndex);

      resultPath.add((long) leftBucketEntry.getPageIndex());
      return new UpdateBucketSearchResult(itemPointers, resultPath, keyIndex);
    }

    resultPath.add((long) rightBucketEntry.getPageIndex());
    itemPointers.add(0);

    if (splitLeaf) {
      itemPointers.add(keyIndex - indexToSplit);
      return new UpdateBucketSearchResult(itemPointers, resultPath, keyIndex - indexToSplit);
    }

    itemPointers.add(keyIndex - indexToSplit - 1);
    return new UpdateBucketSearchResult(itemPointers, resultPath, keyIndex - indexToSplit - 1);
  }

  private BucketSearchResult findBucket(final K key, final OAtomicOperation atomicOperation) throws IOException {
    long pageIndex = ROOT_INDEX;

    int depth = 0;
    while (true) {
      depth++;
      if (depth > MAX_PATH_LENGTH) {
        throw new OCellBTreeSingleValueV1Exception(
            "We reached max level of depth of SBTree but still found nothing, seems like tree is in corrupted state. You should rebuild index related to given query.",
            this);
      }

      final OCacheEntry bucketEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
      try {
        final CellBTreeBucketSingleValueV1<K> keyBucket = new CellBTreeBucketSingleValueV1<>(bucketEntry);
        final int index = keyBucket.find(key, keySerializer, encryption);

        if (keyBucket.isLeaf()) {
          return new BucketSearchResult(index, pageIndex);
        }

        if (index >= 0) {
          pageIndex = keyBucket.getRight(index);
        } else {
          final int insertionIndex = -index - 1;
          if (insertionIndex >= keyBucket.size()) {
            pageIndex = keyBucket.getRight(insertionIndex - 1);
          } else {
            pageIndex = keyBucket.getLeft(insertionIndex);
          }
        }
      } finally {
        releasePageFromRead(atomicOperation, bucketEntry);
      }
    }
  }

  private UpdateBucketSearchResult findBucketForUpdate(final K key, final OAtomicOperation atomicOperation) throws IOException {
    long pageIndex = ROOT_INDEX;

    final ArrayList<Long> path = new ArrayList<>(8);
    final ArrayList<Integer> itemIndexes = new ArrayList<>(8);

    while (true) {
      if (path.size() > MAX_PATH_LENGTH) {
        throw new OCellBTreeSingleValueV1Exception(
            "We reached max level of depth of SBTree but still found nothing, seems like tree is in corrupted state. You should rebuild index related to given query.",
            this);
      }

      path.add(pageIndex);
      final OCacheEntry bucketEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
      try {
        final CellBTreeBucketSingleValueV1<K> keyBucket = new CellBTreeBucketSingleValueV1<>(bucketEntry);
        final int index = keyBucket.find(key, keySerializer, encryption);

        if (keyBucket.isLeaf()) {
          itemIndexes.add(index);
          return new UpdateBucketSearchResult(itemIndexes, path, index);
        }

        if (index >= 0) {
          pageIndex = keyBucket.getRight(index);
          itemIndexes.add(index + 1);
        } else {
          final int insertionIndex = -index - 1;

          if (insertionIndex >= keyBucket.size()) {
            pageIndex = keyBucket.getRight(insertionIndex - 1);
          } else {
            pageIndex = keyBucket.getLeft(insertionIndex);
          }

          itemIndexes.add(insertionIndex);
        }
      } finally {
        releasePageFromRead(atomicOperation, bucketEntry);
      }
    }
  }

  private K enhanceCompositeKey(final K key, final PartialSearchMode partialSearchMode) {
    if (!(key instanceof OCompositeKey)) {
      return key;
    }

    final OCompositeKey compositeKey = (OCompositeKey) key;

    if (!(keySize == 1 || compositeKey.getKeys().size() == keySize || partialSearchMode.equals(PartialSearchMode.NONE))) {
      final OCompositeKey fullKey = new OCompositeKey(compositeKey);
      final int itemsToAdd = keySize - fullKey.getKeys().size();

      final Comparable<?> keyItem;
      if (partialSearchMode.equals(PartialSearchMode.HIGHEST_BOUNDARY)) {
        keyItem = ALWAYS_GREATER_KEY;
      } else {
        keyItem = ALWAYS_LESS_KEY;
      }

      for (int i = 0; i < itemsToAdd; i++) {
        fullKey.addKey(keyItem);
      }

      //noinspection unchecked
      return (K) fullKey;
    }

    return key;
  }

  private Map.Entry<K, ORID> convertToMapEntry(final CellBTreeBucketSingleValueV1.SBTreeEntry<K> treeEntry) {
    final K key = treeEntry.key;
    final ORID value = treeEntry.value;

    return new Map.Entry<K, ORID>() {
      @Override
      public K getKey() {
        return key;
      }

      @Override
      public ORID getValue() {
        return value;
      }

      @Override
      public ORID setValue(final ORID value) {
        throw new UnsupportedOperationException("setValue");
      }
    };
  }

  /**
   * Indicates search behavior in case of {@link OCompositeKey} keys that have less amount of internal keys are used, whether lowest
   * or highest partially matched key should be used.
   */
  private enum PartialSearchMode {
    /**
     * Any partially matched key will be used as search result.
     */
    NONE,
    /**
     * The biggest partially matched key will be used as search result.
     */
    HIGHEST_BOUNDARY,

    /**
     * The smallest partially matched key will be used as search result.
     */
    LOWEST_BOUNDARY
  }

  private static final class BucketSearchResult {
    private final int  itemIndex;
    private final long pageIndex;

    private BucketSearchResult(final int itemIndex, final long pageIndex) {
      this.itemIndex = itemIndex;
      this.pageIndex = pageIndex;
    }
  }

  private static final class UpdateBucketSearchResult {
    private final List<Integer>   insertionIndexes;
    private final ArrayList<Long> path;
    private final int             itemIndex;

    private UpdateBucketSearchResult(final List<Integer> insertionIndexes, final ArrayList<Long> path, final int itemIndex) {
      this.insertionIndexes = insertionIndexes;
      this.path = path;
      this.itemIndex = itemIndex;
    }

    final long getLastPathItem() {
      return path.get(path.size() - 1);
    }
  }

  private static final class PagePathItemUnit {
    private final long pageIndex;
    private final int  itemIndex;

    private PagePathItemUnit(final long pageIndex, final int itemIndex) {
      this.pageIndex = pageIndex;
      this.itemIndex = itemIndex;
    }
  }

  public final class OSBTreeFullKeyCursor implements OCellBTreeKeyCursor<K> {
    private long pageIndex;
    private int  itemIndex;

    private List<K>     keysCache    = new ArrayList<>();
    private Iterator<K> keysIterator = new OEmptyIterator<>();

    OSBTreeFullKeyCursor(final long startPageIndex) {
      pageIndex = startPageIndex;
      itemIndex = 0;
    }

    @Override
    public K next(int prefetchSize) {
      if (keysIterator == null) {
        return null;
      }

      if (keysIterator.hasNext()) {
        return keysIterator.next();
      }

      keysCache.clear();

      if (prefetchSize < 0 || prefetchSize > OGlobalConfiguration.INDEX_CURSOR_PREFETCH_SIZE.getValueAsInteger()) {
        prefetchSize = OGlobalConfiguration.INDEX_CURSOR_PREFETCH_SIZE.getValueAsInteger();
      }

      if (prefetchSize == 0) {
        prefetchSize = 1;
      }

      atomicOperationsManager.acquireReadLock(CellBTreeSingleValueV1.this);
      try {
        acquireSharedLock();
        try {
          final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();

          while (keysCache.size() < prefetchSize) {
            if (pageIndex == -1) {
              break;
            }

            final OCacheEntry entryPointCacheEntry = loadPageForRead(atomicOperation, fileId, ENTRY_POINT_INDEX, false);
            try {
              final CellBTreeSingleValueEntryPointV1 entryPoint = new CellBTreeSingleValueEntryPointV1(entryPointCacheEntry);
              if (pageIndex >= entryPoint.getPagesSize() + 1) {
                pageIndex = -1;
                break;
              }
            } finally {
              releasePageFromRead(atomicOperation, entryPointCacheEntry);
            }

            final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
            try {
              final CellBTreeBucketSingleValueV1<K> bucket = new CellBTreeBucketSingleValueV1<>(cacheEntry);

              final int bucketSize = bucket.size();

              if (itemIndex >= bucketSize) {
                pageIndex = bucket.getRightSibling();
                itemIndex = 0;
                continue;
              }

              while (itemIndex < bucketSize && keysCache.size() < prefetchSize) {
                final Map.Entry<K, ORID> entry = convertToMapEntry(bucket.getEntry(itemIndex, encryption, keySerializer));
                itemIndex++;

                keysCache.add(entry.getKey());
              }
            } finally {
              releasePageFromRead(atomicOperation, cacheEntry);
            }
          }
        } finally {
          releaseSharedLock();
        }
      } catch (final IOException e) {
        throw OException
            .wrapException(new OCellBTreeSingleValueV1Exception("Error during element iteration", CellBTreeSingleValueV1.this), e);
      } finally {
        atomicOperationsManager.releaseReadLock(CellBTreeSingleValueV1.this);
      }

      if (keysCache.isEmpty()) {
        keysCache = null;
        return null;
      }

      keysIterator = keysCache.iterator();
      return keysIterator.next();
    }
  }

  private final class OCellBTreeCursorForward implements OCellBTreeCursor<K, ORID> {
    private       K       fromKey;
    private final K       toKey;
    private       boolean fromKeyInclusive;
    private final boolean toKeyInclusive;

    private final List<Map.Entry<K, ORID>>     dataCache         = new ArrayList<>();
    @SuppressWarnings("unchecked")
    private       Iterator<Map.Entry<K, ORID>> dataCacheIterator = OEmptyMapEntryIterator.INSTANCE;

    private OCellBTreeCursorForward(final K fromKey, final K toKey, final boolean fromKeyInclusive, final boolean toKeyInclusive) {
      this.fromKey = fromKey;
      this.toKey = toKey;
      this.fromKeyInclusive = fromKeyInclusive;
      this.toKeyInclusive = toKeyInclusive;

      if (fromKey == null) {
        this.fromKeyInclusive = true;
      }
    }

    public Map.Entry<K, ORID> next(int prefetchSize) {
      if (dataCacheIterator == null) {
        return null;
      }

      if (dataCacheIterator.hasNext()) {
        final Map.Entry<K, ORID> entry = dataCacheIterator.next();

        fromKey = entry.getKey();
        fromKeyInclusive = false;

        return entry;
      }

      dataCache.clear();

      if (prefetchSize < 0 || prefetchSize > OGlobalConfiguration.INDEX_CURSOR_PREFETCH_SIZE.getValueAsInteger()) {
        prefetchSize = OGlobalConfiguration.INDEX_CURSOR_PREFETCH_SIZE.getValueAsInteger();
      }

      if (prefetchSize == 0) {
        prefetchSize = 1;
      }

      atomicOperationsManager.acquireReadLock(CellBTreeSingleValueV1.this);
      try {
        acquireSharedLock();
        try {
          final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();

          final BucketSearchResult bucketSearchResult;

          if (fromKey != null) {
            bucketSearchResult = findBucket(fromKey, atomicOperation);
          } else {
            bucketSearchResult = firstItem(atomicOperation);
          }

          if (bucketSearchResult == null) {
            dataCacheIterator = null;
            return null;
          }

          long pageIndex = bucketSearchResult.pageIndex;
          int itemIndex;

          if (bucketSearchResult.itemIndex >= 0) {
            itemIndex = fromKeyInclusive ? bucketSearchResult.itemIndex : bucketSearchResult.itemIndex + 1;
          } else {
            itemIndex = -bucketSearchResult.itemIndex - 1;
          }

          mainCycle:
          while (dataCache.size() < prefetchSize) {
            if (pageIndex == -1) {
              break;
            }

            final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
            try {
              final CellBTreeBucketSingleValueV1<K> bucket = new CellBTreeBucketSingleValueV1<>(cacheEntry);

              final int bucketSize = bucket.size();
              if (itemIndex >= bucketSize) {
                pageIndex = bucket.getRightSibling();
                itemIndex = 0;
                continue;
              }

              while (itemIndex < bucketSize && dataCache.size() < prefetchSize) {
                final Map.Entry<K, ORID> entry = convertToMapEntry(bucket.getEntry(itemIndex, encryption, keySerializer));
                itemIndex++;

                if (fromKey != null && (fromKeyInclusive
                    ? comparator.compare(entry.getKey(), fromKey) < 0
                    : comparator.compare(entry.getKey(), fromKey) <= 0)) {
                  continue;
                }

                if (toKey != null && (toKeyInclusive
                    ? comparator.compare(entry.getKey(), toKey) > 0
                    : comparator.compare(entry.getKey(), toKey) >= 0)) {
                  break mainCycle;
                }

                dataCache.add(entry);
              }

            } finally {
              releasePageFromRead(atomicOperation, cacheEntry);
            }
          }
        } finally {
          releaseSharedLock();
        }
      } catch (final IOException e) {
        throw OException
            .wrapException(new OCellBTreeSingleValueV1Exception("Error during element iteration", CellBTreeSingleValueV1.this), e);
      } finally {
        atomicOperationsManager.releaseReadLock(CellBTreeSingleValueV1.this);
      }

      if (dataCache.isEmpty()) {
        dataCacheIterator = null;
        return null;
      }

      dataCacheIterator = dataCache.iterator();

      final Map.Entry<K, ORID> entry = dataCacheIterator.next();

      fromKey = entry.getKey();
      fromKeyInclusive = false;

      return entry;
    }
  }

  private final class OCellBTreeCursorBackward implements OCellBTreeCursor<K, ORID> {
    private final K       fromKey;
    private       K       toKey;
    private final boolean fromKeyInclusive;
    private       boolean toKeyInclusive;

    private final List<Map.Entry<K, ORID>>     dataCache         = new ArrayList<>();
    @SuppressWarnings("unchecked")
    private       Iterator<Map.Entry<K, ORID>> dataCacheIterator = OEmptyMapEntryIterator.INSTANCE;

    private OCellBTreeCursorBackward(final K fromKey, final K toKey, final boolean fromKeyInclusive, final boolean toKeyInclusive) {
      this.fromKey = fromKey;
      this.toKey = toKey;
      this.fromKeyInclusive = fromKeyInclusive;
      this.toKeyInclusive = toKeyInclusive;

      if (toKey == null) {
        this.toKeyInclusive = true;
      }

    }

    public Map.Entry<K, ORID> next(int prefetchSize) {
      if (dataCacheIterator == null) {
        return null;
      }

      if (dataCacheIterator.hasNext()) {
        final Map.Entry<K, ORID> entry = dataCacheIterator.next();
        toKey = entry.getKey();

        toKeyInclusive = false;
        return entry;
      }

      dataCache.clear();

      if (prefetchSize < 0 || prefetchSize > OGlobalConfiguration.INDEX_CURSOR_PREFETCH_SIZE.getValueAsInteger()) {
        prefetchSize = OGlobalConfiguration.INDEX_CURSOR_PREFETCH_SIZE.getValueAsInteger();
      }

      atomicOperationsManager.acquireReadLock(CellBTreeSingleValueV1.this);
      try {
        acquireSharedLock();
        try {
          final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();

          final BucketSearchResult bucketSearchResult;

          if (toKey != null) {
            bucketSearchResult = findBucket(toKey, atomicOperation);
          } else {
            bucketSearchResult = lastItem(atomicOperation);
          }

          if (bucketSearchResult == null) {
            dataCacheIterator = null;
            return null;
          }

          long pageIndex = bucketSearchResult.pageIndex;

          int itemIndex;
          if (bucketSearchResult.itemIndex >= 0) {
            itemIndex = toKeyInclusive ? bucketSearchResult.itemIndex : bucketSearchResult.itemIndex - 1;
          } else {
            itemIndex = -bucketSearchResult.itemIndex - 2;
          }

          mainCycle:
          while (dataCache.size() < prefetchSize) {
            if (pageIndex == -1) {
              break;
            }

            final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
            try {
              final CellBTreeBucketSingleValueV1<K> bucket = new CellBTreeBucketSingleValueV1<>(cacheEntry);

              if (itemIndex >= bucket.size()) {
                itemIndex = bucket.size() - 1;
              }

              if (itemIndex < 0) {
                pageIndex = bucket.getLeftSibling();
                itemIndex = Integer.MAX_VALUE;
                continue;
              }

              while (itemIndex >= 0 && dataCache.size() < prefetchSize) {
                final Map.Entry<K, ORID> entry = convertToMapEntry(bucket.getEntry(itemIndex, encryption, keySerializer));
                itemIndex--;

                if (toKey != null && (toKeyInclusive
                    ? comparator.compare(entry.getKey(), toKey) > 0
                    : comparator.compare(entry.getKey(), toKey) >= 0)) {
                  continue;
                }

                if (fromKey != null && (fromKeyInclusive
                    ? comparator.compare(entry.getKey(), fromKey) < 0
                    : comparator.compare(entry.getKey(), fromKey) <= 0)) {
                  break mainCycle;
                }

                dataCache.add(entry);
              }

            } finally {
              releasePageFromRead(atomicOperation, cacheEntry);
            }
          }
        } finally {
          releaseSharedLock();
        }
      } catch (final IOException e) {
        throw OException
            .wrapException(new OCellBTreeSingleValueV1Exception("Error during element iteration", CellBTreeSingleValueV1.this), e);
      } finally {
        atomicOperationsManager.releaseReadLock(CellBTreeSingleValueV1.this);
      }

      if (dataCache.isEmpty()) {
        dataCacheIterator = null;
        return null;
      }

      dataCacheIterator = dataCache.iterator();

      final Map.Entry<K, ORID> entry = dataCacheIterator.next();

      toKey = entry.getKey();
      toKeyInclusive = false;

      return entry;
    }
  }
}
