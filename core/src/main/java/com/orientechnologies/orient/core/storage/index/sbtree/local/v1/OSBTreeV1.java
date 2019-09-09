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

package com.orientechnologies.orient.core.storage.index.sbtree.local.v1;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.exception.NotEmptyComponentCanNotBeRemovedException;
import com.orientechnologies.orient.core.exception.OTooBigIndexKeyException;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.index.engine.OBaseIndexEngine;
import com.orientechnologies.orient.core.iterator.OEmptyIterator;
import com.orientechnologies.orient.core.iterator.OEmptyMapEntryIterator;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.OComponentOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.sbtree.OSBTreePutCO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.sbtree.OSBTreeRemoveCO;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

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
public final class OSBTreeV1<K, V> extends ODurableComponent
    implements com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTree<K, V> {
  private static final int               MAX_KEY_SIZE            = OGlobalConfiguration.SBTREE_MAX_KEY_SIZE.getValueAsInteger();
  private static final int               MAX_EMBEDDED_VALUE_SIZE = OGlobalConfiguration.SBTREE_MAX_EMBEDDED_VALUE_SIZE
      .getValueAsInteger();
  private static final OAlwaysLessKey    ALWAYS_LESS_KEY         = new OAlwaysLessKey();
  private static final OAlwaysGreaterKey ALWAYS_GREATER_KEY      = new OAlwaysGreaterKey();

  private static final int MAX_PATH_LENGTH = OGlobalConfiguration.SBTREE_MAX_DEPTH.getValueAsInteger();

  private static final long                  ROOT_INDEX       = 0;
  private final        Comparator<? super K> comparator       = ODefaultComparator.INSTANCE;
  private final        String                nullFileExtension;
  private              long                  fileId;
  private              long                  nullBucketFileId = -1;
  private              int                   keySize;
  private              OBinarySerializer<K>  keySerializer;
  private              OType[]               keyTypes;
  private              OBinarySerializer<V>  valueSerializer;
  private              boolean               nullPointerSupport;
  private final        AtomicLong            bonsayFileId     = new AtomicLong(0);
  private              OEncryption           encryption;
  private final        int                   indexId;

  public OSBTreeV1(int indexId, final String name, final String dataFileExtension, final String nullFileExtension,
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
  public void create(final OBinarySerializer<K> keySerializer, final OBinarySerializer<V> valueSerializer, final OType[] keyTypes,
      final int keySize, final boolean nullPointerSupport, final OEncryption encryption) throws IOException {
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

        this.valueSerializer = valueSerializer;
        this.nullPointerSupport = nullPointerSupport;

        fileId = addFile(atomicOperation, getFullName());

        if (nullPointerSupport) {
          nullBucketFileId = addFile(atomicOperation, getName() + nullFileExtension);
        }

        final OCacheEntry rootCacheEntry = addPage(atomicOperation, fileId);
        try {
          final OSBTreeBucketV1<K, V> rootBucket = new OSBTreeBucketV1<>(rootCacheEntry);
          rootBucket.init(true);

        } finally {
          releasePageFromWrite(atomicOperation, rootCacheEntry);
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
  public boolean isNullPointerSupport() {
    acquireSharedLock();
    try {
      return nullPointerSupport;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public V get(K key) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        checkNullSupport(key);

        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
        if (key != null) {
          //noinspection RedundantCast
          key = keySerializer.preprocess(key, (Object[]) keyTypes);

          final BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);
          if (bucketSearchResult.itemIndex < 0) {
            return null;
          }

          final long pageIndex = bucketSearchResult.getLastPathItem();
          final OCacheEntry keyBucketCacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
          try {
            final OSBTreeBucketV1<K, V> keyBucket = new OSBTreeBucketV1<>(keyBucketCacheEntry);

            final OSBTreeBucketV1.SBTreeEntry<K, V> treeEntry = keyBucket
                .getEntry(bucketSearchResult.itemIndex, encryption, keySerializer, valueSerializer);
            return treeEntry.value.getValue();
          } finally {
            releasePageFromRead(atomicOperation, keyBucketCacheEntry);
          }
        } else {
          if (getFilledUpTo(atomicOperation, nullBucketFileId) == 0) {
            return null;
          }

          final OCacheEntry nullBucketCacheEntry = loadPageForRead(atomicOperation, nullBucketFileId, 0, false);
          try {
            final OSBTreeNullBucketV1<V> nullBucket = new OSBTreeNullBucketV1<>(nullBucketCacheEntry);
            final OSBTreeValue<V> treeValue = nullBucket.getValue(valueSerializer);
            if (treeValue == null) {
              return null;
            }

            return treeValue.getValue();
          } finally {
            releasePageFromRead(atomicOperation, nullBucketCacheEntry);
          }
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException.wrapException(new OSBTreeException("Error during retrieving  of sbtree with name " + getName(), this), e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public void put(final K key, final V value) throws IOException {
    put(key, value, null);
  }

  @Override
  public boolean validatedPut(final K key, final V value, final OBaseIndexEngine.Validator<K, V> validator) throws IOException {
    return put(key, value, validator);
  }

  private boolean put(final K key, final V value, final OBaseIndexEngine.Validator<K, V> validator) throws IOException {
    return update(key, (x, bonsayFileId) -> OIndexUpdateAction.changed(value), validator);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean update(K key, final OIndexKeyUpdater<V> updater, final OBaseIndexEngine.Validator<K, V> validator)
      throws IOException {
    boolean rollback = false;
    final OAtomicOperation atomicOperation = startAtomicOperation(true);
    try {
      acquireExclusiveLock();
      try {
        checkNullSupport(key);

        OComponentOperationRecord operationRecord = null;
        if (key != null) {
          //noinspection RedundantCast
          key = keySerializer.preprocess(key, (Object[]) keyTypes);

          if (keySize > MAX_KEY_SIZE) {
            throw new OTooBigIndexKeyException(
                "Key size is more than allowed, operation was canceled. Current key size " + keySize + ", allowed  " + MAX_KEY_SIZE,
                getName());
          }

          BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);

          OCacheEntry keyBucketCacheEntry = loadPageForWrite(atomicOperation, fileId, bucketSearchResult.getLastPathItem(), false,
              true);
          OSBTreeBucketV1<K, V> keyBucket = new OSBTreeBucketV1<>(keyBucketCacheEntry);

          final byte[] oldRawValue = bucketSearchResult.itemIndex > -1 ? keyBucket
              .getRawValue(bucketSearchResult.itemIndex, encryption != null, keySerializer, valueSerializer) : null;
          final V oldValue;
          if (oldRawValue == null) {
            oldValue = null;
          } else {
            oldValue = valueSerializer.deserializeNativeObject(oldRawValue, 0);
          }

          final OIndexUpdateAction<V> updatedValue = updater.update(oldValue, bonsayFileId);
          if (updatedValue.isChange()) {
            V value = updatedValue.getValue();

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

                value = (V) result;
                failure = false;
              } finally {
                if (failure || ignored) {
                  releasePageFromWrite(atomicOperation, keyBucketCacheEntry);
                }
              }
            }

            final int valueSize = valueSerializer.getObjectSize(value);
            final byte[] serializeValue = new byte[valueSize];
            valueSerializer.serializeNativeObject(value, serializeValue, 0);
            final byte[] rawKey = serializeKey(key);

            final boolean createLinkToTheValue = valueSize > MAX_EMBEDDED_VALUE_SIZE;
            assert !createLinkToTheValue;

            int insertionIndex;
            final int sizeDiff;
            if (bucketSearchResult.itemIndex >= 0) {

              assert oldRawValue != null;
              if (oldRawValue.length == serializeValue.length) {
                keyBucket.updateValue(bucketSearchResult.itemIndex, serializeValue, rawKey.length);
                releasePageFromWrite(atomicOperation, keyBucketCacheEntry);

                if (indexId >= 0) {
                  atomicOperation.addComponentOperation(
                      new OSBTreePutCO(indexId, encryption != null ? encryption.name() : null, keySerializer.getId(), null,
                          valueSerializer.getId(), serializeValue, oldRawValue));
                }
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

            while (!keyBucket.addLeafEntry(insertionIndex, rawKey, serializeValue)) {
              releasePageFromWrite(atomicOperation, keyBucketCacheEntry);

              bucketSearchResult = splitBucket(bucketSearchResult.path, insertionIndex, key, atomicOperation);

              insertionIndex = bucketSearchResult.itemIndex;

              keyBucketCacheEntry = loadPageForWrite(atomicOperation, fileId, bucketSearchResult.getLastPathItem(), false, true);

              keyBucket = new OSBTreeBucketV1<>(keyBucketCacheEntry);
            }

            releasePageFromWrite(atomicOperation, keyBucketCacheEntry);

            if (sizeDiff != 0) {
              updateSize(sizeDiff, atomicOperation);
            }

            if (indexId >= 0) {
              operationRecord = new OSBTreePutCO(indexId, encryption != null ? encryption.name() : null, keySerializer.getId(),
                  rawKey, valueSerializer.getId(), serializeValue, oldRawValue);
            }

          } else if (updatedValue.isRemove()) {
            final byte[] rawKey = serializeKey(key);
            final byte[] removedValue = removeKey(atomicOperation, bucketSearchResult, rawKey);
            releasePageFromWrite(atomicOperation, keyBucketCacheEntry);

            if (indexId >= 0) {
              operationRecord = new OSBTreeRemoveCO(indexId, encryption != null ? encryption.name() : null, keySerializer.getId(),
                  rawKey, removedValue, valueSerializer.getId());
            }
          } else if (updatedValue.isNothing()) {
            releasePageFromWrite(atomicOperation, keyBucketCacheEntry);
          }
        } else {
          final OCacheEntry cacheEntry;
          boolean isNew = false;

          if (getFilledUpTo(atomicOperation, nullBucketFileId) == 0) {
            cacheEntry = addPage(atomicOperation, nullBucketFileId);
            isNew = true;
          } else {
            cacheEntry = loadPageForWrite(atomicOperation, nullBucketFileId, 0, false, true);
          }

          int sizeDiff = 0;

          try {
            final OSBTreeNullBucketV1<V> nullBucket = new OSBTreeNullBucketV1<>(cacheEntry);
            if (isNew) {
              nullBucket.init();
            }

            final byte[] oldRawValue = nullBucket.getRawValue(valueSerializer);
            final V oldValue = oldRawValue == null ? null : valueSerializer.deserializeNativeObject(oldRawValue, 0);

            final OIndexUpdateAction<V> updatedValue = updater.update(oldValue, bonsayFileId);
            if (updatedValue.isChange()) {
              final V value = updatedValue.getValue();
              final int valueSize = valueSerializer.getObjectSize(value);

              if (validator != null) {
                final Object result = validator.validate(null, oldValue, value);
                if (result == OBaseIndexEngine.Validator.IGNORE) {
                  return false;
                }
              }

              if (oldValue != null) {
                sizeDiff = -1;
              }

              final byte[] serializeValue = new byte[valueSize];
              valueSerializer.serializeNativeObject(value, serializeValue, 0);

              nullBucket.setValue(serializeValue, valueSerializer);

              if (indexId >= 0) {
                operationRecord = new OSBTreePutCO(indexId, encryption != null ? encryption.name() : null, keySerializer.getId(),
                    null, valueSerializer.getId(), serializeValue, oldRawValue);
              }
            } else if (updatedValue.isRemove()) {
              final V removedValue = removeNullBucket(atomicOperation);

              if (indexId >= 0 && removedValue != null) {
                final byte[] serializedValue = valueSerializer.serializeNativeAsWhole(removedValue);

                operationRecord = new OSBTreeRemoveCO(indexId, encryption != null ? encryption.name() : null, keySerializer.getId(),
                    null, serializedValue, valueSerializer.getId());
              }
            } else //noinspection StatementWithEmptyBody
              if (updatedValue.isNothing()) {
                //Do Nothing
              }
          } finally {
            releasePageFromWrite(atomicOperation, cacheEntry);
          }

          sizeDiff++;
          updateSize(sizeDiff, atomicOperation);
        }

        if (operationRecord != null) {
          atomicOperation.addComponentOperation(operationRecord);
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

  private byte[] serializeKey(final K key) {
    //noinspection RedundantCast
    final byte[] serializedKey = keySerializer.serializeNativeAsWhole(key, (Object[]) keyTypes);
    final byte[] rawKey;

    if (encryption == null) {
      rawKey = serializedKey;
    } else {
      final byte[] encryptedKey = encryption.encrypt(serializedKey);

      rawKey = new byte[OIntegerSerializer.INT_SIZE + encryptedKey.length];
      OIntegerSerializer.INSTANCE.serializeNative(encryptedKey.length, rawKey, 0);
      System.arraycopy(encryptedKey, 0, rawKey, OIntegerSerializer.INT_SIZE, encryptedKey.length);
    }

    return rawKey;
  }

  @Override
  public void close(final boolean flush) {
    acquireExclusiveLock();
    try {
      readCache.closeFile(fileId, flush, writeCache);

      if (nullPointerSupport) {
        readCache.closeFile(nullBucketFileId, flush, writeCache);
      }

    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void close() {
    close(true);
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

        if (nullPointerSupport) {
          deleteFile(atomicOperation, nullBucketFileId);
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
  public void load(final String name, final OBinarySerializer<K> keySerializer, final OBinarySerializer<V> valueSerializer,
      final OType[] keyTypes, final int keySize, final boolean nullPointerSupport, final OEncryption encryption) {
    acquireExclusiveLock();
    try {
      this.keySize = keySize;
      if (keyTypes != null) {
        this.keyTypes = Arrays.copyOf(keyTypes, keyTypes.length);
      } else {
        this.keyTypes = null;
      }

      this.encryption = encryption;
      this.nullPointerSupport = nullPointerSupport;

      final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();

      fileId = openFile(atomicOperation, getFullName());
      if (nullPointerSupport) {
        nullBucketFileId = openFile(atomicOperation, name + nullFileExtension);
      }

      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;
    } catch (final IOException e) {
      throw OException.wrapException(new OSBTreeException("Exception during loading of sbtree " + name, this), e);
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

        final OCacheEntry rootCacheEntry = loadPageForRead(atomicOperation, fileId, ROOT_INDEX, false);
        try {
          final OSBTreeBucketV1<K, V> rootBucket = new OSBTreeBucketV1<>(rootCacheEntry);
          return rootBucket.getTreeSize();
        } finally {
          releasePageFromRead(atomicOperation, rootCacheEntry);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException.wrapException(new OSBTreeException("Error during retrieving of size of index " + getName(), this), e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public V remove(K key) throws IOException {
    boolean rollback = false;
    final OAtomicOperation atomicOperation = startAtomicOperation(true);
    try {
      acquireExclusiveLock();
      try {
        final V removedValue;

        if (key != null) {
          //noinspection RedundantCast
          key = keySerializer.preprocess(key, (Object[]) keyTypes);

          final BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);
          if (bucketSearchResult.itemIndex < 0) {
            return null;
          }

          final byte[] rawKey = serializeKey(key);
          final byte[] rawRemovedValue = removeKey(atomicOperation, bucketSearchResult, rawKey);
          removedValue = valueSerializer.deserializeNativeObject(rawRemovedValue, 0);

          if (indexId >= 0) {
            atomicOperation.addComponentOperation(
                new OSBTreeRemoveCO(indexId, encryption != null ? encryption.name() : null, keySerializer.getId(), rawKey,
                    rawRemovedValue, valueSerializer.getId()));
          }
        } else {
          if (getFilledUpTo(atomicOperation, nullBucketFileId) == 0) {
            return null;
          }

          removedValue = removeNullBucket(atomicOperation);
          if (removedValue != null && indexId >= 0) {
            final byte[] serializedValue = valueSerializer.serializeNativeAsWhole(removedValue);
            atomicOperation.addComponentOperation(
                new OSBTreeRemoveCO(indexId, encryption != null ? encryption.name() : null, keySerializer.getId(), null,
                    serializedValue, valueSerializer.getId()));
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

  private V removeNullBucket(final OAtomicOperation atomicOperation) throws IOException {
    V removedValue;
    final OCacheEntry nullCacheEntry = loadPageForWrite(atomicOperation, nullBucketFileId, 0, false, true);
    try {
      final OSBTreeNullBucketV1<V> nullBucket = new OSBTreeNullBucketV1<>(nullCacheEntry);
      final OSBTreeValue<V> treeValue = nullBucket.getValue(valueSerializer);

      if (treeValue != null) {
        removedValue = treeValue.getValue();
        nullBucket.removeValue(valueSerializer);
      } else {
        removedValue = null;
      }
    } finally {
      releasePageFromWrite(atomicOperation, nullCacheEntry);
    }

    if (removedValue != null) {
      updateSize(-1, atomicOperation);
    }
    return removedValue;
  }

  private byte[] removeKey(final OAtomicOperation atomicOperation, final BucketSearchResult bucketSearchResult, final byte[] key)
      throws IOException {
    byte[] removedValue;
    final OCacheEntry keyBucketCacheEntry = loadPageForWrite(atomicOperation, fileId, bucketSearchResult.getLastPathItem(), false,
        true);
    try {
      final OSBTreeBucketV1<K, V> keyBucket = new OSBTreeBucketV1<>(keyBucketCacheEntry);

      removedValue = keyBucket.getRawValue(bucketSearchResult.itemIndex, encryption != null, keySerializer, valueSerializer);
      keyBucket.removeLeafEntry(bucketSearchResult.itemIndex, key, removedValue);
      updateSize(-1, atomicOperation);
    } finally {
      releasePageFromWrite(atomicOperation, keyBucketCacheEntry);
    }
    return removedValue;
  }

  @Override
  public OSBTreeCursor<K, V> iterateEntriesMinor(final K key, final boolean inclusive, final boolean ascSortOrder) {

    if (!ascSortOrder) {
      return iterateEntriesMinorDesc(key, inclusive);
    }

    return iterateEntriesMinorAsc(key, inclusive);
  }

  @Override
  public OSBTreeCursor<K, V> iterateEntriesMajor(final K key, final boolean inclusive, final boolean ascSortOrder) {
    if (ascSortOrder) {
      return iterateEntriesMajorAsc(key, inclusive);
    }

    return iterateEntriesMajorDesc(key, inclusive);
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

        final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, searchResult.getLastPathItem(), false);
        try {
          final OSBTreeBucketV1<K, V> bucket = new OSBTreeBucketV1<>(cacheEntry);
          return bucket.getKey(searchResult.itemIndex, encryption, keySerializer);
        } finally {
          releasePageFromRead(atomicOperation, cacheEntry);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException.wrapException(new OSBTreeException("Error during finding first key in sbtree [" + getName() + "]", this), e);
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

        final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, searchResult.getLastPathItem(), false);
        try {
          final OSBTreeBucketV1<K, V> bucket = new OSBTreeBucketV1<>(cacheEntry);
          return bucket.getKey(searchResult.itemIndex, encryption, keySerializer);
        } finally {
          releasePageFromRead(atomicOperation, cacheEntry);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException.wrapException(new OSBTreeException("Error during finding last key in sbtree [" + getName() + "]", this), e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public OSBTreeKeyCursor<K> keyCursor() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
        final BucketSearchResult searchResult = firstItem(atomicOperation);
        if (searchResult == null) {
          return prefetchSize -> null;
        }

        return new OSBTreeFullKeyCursor(searchResult.getLastPathItem());
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException.wrapException(new OSBTreeException("Error during finding first key in sbtree [" + getName() + "]", this), e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public OSBTreeCursor<K, V> iterateEntriesBetween(final K keyFrom, final boolean fromInclusive, final K keyTo,
      final boolean toInclusive, final boolean ascSortOrder) {

    if (ascSortOrder) {
      return iterateEntriesBetweenAscOrder(keyFrom, fromInclusive, keyTo, toInclusive);
    } else {
      return iterateEntriesBetweenDescOrder(keyFrom, fromInclusive, keyTo, toInclusive);
    }
  }

  @Override
  public void flush() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        writeCache.flush();
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

  private void checkNullSupport(final K key) {
    if (key == null && !nullPointerSupport) {
      throw new OSBTreeException("Null keys are not supported.", this);
    }
  }

  private void updateSize(final long diffSize, final OAtomicOperation atomicOperation) throws IOException {
    final OCacheEntry rootCacheEntry = loadPageForWrite(atomicOperation, fileId, ROOT_INDEX, false, true);
    try {
      final OSBTreeBucketV1<K, V> rootBucket = new OSBTreeBucketV1<>(rootCacheEntry);
      rootBucket.setTreeSize(rootBucket.getTreeSize() + diffSize);
    } finally {
      releasePageFromWrite(atomicOperation, rootCacheEntry);
    }
  }

  private OSBTreeCursor<K, V> iterateEntriesMinorDesc(K key, final boolean inclusive) {
    //noinspection RedundantCast
    key = keySerializer.preprocess(key, (Object[]) keyTypes);
    key = enhanceCompositeKeyMinorDesc(key, inclusive);

    return new OSBTreeCursorBackward(null, key, false, inclusive);
  }

  private OSBTreeCursor<K, V> iterateEntriesMinorAsc(K key, final boolean inclusive) {
    //noinspection RedundantCast
    key = keySerializer.preprocess(key, (Object[]) keyTypes);
    key = enhanceCompositeKeyMinorAsc(key, inclusive);

    return new OSBTreeCursorForward(null, key, false, inclusive);
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

  private OSBTreeCursor<K, V> iterateEntriesMajorAsc(K key, final boolean inclusive) {
    //noinspection RedundantCast
    key = keySerializer.preprocess(key, (Object[]) keyTypes);
    key = enhanceCompositeKeyMajorAsc(key, inclusive);

    return new OSBTreeCursorForward(key, null, inclusive, false);
  }

  private OSBTreeCursor<K, V> iterateEntriesMajorDesc(K key, final boolean inclusive) {
    //noinspection RedundantCast
    key = keySerializer.preprocess(key, (Object[]) keyTypes);
    key = enhanceCompositeKeyMajorDesc(key, inclusive);

    return new OSBTreeCursorBackward(key, null, inclusive, false);
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
      OSBTreeBucketV1<K, V> bucket = new OSBTreeBucketV1<>(cacheEntry);

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
              final OSBTreeBucketV1.SBTreeEntry<K, V> entry = bucket
                  .getEntry(itemIndex, encryption, keySerializer, valueSerializer);
              bucketIndex = entry.leftChild;
            } else {
              final OSBTreeBucketV1.SBTreeEntry<K, V> entry = bucket
                  .getEntry(itemIndex - 1, encryption, keySerializer, valueSerializer);
              bucketIndex = entry.rightChild;
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
            final ArrayList<Long> resultPath = new ArrayList<>(path.size() + 1);
            for (final PagePathItemUnit pathItemUnit : path) {
              resultPath.add(pathItemUnit.pageIndex);
            }

            resultPath.add(bucketIndex);
            return new BucketSearchResult(0, resultPath);
          }
        }

        releasePageFromRead(atomicOperation, cacheEntry);

        cacheEntry = loadPageForRead(atomicOperation, fileId, bucketIndex, false);

        bucket = new OSBTreeBucketV1<>(cacheEntry);
      }
    } finally {
      releasePageFromRead(atomicOperation, cacheEntry);
    }
  }

  private BucketSearchResult lastItem(final OAtomicOperation atomicOperation) throws IOException {
    final LinkedList<PagePathItemUnit> path = new LinkedList<>();

    long bucketIndex = ROOT_INDEX;

    OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, bucketIndex, false);

    OSBTreeBucketV1<K, V> bucket = new OSBTreeBucketV1<>(cacheEntry);

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
              final OSBTreeBucketV1.SBTreeEntry<K, V> entry = bucket
                  .getEntry(itemIndex, encryption, keySerializer, valueSerializer);
              bucketIndex = entry.rightChild;
            } else {
              final OSBTreeBucketV1.SBTreeEntry<K, V> entry = bucket.getEntry(0, encryption, keySerializer, valueSerializer);
              bucketIndex = entry.leftChild;
            }

            itemIndex = OSBTreeBucketV1.MAX_PAGE_SIZE_BYTES + 1;
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
            final ArrayList<Long> resultPath = new ArrayList<>(path.size() + 1);
            for (final PagePathItemUnit pathItemUnit : path) {
              resultPath.add(pathItemUnit.pageIndex);
            }

            resultPath.add(bucketIndex);

            return new BucketSearchResult(bucket.size() - 1, resultPath);
          }
        }

        releasePageFromRead(atomicOperation, cacheEntry);

        cacheEntry = loadPageForRead(atomicOperation, fileId, bucketIndex, false);

        bucket = new OSBTreeBucketV1<>(cacheEntry);
        if (itemIndex == OSBTreeBucketV1.MAX_PAGE_SIZE_BYTES + 1) {
          itemIndex = bucket.size() - 1;
        }
      }
    } finally {
      releasePageFromRead(atomicOperation, cacheEntry);
    }
  }

  private OSBTreeCursor<K, V> iterateEntriesBetweenAscOrder(K keyFrom, final boolean fromInclusive, K keyTo,
      final boolean toInclusive) {
    //noinspection RedundantCast
    keyFrom = keySerializer.preprocess(keyFrom, (Object[]) keyTypes);
    //noinspection RedundantCast
    keyTo = keySerializer.preprocess(keyTo, (Object[]) keyTypes);

    keyFrom = enhanceFromCompositeKeyBetweenAsc(keyFrom, fromInclusive);
    keyTo = enhanceToCompositeKeyBetweenAsc(keyTo, toInclusive);

    return new OSBTreeCursorForward(keyFrom, keyTo, fromInclusive, toInclusive);
  }

  private OSBTreeCursor<K, V> iterateEntriesBetweenDescOrder(K keyFrom, final boolean fromInclusive, K keyTo,
      final boolean toInclusive) {
    //noinspection RedundantCast
    keyFrom = keySerializer.preprocess(keyFrom, (Object[]) keyTypes);
    //noinspection RedundantCast
    keyTo = keySerializer.preprocess(keyTo, (Object[]) keyTypes);

    keyFrom = enhanceFromCompositeKeyBetweenDesc(keyFrom, fromInclusive);
    keyTo = enhanceToCompositeKeyBetweenDesc(keyTo, toInclusive);

    return new OSBTreeCursorBackward(keyFrom, keyTo, fromInclusive, toInclusive);
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

  private BucketSearchResult splitBucket(final List<Long> path, final int keyIndex, final K keyToInsert,
      final OAtomicOperation atomicOperation) throws IOException {
    final long pageIndex = path.get(path.size() - 1);

    final OCacheEntry bucketEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, false, true);
    try {
      final OSBTreeBucketV1<K, V> bucketToSplit = new OSBTreeBucketV1<>(bucketEntry);

      final boolean splitLeaf = bucketToSplit.isLeaf();
      final int bucketSize = bucketToSplit.size();

      final int indexToSplit = bucketSize >>> 1;
      final K separationKey = bucketToSplit.getKey(indexToSplit, encryption, keySerializer);
      final List<byte[]> rightEntries = new ArrayList<>(indexToSplit);

      final int startRightIndex = splitLeaf ? indexToSplit : indexToSplit + 1;

      for (int i = startRightIndex; i < bucketSize; i++) {
        rightEntries.add(bucketToSplit.getRawEntry(i, encryption != null, keySerializer, valueSerializer));
      }

      if (pageIndex != ROOT_INDEX) {
        return splitNonRootBucket(path, keyIndex, keyToInsert, pageIndex, bucketToSplit, splitLeaf, indexToSplit, separationKey,
            rightEntries, atomicOperation);
      } else {
        return splitRootBucket(path, keyIndex, keyToInsert, bucketEntry, bucketToSplit, splitLeaf, indexToSplit, separationKey,
            rightEntries, atomicOperation);
      }
    } finally {
      releasePageFromWrite(atomicOperation, bucketEntry);
    }
  }

  private BucketSearchResult splitNonRootBucket(final List<Long> path, final int keyIndex, final K keyToInsert,
      final long pageIndex, final OSBTreeBucketV1<K, V> bucketToSplit, final boolean splitLeaf, final int indexToSplit,
      final K separationKey, final List<byte[]> rightEntries, final OAtomicOperation atomicOperation) throws IOException {
    final OCacheEntry rightBucketEntry = addPage(atomicOperation, fileId);

    try {
      final OSBTreeBucketV1<K, V> newRightBucket = new OSBTreeBucketV1<>(rightBucketEntry);
      newRightBucket.init(splitLeaf);
      newRightBucket.addAll(rightEntries, encryption != null, keySerializer, valueSerializer);

      bucketToSplit.shrink(indexToSplit, encryption != null, keySerializer, valueSerializer);
      if (splitLeaf) {
        final long rightSiblingPageIndex = bucketToSplit.getRightSibling();

        newRightBucket.setRightSibling(rightSiblingPageIndex);
        newRightBucket.setLeftSibling(pageIndex);

        bucketToSplit.setRightSibling(rightBucketEntry.getPageIndex());

        if (rightSiblingPageIndex >= 0) {
          final OCacheEntry rightSiblingBucketEntry = loadPageForWrite(atomicOperation, fileId, rightSiblingPageIndex, false, true);
          final OSBTreeBucketV1<K, V> rightSiblingBucket = new OSBTreeBucketV1<>(rightSiblingBucketEntry);
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
        OSBTreeBucketV1<K, V> parentBucket = new OSBTreeBucketV1<>(parentCacheEntry);
        int insertionIndex = parentBucket.find(separationKey, encryption, keySerializer);
        assert insertionIndex < 0;

        insertionIndex = -insertionIndex - 1;
        final byte[] rawSeparationKey = serializeKey(separationKey);
        while (!parentBucket.addNonLeafEntry(insertionIndex, rawSeparationKey, pageIndex, rightBucketEntry.getPageIndex(), true)) {
          releasePageFromWrite(atomicOperation, parentCacheEntry);

          final BucketSearchResult bucketSearchResult = splitBucket(path.subList(0, path.size() - 1), insertionIndex, separationKey,
              atomicOperation);

          parentIndex = bucketSearchResult.getLastPathItem();
          parentCacheEntry = loadPageForWrite(atomicOperation, fileId, parentIndex, false, true);

          insertionIndex = bucketSearchResult.itemIndex;

          parentBucket = new OSBTreeBucketV1<>(parentCacheEntry);
        }

      } finally {
        releasePageFromWrite(atomicOperation, parentCacheEntry);
      }

    } finally {
      releasePageFromWrite(atomicOperation, rightBucketEntry);
    }

    final ArrayList<Long> resultPath = new ArrayList<>(path.subList(0, path.size() - 1));

    if (comparator.compare(keyToInsert, separationKey) < 0) {
      resultPath.add(pageIndex);
      return new BucketSearchResult(keyIndex, resultPath);
    }

    resultPath.add((long) rightBucketEntry.getPageIndex());
    if (splitLeaf) {
      return new BucketSearchResult(keyIndex - indexToSplit, resultPath);
    }

    resultPath.add((long) rightBucketEntry.getPageIndex());
    return new BucketSearchResult(keyIndex - indexToSplit - 1, resultPath);
  }

  private BucketSearchResult splitRootBucket(final List<Long> path, final int keyIndex, final K keyToInsert,
      final OCacheEntry bucketEntry, OSBTreeBucketV1<K, V> bucketToSplit, final boolean splitLeaf, final int indexToSplit,
      final K separationKey, final List<byte[]> rightEntries, final OAtomicOperation atomicOperation) throws IOException {
    final long treeSize = bucketToSplit.getTreeSize();

    final List<byte[]> leftEntries = new ArrayList<>(indexToSplit);

    for (int i = 0; i < indexToSplit; i++) {
      leftEntries.add(bucketToSplit.getRawEntry(i, encryption != null, keySerializer, valueSerializer));
    }

    final OCacheEntry leftBucketEntry = addPage(atomicOperation, fileId);

    final OCacheEntry rightBucketEntry = addPage(atomicOperation, fileId);
    try {
      final OSBTreeBucketV1<K, V> newLeftBucket = new OSBTreeBucketV1<>(leftBucketEntry);
      newLeftBucket.init(splitLeaf);
      newLeftBucket.addAll(leftEntries, encryption != null, keySerializer, valueSerializer);

      if (splitLeaf) {
        newLeftBucket.setRightSibling(rightBucketEntry.getPageIndex());
      }

    } finally {
      releasePageFromWrite(atomicOperation, leftBucketEntry);
    }

    try {
      final OSBTreeBucketV1<K, V> newRightBucket = new OSBTreeBucketV1<>(rightBucketEntry);
      newRightBucket.init(splitLeaf);
      newRightBucket.addAll(rightEntries, encryption != null, keySerializer, valueSerializer);

      if (splitLeaf) {
        newRightBucket.setLeftSibling(leftBucketEntry.getPageIndex());
      }
    } finally {
      releasePageFromWrite(atomicOperation, rightBucketEntry);
    }

    bucketToSplit = new OSBTreeBucketV1<>(bucketEntry);
    bucketToSplit.shrink(0, encryption != null, keySerializer, valueSerializer);
    if (splitLeaf) {
      bucketToSplit.switchBucketType();
    }

    bucketToSplit.setTreeSize(treeSize);
    bucketToSplit
        .addNonLeafEntry(0, serializeKey(separationKey), leftBucketEntry.getPageIndex(), rightBucketEntry.getPageIndex(), true);

    final ArrayList<Long> resultPath = new ArrayList<>(path.subList(0, path.size() - 1));
    if (comparator.compare(keyToInsert, separationKey) < 0) {
      resultPath.add((long) leftBucketEntry.getPageIndex());
      return new BucketSearchResult(keyIndex, resultPath);
    }

    resultPath.add((long) rightBucketEntry.getPageIndex());

    if (splitLeaf) {
      return new BucketSearchResult(keyIndex - indexToSplit, resultPath);
    }

    return new BucketSearchResult(keyIndex - indexToSplit - 1, resultPath);
  }

  private BucketSearchResult findBucket(final K key, final OAtomicOperation atomicOperation) throws IOException {
    long pageIndex = ROOT_INDEX;
    final ArrayList<Long> path = new ArrayList<>(8);

    while (true) {
      if (path.size() > MAX_PATH_LENGTH) {
        throw new OSBTreeException(
            "We reached max level of depth of SBTree but still found nothing, seems like tree is in corrupted state. You should rebuild index related to given query.",
            this);
      }

      path.add(pageIndex);
      final OCacheEntry bucketEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
      final OSBTreeBucketV1.SBTreeEntry<K, V> entry;

      try {
        final OSBTreeBucketV1<K, V> keyBucket = new OSBTreeBucketV1<>(bucketEntry);
        final int index = keyBucket.find(key, encryption, keySerializer);

        if (keyBucket.isLeaf()) {
          return new BucketSearchResult(index, path);
        }

        if (index >= 0) {
          entry = keyBucket.getEntry(index, encryption, keySerializer, valueSerializer);
        } else {
          final int insertionIndex = -index - 1;
          if (insertionIndex >= keyBucket.size()) {
            entry = keyBucket.getEntry(insertionIndex - 1, encryption, keySerializer, valueSerializer);
          } else {
            entry = keyBucket.getEntry(insertionIndex, encryption, keySerializer, valueSerializer);
          }
        }

      } finally {
        releasePageFromRead(atomicOperation, bucketEntry);
      }

      if (comparator.compare(key, entry.key) >= 0) {
        pageIndex = entry.rightChild;
      } else {
        pageIndex = entry.leftChild;
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

  private Map.Entry<K, V> convertToMapEntry(final OSBTreeBucketV1.SBTreeEntry<K, V> treeEntry) {
    final K key = treeEntry.key;
    final V value = treeEntry.value.getValue();

    return new Map.Entry<K, V>() {
      @Override
      public K getKey() {
        return key;
      }

      @Override
      public V getValue() {
        return value;
      }

      @Override
      public V setValue(final V value) {
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

  private static class BucketSearchResult {
    private final int             itemIndex;
    private final ArrayList<Long> path;

    private BucketSearchResult(final int itemIndex, final ArrayList<Long> path) {
      this.itemIndex = itemIndex;
      this.path = path;
    }

    long getLastPathItem() {
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

  public class OSBTreeFullKeyCursor implements OSBTreeKeyCursor<K> {
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

      atomicOperationsManager.acquireReadLock(OSBTreeV1.this);
      try {
        acquireSharedLock();
        try {
          final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();

          while (keysCache.size() < prefetchSize) {
            if (pageIndex == -1) {
              break;
            }

            if (pageIndex >= getFilledUpTo(atomicOperation, fileId)) {
              pageIndex = -1;
              break;
            }

            final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
            try {
              final OSBTreeBucketV1<K, V> bucket = new OSBTreeBucketV1<>(cacheEntry);

              if (itemIndex >= bucket.size()) {
                pageIndex = bucket.getRightSibling();
                itemIndex = 0;
                continue;
              }

              final Map.Entry<K, V> entry = convertToMapEntry(
                  bucket.getEntry(itemIndex, encryption, keySerializer, valueSerializer));
              itemIndex++;

              keysCache.add(entry.getKey());
            } finally {
              releasePageFromRead(atomicOperation, cacheEntry);
            }
          }
        } finally {
          releaseSharedLock();
        }
      } catch (final IOException e) {
        throw OException.wrapException(new OSBTreeException("Error during element iteration", OSBTreeV1.this), e);
      } finally {
        atomicOperationsManager.releaseReadLock(OSBTreeV1.this);
      }

      if (keysCache.isEmpty()) {
        keysCache = null;
        return null;
      }

      keysIterator = keysCache.iterator();
      return keysIterator.next();
    }
  }

  private final class OSBTreeCursorForward implements OSBTreeCursor<K, V> {
    private       K       fromKey;
    private final K       toKey;
    private       boolean fromKeyInclusive;
    private final boolean toKeyInclusive;

    private final List<Map.Entry<K, V>>     dataCache         = new ArrayList<>();
    @SuppressWarnings("unchecked")
    private       Iterator<Map.Entry<K, V>> dataCacheIterator = OEmptyMapEntryIterator.INSTANCE;

    private OSBTreeCursorForward(final K fromKey, final K toKey, final boolean fromKeyInclusive, final boolean toKeyInclusive) {
      this.fromKey = fromKey;
      this.toKey = toKey;
      this.fromKeyInclusive = fromKeyInclusive;
      this.toKeyInclusive = toKeyInclusive;

      if (fromKey == null) {
        this.fromKeyInclusive = true;
      }
    }

    public Map.Entry<K, V> next(int prefetchSize) {
      if (dataCacheIterator == null) {
        return null;
      }

      if (dataCacheIterator.hasNext()) {
        final Map.Entry<K, V> entry = dataCacheIterator.next();

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

      atomicOperationsManager.acquireReadLock(OSBTreeV1.this);
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

          long pageIndex = bucketSearchResult.getLastPathItem();
          int itemIndex;

          if (bucketSearchResult.itemIndex >= 0) {
            itemIndex = fromKeyInclusive ? bucketSearchResult.itemIndex : bucketSearchResult.itemIndex + 1;
          } else {
            itemIndex = -bucketSearchResult.itemIndex - 1;
          }

          while (dataCache.size() < prefetchSize) {
            if (pageIndex == -1) {
              break;
            }

            final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
            try {
              final OSBTreeBucketV1<K, V> bucket = new OSBTreeBucketV1<>(cacheEntry);

              if (itemIndex >= bucket.size()) {
                pageIndex = bucket.getRightSibling();
                itemIndex = 0;
                continue;
              }

              final Map.Entry<K, V> entry = convertToMapEntry(
                  bucket.getEntry(itemIndex, encryption, keySerializer, valueSerializer));
              itemIndex++;

              if (fromKey != null && (fromKeyInclusive
                  ? comparator.compare(entry.getKey(), fromKey) < 0
                  : comparator.compare(entry.getKey(), fromKey) <= 0)) {
                continue;
              }

              if (toKey != null && (toKeyInclusive
                  ? comparator.compare(entry.getKey(), toKey) > 0
                  : comparator.compare(entry.getKey(), toKey) >= 0)) {
                break;
              }

              dataCache.add(entry);
            } finally {
              releasePageFromRead(atomicOperation, cacheEntry);
            }
          }
        } finally {
          releaseSharedLock();
        }
      } catch (final IOException e) {
        throw OException.wrapException(new OSBTreeException("Error during element iteration", OSBTreeV1.this), e);
      } finally {
        atomicOperationsManager.releaseReadLock(OSBTreeV1.this);
      }

      if (dataCache.isEmpty()) {
        dataCacheIterator = null;
        return null;
      }

      dataCacheIterator = dataCache.iterator();

      final Map.Entry<K, V> entry = dataCacheIterator.next();

      fromKey = entry.getKey();
      fromKeyInclusive = false;

      return entry;
    }
  }

  private final class OSBTreeCursorBackward implements OSBTreeCursor<K, V> {
    private final K       fromKey;
    private       K       toKey;
    private final boolean fromKeyInclusive;
    private       boolean toKeyInclusive;

    private final List<Map.Entry<K, V>>     dataCache         = new ArrayList<>();
    @SuppressWarnings("unchecked")
    private       Iterator<Map.Entry<K, V>> dataCacheIterator = OEmptyMapEntryIterator.INSTANCE;

    private OSBTreeCursorBackward(final K fromKey, final K toKey, final boolean fromKeyInclusive, final boolean toKeyInclusive) {
      this.fromKey = fromKey;
      this.toKey = toKey;
      this.fromKeyInclusive = fromKeyInclusive;
      this.toKeyInclusive = toKeyInclusive;

      if (toKey == null) {
        this.toKeyInclusive = true;
      }

    }

    public Map.Entry<K, V> next(int prefetchSize) {
      if (dataCacheIterator == null) {
        return null;
      }

      if (dataCacheIterator.hasNext()) {
        final Map.Entry<K, V> entry = dataCacheIterator.next();
        toKey = entry.getKey();

        toKeyInclusive = false;
        return entry;
      }

      dataCache.clear();

      if (prefetchSize < 0 || prefetchSize > OGlobalConfiguration.INDEX_CURSOR_PREFETCH_SIZE.getValueAsInteger()) {
        prefetchSize = OGlobalConfiguration.INDEX_CURSOR_PREFETCH_SIZE.getValueAsInteger();
      }

      atomicOperationsManager.acquireReadLock(OSBTreeV1.this);
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

          long pageIndex = bucketSearchResult.getLastPathItem();

          int itemIndex;
          if (bucketSearchResult.itemIndex >= 0) {
            itemIndex = toKeyInclusive ? bucketSearchResult.itemIndex : bucketSearchResult.itemIndex - 1;
          } else {
            itemIndex = -bucketSearchResult.itemIndex - 2;
          }

          while (dataCache.size() < prefetchSize) {
            if (pageIndex == -1) {
              break;
            }

            final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
            try {
              final OSBTreeBucketV1<K, V> bucket = new OSBTreeBucketV1<>(cacheEntry);

              if (itemIndex >= bucket.size()) {
                itemIndex = bucket.size() - 1;
              }

              if (itemIndex < 0) {
                pageIndex = bucket.getLeftSibling();
                itemIndex = Integer.MAX_VALUE;
                continue;
              }

              final Map.Entry<K, V> entry = convertToMapEntry(
                  bucket.getEntry(itemIndex, encryption, keySerializer, valueSerializer));
              itemIndex--;

              if (toKey != null && (toKeyInclusive
                  ? comparator.compare(entry.getKey(), toKey) > 0
                  : comparator.compare(entry.getKey(), toKey) >= 0)) {
                continue;
              }

              if (fromKey != null && (fromKeyInclusive
                  ? comparator.compare(entry.getKey(), fromKey) < 0
                  : comparator.compare(entry.getKey(), fromKey) <= 0)) {
                break;
              }

              dataCache.add(entry);
            } finally {
              releasePageFromRead(atomicOperation, cacheEntry);
            }
          }
        } finally {
          releaseSharedLock();
        }
      } catch (final IOException e) {
        throw OException.wrapException(new OSBTreeException("Error during element iteration", OSBTreeV1.this), e);
      } finally {
        atomicOperationsManager.releaseReadLock(OSBTreeV1.this);
      }

      if (dataCache.isEmpty()) {
        dataCacheIterator = null;
        return null;
      }

      dataCacheIterator = dataCache.iterator();

      final Map.Entry<K, V> entry = dataCacheIterator.next();

      toKey = entry.getKey();
      toKeyInclusive = false;

      return entry;
    }
  }
}
