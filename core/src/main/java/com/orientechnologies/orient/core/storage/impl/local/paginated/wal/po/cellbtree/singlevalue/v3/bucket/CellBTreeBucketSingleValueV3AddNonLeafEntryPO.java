package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v3.bucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v3.CellBTreeSingleValueBucketV3;

import java.nio.ByteBuffer;

public final class CellBTreeBucketSingleValueV3AddNonLeafEntryPO extends PageOperationRecord {
  private int     index;
  private byte[]  key;
  private boolean updateNeighbours;

  private int leftChild;
  private int rightChild;

  private int prevChild;

  public CellBTreeBucketSingleValueV3AddNonLeafEntryPO() {
  }

  public CellBTreeBucketSingleValueV3AddNonLeafEntryPO(int index, byte[] key, boolean updateNeighbours, int leftChild,
      int rightChild, int prevChild) {
    this.index = index;
    this.key = key;
    this.updateNeighbours = updateNeighbours;
    this.leftChild = leftChild;
    this.rightChild = rightChild;
    this.prevChild = prevChild;
  }

  public int getIndex() {
    return index;
  }

  public byte[] getKey() {
    return key;
  }

  public boolean isUpdateNeighbours() {
    return updateNeighbours;
  }

  public int getLeftChild() {
    return leftChild;
  }

  public int getRightChild() {
    return rightChild;
  }

  public int getPrevChild() {
    return prevChild;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final CellBTreeSingleValueBucketV3 bucket = new CellBTreeSingleValueBucketV3(cacheEntry);
    final boolean added = bucket.addNonLeafEntry(index, leftChild, rightChild, key, updateNeighbours);
    if (!added) {
      throw new IllegalStateException("Can not redo operation of addition of non leaf entry.");
    }
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final CellBTreeSingleValueBucketV3 bucket = new CellBTreeSingleValueBucketV3(cacheEntry);
    bucket.removeNonLeafEntry(index, key, prevChild);
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CELL_BTREE_BUCKET_SINGLE_VALUE_V3_ADD_NON_LEAF_ENTRY_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 5 * OIntegerSerializer.INT_SIZE + key.length + OByteSerializer.BYTE_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(index);

    buffer.putInt(key.length);
    buffer.put(key);

    buffer.put(updateNeighbours ? (byte) 1 : 0);

    buffer.putInt(leftChild);
    buffer.putInt(rightChild);

    buffer.putInt(prevChild);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    index = buffer.getInt();
    final int len = buffer.getInt();
    key = new byte[len];
    buffer.get(key);

    updateNeighbours = buffer.get() > 0;

    leftChild = buffer.getInt();
    rightChild = buffer.getInt();

    prevChild = buffer.getInt();
  }
}
