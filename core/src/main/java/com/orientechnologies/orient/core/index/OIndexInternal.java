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
package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.OSecurityInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;

import java.util.Collection;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

/**
 * Interface to handle index.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public interface OIndexInternal<T> extends OIndex<T> {

  String CONFIG_KEYTYPE            = "keyType";
  String CONFIG_AUTOMATIC          = "automatic";
  String CONFIG_TYPE               = "type";
  String ALGORITHM                 = "algorithm";
  String VALUE_CONTAINER_ALGORITHM = "valueContainerAlgorithm";
  String CONFIG_NAME               = "name";
  String INDEX_DEFINITION          = "indexDefinition";
  String INDEX_DEFINITION_CLASS    = "indexDefinitionClass";
  String INDEX_VERSION             = "indexVersion";
  String METADATA                  = "metadata";

  Object getCollatingValue(final Object key);

  /**
   * Loads the index giving the configuration.
   *
   * @param iConfig ODocument instance containing the configuration
   */
  boolean loadFromConfiguration(ODocument iConfig);

  /**
   * Saves the index configuration to disk.
   *
   * @return The configuration as ODocument instance
   *
   * @see #getConfiguration()
   */
  ODocument updateConfiguration();

  /**
   * Add given cluster to the list of clusters that should be automatically indexed.
   *
   * @param iClusterName Cluster to add.
   *
   * @return Current index instance.
   */
  OIndex<T> addCluster(final String iClusterName);

  /**
   * Remove given cluster from the list of clusters that should be automatically indexed.
   *
   * @param iClusterName Cluster to remove.
   *
   * @return Current index instance.
   */
  OIndex<T> removeCluster(final String iClusterName);

  /**
   * Indicates whether given index can be used to calculate result of
   * {@link com.orientechnologies.orient.core.sql.operator.OQueryOperatorEquality} operators.
   *
   * @return {@code true} if given index can be used to calculate result of
   * {@link com.orientechnologies.orient.core.sql.operator.OQueryOperatorEquality} operators.
   */
  boolean canBeUsedInEqualityOperators();

  boolean hasRangeQuerySupport();

  OIndexMetadata loadMetadata(ODocument iConfig);

  void setRebuildingFlag();

  void close();

  void preCommit(OIndexAbstract.IndexTxSnapshot snapshots);

  void addTxOperation(OIndexAbstract.IndexTxSnapshot snapshots, final OTransactionIndexChanges changes);

  void commit(OIndexAbstract.IndexTxSnapshot snapshots);

  void postCommit(OIndexAbstract.IndexTxSnapshot snapshots);

  void setType(OType type);

  /**
   * <p>
   * Returns the index name for a key. The name is always the current index name, but in cases where the index supports key-based
   * sharding.
   *
   * @param key the index key.
   *
   * @return The index name involved
   */
  String getIndexNameByKey(Object key);

  /**
   * <p>
   * Acquires exclusive lock in the active atomic operation running on the current thread for this index.
   *
   * <p>
   * If this index supports a more narrow locking, for example key-based sharding, it may use the provided {@code key} to infer a
   * more narrow lock scope, but that is not a requirement.
   *
   * @param key the index key to lock.
   *
   * @return {@code true} if this index was locked entirely, {@code false} if this index locking is sensitive to the provided {@code
   * key} and only some subset of this index was locked.
   */
  boolean acquireAtomicExclusiveLock(Object key);


  static OIdentifiable securityFilterOnRead(OIndex idx, OIdentifiable item) {
    if (idx.getMetadata() == null) {
      return item;
    }
    String indexClass = idx.getMetadata().getClassName();
    if (indexClass == null) {
      return item;
    }
    ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (db == null) {
      return item;
    }
    OSecurityInternal security = db.getSharedContext().getSecurity();
    return securityFilter(item, "database.class." + indexClass, db, security);
  }

  static OIdentifiable securityFilter(OIdentifiable item, String resource, ODatabaseDocumentInternal db, OSecurityInternal security) {
    if (security.isReadRestrictedBySecurityPolicy(db, resource)) {
      return item.getRecord();
    }
    return item;
  }

  static Collection securityFilterOnRead(OIndex idx, Collection<OIdentifiable> items) {
    if (idx.getMetadata() == null && idx.getDefinition() == null) {
      return items;
    }
    String indexClass = idx.getMetadata() == null ? idx.getDefinition().getClassName() : idx.getMetadata().getClassName();
    if (indexClass == null) {
      return items;
    }
    ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (db == null) {
      return items;
    }
    OSecurityInternal security = db.getSharedContext().getSecurity();
    String resource = "database.class." + indexClass;
    if (security.isReadRestrictedBySecurityPolicy(db, resource)) {
      return items.stream()
              .map(x -> securityFilter(x, resource, db, security))
              .filter(x -> x != null)
              .map(x -> x.getIdentity())
              .collect(Collectors.toList());
    }
    return items;
  }

}
