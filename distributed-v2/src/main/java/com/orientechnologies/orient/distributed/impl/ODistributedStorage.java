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
package com.orientechnologies.orient.distributed.impl;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.*;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OFreezableStorageComponent;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.distributed.impl.task.OBackgroundBackup;
import com.orientechnologies.orient.server.OServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * Distributed storage implementation that routes to the owner node the request.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class ODistributedStorage implements OStorage, OFreezableStorageComponent, OAutoshardedStorage {
  private final    String                    name;
  private volatile OAbstractPaginatedStorage wrapped;

  private ODistributedStorageEventListener eventListener;

  private volatile OBackgroundBackup lastValidBackup = null;

  public ODistributedStorage(final OServer iServer, final String dbName) {
    this.name = dbName;
  }

  /**
   * Supported only in embedded storage. Use <code>SELECT FROM metadata:storage</code> instead.
   */
  @Override
  public String getCreatedAtVersion() {
    throw new UnsupportedOperationException("Supported only in embedded storage. Use 'SELECT FROM metadata:storage' instead.");
  }

  @Override
  public boolean isDistributed() {
    return true;
  }

  @Override
  public boolean isAssigningClusterIds() {
    return true;
  }

  public Object command(final OCommandRequestText iCommand) {
    return wrapped.command(iCommand);
  }

  public void acquireDistributedExclusiveLock(final long timeout) {
  }

  public void releaseDistributedExclusiveLock() {
  }

  public boolean isLocalEnv() {
    return true;
  }

  public OStorageOperationResult<ORawBuffer> readRecord(final ORecordId iRecordId, final String iFetchPlan,
      final boolean iIgnoreCache, final boolean prefetchRecords, final ORecordCallback<ORawBuffer> iCallback) {
    return wrapped.readRecord(iRecordId, iFetchPlan, iIgnoreCache, prefetchRecords, iCallback);
  }

  @Override
  public OStorageOperationResult<ORawBuffer> readRecordIfVersionIsNotLatest(final ORecordId rid, final String fetchPlan,
      final boolean ignoreCache, final int recordVersion) throws ORecordNotFoundException {
    return wrapped.readRecordIfVersionIsNotLatest(rid, fetchPlan, ignoreCache, recordVersion);
  }

  @Override
  public OStorageOperationResult<Boolean> deleteRecord(final ORecordId iRecordId, final int iVersion, final int iMode,
      final ORecordCallback<Boolean> iCallback) {
    // IF is a real delete should be with a tx
    return wrapped.deleteRecord(iRecordId, iVersion, iMode, iCallback);
  }

  @Override
  public OSBTreeCollectionManager getSBtreeCollectionManager() {
    return wrapped.getSBtreeCollectionManager();
  }

  @Override
  public ORecordMetadata getRecordMetadata(ORID rid) {
    return wrapped.getRecordMetadata(rid);
  }

  @Override
  public boolean cleanOutRecord(ORecordId recordId, final int recordVersion, int iMode, ORecordCallback<Boolean> callback) {
    return wrapped.cleanOutRecord(recordId, recordVersion, iMode, callback);
  }

  @Override
  public boolean existsResource(final String iName) {
    return wrapped.existsResource(iName);
  }

  public OCluster getClusterByName(final String iName) {
    return wrapped.getClusterByName(iName);
  }

  @Override
  public String getClusterName(final int clusterId) {
    return wrapped.getClusterName(clusterId);
  }

  @Override
  public boolean setClusterAttribute(final int id, final OCluster.ATTRIBUTES attribute, final Object value) {
    return wrapped.setClusterAttribute(id, attribute, value);
  }

  @Override
  public ORecordConflictStrategy getConflictStrategy() {
    return getUnderlying().getConflictStrategy();
  }

  @Override
  public void setConflictStrategy(final ORecordConflictStrategy iResolver) {
    getUnderlying().setConflictStrategy(iResolver);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T removeResource(final String iName) {
    return (T) wrapped.removeResource(iName);
  }

  @Override
  public <T> T getResource(final String iName, final Callable<T> iCallback) {
    return (T) wrapped.getResource(iName, iCallback);
  }

  @Override
  public void open(final String iUserName, final String iUserPassword, final OContextConfiguration iProperties) {
    wrapped.open(iUserName, iUserPassword, iProperties);
  }

  @Override
  public void create(final OContextConfiguration iProperties) throws IOException {
    wrapped.create(iProperties);
  }

  @Override
  public boolean exists() {
    return wrapped.exists();
  }

  @Override
  public void reload() {
    wrapped.reload();
  }

  @Override
  public void delete() {
    wrapped.delete();
  }

  @Override
  public String incrementalBackup(final String backupDirectory, OCallable<Void, Void> started) {
    return wrapped.incrementalBackup(backupDirectory, started);
  }

  @Override
  public void restoreFromIncrementalBackup(final String filePath) {
    wrapped.restoreFromIncrementalBackup(filePath);
  }

  @Override
  public void close() {
    close(false, false);
  }

  @Override
  public void close(final boolean iForce, final boolean onDelete) {
    wrapped.close(iForce, onDelete);
  }

  @Override
  public boolean isClosed() {
    if (wrapped == null)
      return true;

    return wrapped.isClosed();
  }

  @Override
  public List<ORecordOperation> commit(final OTransactionInternal iTx) {
    return wrapped.commit(iTx);
  }

  @Override
  public void rollback(final OTransactionInternal iTx) {
    wrapped.rollback(iTx);
  }

  @Override
  public OStorageConfiguration getConfiguration() {
    return wrapped.getConfiguration();
  }

  @Override
  public int getClusters() {
    return wrapped.getClusters();
  }

  @Override
  public Set<String> getClusterNames() {
    return wrapped.getClusterNames();
  }

  @Override
  public OCluster getClusterById(int iId) {
    return wrapped.getClusterById(iId);
  }

  @Override
  public Collection<? extends OCluster> getClusterInstances() {
    return wrapped.getClusterInstances();
  }

  @Override
  public int addCluster(final String iClusterName, final Object... iParameters) {
    return wrapped.addCluster(iClusterName, iParameters);
  }

  @Override
  public int addCluster(String iClusterName, int iRequestedId) {
    return wrapped.addCluster(iClusterName, iRequestedId);
  }

  public boolean dropCluster(final String iClusterName) {
    return wrapped.dropCluster(iClusterName);
  }

  @Override
  public boolean dropCluster(final int iId) {
    return wrapped.dropCluster(iId);
  }

  @Override
  public long count(final int iClusterId) {
    return wrapped.count(iClusterId);
  }

  @Override
  public long count(int iClusterId, boolean countTombstones) {
    return wrapped.count(iClusterId, countTombstones);
  }

  public long count(final int[] iClusterIds) {
    return wrapped.count(iClusterIds);
  }

  @Override
  public long count(int[] iClusterIds, boolean countTombstones) {
    // TODO: SUPPORT SHARDING HERE
    return wrapped.count(iClusterIds, countTombstones);
  }

  @Override
  public long getSize() {
    return wrapped.getSize();
  }

  @Override
  public long countRecords() {
    return wrapped.countRecords();
  }

  @Override
  public int getDefaultClusterId() {
    return wrapped.getDefaultClusterId();
  }

  @Override
  public void setDefaultClusterId(final int defaultClusterId) {
    wrapped.setDefaultClusterId(defaultClusterId);
  }

  @Override
  public int getClusterIdByName(String iClusterName) {
    return wrapped.getClusterIdByName(iClusterName);
  }

  @Override
  public String getPhysicalClusterNameById(final int iClusterId) {
    return wrapped.getPhysicalClusterNameById(iClusterId);
  }

  @Override
  public boolean checkForRecordValidity(final OPhysicalPosition ppos) {
    return wrapped.checkForRecordValidity(ppos);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getURL() {
    return wrapped.getURL();
  }

  @Override
  public long getVersion() {
    return wrapped.getVersion();
  }

  @Override
  public void synch() {
    wrapped.synch();
  }

  @Override
  public long[] getClusterDataRange(final int currentClusterId) {
    return wrapped.getClusterDataRange(currentClusterId);
  }

  @Override
  public <V> V callInLock(final Callable<V> iCallable, final boolean iExclusiveLock) {
    return wrapped.callInLock(iCallable, iExclusiveLock);
  }

  public STATUS getStatus() {
    return wrapped.getStatus();
  }

  public void setEventListener(final ODistributedStorageEventListener eventListener) {
    this.eventListener = eventListener;
  }

  @Override
  public OPhysicalPosition[] higherPhysicalPositions(int currentClusterId, OPhysicalPosition entry) {
    return wrapped.higherPhysicalPositions(currentClusterId, entry);
  }

  @Override
  public OPhysicalPosition[] ceilingPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition) {
    return wrapped.ceilingPhysicalPositions(clusterId, physicalPosition);
  }

  @Override
  public OPhysicalPosition[] floorPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition) {
    return wrapped.floorPhysicalPositions(clusterId, physicalPosition);
  }

  @Override
  public OPhysicalPosition[] lowerPhysicalPositions(int currentClusterId, OPhysicalPosition entry) {
    return wrapped.lowerPhysicalPositions(currentClusterId, entry);
  }

  public OStorage getUnderlying() {
    return wrapped;
  }

  @Override
  public boolean isRemote() {
    return false;
  }

  @Override
  public OCurrentStorageComponentsFactory getComponentsFactory() {
    return wrapped.getComponentsFactory();
  }

  @Override
  public String getType() {
    return "distributed";
  }

  @Override
  public void freeze(final boolean throwException) {
    getFreezableStorage().freeze(throwException);
  }

  @Override
  public boolean isFrozen() {
    return getFreezableStorage().isFrozen();
  }

  @Override
  public void release() {
    getFreezableStorage().release();
  }

  @Override
  public List<String> backup(final OutputStream out, final Map<String, Object> options, final Callable<Object> callable,
      final OCommandOutputListener iListener, final int compressionLevel, final int bufferSize) throws IOException {
    return wrapped.backup(out, options, callable, iListener, compressionLevel, bufferSize);
  }

  @Override
  public void restore(final InputStream in, final Map<String, Object> options, final Callable<Object> callable,
      final OCommandOutputListener iListener) throws IOException {
    wrapped.restore(in, options, callable, iListener);
  }

  @Override
  public String getStorageId() {
    return "";
  }

  @Override
  public String getNodeId() {
    return "";
  }

  @Override
  public void shutdown() {
    close(true, false);
  }

  public OBackgroundBackup getLastValidBackup() {
    return lastValidBackup;
  }

  public void setLastValidBackup(final OBackgroundBackup lastValidBackup) {
    this.lastValidBackup = lastValidBackup;
  }

  private OFreezableStorageComponent getFreezableStorage() {
    if (wrapped instanceof OFreezableStorageComponent)
      return ((OFreezableStorageComponent) wrapped);
    else
      throw new UnsupportedOperationException("Storage engine " + wrapped.getType() + " does not support freeze operation");
  }

  @Override
  public void restoreFullIncrementalBackup(InputStream stream) throws UnsupportedOperationException {
    wrapped.restoreFullIncrementalBackup(stream);
  }

  @Override
  public boolean supportIncremental() {
    return wrapped.supportIncremental();
  }

  @Override
  public void fullIncrementalBackup(OutputStream stream) throws UnsupportedOperationException {
    wrapped.fullIncrementalBackup(stream);
  }

  @Override
  public void setSchemaRecordId(String schemaRecordId) {
    wrapped.setSchemaRecordId(schemaRecordId);
  }

  @Override
  public void setDateFormat(String dateFormat) {
    wrapped.setDateFormat(dateFormat);
  }

  @Override
  public void setTimeZone(TimeZone timeZoneValue) {
    wrapped.setTimeZone(timeZoneValue);
  }

  @Override
  public void setLocaleLanguage(String locale) {
    wrapped.setLocaleLanguage(locale);
  }

  @Override
  public void setCharset(String charset) {
    wrapped.setCharset(charset);
  }

  @Override
  public void setIndexMgrRecordId(String indexMgrRecordId) {
    wrapped.setIndexMgrRecordId(indexMgrRecordId);
  }

  @Override
  public void setDateTimeFormat(String dateTimeFormat) {
    wrapped.setDateTimeFormat(dateTimeFormat);
  }

  @Override
  public void setLocaleCountry(String localeCountry) {
    wrapped.setLocaleCountry(localeCountry);
  }

  @Override
  public void setClusterSelection(String clusterSelection) {
    wrapped.setClusterSelection(clusterSelection);
  }

  @Override
  public void setMinimumClusters(int minimumClusters) {
    wrapped.setMinimumClusters(minimumClusters);
  }

  @Override
  public void setValidation(boolean validation) {
    wrapped.setValidation(validation);
  }

  @Override
  public void removeProperty(String property) {
    wrapped.removeProperty(property);
  }

  @Override
  public void setProperty(String property, String value) {
    wrapped.setProperty(property, value);
  }

  @Override
  public void setRecordSerializer(String recordSerializer, int version) {
    wrapped.setRecordSerializer(recordSerializer, version);
  }

  @Override
  public void clearProperties() {
    wrapped.clearProperties();
  }
}
