package com.orientechnologies.orient.distributed.impl.coordinator.network;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.orientechnologies.orient.distributed.impl.network.binary.OBinaryDistributedMessage.DISTRIBUTED_ACK_RESPONSE;

public class ONetworkAck implements ODistributedMessage {
  private OLogId logId;

  public ONetworkAck(OLogId logId) {
    this.logId = logId;
  }

  public ONetworkAck() {
  }

  @Override
  public void write(DataOutput output) throws IOException {
    OLogId.serialize(logId, output);
  }

  @Override
  public void read(DataInput input) throws IOException {
    logId = OLogId.deserialize(input);
  }

  @Override
  public void execute(ONodeIdentity sender, OCoordinatedExecutor executor) {
    executor.executeAck(sender, this);
  }

  @Override
  public byte getCommand() {
    return DISTRIBUTED_ACK_RESPONSE;
  }

  public OLogId getLogId() {
    return logId;
  }

}
