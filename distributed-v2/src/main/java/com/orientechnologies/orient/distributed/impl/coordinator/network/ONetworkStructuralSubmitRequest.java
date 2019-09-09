package com.orientechnologies.orient.distributed.impl.coordinator.network;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralSubmitRequest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.orientechnologies.orient.distributed.impl.network.binary.OBinaryDistributedMessage.DISTRIBUTED_STRUCTURAL_SUBMIT_REQUEST;

public class ONetworkStructuralSubmitRequest implements ODistributedMessage {
  private OStructuralSubmitRequest request;
  private OSessionOperationId      operationId;

  public ONetworkStructuralSubmitRequest(OSessionOperationId operationId, OStructuralSubmitRequest request) {
    this.request = request;
    this.operationId = operationId;
  }

  public ONetworkStructuralSubmitRequest() {
  }

  @Override
  public void write(DataOutput output) throws IOException {
    this.operationId.serialize(output);
    output.writeInt(request.getRequestType());
    request.serialize(output);
  }

  @Override
  public void read(DataInput input) throws IOException {
    this.operationId = new OSessionOperationId();
    this.operationId.deserialize(input);
    int requestType = input.readInt();
    request = OCoordinateMessagesFactory.createStructuralSubmitRequest(requestType);
    request.deserialize(input);
  }

  @Override
  public void execute(ONodeIdentity sender, OCoordinatedExecutor executor) {
    executor.executeStructuralSubmitRequest(sender, this);
  }

  @Override
  public byte getCommand() {
    return DISTRIBUTED_STRUCTURAL_SUBMIT_REQUEST;
  }

  public OStructuralSubmitRequest getRequest() {
    return request;
  }

  public OSessionOperationId getOperationId() {
    return operationId;
  }
}
