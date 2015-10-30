package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.common.hash.OMurmurHash3;
import com.orientechnologies.common.serialization.protobuf.DocumentProtobufSerializer;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by lomak_000 on 10/30/2015.
 */
@Test
public class OProtobufDocumentBinarySerializerV0Test {
  public void testStringIntSerialization() throws Exception {
    ODocument document = new ODocument(new ORecordId(10, 13));

    document.field("f1", "message one");
    document.field("f2", 2);
    document.field("f3", "message three");
    document.field("f4", 4);
    document.field("f5", "message five");

    BytesContainer bytesContainer = new BytesContainer();
    OProtobufDocumentBinarySerializerV0 documentSerializer = new OProtobufDocumentBinarySerializerV0();
    documentSerializer.serialize(document, bytesContainer, false);

    final byte[] data = bytesContainer.fitBytes();
    System.out.println("Message length is " + data.length);
    final DocumentProtobufSerializer.Record pRecord = DocumentProtobufSerializer.Record.parseFrom(data);

    Assert.assertEquals(pRecord.getRid().getClusterId(), document.getIdentity().getClusterId());
    Assert.assertEquals(pRecord.getRid().getClusterPos(), document.getIdentity().getClusterPosition());
  }

  public void testStringIntDeserialization() throws Exception {
    ODocument document = new ODocument(new ORecordId(10, 13));

    final String longString = "ssfsag asf as ffgfgasf afsfa asfa sgadsfasf aasdf gasdfagasdf  adfa sad sgf asdf aasdfsa fs adf ds";
    for (int i = 0; i < 50; i++) {
      document.field("f" + i, i % 10 == 0 ? longString : i);
    }

    BytesContainer bytesContainer = new BytesContainer();
    OProtobufDocumentBinarySerializerV0 documentSerializer = new OProtobufDocumentBinarySerializerV0();
    documentSerializer.serialize(document, bytesContainer, false);

    BytesContainer nContainer = new BytesContainer(bytesContainer.fitBytes());

    long sum = 0;
    // for (int n = 0; n < 5; n++) {
    long start = System.nanoTime();
    for (int i = 0; i < 1000000; i++) {
      ODocument nDoc = new ODocument();
      documentSerializer.deserialize(nDoc, nContainer);
      nContainer.offset = 0;
    }
    long end = System.nanoTime();

    sum += (end - start) / 1000000;
    // }

    System.out.println(sum);
  }

  public void testClassicBinarySerializer() throws Exception {
    ODocument document = new ODocument(new ORecordId(10, 13));

    final String longString = "ssfsag asf as ffgfgasf afsfa asfa sgadsfasf aasdf gasdfagasdf  adfa sad sgf asdf aasdfsa fs adf ds";
    for (int i = 0; i < 50; i++) {
      document.field("f" + i, i % 10 == 0 ? longString : i);
    }

    BytesContainer bytesContainer = new BytesContainer();
    ORecordSerializerBinaryV0 documentSerializer = new ORecordSerializerBinaryV0();
    documentSerializer.serialize(document, bytesContainer, false);

    BytesContainer nContainer = new BytesContainer(bytesContainer.fitBytes());

    long sum = 0;

    for (int n = 0; n < 5; n++) {
      long start = System.nanoTime();

      for (int i = 0; i < 1000000; i++) {
        ODocument nDoc = new ODocument();

        final String fieldName = "f" + (n * 10);
        documentSerializer.deserializePartial(nDoc, nContainer, new String[] { fieldName });
        nContainer.offset = 0;
      }

      long end = System.nanoTime();

      sum += (end - start) / 1000000;
    }

    System.out.println(sum / 5);
  }
}
