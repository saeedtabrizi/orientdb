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

@Test
public class OProtobufDocumentBinarySerializerV0Test {
  public void testStringIntDeserialization() throws Exception {
    ODocument document = new ODocument(new ORecordId(10, 13));

    final String longString = "ssfsag asf as ffgfgasf afsfa asfa sgadsfasf aasdf gasdfagasdf  adfa sad sgf asdf aasdfsa fs adf ds";
    for (int i = 0; i < 50; i++) {
      document.field("field" + i, i % 10 == 0 ? longString : i);
    }

    BytesContainer bytesContainer = new BytesContainer();
    OProtobufDocumentBinarySerializerV0 documentSerializer = new OProtobufDocumentBinarySerializerV0();
    documentSerializer.serialize(document, bytesContainer, false);

    BytesContainer nContainer = new BytesContainer(bytesContainer.fitBytes());

    long start = System.nanoTime();
    for (int i = 0; i < 1000000; i++) {
      ODocument nDoc = new ODocument();
      documentSerializer.deserialize(nDoc, nContainer);
      nContainer.offset = 0;
    }
    long end = System.nanoTime();

    System.out.println((end - start) / 1000000);
  }

  public void testClassicBinarySerializer() throws Exception {
    ODocument document = new ODocument(new ORecordId(10, 13));

    final String longString = "ssfsag asf as ffgfgasf afsfa asfa sgadsfasf aasdf gasdfagasdf  adfa sad sgf asdf aasdfsa fs adf ds";
    for (int i = 0; i < 50; i++) {
      document.field("field" + i, i % 10 == 0 ? longString : i);
    }

    BytesContainer bytesContainer = new BytesContainer();
    ORecordSerializerBinaryV0 documentSerializer = new ORecordSerializerBinaryV0();
    documentSerializer.serialize(document, bytesContainer, false);

    BytesContainer nContainer = new BytesContainer(bytesContainer.fitBytes());

    long start = System.nanoTime();

    for (int i = 0; i < 1000000; i++) {
      ODocument nDoc = new ODocument();
      documentSerializer.deserialize(nDoc, nContainer);
      nContainer.offset = 0;
    }

    long end = System.nanoTime();

    System.out.println((end - start) / 1000000);
  }
}
