package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.google.protobuf.*;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentEntry;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

/**
 * Created by lomak_000 on 10/30/2015.
 */
public class OProtobufDocumentBinarySerializerV0 implements ODocumentSerializer {
  private static final Charset UTF8 = Charset.forName("UTF-8");

  /**
   * <code>
   *  message Record {
   *    oneof data {
   *      bytes raw = 1;
   *      Document document = 2;
   *    }
   *
   *    RID rid = 3;
   *    int64 version = 4;
   *  }
   * </code>
   */
  @Override
  public void serialize(ODocument document, BytesContainer bytes, boolean iClassOnly) {
    try {
      final OClass clazz = ODocumentInternal.getImmutableSchemaClass(document);

      final Map<String, OProperty> props = clazz != null ? clazz.propertiesMap() : null;
      final List<Map.Entry<String, ODocumentEntry>> fields = new ArrayList<Map.Entry<String, ODocumentEntry>>(
          ODocumentInternal.rawEntries(document));

      final Map<String, Integer> fieldPositionMap = new HashMap<String, Integer>();

      int containerSize = 0;
      final ArrayList<Integer> messagesSize = new ArrayList<Integer>(64);

      // Document document = 4;
      final int docSize[] = computeDocumentSize(clazz != null ? clazz.getName() : null, document, fields, props);
      for (int ds : docSize) {
        messagesSize.add(ds);
      }

      containerSize += docSize[0];
      containerSize += computeEmbeddedMessageHeaderSize(docSize[0], 2);

      final int offset = bytes.alloc(containerSize);

      final CodedOutputStream codedOutputStream = CodedOutputStream.newInstance(bytes.bytes, offset, containerSize);

      final Iterator<Integer> sizesIterator = messagesSize.iterator();

      writeEmbeddedMessageHeader(sizesIterator.next(), 2, codedOutputStream);
      writeDocument(clazz != null ? clazz.getName() : null, document, fields, codedOutputStream, sizesIterator);

      assert containerSize == codedOutputStream.getTotalBytesWritten();
    } catch (IOException ioe) {
      throw OException.wrapException(new OSerializationException("Error during document serialization"), ioe);
    }
  }

  /**
   * Look at http://www.eternallyconfuzzled.com/tuts/algorithms/jsw_tut_hashing.aspx
   */
  private static long fnvHashCode(String fieldName) {
    long h = 2166136261L;

    final char[] fieldChars = new char[fieldName.length()];
    fieldName.getChars(0, fieldChars.length, fieldChars, 0);

    for (int k = 0; k < fieldName.length(); k++) {
      h = (h * 16777619) ^ fieldChars[k];
    }

    return h;
  }

  /**
   * message Document {<br/>
   * string class = 1;<br/>
   * map<string, Item> fields = 2;<br/>
   * }<br/>
   */
  private static int[] computeDocumentSize(String className, ODocument document, List<Map.Entry<String, ODocumentEntry>> fields,
      final Map<String, OProperty> props) {

    int size = 0;

    if (className != null) {
      size += CodedOutputStream.computeStringSize(1, className);
    }

    int[] sizes = new int[fields.size() * 2 + 1];
    int sizesIndex = 1;

    for (Map.Entry<String, ODocumentEntry> entry : fields) {
      ODocumentEntry docEntry = entry.getValue();

      if (!docEntry.exist())
        continue;

      if (docEntry.property == null && props != null)
        docEntry.property = props.get(entry.getKey());

      final Object value = docEntry.value;

      if (value != null) {
        final OType type;

        if (docEntry.type != null) {
          type = docEntry.type;
        } else {
          type = getFieldType(docEntry);
          docEntry.type = type;
        }

        if (type == null) {
          throw new OSerializationException(
              "Impossible serialize value of type " + value.getClass() + " with the ODocument binary serializer");
        }

        final int[] entrySize = computeMapEntrySize(entry.getKey(), value, type);

        size += entrySize[0];
        size += computeEmbeddedMessageHeaderSize(entrySize[0], 2);

        if (sizesIndex + entrySize.length > sizes.length) {
          int[] newSizes = new int[sizesIndex + entrySize.length];
          System.arraycopy(sizes, 1, newSizes, 1, sizes.length - 1);
          System.arraycopy(entrySize, 0, newSizes, sizesIndex, entrySize.length);
          sizesIndex += entrySize.length;
          sizes = newSizes;
        } else {
          System.arraycopy(entrySize, 0, sizes, sizesIndex, entrySize.length);
          sizesIndex += entrySize.length;
        }
      }
    }

    assert sizesIndex == sizes.length;
    sizes[0] = size;

    return sizes;
  }

  private static void writeDocument(String className, ODocument document, List<Map.Entry<String, ODocumentEntry>> fields,
      CodedOutputStream codedOutputStream, Iterator<Integer> sizesIterator) throws IOException {
    if (className != null) {
      codedOutputStream.writeString(1, className);
    }

    for (Map.Entry<String, ODocumentEntry> entry : fields) {
      ODocumentEntry docEntry = entry.getValue();

      if (!docEntry.exist())
        continue;

      final Object value = docEntry.value;

      if (value != null) {
        final int messageSize = sizesIterator.next();
        writeEmbeddedMessageHeader(messageSize, 2, codedOutputStream);
        writeMapEntry(entry.getKey(), value, docEntry.type, codedOutputStream, sizesIterator);
      }
    }
  }

  /**
   * The map syntax is equivalent to the following on the wire, so protocol buffers implementations that do not support maps can
   * still handle your data:<br/>
   * 
   * message MapFieldEntry {<br/>
   * key_type key = 1;<br/>
   * value_type value = 2;<br/>
   * }<br/>
   * repeated MapFieldEntry map_field = N;<br/>
   */
  private static int[] computeMapEntrySize(String key, Object value, OType type) {
    int size = 0;

    size += CodedOutputStream.computeStringSize(1, key);

    final int[] itemSize = computeItemSize(value, type);
    size += itemSize[0];
    size += computeEmbeddedMessageHeaderSize(itemSize[0], 2);

    final int[] sizes = new int[itemSize.length + 1];
    sizes[0] = size;

    System.arraycopy(itemSize, 0, sizes, 1, itemSize.length);
    return sizes;
  }

  private static void writeMapEntry(String key, Object value, OType type, CodedOutputStream codedOutputStream,
      Iterator<Integer> sizesIterator) throws IOException {
    codedOutputStream.writeString(1, key);

    final int messageSize = sizesIterator.next();

    writeEmbeddedMessageHeader(messageSize, 2, codedOutputStream);
    writeItem(value, type, codedOutputStream, sizesIterator);
  }

  /**
   * <code>
   * message Item {
   *  oneof value {
   *    sint32 val_int = 1; //com.orientechnologies.orient.core.metadata.schema.OType.INTEGER
   *    double val_double = 2; //com.orientechnologies.orient.core.metadata.schema.OType.DOUBLE
   *    string val_string = 3; //com.orientechnologies.orient.core.metadata.schema.OType.STRING
   *    sint64 val_long = 4; //com.orientechnologies.orient.core.metadata.schema.OType.LONG
   *    LinkSet val_link_set = 5; //com.orientechnologies.orient.core.metadata.schema.OType.LINKSET
   *    RID val_link = 6; //com.orientechnologies.orient.core.metadata.schema.OType.LINK
   *    Date val_date = 7; //com.orientechnologies.orient.core.metadata.schema.OType.DATE
   *    DateTime val_date_time = 8; //com.orientechnologies.orient.core.metadata.schema.OType.DATETIME
   *    reserved 9; // LinkBag
   *    Record val_record = 10; //com.orientechnologies.orient.core.metadata.schema.OType.EMBEDDED
   *    List val_embded_list = 11; //com.orientechnologies.orient.core.metadata.schema.OType.EMBEDDEDLIST
   *    LinkList val_link_list = 12; //com.orientechnologies.orient.core.metadata.schema.OType.LINKLIST
   *    Set val_embedded_set = 13; //com.orientechnologies.orient.core.metadata.schema.OType.EMBEDDEDSET
   *    Map val_embedded_map = 14; //com.orientechnologies.orient.core.metadata.schema.OType.EMBEDDEDMAP
   *    LinkMap val_link_map = 15; //com.orientechnologies.orient.core.metadata.schema.OType.LINKMAP
   *    bytes val_byte = 16; //com.orientechnologies.orient.core.metadata.schema.OType.BYTE
   *    bytes val_bytes = 17; //com.orientechnologies.orient.core.metadata.schema.OType.BINARY
   *    bytes val_custom = 18; //com.orientechnologies.orient.core.metadata.schema.OType.CUSTOM
   *    Decimal val_decimal = 19; //com.orientechnologies.orient.core.metadata.schema.OType.DECIMAL
   *    sint32 val_short = 20; //com.orientechnologies.orient.core.metadata.schema.OType.SHORT
   *    float val_float = 21; //com.orientechnologies.orient.core.metadata.schema.OType.FLOAT
   *    bool val_bool = 22; //com.orientechnologies.orient.core.metadata.schema.OType.BOOLEAN
   *  }  
   * reserved 9;
   * }
   * </code>
   */
  private static int[] computeItemSize(Object value, OType type) {
    int size = 0;
    switch (type) {
    case STRING:
      size += computeStringSize(value);
      break;
    case INTEGER:
      size += computeIntSize(value);
      break;
    default:
      throw new OSerializationException("Type " + type + " can not be serialized");
    }

    return new int[] { size };
  }

  private static void writeItem(Object value, OType type, CodedOutputStream codedOutputStream, Iterator<Integer> sizesIterator)
      throws IOException {
    switch (type) {
    case STRING:
      writeString(value, codedOutputStream);
      break;
    case INTEGER:
      writeInt(value, codedOutputStream);
      break;
    default:
      throw new OSerializationException("Type " + type + " can not be serialized");
    }
  }

  /**
   * string val_string = 3; //com.orientechnologies.orient.core.metadata.schema.OType.STRING
   */
  private static int computeStringSize(Object value) {
    return CodedOutputStream.computeStringSize(3, value.toString());
  }

  private static void writeString(Object value, CodedOutputStream codedOutputStream) throws IOException {
    codedOutputStream.writeString(3, value.toString());
  }

  /**
   * sint32 val_int = 1; //com.orientechnologies.orient.core.metadata.schema.OType.INTEGER
   */
  private static int computeIntSize(Object value) {
    return CodedOutputStream.computeSInt32Size(1, ((Number) value).intValue());
  }

  private static void writeInt(Object value, CodedOutputStream codedOutputStream) throws IOException {
    codedOutputStream.writeSInt32(1, ((Number) value).intValue());
  }

  /**
   * message RID { <br/>
   * sint32 cluster_id = 1; <br/>
   * sint64 cluster_pos = 2; <br/>
   * } <br/>
   */
  private static int computeRIDSize(ORID rid) {
    int size = 0;

    // sint32 cluster_id = 1;
    size += CodedOutputStream.computeSInt32Size(1, rid.getClusterId());
    // sint64 cluster_pos = 2;
    size += CodedOutputStream.computeSInt64Size(2, rid.getClusterPosition());

    return size;
  }

  private static void writeRID(ORID rid, CodedOutputStream codedOutputStream) throws IOException {
    codedOutputStream.writeSInt32(1, rid.getClusterId());
    codedOutputStream.writeSInt64(2, rid.getClusterPosition());
  }

  private static int computeEmbeddedMessageHeaderSize(int messageSize, int tag) {
    int size = 0;
    size += CodedOutputStream.computeTagSize(tag);
    size += CodedOutputStream.computeRawVarint32Size(messageSize);

    return size;
  }

  private static void writeEmbeddedMessageHeader(int messageSize, int tag, CodedOutputStream codedOutputStream) throws IOException {
    codedOutputStream.writeTag(tag, 2);
    codedOutputStream.writeRawVarint32(messageSize);
  }

  private static OType getFieldType(final ODocumentEntry entry) {
    OType type = entry.type;
    if (type == null) {
      final OProperty prop = entry.property;
      if (prop != null)
        type = prop.getType();

    }
    if (type == null || OType.ANY == type)
      type = OType.getTypeByValue(entry.value);
    return type;
  }

  private static OType getLinkedType(ODocument document, OType type, String key) {
    if (type != OType.EMBEDDEDLIST && type != OType.EMBEDDEDSET && type != OType.EMBEDDEDMAP)
      return null;
    OClass immutableClass = ODocumentInternal.getImmutableSchemaClass(document);
    if (immutableClass != null) {
      OProperty prop = immutableClass.getProperty(key);
      if (prop != null) {
        return prop.getLinkedType();
      }
    }
    return null;
  }

  @Override
  public int serializeValue(BytesContainer bytes, Object value, OType type, OType linkedType) {
    return 0;
  }

  /**
   * <code>
   *   message Record {
   *    RID rid = 1;
   *    int64 version = 2;
   *
   *    oneof data {
   *      bytes raw = 3;
   *      Document document = 4;
   *    }
   *  }
   * </code>
   */
  @Override
  public void deserialize(ODocument document, BytesContainer bytes) {
    while (bytes.offset < bytes.bytes.length) {
      int tag = readRawVarint32(bytes);
      switch (WireFormat.getTagFieldNumber(tag)) {
      case 4: {
        readDocument(document, bytes, null);
        break;
      }
      default:
        skipField(tag, bytes);
      }
    }

    ORecordInternal.clearSource(document);
  }

  /**
   * message RID { <br/>
   * sint32 cluster_id = 1; <br/>
   * sint64 cluster_pos = 2; <br/>
   * } <br/>
   */
  private static ORecordId readRID(CodedInputStream codedInputStream) throws IOException {
    final int messageLength = codedInputStream.readRawVarint32();
    final int bytesRead = codedInputStream.getTotalBytesRead();

    int clusterId = 0;
    long clusterPosition = 0;

    while (messageLength > codedInputStream.getTotalBytesRead() - bytesRead) {
      final int tag = codedInputStream.readTag();
      switch (WireFormat.getTagFieldNumber(tag)) {
      case 1:
        clusterId = codedInputStream.readSInt32();
        break;
      case 2:
        clusterPosition = codedInputStream.readSInt64();
        break;
      default:
        codedInputStream.skipField(tag);
      }
    }

    return new ORecordId(clusterId, clusterPosition);
  }

  /**
   * message Document {<br/>
   * string class = 1;<br/>
   * map<string, Item> fields = 2;<br/>
   * }<br/>
   */
  private void readDocument(ODocument document, BytesContainer bytesContainer, byte[][] fieldsToInclude) {
    final int messageLength = readRawVarint32(bytesContainer);
    final int bytesRead = bytesContainer.offset;

    int fieldsRead = 0;
    while (messageLength > bytesContainer.offset - bytesRead) {
      if (fieldsToInclude != null && fieldsRead >= fieldsToInclude.length) {
        break;
      } else {
        final int tag = readRawVarint32(bytesContainer);
        switch (tag) {
        case 1: {
          final String className = readString(bytesContainer);
          if (className.length() > 0)
            ODocumentInternal.fillClassNameIfNeeded(document, className);
          break;
        }
        case 18: {
          if (readMapItem(document, bytesContainer, fieldsToInclude))
            fieldsRead++;

          break;
        }
        default:
          skipField(tag, bytesContainer);
        }
      }
    }
  }

  /**
   * The map syntax is equivalent to the following on the wire, so protocol buffers implementations that do not support maps can
   * still handle your data:<br/>
   *
   * message MapFieldEntry {<br/>
   * key_type key = 1;<br/>
   * value_type value = 2;<br/>
   * }<br/>
   * repeated MapFieldEntry map_field = N;<br/>
   */
  private static boolean readMapItem(ODocument document, BytesContainer bytesContainer, byte[][] fieldsToInclude) {
    final int messageLength = readRawVarint32(bytesContainer);
    final int bytesRead = bytesContainer.offset;

    int fieldNameStart = -1;
    int fieldNameLen = -1;
    OPair<OType, Object> itemValue = null;

    itemLoop: while (messageLength > bytesContainer.offset - bytesRead) {
      final int tag = readRawVarint32(bytesContainer);
      switch (tag) {
      case 10: {
        final int len = readRawVarint32(bytesContainer);
        fieldNameStart = bytesContainer.offset;
        fieldNameLen = len;
        bytesContainer.offset += len;
        break;
      }
      case 18: {
        if (fieldsToInclude != null && fieldNameLen >= 0) {
          for (byte[] name : fieldsToInclude) {
            if (fieldNameLen != name.length) {
              skipField(tag, bytesContainer);
              continue itemLoop;
            }

            for (int i = 0; i < fieldNameLen; i++) {
              if (name[i] != bytesContainer.bytes[fieldNameStart + i]) {
                skipField(tag, bytesContainer);
                continue itemLoop;
              }
            }

            itemValue = readItemValue(bytesContainer);
          }
        } else {
          itemValue = readItemValue(bytesContainer);
        }
        break;
      }
      default:
        skipField(tag, bytesContainer);
      }
    }

    if (fieldNameLen >= 0 && itemValue != null)

    {
      ODocumentInternal.rawField(document, new String(bytesContainer.bytes, fieldNameStart, fieldNameLen, UTF8), itemValue.value,
          itemValue.key);
      return true;
    }

    return false;

  }

  /**
   * <code>
   * message Item {
   *  oneof value {
   *    sint32 val_int = 1; //com.orientechnologies.orient.core.metadata.schema.OType.INTEGER
   *    double val_double = 2; //com.orientechnologies.orient.core.metadata.schema.OType.DOUBLE
   *    string val_string = 3; //com.orientechnologies.orient.core.metadata.schema.OType.STRING
   *    sint64 val_long = 4; //com.orientechnologies.orient.core.metadata.schema.OType.LONG
   *    LinkSet val_link_set = 5; //com.orientechnologies.orient.core.metadata.schema.OType.LINKSET
   *    RID val_link = 6; //com.orientechnologies.orient.core.metadata.schema.OType.LINK
   *    Date val_date = 7; //com.orientechnologies.orient.core.metadata.schema.OType.DATE
   *    DateTime val_date_time = 8; //com.orientechnologies.orient.core.metadata.schema.OType.DATETIME
   *    reserved 9; // LinkBag
   *    Record val_record = 10; //com.orientechnologies.orient.core.metadata.schema.OType.EMBEDDED
   *    List val_embded_list = 11; //com.orientechnologies.orient.core.metadata.schema.OType.EMBEDDEDLIST
   *    LinkList val_link_list = 12; //com.orientechnologies.orient.core.metadata.schema.OType.LINKLIST
   *    Set val_embedded_set = 13; //com.orientechnologies.orient.core.metadata.schema.OType.EMBEDDEDSET
   *    Map val_embedded_map = 14; //com.orientechnologies.orient.core.metadata.schema.OType.EMBEDDEDMAP
   *    LinkMap val_link_map = 15; //com.orientechnologies.orient.core.metadata.schema.OType.LINKMAP
   *    bytes val_byte = 16; //com.orientechnologies.orient.core.metadata.schema.OType.BYTE
   *    bytes val_bytes = 17; //com.orientechnologies.orient.core.metadata.schema.OType.BINARY
   *    bytes val_custom = 18; //com.orientechnologies.orient.core.metadata.schema.OType.CUSTOM
   *    Decimal val_decimal = 19; //com.orientechnologies.orient.core.metadata.schema.OType.DECIMAL
   *    sint32 val_short = 20; //com.orientechnologies.orient.core.metadata.schema.OType.SHORT
   *    float val_float = 21; //com.orientechnologies.orient.core.metadata.schema.OType.FLOAT
   *    bool val_bool = 22; //com.orientechnologies.orient.core.metadata.schema.OType.BOOLEAN
   *  }
   * reserved 9;
   * }
   * </code>
   */
  private static OPair<OType, Object> readItemValue(BytesContainer bytesContainer) {
    final int messageLength = readRawVarint32(bytesContainer);
    final int bytesRead = bytesContainer.offset;

    OPair<OType, Object> result = null;

    while (messageLength > bytesContainer.offset - bytesRead) {
      final int tag = readRawVarint32(bytesContainer);
      switch (tag) {
      case 8: {
        result = new OPair<OType, Object>(OType.INTEGER, readSInt32(bytesContainer));
        break;
      }
      case 26: {
        result = new OPair<OType, Object>(OType.STRING, readString(bytesContainer));
        break;
      }
      default:
        skipField(tag, bytesContainer);
      }
    }

    return result;
  }

  private static int readSInt32(BytesContainer bytesContainer) {
    return decodeZigZag32(readRawVarint32(bytesContainer));
  }

  private static int decodeZigZag32(int n) {
    return n >>> 1 ^ -(n & 1);
  }

  @Override
  public void deserializePartial(ODocument document, BytesContainer bytes, String[] iFields) {
    byte[][] fieldNames = new byte[iFields.length][];
    for (int i = 0; i < iFields.length; i++) {
      fieldNames[i] = iFields[i].getBytes(UTF8);
    }

    while (bytes.offset <= bytes.bytes.length) {
      int tag = readRawVarint32(bytes);
      if (tag == 34) {
        readDocument(document, bytes, fieldNames);
        break;
      } else {
        skipField(tag, bytes);
      }
    }
  }

  private static int readRawVarint32(BytesContainer bytesContainer) {
    byte tmp = bytesContainer.bytes[bytesContainer.offset++];
    if (tmp >= 0) {
      return tmp;
    }

    int result = tmp & 0x7f;
    if ((tmp = bytesContainer.bytes[bytesContainer.offset++]) >= 0) {
      result |= tmp << 7;
    } else {
      result |= (tmp & 0x7f) << 7;
      if ((tmp = bytesContainer.bytes[bytesContainer.offset++]) >= 0) {
        result |= tmp << 14;
      } else {
        result |= (tmp & 0x7f) << 14;
        if ((tmp = bytesContainer.bytes[bytesContainer.offset++]) >= 0) {
          result |= tmp << 21;
        } else {
          result |= (tmp & 0x7f) << 21;
          result |= (tmp = bytesContainer.bytes[bytesContainer.offset++]) << 28;
          if (tmp < 0) {
            // Discard upper 32 bits.
            for (int i = 0; i < 5; i++) {
              if (bytesContainer.bytes[bytesContainer.offset++] >= 0) {
                return result;
              }
            }
            throw new OSerializationException("Mailformed format");
          }
        }
      }
    }

    return result;
  }

  private static boolean skipField(int tag, BytesContainer bytesContainer) {
    switch (getTagWireType(tag)) {
    case 0:
      skipRawVarint(bytesContainer);
      return true;
    case 1:
      bytesContainer.offset += 8;
      return true;
    case 2:
      final int size = readRawVarint32(bytesContainer);
      bytesContainer.offset += size;
      return true;
    case 3:
      // we do not use it
      return true;
    case 4:
      return false;
    case 5:
      bytesContainer.offset += 4;
      return true;
    default:
      throw new OSerializationException("Invalid format");
    }
  }

  private static int getTagWireType(int tag) {
    return tag & 7;
  }

  private static String readString(BytesContainer bytesContainer) {
    int size = readRawVarint32(bytesContainer);
    String result = new String(bytesContainer.bytes, bytesContainer.offset, size, UTF8);
    bytesContainer.offset += size;
    return result;
  }

  private static void skipRawVarint(BytesContainer bytesContainer) {
    byte[] bytes = bytesContainer.bytes;
    int pos = bytesContainer.offset;

    for (int i = 0; i < 10; ++i) {
      if (bytes[pos++] >= 0) {
        bytesContainer.offset = pos;
        return;
      }
    }
  }

  @Override
  public Object deserializeValue(BytesContainer bytes, OType type, ODocument ownerDocument) {
    return null;
  }

  @Override
  public OBinaryField deserializeField(BytesContainer bytes, OClass iClass, String iFieldName) {

    return null;
  }

  @Override
  public OBinaryComparator getComparator() {
    return null;
  }

  @Override
  public String[] getFieldNames(BytesContainer iBytes) {
    return new String[0];
  }
}
