package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.index.OIndexTxAwareOneValue;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Test
public class IndexTxAwareOneValueGetEntriesTest extends DocumentDBBaseTest {

  private static final String CLASS_NAME    = "idxTxAwareOneValueGetEntriesTest";
  private static final String PROPERTY_NAME = "value";
  private static final String INDEX         = "idxTxAwareOneValueGetEntriesTestIndex";

  @Parameters(value = "url")
  public IndexTxAwareOneValueGetEntriesTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    OClass cls = database.getMetadata().getSchema().createClass(CLASS_NAME);
    cls.createProperty(PROPERTY_NAME, OType.INTEGER);
    cls.createIndex(INDEX, OClass.INDEX_TYPE.UNIQUE, PROPERTY_NAME);
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    database.getMetadata().getSchema().getClass(CLASS_NAME).truncate();

    super.afterMethod();
  }

  @Test
  public void testPut() {
    if (((ODatabaseInternal) database).getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();
    final OIndex<?> index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1).save();
    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 2).save();

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> resultOne = new HashSet<>();
    OIndexCursor cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultOne);
    Assert.assertEquals(resultOne.size(), 2);

    database.begin();

    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 3).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> resultTwo = new HashSet<>();
    cursor = index.iterateEntries(Arrays.asList(1, 2, 3), true);
    cursorToSet(cursor, resultTwo);
    Assert.assertEquals(resultTwo.size(), 3);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> resultThree = new HashSet<>();
    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultThree);

    Assert.assertEquals(resultThree.size(), 2);
  }

  @Test
  public void testRemove() {
    if (((ODatabaseInternal) database).getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();
    final OIndex<?> index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    ODocument document = new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1).save();
    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 2).save();

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> resultOne = new HashSet<>();
    OIndexCursor cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultOne);
    Assert.assertEquals(resultOne.size(), 2);

    database.begin();

    document.delete();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> resultTwo = new HashSet<>();
    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultTwo);
    Assert.assertEquals(resultTwo.size(), 1);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> resultThree = new HashSet<>();
    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultThree);
    Assert.assertEquals(resultThree.size(), 2);
  }

  @Test
  public void testRemoveAndPut() {
    if (((ODatabaseInternal) database).getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();
    final OIndex<?> index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    ODocument document = new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1).save();
    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 2).save();

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> resultOne = new HashSet<>();
    OIndexCursor cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultOne);
    Assert.assertEquals(resultOne.size(), 2);

    database.begin();

    document.removeField(PROPERTY_NAME);
    document.save();

    document.field(PROPERTY_NAME, 1).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> resultTwo = new HashSet<>();
    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultTwo);
    Assert.assertEquals(resultTwo.size(), 2);

    database.rollback();
  }

  @Test
  public void testMultiPut() {
    if (((ODatabaseInternal) database).getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex<?> index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    ODocument document = new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1).save();
    document.field(PROPERTY_NAME, 0);
    document.field(PROPERTY_NAME, 1);
    document.save();

    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 2).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> result = new HashSet<>();
    OIndexCursor cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, result);

    Assert.assertEquals(result.size(), 2);

    database.commit();

    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, result);
    Assert.assertEquals(result.size(), 2);
  }

  @Test
  public void testPutAfterTransaction() {
    if (((ODatabaseInternal) database).getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex<?> index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1).save();
    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 2).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> result = new HashSet<>();
    OIndexCursor cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, result);

    Assert.assertEquals(result.size(), 2);
    database.commit();

    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 3).save();

    cursor = index.iterateEntries(Arrays.asList(1, 2, 3), true);
    cursorToSet(cursor, result);
    Assert.assertEquals(result.size(), 3);
  }

  @Test
  public void testRemoveOneWithinTransaction() {
    if (((ODatabaseInternal) database).getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex<?> index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final ODocument document = new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1).save();
    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 2).save();

    document.delete();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> result = new HashSet<>();
    OIndexCursor cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, result);
    Assert.assertEquals(result.size(), 1);

    database.commit();

    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, result);
    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testPutAfterRemove() {
    if (((ODatabaseInternal) database).getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex<?> index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    ODocument document = new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1).save();
    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 2).save();

    document.removeField(PROPERTY_NAME);
    document.save();

    document.field(PROPERTY_NAME, 1).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> result = new HashSet<>();
    OIndexCursor cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, result);

    Assert.assertEquals(result.size(), 2);

    database.commit();

    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, result);
    Assert.assertEquals(result.size(), 2);
  }

  private void cursorToSet(OIndexCursor cursor, Set<OIdentifiable> result) {
    result.clear();
    Map.Entry<Object, OIdentifiable> entry = cursor.nextEntry();
    while (entry != null) {
      result.add(entry.getValue());
      entry = cursor.nextEntry();
    }
  }

}
