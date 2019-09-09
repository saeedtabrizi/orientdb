package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexKeyCursor;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.test.domain.whiz.Mapper;
import org.testng.Assert;
import org.testng.annotations.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author LomakiA <a href="mailto:a.lomakin@orientechnologies.com">Andrey Lomakin</a>
 * @since 21.12.11
 */
@Test(groups = { "index" })
public class MapIndexTest extends ObjectDBBaseTest {

  @Parameters(value = "url")
  public MapIndexTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  public void setupSchema() {
    database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain.whiz");

    final OClass mapper = database.getMetadata().getSchema().getClass("Mapper");
    mapper.createProperty("id", OType.STRING);
    mapper.createProperty("intMap", OType.EMBEDDEDMAP, OType.INTEGER);

    mapper.createIndex("mapIndexTestKey", OClass.INDEX_TYPE.NOTUNIQUE, "intMap");
    mapper.createIndex("mapIndexTestValue", OClass.INDEX_TYPE.NOTUNIQUE, "intMap by value");

    final OClass movie = database.getMetadata().getSchema().createClass("MapIndexTestMovie");
    movie.createProperty("title", OType.STRING);
    movie.createProperty("thumbs", OType.EMBEDDEDMAP, OType.INTEGER);

    movie.createIndex("indexForMap", OClass.INDEX_TYPE.NOTUNIQUE, "thumbs by key");
  }

  @AfterClass
  public void destroySchema() {
    database.open("admin", "admin");
    database.getMetadata().getSchema().dropClass("Mapper");
    database.getMetadata().getSchema().dropClass("MapIndexTestMovie");
    database.close();
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    database.command(new OCommandSQL("delete from Mapper")).execute();
    database.command(new OCommandSQL("delete from MapIndexTestMovie")).execute();

    super.afterMethod();
  }

  public void testIndexMap() {
    checkEmbeddedDB();

    final Mapper mapper = new Mapper();
    Map<String, Integer> map = new HashMap<String, Integer>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setIntMap(map);
    database.save(mapper);

    OIndex keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getSize(), 2);

    OIndexKeyCursor keyCursor = keyIndex.keyCursor();
    String key = (String) keyCursor.next(-1);

    while (key != null) {
      if (!key.equals("key1") && !key.equals("key2")) {
        Assert.fail("Unknown key found: " + key);
      }

      key = (String) keyCursor.next(-1);
    }

    OIndex valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getSize(), 2);

    OIndexKeyCursor valueCursor = valueIndex.keyCursor();
    Integer value = (Integer) valueCursor.next(-1);

    while (value != null) {
      if (!value.equals(10) && !value.equals(20)) {
        Assert.fail("Unknown value found: " + value);
      }

      value = (Integer) valueCursor.next(-1);
    }
  }

  public void testIndexMapInTx() {
    checkEmbeddedDB();

    try {
      database.begin();
      final Mapper mapper = new Mapper();
      Map<String, Integer> map = new HashMap<String, Integer>();

      map.put("key1", 10);
      map.put("key2", 20);

      mapper.setIntMap(map);
      database.save(mapper);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex keyIndex = getIndex("mapIndexTestKey");

    Assert.assertEquals(keyIndex.getSize(), 2);
    OIndexKeyCursor keyCursor = keyIndex.keyCursor();
    String key = (String) keyCursor.next(-1);

    while (key != null) {
      if (!key.equals("key1") && !key.equals("key2")) {
        Assert.fail("Unknown key found: " + key);
      }

      key = (String) keyCursor.next(-1);
    }

    OIndex valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getSize(), 2);

    OIndexKeyCursor valueCursor = valueIndex.keyCursor();
    Integer value = (Integer) valueCursor.next(-1);

    while (value != null) {
      if (!value.equals(10) && !value.equals(20)) {
        Assert.fail("Unknown value found: " + value);
      }

      value = (Integer) valueCursor.next(-1);
    }
  }

  public void testIndexMapUpdateOne() {
    checkEmbeddedDB();

    Mapper mapper = new Mapper();
    Map<String, Integer> mapOne = new HashMap<String, Integer>();

    mapOne.put("key1", 10);
    mapOne.put("key2", 20);

    mapper.setIntMap(mapOne);
    mapper = database.save(mapper);

    final Map<String, Integer> mapTwo = new HashMap<String, Integer>();

    mapTwo.put("key3", 30);
    mapTwo.put("key2", 20);

    mapper.setIntMap(mapTwo);
    database.save(mapper);

    OIndex keyIndex = getIndex("mapIndexTestKey");

    Assert.assertEquals(keyIndex.getSize(), 2);

    OIndexKeyCursor keyCursor = keyIndex.keyCursor();
    String key = (String) keyCursor.next(-1);

    while (key != null) {
      if (!key.equals("key2") && !key.equals("key3")) {
        Assert.fail("Unknown key found: " + key);
      }

      key = (String) keyCursor.next(-1);
    }

    OIndex valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getSize(), 2);

    OIndexKeyCursor valueCursor = valueIndex.keyCursor();
    Integer value = (Integer) valueCursor.next(-1);

    while (value != null) {
      if (!value.equals(30) && !value.equals(20)) {
        Assert.fail("Unknown key found: " + value);
      }

      value = (Integer) valueCursor.next(-1);
    }
  }

  public void testIndexMapUpdateOneTx() {
    checkEmbeddedDB();

    Mapper mapper = new Mapper();
    Map<String, Integer> mapOne = new HashMap<String, Integer>();

    mapOne.put("key1", 10);
    mapOne.put("key2", 20);

    mapper.setIntMap(mapOne);
    mapper = database.save(mapper);

    database.begin();
    try {
      final Map<String, Integer> mapTwo = new HashMap<String, Integer>();

      mapTwo.put("key3", 30);
      mapTwo.put("key2", 20);

      mapper.setIntMap(mapTwo);
      database.save(mapper);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex keyIndex = getIndex("mapIndexTestKey");

    Assert.assertEquals(keyIndex.getSize(), 2);

    OIndexKeyCursor keyCursor = keyIndex.keyCursor();
    String key = (String) keyCursor.next(-1);

    while (key != null) {
      if (!key.equals("key2") && !key.equals("key3")) {
        Assert.fail("Unknown key found: " + key);
      }

      key = (String) keyCursor.next(-1);
    }

    OIndex valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getSize(), 2);

    OIndexKeyCursor valueCursor = valueIndex.keyCursor();
    Integer value = (Integer) valueCursor.next(-1);

    while (value != null) {
      if (!value.equals(30) && !value.equals(20)) {
        Assert.fail("Unknown key found: " + value);
      }

      value = (Integer) valueCursor.next(-1);
    }
  }

  public void testIndexMapUpdateOneTxRollback() {
    checkEmbeddedDB();

    Mapper mapper = new Mapper();
    Map<String, Integer> mapOne = new HashMap<String, Integer>();

    mapOne.put("key1", 10);
    mapOne.put("key2", 20);

    mapper.setIntMap(mapOne);
    mapper = database.save(mapper);

    database.begin();
    final Map<String, Integer> mapTwo = new HashMap<String, Integer>();

    mapTwo.put("key3", 30);
    mapTwo.put("key2", 20);

    mapper.setIntMap(mapTwo);
    database.save(mapper);
    database.rollback();

    OIndex keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getSize(), 2);

    OIndexKeyCursor keyCursor = keyIndex.keyCursor();
    String key = (String) keyCursor.next(-1);

    while (key != null) {
      if (!key.equals("key2") && !key.equals("key1")) {
        Assert.fail("Unknown key found: " + key);
      }

      key = (String) keyCursor.next(-1);
    }

    OIndex valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getSize(), 2);

    OIndexKeyCursor valueCursor = valueIndex.keyCursor();
    Integer value = (Integer) valueCursor.next(-1);

    while (value != null) {
      if (!value.equals(10) && !value.equals(20)) {
        Assert.fail("Unknown key found: " + value);
      }

      value = (Integer) valueCursor.next(-1);
    }
  }

  public void testIndexMapAddItem() {
    checkEmbeddedDB();

    Mapper mapper = new Mapper();
    Map<String, Integer> map = new HashMap<String, Integer>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setIntMap(map);
    mapper = database.save(mapper);

    database.command(new OCommandSQL("UPDATE " + mapper.getId() + " put intMap = 'key3', 30")).execute();

    OIndex keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getSize(), 3);

    OIndexKeyCursor keyCursor = keyIndex.keyCursor();
    String key = (String) keyCursor.next(-1);

    while (key != null) {
      if (!key.equals("key1") && !key.equals("key2") && !key.equals("key3")) {
        Assert.fail("Unknown key found: " + key);
      }

      key = (String) keyCursor.next(-1);
    }

    OIndex valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getSize(), 3);

    OIndexKeyCursor valueCursor = valueIndex.keyCursor();
    Integer value = (Integer) valueCursor.next(-1);

    while (value != null) {
      if (!value.equals(30) && !value.equals(20) && !value.equals(10)) {
        Assert.fail("Unknown value found: " + value);
      }

      value = (Integer) valueCursor.next(-1);
    }
  }

  public void testIndexMapAddItemTx() {
    checkEmbeddedDB();

    Mapper mapper = new Mapper();
    Map<String, Integer> map = new HashMap<String, Integer>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setIntMap(map);
    mapper = database.save(mapper);

    try {
      database.begin();
      Mapper loadedMapper = database.load(new ORecordId(mapper.getId()));
      loadedMapper.getIntMap().put("key3", 30);
      database.save(loadedMapper);

      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getSize(), 3);

    OIndexKeyCursor keyCursor = keyIndex.keyCursor();
    String key = (String) keyCursor.next(-1);

    while (key != null) {
      if (!key.equals("key1") && !key.equals("key2") && !key.equals("key3")) {
        Assert.fail("Unknown key found: " + key);
      }

      key = (String) keyCursor.next(-1);
    }

    OIndex valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getSize(), 3);

    OIndexKeyCursor valueCursor = valueIndex.keyCursor();
    Integer value = (Integer) valueCursor.next(-1);

    while (value != null) {
      if (!value.equals(30) && !value.equals(20) && !value.equals(10)) {
        Assert.fail("Unknown value found: " + value);
      }

      value = (Integer) valueCursor.next(-1);
    }
  }

  public void testIndexMapAddItemTxRollback() {
    checkEmbeddedDB();

    Mapper mapper = new Mapper();
    Map<String, Integer> map = new HashMap<String, Integer>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setIntMap(map);
    mapper = database.save(mapper);

    database.begin();
    Mapper loadedMapper = database.load(new ORecordId(mapper.getId()));
    loadedMapper.getIntMap().put("key3", 30);
    database.save(loadedMapper);
    database.rollback();

    OIndex keyIndex = getIndex("mapIndexTestKey");

    Assert.assertEquals(keyIndex.getSize(), 2);

    OIndexKeyCursor keyCursor = keyIndex.keyCursor();
    String key = (String) keyCursor.next(-1);

    while (key != null) {
      if (!key.equals("key1") && !key.equals("key2")) {
        Assert.fail("Unknown key found: " + key);
      }

      key = (String) keyCursor.next(-1);
    }

    OIndex valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getSize(), 2);

    OIndexKeyCursor valueCursor = valueIndex.keyCursor();
    Integer value = (Integer) valueCursor.next(-1);

    while (value != null) {
      if (!value.equals(20) && !value.equals(10)) {
        Assert.fail("Unknown key found: " + value);
      }

      value = (Integer) valueCursor.next(-1);
    }
  }

  public void testIndexMapUpdateItem() {
    checkEmbeddedDB();

    Mapper mapper = new Mapper();
    Map<String, Integer> map = new HashMap<String, Integer>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setIntMap(map);
    mapper = database.save(mapper);

    database.command(new OCommandSQL("UPDATE " + mapper.getId() + " put intMap = 'key2', 40")).execute();

    OIndex keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getSize(), 2);

    OIndexKeyCursor keyCursor = keyIndex.keyCursor();
    String key = (String) keyCursor.next(-1);

    while (key != null) {
      if (!key.equals("key1") && !key.equals("key2")) {
        Assert.fail("Unknown key found: " + key);
      }

      key = (String) keyCursor.next(-1);
    }

    OIndex valueIndex = getIndex("mapIndexTestValue");

    Assert.assertEquals(valueIndex.getSize(), 2);

    OIndexKeyCursor valueCursor = valueIndex.keyCursor();
    Integer value = (Integer) valueCursor.next(-1);

    while (value != null) {
      if (!value.equals(10) && !value.equals(40)) {
        Assert.fail("Unknown key found: " + value);
      }

      value = (Integer) valueCursor.next(-1);
    }
  }

  public void testIndexMapUpdateItemInTx() {
    checkEmbeddedDB();

    Mapper mapper = new Mapper();
    Map<String, Integer> map = new HashMap<String, Integer>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setIntMap(map);
    mapper = database.save(mapper);

    try {
      database.begin();
      Mapper loadedMapper = database.load(new ORecordId(mapper.getId()));
      loadedMapper.getIntMap().put("key2", 40);
      database.save(loadedMapper);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getSize(), 2);

    OIndexKeyCursor keyCursor = keyIndex.keyCursor();
    String key = (String) keyCursor.next(-1);

    while (key != null) {
      if (!key.equals("key1") && !key.equals("key2")) {
        Assert.fail("Unknown key found: " + key);
      }

      key = (String) keyCursor.next(-1);
    }

    OIndex valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getSize(), 2);

    OIndexKeyCursor valueCursor = valueIndex.keyCursor();
    Integer value = (Integer) valueCursor.next(-1);

    while (value != null) {
      if (!value.equals(10) && !value.equals(40)) {
        Assert.fail("Unknown value found: " + value);
      }

      value = (Integer) valueCursor.next(-1);
    }
  }

  public void testIndexMapUpdateItemInTxRollback() {
    checkEmbeddedDB();

    Mapper mapper = new Mapper();
    Map<String, Integer> map = new HashMap<String, Integer>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setIntMap(map);
    mapper = database.save(mapper);

    database.begin();
    Mapper loadedMapper = database.load(new ORecordId(mapper.getId()));
    loadedMapper.getIntMap().put("key2", 40);
    database.save(loadedMapper);
    database.rollback();

    OIndex keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getSize(), 2);

    OIndexKeyCursor keyCursor = keyIndex.keyCursor();
    String key = (String) keyCursor.next(-1);

    while (key != null) {
      if (!key.equals("key1") && !key.equals("key2")) {
        Assert.fail("Unknown key found: " + key);
      }

      key = (String) keyCursor.next(-1);
    }

    OIndex valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getSize(), 2);

    OIndexKeyCursor valueCursor = valueIndex.keyCursor();
    Integer value = (Integer) valueCursor.next(-1);

    while (value != null) {
      if (!value.equals(10) && !value.equals(20)) {
        Assert.fail("Unknown value found: " + value);
      }

      value = (Integer) valueCursor.next(-1);
    }
  }

  public void testIndexMapRemoveItem() {
    checkEmbeddedDB();

    Mapper mapper = new Mapper();
    Map<String, Integer> map = new HashMap<String, Integer>();

    map.put("key1", 10);
    map.put("key2", 20);
    map.put("key3", 30);

    mapper.setIntMap(map);
    mapper = database.save(mapper);

    database.command(new OCommandSQL("UPDATE " + mapper.getId() + " remove intMap = 'key2'")).execute();

    OIndex keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getSize(), 2);

    OIndexKeyCursor keyCursor = keyIndex.keyCursor();
    String key = (String) keyCursor.next(-1);

    while (key != null) {
      if (!key.equals("key1") && !key.equals("key3")) {
        Assert.fail("Unknown key found: " + key);
      }

      key = (String) keyCursor.next(-1);
    }

    OIndex valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getSize(), 2);

    OIndexKeyCursor valueCursor = valueIndex.keyCursor();
    Integer value = (Integer) valueCursor.next(-1);

    while (value != null) {
      if (!value.equals(10) && !value.equals(30)) {
        Assert.fail("Unknown value found: " + value);
      }

      value = (Integer) valueCursor.next(-1);
    }
  }

  public void testIndexMapRemoveItemInTx() {
    checkEmbeddedDB();

    Mapper mapper = new Mapper();
    Map<String, Integer> map = new HashMap<String, Integer>();

    map.put("key1", 10);
    map.put("key2", 20);
    map.put("key3", 30);

    mapper.setIntMap(map);
    mapper = database.save(mapper);

    try {
      database.begin();
      Mapper loadedMapper = database.load(new ORecordId(mapper.getId()));
      loadedMapper.getIntMap().remove("key2");
      database.save(loadedMapper);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getSize(), 2);

    OIndexKeyCursor keyCursor = keyIndex.keyCursor();
    String key = (String) keyCursor.next(-1);

    while (key != null) {
      if (!key.equals("key1") && !key.equals("key3")) {
        Assert.fail("Unknown key found: " + key);
      }

      key = (String) keyCursor.next(-1);
    }

    OIndex valueIndex = getIndex("mapIndexTestValue");

    Assert.assertEquals(valueIndex.getSize(), 2);
    OIndexKeyCursor valueCursor = valueIndex.keyCursor();
    Integer value = (Integer) valueCursor.next(-1);

    while (value != null) {
      if (!value.equals(10) && !value.equals(30)) {
        Assert.fail("Unknown value found: " + value);
      }

      value = (Integer) valueCursor.next(-1);
    }
  }

  public void testIndexMapRemoveItemInTxRollback() {
    checkEmbeddedDB();

    Mapper mapper = new Mapper();
    Map<String, Integer> map = new HashMap<String, Integer>();

    map.put("key1", 10);
    map.put("key2", 20);
    map.put("key3", 30);

    mapper.setIntMap(map);
    mapper = database.save(mapper);

    database.begin();
    Mapper loadedMapper = database.load(new ORecordId(mapper.getId()));
    loadedMapper.getIntMap().remove("key2");
    database.save(loadedMapper);
    database.rollback();

    OIndex keyIndex = getIndex("mapIndexTestKey");

    Assert.assertEquals(keyIndex.getSize(), 3);
    OIndexKeyCursor keyCursor = keyIndex.keyCursor();
    String key = (String) keyCursor.next(-1);

    while (key != null) {
      if (!key.equals("key1") && !key.equals("key2") && !key.equals("key3")) {
        Assert.fail("Unknown key found: " + key);
      }

      key = (String) keyCursor.next(-1);
    }

    OIndex valueIndex = getIndex("mapIndexTestValue");

    Assert.assertEquals(valueIndex.getSize(), 3);
    OIndexKeyCursor valueCursor = valueIndex.keyCursor();
    Integer value = (Integer) valueCursor.next(-1);

    while (value != null) {
      if (!value.equals(10) && !value.equals(20) && !value.equals(30)) {
        Assert.fail("Unknown key found: " + value);
      }

      value = (Integer) valueCursor.next(-1);
    }
  }

  public void testIndexMapRemove() {
    checkEmbeddedDB();

    Mapper mapper = new Mapper();
    Map<String, Integer> map = new HashMap<String, Integer>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setIntMap(map);
    mapper = database.save(mapper);
    database.delete(mapper);

    OIndex keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getSize(), 0);

    OIndex valueIndex = getIndex("mapIndexTestValue");

    Assert.assertEquals(valueIndex.getSize(), 0);
  }

  public void testIndexMapRemoveInTx() {
    checkEmbeddedDB();

    Mapper mapper = new Mapper();
    Map<String, Integer> map = new HashMap<String, Integer>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setIntMap(map);
    mapper = database.save(mapper);

    try {
      database.begin();
      database.delete(mapper);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getSize(), 0);

    OIndex valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getSize(), 0);
  }

  public void testIndexMapRemoveInTxRollback() {
    checkEmbeddedDB();

    Mapper mapper = new Mapper();
    Map<String, Integer> map = new HashMap<String, Integer>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setIntMap(map);
    mapper = database.save(mapper);

    database.begin();
    database.delete(mapper);
    database.rollback();

    OIndex keyIndex = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndex.getSize(), 2);

    OIndexKeyCursor keyCursor = keyIndex.keyCursor();
    String key = (String) keyCursor.next(-1);

    while (key != null) {
      if (!key.equals("key1") && !key.equals("key2")) {
        Assert.fail("Unknown key found: " + key);
      }

      key = (String) keyCursor.next(-1);
    }

    OIndex valueIndex = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndex.getSize(), 2);

    OIndexKeyCursor valueCursor = valueIndex.keyCursor();
    Integer value = (Integer) valueCursor.next(-1);

    while (value != null) {
      if (!value.equals(10) && !value.equals(20)) {
        Assert.fail("Unknown value found: " + value);
      }

      value = (Integer) valueCursor.next(-1);
    }
  }

  public void testIndexMapSQL() {
    Mapper mapper = new Mapper();
    Map<String, Integer> map = new HashMap<String, Integer>();

    map.put("key1", 10);
    map.put("key2", 20);

    mapper.setIntMap(map);
    mapper = database.save(mapper);

    final List<Mapper> resultByKey = database
        .query(new OSQLSynchQuery<Mapper>("select * from Mapper where intMap containskey ?"), "key1");
    Assert.assertNotNull(resultByKey);
    Assert.assertEquals(resultByKey.size(), 1);

    Assert.assertEquals(map, resultByKey.get(0).getIntMap());

    final List<Mapper> resultByValue = database
        .query(new OSQLSynchQuery<Mapper>("select * from Mapper where intMap containsvalue ?"), 10);
    Assert.assertNotNull(resultByValue);
    Assert.assertEquals(resultByValue.size(), 1);

    Assert.assertEquals(map, resultByValue.get(0).getIntMap());
  }
}
