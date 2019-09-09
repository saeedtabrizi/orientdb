package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexKeyCursor;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.*;

import java.util.*;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 3/28/14
 */
public class LinkSetIndexTest extends DocumentDBBaseTest {
  @Parameters(value = "url")
  public LinkSetIndexTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  public void setupSchema() {
    final OClass ridBagIndexTestClass = database.getMetadata().getSchema().createClass("LinkSetIndexTestClass");

    ridBagIndexTestClass.createProperty("linkSet", OType.LINKSET);

    ridBagIndexTestClass.createIndex("linkSetIndex", OClass.INDEX_TYPE.NOTUNIQUE, "linkSet");
    database.close();
  }

  @BeforeMethod
  public void beforeMethod() {
    database.open("admin", "admin");
  }

  @AfterMethod
  public void afterMethod() {
    database.command(new OCommandSQL("DELETE FROM LinkSetIndexTestClass")).execute();

    List<ODocument> result = database.command(new OCommandSQL("select from LinkSetIndexTestClass")).execute();
    Assert.assertEquals(result.size(), 0);

    if (((ODatabaseDocumentInternal) database).getStorage().isRemote()) {
      OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, "linkSetIndex");
      Assert.assertEquals(index.getSize(), 0);

    }

    database.close();
  }

  public void testIndexLinkSet() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    document.save();

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getSize(), 2);

    OIndexKeyCursor keyCursor = index.keyCursor();
    OIdentifiable key = (OIdentifiable) keyCursor.next(-1);

    while (key != null) {
      if (!key.getIdentity().equals(docOne.getIdentity()) && !key.getIdentity().equals(docTwo.getIdentity())) {
        Assert.fail("Unknown key found: " + key);
      }

      key = (OIdentifiable) keyCursor.next(-1);
    }
  }

  public void testIndexLinkSetInTx() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    try {
      database.begin();
      final ODocument document = new ODocument("LinkSetIndexTestClass");
      final Set<OIdentifiable> linkSet = new HashSet<>();
      linkSet.add(docOne);
      linkSet.add(docTwo);

      document.field("linkSet", linkSet);
      document.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getSize(), 2);

    OIndexKeyCursor keyCursor = index.keyCursor();
    OIdentifiable key = (OIdentifiable) keyCursor.next(-1);

    while (key != null) {
      if (!key.getIdentity().equals(docOne.getIdentity()) && !key.getIdentity().equals(docTwo.getIdentity())) {
        Assert.fail("Unknown key found: " + key);
      }

      key = (OIdentifiable) keyCursor.next(-1);
    }
  }

  public void testIndexLinkSetUpdate() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSetOne = new HashSet<>();
    linkSetOne.add(docOne);
    linkSetOne.add(docTwo);

    document.field("linkSet", linkSetOne);
    document.save();

    final Set<OIdentifiable> linkSetTwo = new HashSet<>();
    linkSetTwo.add(docOne);
    linkSetTwo.add(docThree);

    document.field("linkSet", linkSetTwo);
    document.save();

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getSize(), 2);

    OIndexKeyCursor keyCursor = index.keyCursor();
    OIdentifiable key = (OIdentifiable) keyCursor.next(-1);

    while (key != null) {
      if (!key.getIdentity().equals(docOne.getIdentity()) && !key.getIdentity().equals(docThree.getIdentity())) {
        Assert.fail("Unknown key found: " + key);
      }

      key = (OIdentifiable) keyCursor.next(-1);
    }
  }

  public void testIndexLinkSetUpdateInTx() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSetOne = new HashSet<>();
    linkSetOne.add(docOne);
    linkSetOne.add(docTwo);

    document.field("linkSet", linkSetOne);
    document.save();

    try {
      database.begin();

      final Set<OIdentifiable> linkSetTwo = new HashSet<>();
      linkSetTwo.add(docOne);
      linkSetTwo.add(docThree);

      document.field("linkSet", linkSetTwo);
      document.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getSize(), 2);

    OIndexKeyCursor keyCursor = index.keyCursor();
    OIdentifiable key = (OIdentifiable) keyCursor.next(-1);

    while (key != null) {
      if (!key.getIdentity().equals(docOne.getIdentity()) && !key.getIdentity().equals(docThree.getIdentity())) {
        Assert.fail("Unknown key found: " + key);
      }

      key = (OIdentifiable) keyCursor.next(-1);
    }
  }

  public void testIndexLinkSetUpdateInTxRollback() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    final Set<OIdentifiable> linkSetOne = new HashSet<>();
    linkSetOne.add(docOne);
    linkSetOne.add(docTwo);

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    document.field("linkSet", linkSetOne);
    document.save();

    database.begin();

    final Set<OIdentifiable> linkSetTwo = new HashSet<>();
    linkSetTwo.add(docOne);
    linkSetTwo.add(docThree);

    document.field("linkSet", linkSetTwo);
    document.save();
    database.rollback();

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getSize(), 2);

    OIndexKeyCursor keyCursor = index.keyCursor();
    OIdentifiable key = (OIdentifiable) keyCursor.next(-1);

    while (key != null) {
      if (!key.getIdentity().equals(docOne.getIdentity()) && !key.getIdentity().equals(docTwo.getIdentity())) {
        Assert.fail("Unknown key found: " + key);
      }

      key = (OIdentifiable) keyCursor.next(-1);
    }
  }

  public void testIndexLinkSetUpdateAddItem() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);
    document.field("linkSet", linkSet);

    document.save();

    database.command(new OCommandSQL("UPDATE " + document.getIdentity() + " add linkSet = " + docThree.getIdentity())).execute();

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getSize(), 3);

    OIndexKeyCursor keyCursor = index.keyCursor();
    OIdentifiable key = (OIdentifiable) keyCursor.next(-1);

    while (key != null) {
      if (!key.getIdentity().equals(docOne.getIdentity()) && !key.getIdentity().equals(docTwo.getIdentity()) && !key.getIdentity()
          .equals(docThree.getIdentity())) {
        Assert.fail("Unknown key found: " + key);
      }

      key = (OIdentifiable) keyCursor.next(-1);
    }
  }

  public void testIndexLinkSetUpdateAddItemInTx() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    document.save();

    try {
      database.begin();
      ODocument loadedDocument = database.load(document.getIdentity());
      loadedDocument.<Set<OIdentifiable>>field("linkSet").add(docThree);
      document.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getSize(), 3);

    OIndexKeyCursor keyCursor = index.keyCursor();
    OIdentifiable key = (OIdentifiable) keyCursor.next(-1);

    while (key != null) {
      if (!key.getIdentity().equals(docOne.getIdentity()) && !key.getIdentity().equals(docTwo.getIdentity()) && !key.getIdentity()
          .equals(docThree.getIdentity())) {
        Assert.fail("Unknown key found: " + key);
      }

      key = (OIdentifiable) keyCursor.next(-1);
    }
  }

  public void testIndexLinkSetUpdateAddItemInTxRollback() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    document.save();

    database.begin();
    ODocument loadedDocument = database.load(document.getIdentity());
    loadedDocument.<Set<OIdentifiable>>field("linkSet").add(docThree);
    loadedDocument.save();
    database.rollback();

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getSize(), 2);

    OIndexKeyCursor keyCursor = index.keyCursor();
    OIdentifiable key = (OIdentifiable) keyCursor.next(-1);

    while (key != null) {
      if (!key.getIdentity().equals(docOne.getIdentity()) && !key.getIdentity().equals(docTwo.getIdentity())) {
        Assert.fail("Unknown key found: " + key);
      }

      key = (OIdentifiable) keyCursor.next(-1);
    }
  }

  public void testIndexLinkSetUpdateRemoveItemInTx() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);
    document.field("linkSet", linkSet);
    document.save();

    try {
      database.begin();
      ODocument loadedDocument = database.load(document.getIdentity());
      loadedDocument.<Set<OIdentifiable>>field("linkSet").remove(docTwo);
      loadedDocument.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getSize(), 1);

    OIndexKeyCursor keyCursor = index.keyCursor();
    OIdentifiable key = (OIdentifiable) keyCursor.next(-1);

    while (key != null) {
      if (!key.getIdentity().equals(docOne.getIdentity())) {
        Assert.fail("Unknown key found: " + key);
      }

      key = (OIdentifiable) keyCursor.next(-1);
    }
  }

  public void testIndexLinkSetUpdateRemoveItemInTxRollback() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);
    document.field("linkSet", linkSet);
    document.save();

    database.begin();
    ODocument loadedDocument = database.load(document.getIdentity());
    loadedDocument.<Set<OIdentifiable>>field("linkSet").remove(docTwo);
    loadedDocument.save();
    database.rollback();

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getSize(), 2);

    OIndexKeyCursor keyCursor = index.keyCursor();
    OIdentifiable key = (OIdentifiable) keyCursor.next(-1);

    while (key != null) {
      if (!key.getIdentity().equals(docOne.getIdentity()) && !key.getIdentity().equals(docTwo.getIdentity())) {
        Assert.fail("Unknown key found: " + key);
      }

      key = (OIdentifiable) keyCursor.next(-1);
    }
  }

  public void testIndexLinkSetUpdateRemoveItem() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    document.save();

    database.command(new OCommandSQL("UPDATE " + document.getIdentity() + " remove linkSet = " + docTwo.getIdentity())).execute();

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getSize(), 1);

    OIndexKeyCursor keyCursor = index.keyCursor();
    OIdentifiable key = (OIdentifiable) keyCursor.next(-1);

    while (key != null) {
      if (!key.getIdentity().equals(docOne.getIdentity())) {
        Assert.fail("Unknown key found: " + key);
      }

      key = (OIdentifiable) keyCursor.next(-1);
    }
  }

  public void testIndexLinkSetRemove() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkSetIndexTestClass");

    final Set<OIdentifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    document.save();
    document.delete();

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getSize(), 0);
  }

  public void testIndexLinkSetRemoveInTx() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkSetIndexTestClass");

    final Set<OIdentifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    document.save();
    try {
      database.begin();
      document.delete();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getSize(), 0);
  }

  public void testIndexLinkSetRemoveInTxRollback() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSet = new HashSet<>();


    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    document.save();

    database.begin();
    document.delete();
    database.rollback();

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getSize(), 2);

    OIndexKeyCursor keyCursor = index.keyCursor();
    OIdentifiable key = (OIdentifiable) keyCursor.next(-1);

    while (key != null) {
      if (!key.getIdentity().equals(docOne.getIdentity()) && !key.getIdentity().equals(docTwo.getIdentity())) {
        Assert.fail("Unknown key found: " + key);
      }

      key = (OIdentifiable) keyCursor.next(-1);
    }
  }

  public void testIndexLinkSetSQL() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSetOne = new HashSet<>();
    linkSetOne.add(docOne);
    linkSetOne.add(docTwo);

    document.field("linkSet", linkSetOne);
    document.save();

    document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSet = new HashSet<>();
    linkSet.add(docThree);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    document.save();

    List<ODocument> result = database
        .query(new OSQLSynchQuery<ODocument>("select * from LinkSetIndexTestClass where linkSet contains ?"), docOne.getIdentity());
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 1);

    List<OIdentifiable> listResult = new ArrayList<>(result.get(0).<Set<OIdentifiable>>field("linkSet"));
    Assert.assertEquals(listResult.size(), 2);
    Assert.assertTrue(listResult.containsAll(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
  }
}
