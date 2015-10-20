package org.pentaho.di.core.database;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.row.value.ValueMetaBoolean;
import org.pentaho.di.core.row.value.ValueMetaDate;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Created by bryan on 10/19/15.
 */
public class Hive2DatabaseMetaTest {

  private Hive2DatabaseMeta hive2DatabaseMeta;

  @Before
  public void setup() throws Throwable {
    hive2DatabaseMeta = new Hive2DatabaseMeta();
  }

  @Test
  public void testGetAccessTypeList() {
    assertArrayEquals( new int[] { DatabaseMeta.TYPE_ACCESS_NATIVE }, hive2DatabaseMeta.getAccessTypeList() );
  }

  @Test
  public void testGetDriverClass() {
    assertEquals( Hive2DatabaseMeta.DRIVER_CLASS_NAME, hive2DatabaseMeta.getDriverClass() );
  }

  @Test
  public void testGetFieldDefinitionBoolean() {
    String boolName = "boolName";
    ValueMetaBoolean valueMetaBoolean = new ValueMetaBoolean();
    valueMetaBoolean.setName( boolName );
    assertEquals( "BOOLEAN", hive2DatabaseMeta.getFieldDefinition( valueMetaBoolean, null, null, false, false,
      false ) );
    assertEquals( boolName + " BOOLEAN",
      hive2DatabaseMeta.getFieldDefinition( valueMetaBoolean, null, null, false, true, false ) );
  }

  @Test
  public void testGetFieldDefinitionDate() {
    String dateName = "dateName";
    ValueMetaDate valueMetaDate = new ValueMetaDate();
    valueMetaDate.setName( dateName );
    assertEquals( "DATE", hive2DatabaseMeta.getFieldDefinition( valueMetaDate, null, null, false, false,
      false ) );
    assertEquals( dateName + " DATE",
      hive2DatabaseMeta.getFieldDefinition( valueMetaDate, null, null, false, true, false ) );
  }
}
