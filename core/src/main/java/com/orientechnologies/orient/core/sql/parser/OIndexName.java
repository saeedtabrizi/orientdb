/* Generated By:JJTree: Do not edit this line. OIndexName.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;

import java.util.Map;

public class OIndexName extends SimpleNode {

  protected String value;

  public OIndexName(int id) {
    super(id);
  }

  public OIndexName(OrientSql p, int id) {
    super(p, id);
  }

  public String getValue() {
    return value;
  }

  @Override
  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append(getValue());
  }

  public OIndexName copy() {
    OIndexName result = new OIndexName(-1);
    result.value = value;
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OIndexName that = (OIndexName) o;

    if (value != null ? !value.equals(that.value) : that.value != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    return value != null ? value.hashCode() : 0;
  }

  public OResult serialize() {
    OResultInternal result = new OResultInternal();
    result.setProperty("value", value);
    return result;
  }

  public void deserialize(OResult fromResult) {
    value = fromResult.getProperty("value");
  }

  public void setValue(String value) {
    this.value = value;
  }
}
/* JavaCC - OriginalChecksum=06c827926e7e9ee650b76d42e31feb46 (do not edit this line) */
