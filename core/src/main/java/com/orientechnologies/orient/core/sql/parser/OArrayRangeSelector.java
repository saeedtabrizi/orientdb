/* Generated By:JJTree: Do not edit this line. OArrayRangeSelector.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;

import java.lang.reflect.Array;
import java.util.*;

public class OArrayRangeSelector extends SimpleNode {
  protected Integer from;
  protected Integer to;
  protected boolean newRange = false;
  protected boolean included = false;

  protected OArrayNumberSelector fromSelector;
  protected OArrayNumberSelector toSelector;

  public OArrayRangeSelector(int id) {
    super(id);
  }

  public OArrayRangeSelector(OrientSql p, int id) {
    super(p, id);
  }

  /**
   * Accept the visitor.
   **/
  public Object jjtAccept(OrientSqlVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    if (from != null) {
      builder.append(from);
    } else {
      fromSelector.toString(params, builder);
    }
    if (newRange) {
      builder.append("..");
      if (included) {
        builder.append('.');
      }
    } else {
      builder.append("-");
    }
    if (to != null) {
      builder.append(to);
    } else {
      toSelector.toString(params, builder);
    }
  }

  public Object execute(OIdentifiable iCurrentRecord, Object result, OCommandContext ctx) {
    if (result == null) {
      return null;
    }
    if (!OMultiValue.isMultiValue(result)) {
      return null;
    }
    Integer lFrom = from;
    if (fromSelector != null) {
      lFrom = fromSelector.getValue(iCurrentRecord, result, ctx);
    }
    if (lFrom == null) {
      lFrom = 0;
    }
    Integer lTo = to;
    if (toSelector != null) {
      lTo = toSelector.getValue(iCurrentRecord, result, ctx);
    }
    if(included){
      lTo++;
    }
    if (lFrom > lTo) {
      return null;
    }
    Object[] arrayResult = OMultiValue.array(result);

    if (arrayResult == null || arrayResult.length == 0) {
      return arrayResult;
    }
    lFrom = Math.max(lFrom, 0);
    if (arrayResult.length < lFrom) {
      return null;
    }
    lFrom = Math.min(lFrom, arrayResult.length - 1);

    lTo = Math.min(lTo, arrayResult.length);

    return Arrays.asList(Arrays.copyOfRange(arrayResult, lFrom, lTo));
  }

  public Object execute(OResult iCurrentRecord, Object result, OCommandContext ctx) {
    if (result == null) {
      return null;
    }
    if (!OMultiValue.isMultiValue(result)) {
      return null;
    }
    Integer lFrom = from;
    if (fromSelector != null) {
      lFrom = fromSelector.getValue(iCurrentRecord, result, ctx);
    }
    if (lFrom == null) {
      lFrom = 0;
    }
    Integer lTo = to;
    if (toSelector != null) {
      lTo = toSelector.getValue(iCurrentRecord, result, ctx);
    }
    if(included){
      lTo++;
    }
    if (lFrom > lTo) {
      return null;
    }
    Object[] arrayResult = OMultiValue.array(result);

    if (arrayResult == null || arrayResult.length == 0) {
      return arrayResult;
    }
    lFrom = Math.max(lFrom, 0);
    if (arrayResult.length < lFrom) {
      return null;
    }
    lFrom = Math.min(lFrom, arrayResult.length - 1);

    lTo = Math.min(lTo, arrayResult.length);

    return Arrays.asList(Arrays.copyOfRange(arrayResult, lFrom, lTo));
  }

  public boolean needsAliases(Set<String> aliases) {
    if (fromSelector != null && fromSelector.needsAliases(aliases)) {
      return true;
    }
    if (toSelector != null && toSelector.needsAliases(aliases)) {
      return true;
    }
    return false;
  }

  public OArrayRangeSelector copy() {
    OArrayRangeSelector result = new OArrayRangeSelector(-1);
    result.from = from;
    result.to = to;
    result.newRange = newRange;
    result.included = included;

    result.fromSelector = fromSelector == null ? null : fromSelector.copy();
    result.toSelector = toSelector == null ? null : toSelector.copy();

    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OArrayRangeSelector that = (OArrayRangeSelector) o;

    if (newRange != that.newRange)
      return false;
    if (included != that.included)
      return false;
    if (from != null ? !from.equals(that.from) : that.from != null)
      return false;
    if (to != null ? !to.equals(that.to) : that.to != null)
      return false;
    if (fromSelector != null ? !fromSelector.equals(that.fromSelector) : that.fromSelector != null)
      return false;
    if (toSelector != null ? !toSelector.equals(that.toSelector) : that.toSelector != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = from != null ? from.hashCode() : 0;
    result = 31 * result + (to != null ? to.hashCode() : 0);
    result = 31 * result + (newRange ? 1 : 0);
    result = 31 * result + (included ? 1 : 0);
    result = 31 * result + (fromSelector != null ? fromSelector.hashCode() : 0);
    result = 31 * result + (toSelector != null ? toSelector.hashCode() : 0);
    return result;
  }

  public void extractSubQueries(SubQueryCollector collector) {
    if (fromSelector != null) {
      fromSelector.extractSubQueries(collector);
    }
    if (toSelector != null) {
      toSelector.extractSubQueries(collector);
    }
  }

  public boolean refersToParent() {
    if (fromSelector != null && fromSelector.refersToParent()) {
      return true;
    }
    if (toSelector != null && toSelector.refersToParent()) {
      return true;
    }
    return false;
  }

  /**
   * @param target
   * @param value
   * @param ctx
   *
   * @return
   */
  public void setValue(Object target, Object value, OCommandContext ctx) {
    if (target == null) {
      return;
    }
    if (target.getClass().isArray()) {
      setArrayValue(target, value, ctx);
    } else if (target instanceof List) {
      setValue((List) target, value, ctx);
    } else if (OMultiValue.isMultiValue(value)) {
      //TODO
    }
    //TODO

  }

  public void setValue(List target, Object value, OCommandContext ctx) {
    int from = this.from == null ? 0 : this.from;
    int to = target.size() - 1;
    if (this.to != null) {
      to = this.to;
      if (!included) {
        to--;
      }
    }
    if (from > to) {
      target.clear();
      return;
    }
    for (int i = 0; i <= to; i++) {
      if (i < from && target.size() - 1 < i) {
        target.set(i, null);
      } else if (i >= from) {
        target.set(i, value);
      }
      //else leave untouched the existing element
    }
  }

  public void setValue(Set target, Object value, OCommandContext ctx) {
    Set result = new LinkedHashSet<>();
    int from = this.from == null ? 0 : this.from;
    int to = target.size() - 1;
    if (this.to != null) {
      to = this.to;
      if (!included) {
        to--;
      }
    }
    if (from > to) {
      target.clear();
      return;
    }
    Iterator targetIterator = target.iterator();
    for (int i = 0; i <= to; i++) {
      Object next = null;
      if (targetIterator.hasNext()) {
        next = targetIterator.next();
      }
      if (i < from && target.size() - 1 < i) {
        result.add(null);
      } else if (i >= from) {
        result.add(value);
      } else {
        result.add(next);
      }
      target.clear();
      target.addAll(result);
    }
  }

  public void setValue(Map target, Object value, OCommandContext ctx) {
    int from = this.from == null ? 0 : this.from;
    int to = this.to;
    if (!included) {
      to--;
    }
    if (from > to) {
      target.clear();
      return;
    }
    for (int i = from; i <= to; i++) {
      target.put(i, value);
    }
  }

  private void setArrayValue(Object target, Object value, OCommandContext ctx) {

    int from = this.from == null ? 0 : this.from;
    int to = Array.getLength(target) - 1;
    if (this.to != null) {
      to = this.to;
      if (!included) {
        to--;
      }
    }
    if (from > to || from >= Array.getLength(target)) {
      return;
    }
    to = Math.min(to, Array.getLength(target) - 1);
    for (int i = from; i <= to; i++) {
      Array.set(target, i, value);//TODO type conversion?
    }
  }

  public void applyRemove(Object currentValue, OResultInternal originalRecord, OCommandContext ctx) {
    if (currentValue == null) {
      return;
    }
    Integer from = this.from;
    if (fromSelector != null) {
      from = fromSelector.getValue(originalRecord, null, ctx);
    }
    Integer to = this.to;
    if (toSelector != null) {
      to = toSelector.getValue(originalRecord, null, ctx);
    }
    if (from == null || to == null) {
      throw new OCommandExecutionException("Invalid range expression: " + toString() + " one of the elements is null");
    }
    if (included) {
      to++;
    }
    if (from < 0) {
      from = 0;
    }
    if (from >= to) {
      return;
    }
    int range = to - from;
    if (currentValue instanceof List) {
      List list = (List) currentValue;
      for (int i = 0; i < range; i++) {
        if (list.size() > from) {
          list.remove(from);
        } else {
          break;
        }
      }
    } else if (currentValue instanceof Set) {
      Iterator iter = ((Set) currentValue).iterator();
      int count = 0;
      while (iter.hasNext()) {
        iter.next();
        if (count >= from) {
          if (count < to) {
            iter.remove();
          } else {
            break;
          }
        }
        count++;
      }
    } else {
      throw new OCommandExecutionException(
          "Trying to remove elements from " + currentValue + " (" + currentValue.getClass().getSimpleName() + ")");
    }
  }

  public OResult serialize() {
    OResultInternal result = new OResultInternal();
    result.setProperty("from", from);
    result.setProperty("to", to);
    result.setProperty("newRange", newRange);
    result.setProperty("included", included);

    if (fromSelector != null) {
      result.setProperty("fromSelector", fromSelector.serialize());
    }
    if (toSelector != null) {
      result.setProperty("toSelector", toSelector.serialize());
    }
    return result;
  }

  public void deserialize(OResult fromResult) {
    from = fromResult.getProperty("from");
    to = fromResult.getProperty("to");
    newRange = fromResult.getProperty("newRange");
    included = fromResult.getProperty("included");

    if (fromResult.getProperty("fromSelector") != null) {
      fromSelector = new OArrayNumberSelector(-1);
      fromSelector.deserialize(fromResult.getProperty("fromSelector"));
    }
    if (fromResult.getProperty("toSelector") != null) {
      toSelector = new OArrayNumberSelector(-1);
      toSelector.deserialize(fromResult.getProperty("toSelector"));
    }
  }
}
/* JavaCC - OriginalChecksum=594a372e31fcbcd3ed962c2260e76468 (do not edit this line) */
