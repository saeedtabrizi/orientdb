/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.common.collection;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import com.orientechnologies.common.log.OLogManager;

/**
 * Handles Multi-value types such as Arrays, Collections and Maps
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
@SuppressWarnings("unchecked")
public class OMultiValue {

  /**
   * Checks if a class is a multi-value type.
   * 
   * @param iType
   *          Class to check
   * @return true if it's an array, a collection or a map, otherwise false
   */
  public static boolean isMultiValue(final Class<?> iType) {
    return (iType.isArray() || Collection.class.isAssignableFrom(iType) || Map.class.isAssignableFrom(iType));
  }

  /**
   * Checks if the object is a multi-value type.
   * 
   * @param iObject
   *          Object to check
   * @return true if it's an array, a collection or a map, otherwise false
   */
  public static boolean isMultiValue(final Object iObject) {
    return iObject == null ? false : isMultiValue(iObject.getClass());
  }

  public static boolean isIterable(final Object iObject) {
    return iObject == null ? false : iObject instanceof Iterable<?> ? true : iObject instanceof Iterator<?>;
  }

  /**
   * Returns the size of the multi-value object
   * 
   * @param iObject
   *          Multi-value object (array, collection or map)
   * @return the size of the multi value object
   */
  public static int getSize(final Object iObject) {
    if (iObject == null)
      return 0;

    if (!isMultiValue(iObject))
      return 0;

    if (iObject instanceof Collection<?>)
      return ((Collection<Object>) iObject).size();
    if (iObject instanceof Map<?, ?>)
      return ((Map<?, Object>) iObject).size();
    if (iObject.getClass().isArray())
      return Array.getLength(iObject);
    return 0;
  }

  /**
   * Returns the first item of the Multi-value object (array, collection or map)
   * 
   * @param iObject
   *          Multi-value object (array, collection or map)
   * @return The first item if any
   */
  public static Object getFirstValue(final Object iObject) {
    if (iObject == null)
      return null;

    if (!isMultiValue(iObject) || getSize(iObject) == 0)
      return null;

    try {
      if (iObject instanceof List<?>)
        return ((List<Object>) iObject).get(0);
      else if (iObject instanceof Collection<?>)
        return ((Collection<Object>) iObject).iterator().next();
      else if (iObject instanceof Map<?, ?>)
        return ((Map<?, Object>) iObject).values().iterator().next();
      else if (iObject.getClass().isArray())
        return Array.get(iObject, 0);
    } catch (Exception e) {
      // IGNORE IT
      OLogManager.instance().debug(iObject, "Error on reading the first item of the Multi-value field '%s'", iObject);
    }

    return null;
  }

  /**
   * Returns the last item of the Multi-value object (array, collection or map)
   * 
   * @param iObject
   *          Multi-value object (array, collection or map)
   * @return The last item if any
   */
  public static Object getLastValue(final Object iObject) {
    if (iObject == null)
      return null;

    if (!isMultiValue(iObject))
      return null;

    try {
      if (iObject instanceof List<?>)
        return ((List<Object>) iObject).get(((List<Object>) iObject).size() - 1);
      else if (iObject instanceof Collection<?>) {
        Object last = null;
        for (Object o : (Collection<Object>) iObject)
          last = o;
        return last;
      } else if (iObject instanceof Map<?, ?>) {
        Object last = null;
        for (Object o : ((Map<?, Object>) iObject).values())
          last = o;
        return last;
      } else if (iObject.getClass().isArray())
        return Array.get(iObject, Array.getLength(iObject) - 1);
    } catch (Exception e) {
      // IGNORE IT
      OLogManager.instance().debug(iObject, "Error on reading the last item of the Multi-value field '%s'", iObject);
    }

    return null;
  }

  /**
   * Returns the iIndex item of the Multi-value object (array, collection or map)
   * 
   * @param iObject
   *          Multi-value object (array, collection or map)
   * @param iIndex
   *          integer as the position requested
   * @return The first item if any
   */
  public static Object getValue(final Object iObject, final int iIndex) {
    if (iObject == null)
      return null;

    if (!isMultiValue(iObject))
      return null;

    if (iIndex > getSize(iObject))
      return null;

    try {
      if (iObject instanceof List<?>)
        return ((List<?>) iObject).get(iIndex);
      else if (iObject instanceof Set<?>) {
        int i = 0;
        for (Object o : ((Set<?>) iObject)) {
          if (i++ == iIndex) {
            return o;
          }
        }
      } else if (iObject instanceof Map<?, ?>) {
        int i = 0;
        for (Object o : ((Map<?, ?>) iObject).values()) {
          if (i++ == iIndex) {
            return o;
          }
        }
      } else if (iObject.getClass().isArray())
        return Array.get(iObject, iIndex);
    } catch (Exception e) {
      // IGNORE IT
      OLogManager.instance().debug(iObject, "Error on reading the first item of the Multi-value field '%s'", iObject);
    }
    return null;
  }

  /**
   * Returns an Iterable<Object> object to browse the multi-value instance (array, collection or map)
   * 
   * @param iObject
   *          Multi-value object (array, collection or map)
   */
  public static Iterable<Object> getMultiValueIterable(final Object iObject) {
    if (iObject == null)
      return null;

    if (iObject instanceof Collection<?>)
      return ((Collection<Object>) iObject);
    if (iObject instanceof Map<?, ?>)
      return ((Map<?, Object>) iObject).values();
    if (iObject.getClass().isArray())
      return new OIterableObjectArray<Object>(iObject);

    if (iObject instanceof Iterator<?>) {
      final List<Object> temp = new ArrayList<Object>();
      for (Iterator<Object> it = (Iterator<Object>) iObject; it.hasNext();)
        temp.add(it.next());
      return temp;
    } else if (iObject instanceof Iterable<?>)
      return (Iterable<Object>) iObject;

    return null;
  }

  /**
   * Returns an Iterator<Object> object to browse the multi-value instance (array, collection or map)
   * 
   * @param iObject
   *          Multi-value object (array, collection or map)
   */

  public static Iterator<Object> getMultiValueIterator(final Object iObject) {
    if (iObject == null)
      return null;

    if (!isMultiValue(iObject))
      return null;

    if (iObject instanceof Collection<?>)
      return ((Collection<Object>) iObject).iterator();
    if (iObject instanceof Map<?, ?>)
      return ((Map<?, Object>) iObject).values().iterator();
    if (iObject.getClass().isArray())
      return new OIterableObjectArray<Object>(iObject).iterator();
    return null;
  }

  /**
   * Returns a stringified version of the multi-value object.
   * 
   * @param iObject
   *          Multi-value object (array, collection or map)
   * @return a stringified version of the multi-value object.
   */
  public static String toString(final Object iObject) {
    final StringBuilder sb = new StringBuilder();

    if (iObject instanceof Collection<?>) {
      final Collection<Object> coll = (Collection<Object>) iObject;

      sb.append('[');
      for (final Iterator<Object> it = coll.iterator(); it.hasNext();) {
        try {
          Object e = it.next();
          sb.append(e == iObject ? "(this Collection)" : e);
          if (it.hasNext())
            sb.append(", ");
        } catch (NoSuchElementException ex) {
          // IGNORE THIS
        }
      }
      return sb.append(']').toString();
    } else if (iObject instanceof Map<?, ?>) {
      final Map<String, Object> map = (Map<String, Object>) iObject;

      Entry<String, Object> e;

      sb.append('{');
      for (final Iterator<Entry<String, Object>> it = map.entrySet().iterator(); it.hasNext();) {
        try {
          e = it.next();

          sb.append(e.getKey());
          sb.append(":");
          sb.append(e.getValue() == iObject ? "(this Map)" : e.getValue());
          if (it.hasNext())
            sb.append(", ");
        } catch (NoSuchElementException ex) {
          // IGNORE THIS
        }
      }
      return sb.append('}').toString();
    }

    return iObject.toString();
  }

  /**
   * Utility function that add a value to the main object. It takes care about collections/array and single values.
   * 
   * @param iObject
   *          MultiValue where to add value(s)
   * @param iToAdd
   *          Single value, array of values or collections of values. Map are not supported.
   * @return
   */
  public static Object add(final Object iObject, final Object iToAdd) {
    if (iObject != null) {
      if (iObject instanceof Collection<?>) {
        // COLLECTION - ?
        final Collection<Object> coll = (Collection<Object>) iObject;

        if (iToAdd instanceof Collection<?>)
          // COLLECTION - COLLECTION
          coll.addAll((Collection<Object>) iToAdd);

        else if (iToAdd != null && iToAdd.getClass().isArray()) {
          // ARRAY - COLLECTION
          for (int i = 0; i < Array.getLength(iToAdd); ++i)
            coll.add(Array.get(iToAdd, i));

        } else if (iObject instanceof Map<?, ?>) {
          // MAP
          for (Entry<Object, Object> entry : ((Map<Object, Object>) iObject).entrySet())
            coll.add(entry.getValue());
        } else
          coll.add(iToAdd);

      } else if (iObject.getClass().isArray()) {
        // ARRAY - ?

        final Object[] copy;
        if (iToAdd instanceof Collection<?>) {
          // ARRAY - COLLECTION
          final int tot = Array.getLength(iObject) + ((Collection<Object>) iToAdd).size();
          copy = Arrays.copyOf((Object[]) iObject, tot);
          final Iterator<Object> it = ((Collection<Object>) iToAdd).iterator();
          for (int i = Array.getLength(iObject); i < tot; ++i)
            copy[i] = it.next();

        } else if (iToAdd != null && iToAdd.getClass().isArray()) {
          // ARRAY - ARRAY
          final int tot = Array.getLength(iObject) + Array.getLength(iToAdd);
          copy = Arrays.copyOf((Object[]) iObject, tot);
          System.arraycopy(iToAdd, 0, iObject, Array.getLength(iObject), Array.getLength(iToAdd));

        } else {
          copy = Arrays.copyOf((Object[]) iObject, Array.getLength(iObject) + 1);
          copy[copy.length - 1] = iToAdd;
        }
        return copy;

      } else
        throw new IllegalArgumentException("Object " + iObject + " is not a multi value");
    }

    return iObject;
  }
}
