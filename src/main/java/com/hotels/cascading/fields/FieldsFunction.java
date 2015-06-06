package com.hotels.cascading.fields;

import cascading.tuple.TupleEntry;

import java.lang.reflect.Type;

/**
 * A general map side operation, intended for binding to a single output field
 * @param <R> the type of the parsed form of the TupleEntry
 */
public interface FieldsFunction<R> {

  /**
   * This will be called once for each incoming TupleEntry
   * @param tupleEntry the incoming tuple entry
   * @param raw the "parsed" form of the tuple entry; possibly null, if no parsing is done
   * @return the output of this operation
   */
  Object operate(TupleEntry tupleEntry, R raw);

  /**
   * Prepare this op by passing the binding information
   * @param pos the position of this op in the output fields
   * @param declaredFieldName the name of the output field that this op is bound to
   * @param declaredType the type of the output field that this op is bound to
   */
  void prepare(int pos, String declaredFieldName, Type declaredType);

}
