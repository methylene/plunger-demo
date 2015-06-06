package com.hotels.cascading.fields;

import java.io.Serializable;
import java.lang.reflect.Type;

public abstract class FieldsBaseOperation<R> implements FieldsFunction<R>, Serializable {

  private int pos;
  private String declaredFieldName;
  private Type declaredType;

  public void prepare(int pos, String declaredFieldName, Type declaredType) {
    this.pos = pos;
    this.declaredFieldName = declaredFieldName;
    this.declaredType = declaredType;
  }

  protected int getPos() {
    return pos;
  }

  protected String getDeclaredFieldName() {
    return declaredFieldName;
  }

  protected Type getDeclaredType() {
    return declaredType;
  }

}
