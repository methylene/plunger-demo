package com.hotels.cascading.fields;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;

import java.util.ArrayList;
import java.util.List;

public class FieldsSplitter<R> extends BaseOperation<Tuple> implements Function<Tuple> {

  private final com.google.common.base.Function<TupleEntry, R> parser;

  private final List<FieldsFunction<R>> fieldsFunctions;

  /**
   * Create a new splitter
   * @param declaredFields the declared fields of this operation
   * @param parser the parser that parses each incoming TupleEntry
   */
  public FieldsSplitter(Fields declaredFields, com.google.common.base.Function<TupleEntry, R> parser) {
    super(declaredFields);
    this.parser = Preconditions.checkNotNull(parser);
    this.fieldsFunctions = new ArrayList<>(declaredFields.size());
    Preconditions.checkArgument(declaredFields.isDefined());
    for (int i = 0; i < declaredFields.size(); i++) {
      fieldsFunctions.add(null);
      Preconditions.checkArgument(fieldDeclaration.get(i) instanceof String);
    }
  }

  @Override
  public void operate(FlowProcess flowProcess, FunctionCall<Tuple> functionCall) {
    Tuple tuple = functionCall.getContext();
    TupleEntry tupleEntry = functionCall.getArguments();
    R raw = parser.apply(tupleEntry);
    for (int i = 0; i < getFieldDeclaration().size(); i++) {
      tuple.set(i, fieldsFunctions.get(i).operate(tupleEntry, raw));
    }
    functionCall.getOutputCollector().add(tuple);
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<Tuple> operationCall) {
    operationCall.setContext(Tuple.size(getFieldDeclaration().size()));
    Preconditions.checkState(fieldsFunctions.size() == getFieldDeclaration().size());
    for (int i = 0; i < getFieldDeclaration().size(); i++) {
      Preconditions.checkNotNull(fieldsFunctions.get(i));
    }
  }

  /**
   * Bind an op top the fields that are specified by the predicate. All outputFields must
   * be bound to a fieldsFunction when the flow is connected. It's not an error to bind
   * a specific field more than once; the bindings will replace any existing bindings.
   * @param outputFields predicate that specifies the fields to bind to; the Fields instance
   *                     that is passed to this predicate is guaranteed to be of size 1
   * @param fieldsFunction the op to bind to all output fields specified by {@code outputFields}
   * @return this instance
   */
  public FieldsSplitter bind(Predicate<Fields> outputFields, FieldsFunction<R> fieldsFunction) {
    for (int i = 0; i < getFieldDeclaration().size(); i++) {
      Fields fields = new Fields(getFieldDeclaration().get(i), getFieldDeclaration().getType(i));
      if (outputFields.apply(fields)) {
        fieldsFunction.prepare(i, (String) getFieldDeclaration().get(i), getFieldDeclaration().getType(i));
        fieldsFunctions.set(i, fieldsFunction);
      }
    }
    return this;
  }

}
