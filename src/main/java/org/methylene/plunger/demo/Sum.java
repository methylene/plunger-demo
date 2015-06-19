package org.methylene.plunger.demo;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.google.common.base.Preconditions;

public class Sum extends BaseOperation<Tuple> implements Aggregator<Tuple> {

  private final Fields inputFields;

  public Sum(Fields declaredFields, Fields inputFields) {
    super(declaredFields);
    Preconditions.checkState(inputFields.isDefined());
    Preconditions.checkState(inputFields.size() == 1);
    Preconditions.checkState(inputFields.getType(0) == Double.TYPE);
    Preconditions.checkState(declaredFields.isDefined());
    Preconditions.checkState(declaredFields.size() == 1);
    Preconditions.checkState(declaredFields.getType(0) == Double.TYPE);
    this.inputFields = inputFields;
  }

  @Override
  public void start(FlowProcess flowProcess, AggregatorCall<Tuple> aggregatorCall) {
    Tuple context = Tuple.size(1);
    context.setDouble(0, 0d);
    aggregatorCall.setContext(context);
  }

  @Override
  public void aggregate(FlowProcess flowProcess, AggregatorCall<Tuple> aggregatorCall) {
    double incoming = aggregatorCall.getArguments().getDouble(inputFields);
    aggregatorCall.getContext().set(0, aggregatorCall.getContext().getDouble(0) + incoming);
  }

  @Override
  public void complete(FlowProcess flowProcess, AggregatorCall<Tuple> aggregatorCall) {
    aggregatorCall.getOutputCollector().add(aggregatorCall.getContext());
  }

}
