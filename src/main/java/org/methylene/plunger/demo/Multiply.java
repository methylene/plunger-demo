package org.methylene.plunger.demo;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.google.common.base.Preconditions;

public class Multiply extends BaseOperation<Tuple> implements Function<Tuple> {

  private final Fields input;

  /**
   * Take the product of the input fields.
   * @param output this is the declared output, it must be a Fields instance of size 1 and of type double
   * @param input the input to the product, all of these fields must be of type double
   */
  public Multiply(Fields output, Fields... input) {
    super(output);
    this.input = Fields.join(input);
    for (int i = 0; i < this.input.size(); i++) {
      Preconditions.checkState(this.input.getType(i) == Double.TYPE || this.input.getType(i) == Double.class);
    }
    Preconditions.checkState(output.isDefined());
    Preconditions.checkState(output.size() == 1);
    Preconditions.checkState(output.getType(0) == Double.TYPE || output.getType(0) == Double.class);
  }

  @Override
  public void operate(FlowProcess flowProcess, FunctionCall<Tuple> functionCall) {
    double p = 1d;
    for (int i = 0; i < input.size(); i++) {
      p *= functionCall.getArguments().getDouble(input.get(i));
    }
    functionCall.getContext().setDouble(0, p);
    functionCall.getOutputCollector().add(functionCall.getContext());
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<Tuple> operationCall) {
    operationCall.setContext(Tuple.size(1));
  }

}
