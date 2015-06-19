package org.methylene.plunger.demo;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;
import cascading.pipe.joiner.LeftJoin;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * Join revenue and conversion
 */
public class FlowFactory {

  public static final Fields REVENUE = new Fields("revenue", Double.TYPE);
  public static final Fields CURRENCY = new Fields("currency", String.class);
  public static final Fields FACTOR = new Fields("factor", Double.TYPE);
  public static final Fields TMP_CURRENCY = new Fields("tmp_currency", String.class);
  public static final Fields REVENUE_USD = new Fields("revenue_usd", Double.TYPE);
  public static final Fields REVENUE_USD_SUM = new Fields("revenue_usd_sum", Double.TYPE);

  final Tap revenue;
  final Tap conversion;
  final Tap sink;

  public FlowFactory(Tap revenue, Tap conversion, Tap sink) {
    this.revenue = revenue;
    this.conversion = conversion;
    this.sink = sink;
  }

  public Flow createFlow(FlowConnector flowConnector) {

    // create pipe assembly
    Pipe revenuePipe = new Pipe("revenue");
    Pipe conversionPipe = new Pipe("conversion");
    Pipe joinPipe = new HashJoin(revenuePipe, CURRENCY, conversionPipe, CURRENCY,
        Fields.join(REVENUE, CURRENCY, TMP_CURRENCY, FACTOR), new LeftJoin());
    joinPipe = new Retain(joinPipe, Fields.join(REVENUE, CURRENCY, FACTOR));
    joinPipe = new Each(joinPipe, new Multiply(REVENUE_USD, REVENUE, FACTOR), Fields.ALL);
    joinPipe = new GroupBy(joinPipe, CURRENCY);
    joinPipe = new Every(joinPipe, new Sum(REVENUE_USD_SUM, REVENUE_USD));

    // bind taps to pipes
    FlowDef flowDef = new FlowDef()
        .addSource(revenuePipe, revenue)
        .addSource(conversionPipe, conversion)
        .addTailSink(joinPipe, sink);

    // connect the flow
    return flowConnector.connect(flowDef);
  }

}
