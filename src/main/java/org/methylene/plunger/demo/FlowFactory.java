package org.methylene.plunger.demo;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
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
  final Tap revenue;
  final Tap conversion;
  final Tap sink;

  public FlowFactory(Tap revenue, Tap conversion, Tap sink) {
    this.revenue = revenue;
    this.conversion = conversion;
    this.sink = sink;
  }

  public Flow createFlow(FlowConnector flowConnector) {
    Pipe revenuePipe = new Pipe("revenue");
    Pipe conversionPipe = new Pipe("conversion");
    Pipe sinkPipe = new HashJoin(revenuePipe, CURRENCY, conversionPipe, CURRENCY,
        Fields.join(REVENUE, CURRENCY, new Fields("tmp_currency"), FACTOR), new LeftJoin());
    sinkPipe = new Retain(sinkPipe, Fields.join(REVENUE, CURRENCY, FACTOR));
    FlowDef flowDef = new FlowDef()
        .addSource(revenuePipe, this.revenue)
        .addSource(conversionPipe, conversion)
        .addTailSink(sinkPipe, sink);
    return flowConnector.connect(flowDef);
  }

}
