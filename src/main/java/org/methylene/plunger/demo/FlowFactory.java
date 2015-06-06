package org.methylene.plunger.demo;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.hotels.cascading.fields.FieldsBaseOperation;
import com.hotels.cascading.fields.FieldsSplitter;

import java.util.List;

/**
 * Simple cleanse
 */
public class FlowFactory {

  public static final Fields LINE = new Fields("line", String.class);
  public static final Fields REVENUE = new Fields("revenue", Double.TYPE);
  public static final Fields CURRENCY = new Fields("currency", String.class);

  private static final com.google.common.base.Splitter TAB_SPLITTER = com.google.common.base.Splitter.on('\t');

  final Tap revenue;
  final Tap sink;

  public FlowFactory(Tap revenue, Tap sink) {
    this.revenue = revenue;
    this.sink = sink;
  }

  public Flow createFlow(FlowConnector flowConnector) {

    Pipe revenuePipe = new Pipe("revenue");

    // the parse operation is to split the line
    FieldsSplitter<List<String>> splitter = new FieldsSplitter<>(Fields.join(REVENUE, CURRENCY),
        new Function<TupleEntry, List<String>>() {
      @Override
      public List<String> apply(TupleEntry tupleEntry) {
        return TAB_SPLITTER.splitToList(tupleEntry.getString(LINE));
      }
    });

    // default op is to return the input string with the same position
    splitter.bind(Predicates.<Fields>alwaysTrue(), new FieldsBaseOperation<List<String>>() {
      @Override
      public Object operate(TupleEntry tupleEntry, List<String> raw) {
        return raw.get(getPos());
      }
    });

    // bind a parsing rule to all float and double fields
    splitter.bind(new Predicate<Fields>() {
      @Override
      public boolean apply(Fields fields) {
        return fields.getType(0) == Double.TYPE || fields.getType(0) == Double.class
        || fields.getType(0) == Float.TYPE || fields.getType(0) == Float.class;
      }
    }, new FieldsBaseOperation<List<String>>() {
      @Override
      public Object operate(TupleEntry tupleEntry, List<String> raw) {
        String rawString = raw.get(getPos());
        if (!rawString.isEmpty() && rawString.charAt(0) == '"' && rawString.charAt(rawString.length() - 1) == '"') {
          rawString = rawString.substring(1, rawString.length() - 1).replace(",","");
        }
        return Double.parseDouble(rawString);
      }
    });

    revenuePipe = new Each(revenuePipe, splitter, Fields.RESULTS);

    // bind taps to pipes
    FlowDef flowDef = new FlowDef()
        .addSource(revenuePipe, revenue)
        .addTailSink(revenuePipe, sink);

    // connect the flow
    return flowConnector.connect(flowDef);
  }

}
