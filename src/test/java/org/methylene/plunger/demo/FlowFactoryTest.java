package org.methylene.plunger.demo;

import cascading.flow.local.LocalFlowConnector;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.google.common.collect.ImmutableList;
import com.hotels.plunger.Bucket;
import com.hotels.plunger.TupleListTap;
import org.junit.Test;

import java.util.List;

import static com.hotels.plunger.asserts.PlungerAssert.tupleEntry;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class FlowFactoryTest {

  public static final Fields CURRENCY = new Fields("currency", String.class);
  public static final Fields FACTOR = new Fields("factor", Double.TYPE);
  public static final Fields REVENUE = new Fields("revenue", Double.TYPE);

  @Test
  public void testCreateFlow() throws Exception {

    Fields revenueFields = Fields.join(REVENUE, CURRENCY);
    TupleListTap revenue = new TupleListTap(revenueFields, ImmutableList.<Tuple>builder()
        .add(new Tuple(12.0d, "USD"))
        .add(new Tuple(10.0d, "GBP"))
        .add(new Tuple(11.99d, "EUR"))
        .add(new Tuple(4.0d, "EUR"))
        .build());
//    TupleListTap revenue = new DataBuilder(revenueFields)
//        .addTuple(12.0d, "USD")
//        .addTuple(10.0d, "GBP")
//        .addTuple(11.99d, "EUR")
//        .addTuple(4.0d, "EUR").toTap();

    Fields conversionFields = Fields.join(CURRENCY, FACTOR);
    TupleListTap conversion = new TupleListTap(conversionFields, ImmutableList.<Tuple>builder()
        .add(new Tuple("USD", 1.0d))
        .add(new Tuple("GBP", 1.53d))
        .add(new Tuple("EUR", 1.09d))
        .build());
//    TupleListTap conversion = new DataBuilder(conversionFields)
//        .addTuple("USD", 1.0d)
//        .addTuple("GBP", 1.53d)
//        .addTuple("EUR", 1.09d).toTap();

    Bucket sink = new Bucket();

    new FlowFactory(revenue, conversion, sink).createFlow(new LocalFlowConnector()).complete();

    Fields sinkFields = Fields.join(REVENUE, CURRENCY, FACTOR);

    List<TupleEntry> result = sink.result().asTupleEntryList();
    assertThat(result.size(), is(4));
    assertThat(result.get(0), is(tupleEntry(sinkFields, 12.0d, "USD", 1.0d)));
    assertThat(result.get(1), is(tupleEntry(sinkFields, 10.0d, "GBP", 1.53d)));
    assertThat(result.get(2), is(tupleEntry(sinkFields, 11.99d, "EUR", 1.09d)));
    assertThat(result.get(3), is(tupleEntry(sinkFields, 4.0d, "EUR", 1.09d)));

  }

}
