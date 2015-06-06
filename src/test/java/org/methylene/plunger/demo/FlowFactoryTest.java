package org.methylene.plunger.demo;

import cascading.flow.local.LocalFlowConnector;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.google.common.collect.ImmutableList;
import com.hotels.plunger.Bucket;
import com.hotels.plunger.DataBuilder;
import com.hotels.plunger.TupleListTap;
import org.junit.Test;

import java.util.List;

import static com.hotels.plunger.asserts.PlungerAssert.tupleEntry;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class FlowFactoryTest {

  public static final Fields LINE = new Fields("line", String.class);
  public static final Fields CURRENCY = new Fields("currency", String.class);
  public static final Fields REVENUE = new Fields("revenue", Double.TYPE);

  @Test
  public void testCreateFlow() throws Exception {

    Tap revenue = new DataBuilder(LINE)
        .addTuple("12.0\tUSD")
        .addTuple("10.0\tGBP")
        .addTuple("\"1,110.25\"\tGBP")
        .addTuple("11.5\tEUR")
        .addTuple("4.0\tEUR").toTap();

    Bucket sink = new Bucket();

    new FlowFactory(revenue, sink).createFlow(new LocalFlowConnector()).complete();

    Fields sinkFields = Fields.join(REVENUE, CURRENCY);

    List<TupleEntry> result = sink.result().asTupleEntryList();
    assertThat(result.size(), is(5));
    assertThat(result.get(0), is(tupleEntry(sinkFields, 12.0d, "USD")));
    assertThat(result.get(1), is(tupleEntry(sinkFields, 10.0d, "GBP")));
    assertThat(result.get(2), is(tupleEntry(sinkFields, 1110.25, "GBP")));
    assertThat(result.get(3), is(tupleEntry(sinkFields, 11.5d, "EUR")));
    assertThat(result.get(4), is(tupleEntry(sinkFields, 4.0d, "EUR")));

  }

}
