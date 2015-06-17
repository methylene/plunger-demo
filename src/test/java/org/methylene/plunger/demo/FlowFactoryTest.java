package org.methylene.plunger.demo;

import cascading.flow.local.LocalFlowConnector;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.hotels.plunger.Bucket;
import com.hotels.plunger.DataBuilder;
import org.junit.Test;

import java.util.List;

import static com.hotels.plunger.asserts.PlungerAssert.tupleEntry;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.number.IsCloseTo.closeTo;
import static org.junit.Assert.assertThat;

public class FlowFactoryTest {

  public static final Fields CURRENCY = new Fields("currency", String.class);
  public static final Fields FACTOR = new Fields("factor", Double.TYPE);
  public static final Fields REVENUE = new Fields("revenue", Double.TYPE);
  public static final Fields REVENUE_USD_SUM = new Fields("revenue_usd_sum", Double.TYPE);

  @Test
  public void testCreateFlow() throws Exception {

    Fields revenueFields = Fields.join(REVENUE, CURRENCY);
    Tap revenue = new DataBuilder(revenueFields)
        .addTuple(12.0d, "USD")
        .addTuple(10.0d, "GBP")
        .addTuple(11.99d, "EUR")
        .addTuple(4.0d, "EUR").toTap();

    Fields conversionFields = Fields.join(CURRENCY, FACTOR);
    Tap conversion = new DataBuilder(conversionFields)
        .addTuple("USD", 1.0d)
        .addTuple("GBP", 1.53d)
        .addTuple("EUR", 1.09d).toTap();

    Bucket sink = new Bucket();

    new FlowFactory(revenue, conversion, sink).createFlow(new LocalFlowConnector()).complete();

    List<TupleEntry> result = sink.result().asTupleEntryList();
    assertThat(result.size(), is(3));
    ImmutableMap.Builder<String, Double> builder = ImmutableMap.builder();
    for (TupleEntry entry: result) {
      builder.put(entry.getString(CURRENCY), entry.getDouble(REVENUE_USD_SUM));
    }
    ImmutableMap<String, Double> revenuePerCurrency = builder.build();
    assertThat(revenuePerCurrency.get("USD"), closeTo(12.0d * 1.0d, 0.001d));
    assertThat(revenuePerCurrency.get("GBP"), closeTo(10.0d * 1.53d, 0.001d));
    assertThat(revenuePerCurrency.get("EUR"), closeTo((11.99d + 4.0d) * 1.09d, 0.001d));

  }

}
