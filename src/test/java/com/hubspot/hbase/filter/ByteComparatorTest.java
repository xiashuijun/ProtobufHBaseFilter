package com.hubspot.hbase.filter;

import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.junit.Test;

import static com.hubspot.hbase.filter.example.ExampleProtos.ExampleProto;
import static org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import static org.fest.assertions.api.Assertions.assertThat;

public class ByteComparatorTest extends BaseProtoFilterTest {
  @Test
  public void itShouldSubStringMatch() throws Exception {
    ExampleProto.Builder exampleProto = ExampleProto.newBuilder()
            .setRequiredTest(1)
            .setName("contains test contains")
            ;

    assertThat(matches(filterBuilder()
            .hasFieldByteComparableTo("name", new SubstringComparator("test"), CompareOp.EQUAL)
            , exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldByteComparableTo("name", new SubstringComparator("test"), CompareOp.NOT_EQUAL)
            , exampleProto)).isFalse();
  }

  @Test
  public void itShouldNotSubstringMatch() throws Exception {
    ExampleProto.Builder exampleProto = ExampleProto.newBuilder()
            .setRequiredTest(1)
            .setName("contains test contains")
            ;

    assertThat(matches(filterBuilder()
            .hasFieldByteComparableTo("name", new SubstringComparator("not"), CompareOp.EQUAL)
            , exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldByteComparableTo("name", new SubstringComparator("not"), CompareOp.NOT_EQUAL)
            , exampleProto)).isTrue();
  }
}
