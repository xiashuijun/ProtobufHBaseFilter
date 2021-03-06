package com.hubspot.hbase.filter;

import com.hubspot.hbase.filter.example.ExampleProtos;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DefaultPrimitiveTest extends BaseProtoFilterTest {
  @Test
  public void itShouldHandleSimpleDefault() throws Exception {
    ExampleProtos.ExampleProto.Builder exampleProto = ExampleProtos.ExampleProto.newBuilder()
            .setRequiredTest(3)
            ;

    assertThat(matches(filterBuilder()
            .hasFieldEqualTo("default_test", 5), exampleProto)).isTrue();
  }
}
