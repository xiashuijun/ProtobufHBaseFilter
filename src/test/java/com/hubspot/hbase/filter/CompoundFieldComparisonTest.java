package com.hubspot.hbase.filter;

import com.hubspot.hbase.filter.example.ExampleProtos;
import org.junit.Test;

import static com.hubspot.hbase.filter.example.ExampleProtos.ChildProto;
import static com.hubspot.hbase.filter.example.ExampleProtos.ExampleProto.TestEnum;
import static org.fest.assertions.api.Assertions.assertThat;

public class CompoundFieldComparisonTest extends BaseProtoFilterTest {
  @Test
  public void itShouldMatchEnums() throws Exception {
    ExampleProtos.ExampleProto.Builder exampleProto = ExampleProtos.ExampleProto.newBuilder()
            .setRequiredTest(1)
            .setTestEnum(TestEnum.APPLE)
            ;

    assertThat(matches(filterBuilder()
            .hasFieldEqualTo("test_enum", TestEnum.APPLE), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldNotEqualTo("test_enum", TestEnum.ORANGE), exampleProto)).isTrue();
  }

  @Test
  public void itShouldMatchChildValues() throws Exception {
    ExampleProtos.ExampleProto.Builder exampleProto = ExampleProtos.ExampleProto.newBuilder()
            .setRequiredTest(1)
            .setTestEnum(TestEnum.APPLE)
            .setChildProto(ChildProto.newBuilder()
                    .setChildProtoVal(5))
            ;

    assertThat(matches(filterBuilder()
            .hasFieldEqualTo("child_proto.child_proto_val", 5), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldGreaterThan("child_proto.child_proto_val", 4), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldNotEqualTo("child_proto.child_proto_val", 4), exampleProto)).isTrue();


  }
}
