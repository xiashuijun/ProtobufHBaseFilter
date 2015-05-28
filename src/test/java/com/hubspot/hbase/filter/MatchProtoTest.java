package com.hubspot.hbase.filter;

import org.junit.Test;

import static com.hubspot.hbase.filter.example.ExampleProtos.ExampleProto;
import static org.assertj.core.api.Assertions.assertThat;

public class MatchProtoTest extends BaseProtoFilterTest {
  @Test
  public void itShouldMatchEquality() throws Exception {
    ExampleProto.Builder exampleProto = ExampleProto.newBuilder()
            .setRequiredTest(1)
            .setName("contains test contains")
            ;

    assertThat(matches(filterBuilder()
            .isEqualTo(exampleProto.build()),
            exampleProto)).isTrue();


    assertThat(matches(filterBuilder()
                    .isEqualTo(exampleProto.build()),
            exampleProto
                    .setFloatTest(1.3f))).isFalse();
  }

  @Test
  public void itShouldMatchAllFields() throws Exception {
    ExampleProto.Builder exampleProto = ExampleProto.newBuilder()
            .setRequiredTest(1)
            .setName("contains test contains")
            ;

    assertThat(matches(filterBuilder()
                    .containingFieldsMatched(exampleProto.build()),
            exampleProto.clone()
                    .setFloatTest(1.3f))).isTrue();

    ExampleProto.Builder operand = exampleProto.clone()
            .setFloatTest(1.3f);

    assertThat(matches(filterBuilder()
                    .containingFieldsMatched(operand
                            .build()),
            exampleProto)).isFalse();
  }


  @Test
  public void itShouldMatchAnyFields() throws Exception {
    ExampleProto.Builder exampleProto = ExampleProto.newBuilder()
            .setRequiredTest(1)
            .setName("contains test contains")
            ;

    assertThat(matches(filterBuilder()
                    .anyFieldsMatched(
                            ExampleProto.newBuilder()
                                    .setRequiredTest(4)
                                    .setName("contains test contains")
                                    .build()
                    ),
            exampleProto.clone()
                    .setFloatTest(1.3f))).isTrue();


    assertThat(matches(filterBuilder()
                    .anyFieldsMatched(
                            ExampleProto.newBuilder()
                                    .setRequiredTest(4)
                                    .setName("__")
                                    .build()
                    ),
            exampleProto.clone()
                    .setFloatTest(1.3f))).isFalse();
  }

  @Test
  public void itShouldMatchNoFields() throws Exception {
    ExampleProto.Builder exampleProto = ExampleProto.newBuilder()
            .setRequiredTest(1)
            .setName("contains test contains")
            ;

    assertThat(matches(filterBuilder()
                    .noFieldsMatched(
                            ExampleProto.newBuilder()
                                    .setRequiredTest(4)
                                    .setName("contains test contains")
                                    .build()
                    ),
            exampleProto.clone()
                    .setFloatTest(1.3f))).isFalse();


    assertThat(matches(filterBuilder()
                    .noFieldsMatched(
                            ExampleProto.newBuilder()
                                    .setRequiredTest(4)
                                    .setName("__")
                                    .build()
                    ),
            exampleProto.clone()
                    .setFloatTest(1.3f))).isTrue();
  }
}
