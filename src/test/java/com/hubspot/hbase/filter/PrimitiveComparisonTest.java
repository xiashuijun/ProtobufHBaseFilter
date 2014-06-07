package com.hubspot.hbase.filter;

import com.google.protobuf.ByteString;
import org.junit.Test;


import static com.hubspot.hbase.filter.example.ExampleProtos.ExampleProto;
import static org.fest.assertions.api.Assertions.assertThat;

public class PrimitiveComparisonTest extends BaseProtoFilterTest {

  @Test
  public void itShouldHandlePrimitiveInequality() throws Exception {
    ExampleProto.Builder exampleProto = ExampleProto.newBuilder()
            .setId(1)
            .setPortalId(2)
            .setName("name")
            .setBoolTest(true)
            .setFloatTest(1.2f)
            .setBytesTest(ByteString.copyFromUtf8("testutf8"))
            .setRequiredTest(3)
            .setDoubleTest(1.5)
            ;

    // Positive
    assertThat(matches(filterBuilder()
            .hasFieldNotEqualTo("id", 0L), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldNotEqualTo("portal_id", 0), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldNotEqualTo("name", ""), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldNotEqualTo("bool_test", false), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldNotEqualTo("float_test", 0f), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldNotEqualTo("double_test", 0d), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldNotEqualTo("required_test", 0), exampleProto)).isTrue();

    // Negative
    assertThat(matches(filterBuilder()
            .hasFieldNotEqualTo("id", 1L), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldNotEqualTo("portal_id", 2), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldNotEqualTo("name", "name"), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldNotEqualTo("bool_test", true), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldNotEqualTo("float_test", 1.2f), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldNotEqualTo("double_test", 2d), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldNotEqualTo("required_test", 3), exampleProto)).isFalse();
  }

  @Test
  public void itShouldHandlePrimitiveEquality() throws Exception {
    ExampleProto.Builder exampleProto = ExampleProto.newBuilder()
            .setId(1)
            .setPortalId(2)
            .setName("name")
            .setBoolTest(true)
            .setFloatTest(1.2f)
            .setBytesTest(ByteString.copyFromUtf8("testutf8"))
            .setRequiredTest(3)
            .setDoubleTest(1.5)
            ;

    // Positive
    assertThat(matches(filterBuilder()
            .hasFieldEqualTo("id", 1L), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldEqualTo("portal_id", 2), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldEqualTo("name", "name"), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldEqualTo("bool_test", true), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldEqualTo("float_test", 1.2f), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldEqualTo("double_test", 1.5), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldEqualTo("required_test", 3), exampleProto)).isTrue();
    
    // Negative
    assertThat(matches(filterBuilder()
            .hasFieldEqualTo("id", 0L), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldEqualTo("portal_id", 0), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldEqualTo("name", ""), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldEqualTo("bool_test", false), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldEqualTo("float_test", 0f), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldEqualTo("double_test", 0d), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldEqualTo("required_test", 0), exampleProto)).isFalse();
  }

  @Test
  public void itShouldHandlePrimitiveLessThan() throws Exception {
    ExampleProto.Builder exampleProto = ExampleProto.newBuilder()
            .setId(1)
            .setPortalId(2)
            .setName("name")
            .setBoolTest(true)
            .setFloatTest(1.2f)
            .setBytesTest(ByteString.copyFromUtf8("testutf8"))
            .setRequiredTest(3)
            .setDoubleTest(1.5)
            ;

    // Negative tests
    assertThat(matches(filterBuilder()
            .hasFieldLessThan("id", 0L), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldLessThan("portal_id", 0), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldLessThan("name", ""), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldLessThan("bool_test", false), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldLessThan("float_test", 0f), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldLessThan("double_test", 0d), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldLessThan("required_test", 0), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldLessThan("id", 1L), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldLessThan("portal_id", 2), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldLessThan("name", "name"), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldLessThan("double_test", 1.4), exampleProto)).isFalse();

    // Positive tests

    assertThat(matches(filterBuilder()
            .hasFieldLessThan("id", 2L), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldLessThan("portal_id", 3), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldLessThan("name", "zzz"), exampleProto)).isTrue();

  }

  @Test
  public void itShouldHandlePrimitiveLessThanEqual() throws Exception {
    ExampleProto.Builder exampleProto = ExampleProto.newBuilder()
            .setId(1)
            .setPortalId(2)
            .setName("name")
            .setBoolTest(true)
            .setFloatTest(1.2f)
            .setBytesTest(ByteString.copyFromUtf8("testutf8"))
            .setRequiredTest(3)
            .setDoubleTest(1.5)
            ;

    // Negative tests
    assertThat(matches(filterBuilder()
            .hasFieldLessThanOrEqualTo("id", 0L), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldLessThanOrEqualTo("portal_id", 0), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldLessThanOrEqualTo("name", ""), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldLessThanOrEqualTo("bool_test", false), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldLessThanOrEqualTo("float_test", 0f), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldLessThanOrEqualTo("double_test", 0d), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldLessThanOrEqualTo("required_test", 0), exampleProto)).isFalse();

    // Positive tests
    
    assertThat(matches(filterBuilder()
            .hasFieldLessThanOrEqualTo("id", 1L), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldLessThanOrEqualTo("portal_id", 2), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldLessThanOrEqualTo("name", "name"), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldLessThanOrEqualTo("double_test", 1.5), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldLessThanOrEqualTo("double_test", 2d), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldLessThanOrEqualTo("id", 2L), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldLessThanOrEqualTo("portal_id", 3), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldLessThanOrEqualTo("name", "zzz"), exampleProto)).isTrue();

  }

  @Test
  public void itShouldHandlePrimitiveGreaterThan() throws Exception {
    ExampleProto.Builder exampleProto = ExampleProto.newBuilder()
            .setId(1)
            .setPortalId(2)
            .setName("name")
            .setBoolTest(true)
            .setFloatTest(1.2f)
            .setBytesTest(ByteString.copyFromUtf8("testutf8"))
            .setRequiredTest(3)
            .setDoubleTest(1.4)
            ;

    // Negative tests
    assertThat(matches(filterBuilder()
            .hasFieldGreaterThan("id", 2L), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldGreaterThan("portal_id", 3), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldGreaterThan("name", "zzzz"), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldGreaterThan("float_test", 2.1f), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldGreaterThan("double_test", 2.5d), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldGreaterThan("required_test", 5), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldGreaterThan("id", 1L), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldGreaterThan("portal_id", 2), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldGreaterThan("name", "name"), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldGreaterThan("double_test", 1.4), exampleProto)).isFalse();

    // Positive tests

    assertThat(matches(filterBuilder()
            .hasFieldGreaterThan("id", 0L), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldGreaterThan("portal_id", 0), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldGreaterThan("name", ""), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldGreaterThan("float_test", 0f), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldGreaterThan("double_test", 0d), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldGreaterThan("required_test", 0), exampleProto)).isTrue();
 }


  @Test
  public void itShouldHandlePrimitiveGreaterThanEqual() throws Exception {
    ExampleProto.Builder exampleProto = ExampleProto.newBuilder()
            .setId(1)
            .setPortalId(2)
            .setName("name")
            .setBoolTest(true)
            .setFloatTest(1.2f)
            .setBytesTest(ByteString.copyFromUtf8("testutf8"))
            .setRequiredTest(3)
            .setDoubleTest(1.4)
            ;

    // Negative tests
    assertThat(matches(filterBuilder()
            .hasFieldGreaterThanOrEqualTo("id", 2L), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldGreaterThanOrEqualTo("portal_id", 3), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldGreaterThanOrEqualTo("name", "zzzz"), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldGreaterThanOrEqualTo("float_test", 2.1f), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldGreaterThanOrEqualTo("double_test", 2.5d), exampleProto)).isFalse();

    assertThat(matches(filterBuilder()
            .hasFieldGreaterThanOrEqualTo("required_test", 5), exampleProto)).isFalse();

    // Positive tests

    assertThat(matches(filterBuilder()
            .hasFieldGreaterThanOrEqualTo("id", 1L), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldGreaterThanOrEqualTo("portal_id", 2), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldGreaterThanOrEqualTo("name", "name"), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldGreaterThanOrEqualTo("double_test", 1.4), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldGreaterThanOrEqualTo("id", 0L), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldGreaterThanOrEqualTo("portal_id", 0), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldGreaterThanOrEqualTo("name", ""), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldGreaterThanOrEqualTo("float_test", 0f), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldGreaterThanOrEqualTo("double_test", 0d), exampleProto)).isTrue();

    assertThat(matches(filterBuilder()
            .hasFieldGreaterThanOrEqualTo("required_test", 0), exampleProto)).isTrue();
  }
}