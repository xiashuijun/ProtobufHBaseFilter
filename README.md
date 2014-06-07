# ProtobufFilter

An HBase filter for filtering on protobuf messages.

## Quick Synopsis

```java
/* Interrogate the protobuf to get the field named 'portal_id' and compare.  */
Filter myFilter = ProtobufFilter.forColumn(toBytes("family"), toBytes("qualifier"), ExampleProto.class)
            .filterIfMissing(true)
            .hasFieldEqualTo("portal_id", 5);
    
/* For every field defined in the ExampleProto provided, check equality. */
Filter otherFilter = ProtobufFilter.forColumn(toBytes("family"), toBytes("qualifier"), ExampleProto.class)
            .filterIfMissing(true)
            .containingFieldsMatched(ExampleProto.newBuilder()
                    .setPortalId(5)
                    .build());

/* Do greater than check on a field in a child protobuf. */
Filter newFilter = ProtobufFilter.forColumn(toBytes("family"), toBytes("qualifier"), ExampleProto.class)
            .filterIfMissing(true)
            .hasFieldGreaterThan("child_proto.child_proto_val", 5);

/* This check will exclude columns that have fields defined other than portalId */
Filter newFilter = ProtobufFilter.forColumn(toBytes("family"), toBytes("qualifier"), ExampleProto.class)
            .filterIfMissing(true)
            .isEqualTo(ExampleProto.newBuilder()
                    .setPortalId(5)
                    .build())
                    
/* This will perform a bytearraycomparable check on field */
Filter newFilter = ProtobufFilter.forColumn(toBytes("family"), toBytes("qualifier"), ExampleProto.class)
            .filterIfMissing(true)
            .hasFieldByteComparableTo("name", new RegexStringComparator(REGEX), CompareOp.EQUAL);

/* This will check that any field matches */
Filter newFilter = ProtobufFilter.forColumn(toBytes("family"), toBytes("qualifier"), ExampleProto.class)
            .filterIfMissing(true)
            .anyFieldsMatched(ExampleProto.newBuilder()
                    .setPortalId(5)
                    .setName("Test")
                    .build())

```

## Lists of filters

Since the filters have to ship the descriptors, it can be expensive to ship multiple filters with the same
descriptor. To alleviate this issue, there is a filter list, `ProtoFilterList`, which accumulates descriptors.
For example:

```java

Filter myFilter = ProtoFilterList.or(
                 ProtobufFilter.forColumn(toBytes("family"), toBytes("qualifier"), ExampleProto.class)
                               .filterIfMissing(true)
                               .hasFieldEqualTo("portal_id", 5),
                 ProtobufFilter.forColumn(toBytes("family"), toBytes("qualifier"), ExampleProto.class)
                               .filterIfMissing(true)
                               .hasFieldEqualTo("portal_id", 6)
);
```                                              

## How to use

### Create descriptor files

For all the magic to work, you need to compile descriptor files and name them
something `*.desc` and put them in the root of your `resources` directory.
For example:

```bash
protoc --java_out=../java --descriptor_set_out=../resources/example.desc --proto_path=. example.proto protobuf_filter.proto
```

### Install the jar on hbase

To be continued...

### Use `ProtobufFilter.forColumn`

The `forColumn` static method returns a builder which allows you to
fluently express how to filter, either deciding to look for a field
to filter by, or just comparing the protobuf with another protobuf.


## Known Issues

- Repeated field interrogation hasn't been spec'd out yet.
- Need clearer handling of cleared values
- Improve pruning of field descriptors to include protobufs which have dependencies
