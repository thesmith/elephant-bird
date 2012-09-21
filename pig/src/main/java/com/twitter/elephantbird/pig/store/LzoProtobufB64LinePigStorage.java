package com.twitter.elephantbird.pig.store;

import java.io.IOException;

import org.apache.hadoop.mapreduce.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.mapreduce.output.LzoProtobufB64LineOutputFormat;

/**
 * Serializes Pig Tuples into Base-64 encoded, line-delimited protocol buffers.
 * The fields in the pig tuple must correspond exactly to the fields in the protobuf, as
 * no name-matching is performed.
 *
 * @param <M> Protocol Buffer Message class being serialized
 */
public class LzoProtobufB64LinePigStorage<M extends Message> extends LzoProtobufBlockPigStorage {
  private static final Logger LOG = LoggerFactory.getLogger(LzoProtobufB64LinePigStorage.class);

  public LzoProtobufB64LinePigStorage(String protoClassName) {
    super(protoClassName);
  }

  @Override
  public OutputFormat<M, ProtobufWritable<M>> getOutputFormat() throws IOException {
    if (typeRef_ == null) {
      LOG.error("Protobuf class must be specified before an OutputFormat can be created. Do not use the no-argument constructor.");
      throw new IllegalArgumentException("Protobuf class must be specified before an OutputFormat can be created. Do not use the no-argument constructor.");
    }
    return new LzoProtobufB64LineOutputFormat<M>(typeRef_);
  }
}
