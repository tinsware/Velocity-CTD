/*
 * Copyright (C) 2024 Velocity Contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.velocitypowered.proxy.queue.cache;

import java.util.UUID;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

/**
 * Creates a new codec for MongoDB to serialize a {@link SerializableQueueEntry}.
 */
public class SerializableQueueEntryCodec implements Codec<SerializableQueueEntry> {
  @Override
  public SerializableQueueEntry decode(BsonReader reader, DecoderContext decoderContext) {
    reader.readStartDocument();

    UUID uuid = null;
    int connectionAttempts = 0;
    boolean waitingForConnection = false;
    int priority = 0;
    boolean fullBypass = false;
    boolean queueBypass = false;

    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      String fieldName = reader.readName();
      switch (fieldName) {
        case "uuid" -> {
          uuid = UUID.fromString(reader.readString());
        }
        case "connection-attempts" -> {
          connectionAttempts = reader.readInt32();
        }
        case "waiting-for-connection" -> {
          waitingForConnection = reader.readBoolean();
        }
        case "priority" -> {
          priority = reader.readInt32();
        }
        case "full-bypass" -> {
          fullBypass = reader.readBoolean();
        }
        case "queue-bypass" -> {
          queueBypass = reader.readBoolean();
        }
        default -> {

        }
      }
    }

    return new SerializableQueueEntry(uuid, connectionAttempts, waitingForConnection, priority, fullBypass, queueBypass);
  }

  @Override
  public void encode(BsonWriter writer, SerializableQueueEntry value, EncoderContext encoderContext) {
    if (value != null) {
      writer.writeStartDocument();

      writer.writeName("uuid");
      writer.writeString(value.getUuid().toString());

      writer.writeName("connection-attempts");
      writer.writeInt32(value.getConnectionAttempts());

      writer.writeName("waiting-for-connection");
      writer.writeBoolean(value.isWaitingForConnection());

      writer.writeName("priority");
      writer.writeInt32(value.getPriority());

      writer.writeName("full-bypass");
      writer.writeBoolean(value.isFullBypass());

      writer.writeName("queue-bypass");
      writer.writeBoolean(value.isQueueBypass());

      writer.writeEndDocument();
    }
  }

  @Override
  public Class<SerializableQueueEntry> getEncoderClass() {
    return SerializableQueueEntry.class;
  }
}
