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

package com.velocitypowered.proxy.redis;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import com.velocitypowered.proxy.VelocityServer;
import com.velocitypowered.proxy.queue.ServerStatus;
import com.velocitypowered.proxy.queue.cache.SerializableQueue;
import com.velocitypowered.proxy.queue.cache.SerializableQueueEntry;
import com.velocitypowered.proxy.queue.cache.SerializableQueueEntryCodec;
import com.velocitypowered.proxy.redis.multiproxy.RemotePlayerInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;

/**
 * The MongoDB handler of the Redis system. Stores information about players online and queues.
 */
public class DatabaseManager {
  private MongoCollection<Document> playerCollection;
  private MongoCollection<Document> queueCollection;

  private static final int THREAD_COUNT = Math.max(1, Runtime.getRuntime().availableProcessors() / 8);
  private static final ScheduledExecutorService SERVICE = Executors.newScheduledThreadPool(THREAD_COUNT);

  /**
   * Initializes a new MongoDB database manager.
   *
   * @param proxy Instance of the proxy.
   */
  public DatabaseManager(VelocityServer proxy) {
    try {
      CodecRegistry registry = CodecRegistries.fromCodecs(new SerializableQueueEntryCodec());
      MongoClientSettings settings = MongoClientSettings.builder()
          .applyConnectionString(new ConnectionString(proxy.getConfiguration().getRedis().getMongodbConnectionString()))
          .applyToConnectionPoolSettings(builder ->
              builder.maxSize(500).minSize(10).maxConnectionIdleTime(1, TimeUnit.MINUTES))
          .uuidRepresentation(UuidRepresentation.STANDARD)
          .codecRegistry(CodecRegistries.fromRegistries(registry, MongoClientSettings.getDefaultCodecRegistry())).build();

      MongoClient client = MongoClients.create(settings);
      MongoDatabase database = client.getDatabase("velocity");
      playerCollection = database.getCollection("player-data");

      try {
        playerCollection.createIndex(Indexes.ascending("name"));
      } catch (Exception ignored) {
        // checkstyle doesn't like empty catch blocks, so, you just wasted your time reading this comment.
      }
      queueCollection = database.getCollection("queue-data");
    } catch (Exception e) {
      System.out.println("Something went wrong trying to connect to MongoDB... Please check your connection string!");
    }
  }

  /**
   * Adds or updates all player entries in MongoDB.
   *
   * @param players The players to add or update.
   */
  public void addOrUpdatePlayers(List<RemotePlayerInfo> players) {
    CompletableFuture.runAsync(() -> {
      List<UpdateOneModel<Document>> bulkOperations = new ArrayList<>();
      UpdateOptions options = new UpdateOptions().upsert(true);

      for (RemotePlayerInfo info : players) {
        Document filter = new Document("_id", info.getUuid().toString());

        Document setDoc = new Document()
            .append("name", info.getName())
            .append("proxy-id", info.getProxyId())
            .append("queue-priority", info.getQueuePriority())
            .append("being-transferred", info.isBeingTransferred())
            .append("full-queue-bypass", info.isFullQueueBypass())
            .append("queue-bypass", info.isQueueBypass());

        if (info.getServerName() != null) {
          setDoc.append("server-name", info.getServerName());
        }
        Document update = new Document("$set", setDoc);

        bulkOperations.add(new UpdateOneModel<>(filter, update, options));
      }

      if (!bulkOperations.isEmpty()) {
        playerCollection.bulkWrite(bulkOperations);
      }
    }, SERVICE);

  }

  /**
   * Remove a list of players from the database.
   *
   * @param players The list of players to remove.
   */
  public void removePlayers(List<RemotePlayerInfo> players) {
    CompletableFuture.runAsync(() -> {
      List<DeleteOneModel<Document>> bulkOperations = new ArrayList<>();

      for (RemotePlayerInfo info : players) {
        Document filter = new Document("_id", info.getUuid().toString());

        bulkOperations.add(new DeleteOneModel<>(filter));
      }

      if (!bulkOperations.isEmpty()) {
        playerCollection.bulkWrite(bulkOperations);
      }
    }, SERVICE);

  }

  /**
   * Get all players from the database.
   *
   * @return All the players.
   */
  public List<RemotePlayerInfo> getPlayers() {
    List<RemotePlayerInfo> players = new ArrayList<>();

    try (MongoCursor<Document> cursor = playerCollection.find().iterator()) {
      while (cursor.hasNext()) {
        Document doc = cursor.next();
        players.add(new RemotePlayerInfo(doc.getString("proxy-id"),
            UUID.fromString(doc.getString("_id")),
            doc.getString("name"),
            doc.get("queue-priority", Map.class),
            doc.getBoolean("full-queue-bypass"),
            doc.getBoolean("queue-bypass")));
      }
    }

    return players;
  }

  /**
   * Get the amount of players cached.
   *
   * @return The amount of players cached.
   */
  public long getCount() {
    return playerCollection.countDocuments();
  }

  /**
   * Search for a player based on UUID.
   *
   * @param uuid The UUID of the player.
   *
   * @return The found player info, or null.
   */
  public CompletableFuture<RemotePlayerInfo> getPlayer(UUID uuid) {
    Document doc = playerCollection.find(new Document("_id", uuid.toString())).first();
    if (doc == null) {
      return null;
    }

    return CompletableFuture.supplyAsync(() -> new RemotePlayerInfo(doc.getString("proxy-id"),
        UUID.fromString(doc.getString("_id")),
        doc.getString("name"),
        doc.get("queue-priority", Map.class),
        doc.getBoolean("full-queue-bypass"),
        doc.getBoolean("queue-bypass")), SERVICE);
  }

  /**
   * Search for a player based on UUID.
   *
   * @param name The username of the player.
   *
   * @return The found player info, or null.
   */
  public CompletableFuture<RemotePlayerInfo> getPlayer(String name) {
    Document doc = playerCollection.find(new Document("name", name)).first();
    if (doc == null) {
      return null;
    }

    return CompletableFuture.supplyAsync(() -> new RemotePlayerInfo(doc.getString("proxy-id"),
        UUID.fromString(doc.getString("_id")),
        doc.getString("name"),
        doc.get("queue-priority", Map.class),
        doc.getBoolean("full-queue-bypass"),
        doc.getBoolean("queue-bypass")), SERVICE);
  }

  /**
   * Add or update a queue in the database.
   *
   * @param queue The serialized queue to add
   */
  public void addOrUpdateQueue(final SerializableQueue queue) {
    CompletableFuture.runAsync(() -> {
      Document filter = new Document("server-name", queue.getServerName());
      Document update = new Document("$set", new Document()
          .append("queue", queue.getQueue()) // Assuming `SerializableQueueEntry` is serializable
          .append("online", queue.getOnline().name()) // Assuming ServerStatus is an Enum
          .append("full", queue.isFull())
          .append("paused", queue.isPaused()));

      queueCollection.updateOne(filter, update, new UpdateOptions().upsert(true));
    }, SERVICE);

  }

  /**
   * Add or update an entry in the queue in the database.
   *
   * @param serverName The name of the server that belongs to the queue.
   * @param entry The serialized player entry to add or update.
   */
  public void addOrUpdateQueueEntry(String serverName, SerializableQueueEntry entry) {
    CompletableFuture.runAsync(() -> {
      Document filter = new Document("server-name", serverName)
          .append("queue.uuid", entry.getUuid().toString()); // Check if entry exists

      Document update = new Document("$set", new Document()
          .append("queue.$.connection-attempts", entry.getConnectionAttempts())
          .append("queue.$.waiting-for-connection", entry.isWaitingForConnection())
          .append("queue.$.priority", entry.getPriority())
          .append("queue.$.full-bypass", entry.isFullBypass())
          .append("queue.$.queue-bypass", entry.isQueueBypass()));

      UpdateResult result = queueCollection.updateOne(filter, update);

      // If no document was modified, the entry doesn't exist, so we add it
      if (result.getModifiedCount() == 0) {
        Document newEntry = new Document("uuid", entry.getUuid().toString())
            .append("connection-attempts", entry.getConnectionAttempts())
            .append("waiting-for-connection", entry.isWaitingForConnection())
            .append("priority", entry.getPriority())
            .append("full-bypass", entry.isFullBypass())
            .append("queue-bypass", entry.isQueueBypass());

        Document pushUpdate = new Document("$push", new Document("queue", newEntry));

        queueCollection.updateOne(new Document("server-name", serverName), pushUpdate, new UpdateOptions().upsert(true));
      }
    }, SERVICE);
  }

  /**
   * Get a queue based on server name.
   *
   * @param serverName The name of the server.
   *
   * @return The serialized queue.
   */
  public SerializableQueue getQueue(final String serverName) {
    Document doc = queueCollection.find(Filters.eq("server-name", serverName)).first();
    if (doc == null) {
      return null; // Return null if no queue found
    }

    return parseDocumentToServerQueue(doc);
  }

  private SerializableQueue parseDocumentToServerQueue(Document doc) {
    List<Document> docs = doc.getList("queue", Document.class);
    ConcurrentLinkedDeque<SerializableQueueEntry> entries = new ConcurrentLinkedDeque<>();
    docs.forEach(d -> {
      entries.add(new SerializableQueueEntry(UUID.fromString(d.getString("uuid")),
          d.getInteger("connection-attempts"),
          d.getBoolean("waiting-for-connection"),
          d.getInteger("priority"),
          d.getBoolean("full-bypass"),
          d.getBoolean("queue-bypass")));
    });
    return new SerializableQueue(
        entries,
        doc.getString("server-name"),
        ServerStatus.valueOf(doc.getString("online")), // Assuming ServerStatus is an Enum
        doc.getBoolean("full"),
        doc.getBoolean("paused")
    );
  }

  /**
   * Get all the queues.
   *
   * @return All the queues in serialized form.
   */
  public List<SerializableQueue> getAllQueues() {
    List<SerializableQueue> queues = new ArrayList<>();
    FindIterable<Document> docs = queueCollection.find(); // Fetch all documents

    for (Document doc : docs) {
      queues.add(parseDocumentToServerQueue(doc));
    }

    return queues;
  }

}
