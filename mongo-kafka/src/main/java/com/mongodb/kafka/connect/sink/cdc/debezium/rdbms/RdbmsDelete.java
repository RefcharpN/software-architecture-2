/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Original Work: Apache License, Version 2.0, Copyright 2017 Hans-Peter Grahsl.
 */

package com.mongodb.kafka.connect.sink.cdc.debezium.rdbms;

import org.apache.kafka.connect.errors.DataException;

import org.bson.BsonDocument;
import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.cdc.CdcOperation;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

public class RdbmsDelete implements CdcOperation {

  @Override
  public void perform(final SinkDocument doc, final MongoSinkTopicConfig config) {

    BsonDocument keyDoc =
        doc.getKeyDoc()
            .orElseThrow(
                () -> new DataException("Key document must not be missing for delete operation"));

    BsonDocument valueDoc =
        doc.getValueDoc()
            .orElseThrow(
                () -> new DataException("Value document must not be missing for delete operation"));

    final BsonDocument before = valueDoc.getDocument("before");

    try {
      MongoClient mongoClient = MongoClients.create("mongodb://root:2517Pass!Part@mongo:27017/");
      MongoDatabase database = mongoClient.getDatabase("db_mirea");
      MongoCollection<Document> collection = database.getCollection("smelkin");

      switch (config.getTopic()) {
        case "mirea.public.institute":
          System.out.println("Delete from \"institute\" record:\n " + before.toJson());

          collection.deleteOne(Filters.eq("id", before.get("id")));
          break;
        case "mirea.public.kafedra":
          System.out.println("Delete from \"kafedra\" record:\n " + before.toJson());

          collection.updateOne(
              Filters.eq("id", before.get("institute_id")),
              Updates.pull("kafedras", Filters.eq("id", before.get("id"))));
          break;
        case "mirea.public.specialnost":
          System.out.println("Delete from \"specialnost\" record:\n " + before.toJson());

          collection.updateOne(
              Filters.and(
                  Filters.eq("kafedras.id", before.get("kafedra_id")),
                  Filters.eq("kafedras.specialnosts.id", before.get("id"))),
              Updates.pull("kafedras.$.specialnosts", Filters.eq("id", before.get("id"))));
          break;
        case "mirea.public.disciplines":
          System.out.println("Delete from \"disciplines\" record:\n " + before.toJson());

          Bson filter = Filters.eq("kafedras.specialnosts.disciplines.id", before.get("id")); // Здесь указываем id элемента, который нужно удалить

          Document foundDocument = collection.find(filter).first();

          if (foundDocument != null) {
            System.out.println("Найденный документ: " + foundDocument.toJson());

            Bson update = Updates.pull("kafedras.$[].specialnosts.$[].disciplines", Filters.eq("id", before.get("id")));

            collection.updateOne(filter, update);

            System.out.println("\n\nЭлемент из массива disciplines успешно удален.\n\n");
          } else {
            System.out.println("\n\nДокумент не найден.\n\n");
          }
          break;
        default:
          throw new Exception("Incorrect topic");
      }

      mongoClient.close();
    } catch (Exception exc) {
      throw new DataException(exc);
    }
  }
}
