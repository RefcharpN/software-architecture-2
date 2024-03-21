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

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.errors.DataException;

import org.bson.BsonDocument;
import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.cdc.CdcOperation;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import org.bson.conversions.Bson;

import static com.mongodb.client.model.Updates.addToSet;

public class RdbmsInsert implements CdcOperation {

  @Override
  public void perform(final SinkDocument doc, final MongoSinkTopicConfig config) {

    BsonDocument keyDoc =
        doc.getKeyDoc()
            .orElseThrow(
                () -> new DataException("Key document must not be missing for insert operation"));

    BsonDocument valueDoc =
        doc.getValueDoc()
            .orElseThrow(
                () -> new DataException("Value document must not be missing for insert operation"));
    final BsonDocument after = valueDoc.getDocument("after");

    try {
      MongoClient mongoClient = MongoClients.create("mongodb://root:2517Pass!Part@mongo:27017/");
      MongoDatabase database = mongoClient.getDatabase("db_mirea");
      MongoCollection<Document> collection = database.getCollection("smelkin");

      switch (config.getTopic()) {
        case "mirea.public.institute":
          System.out.println("Insert into \"mirea.public.institute\" record: " + after.toJson());

          Document institute =
              new Document("id", after.get("id"))
                  .append("name", after.get("name"))
                  .append("kafedras", new ArrayList<>());
          collection.insertOne(institute);
          break;
        case "mirea.public.kafedra":
          System.out.println("Insert into \"mirea.public.kafedra\" record:\n " + after.toJson());

          Document kafedra =
              new Document("id", after.get("id"))
                  .append("name", after.get("name"))
                  .append("specialnosts", new ArrayList<>());

          collection.updateOne(
              Filters.eq("id", after.get("institute_id")), Updates.push("kafedras", kafedra));
          break;
        case "mirea.public.specialnost":
          System.out.println(
              "Insert into \"mirea.public.specialnost\" record:\n " + after.toJson());

          Document specialnost =
              new Document("id", after.get("id"))
                  .append("name", after.get("name"))
                  .append("disciplines", new ArrayList<>());

          collection.updateOne(
              Filters.eq("kafedras.id", after.get("kafedra_id")),
              Updates.push("kafedras.$.specialnosts", specialnost));
          break;
        case "mirea.public.disciplines":
          System.out.println(
              "Insert into \"mirea.public.disciplines\" record:\n " + after.toJson());

//          Document disciplines =
//              new Document("id", after.get("id"))
//                  .append("name", after.get("name"))
//                  .append("check_type", after.get("check_type"))
//                  .append("description", after.get("description"))
//                  .append("technical", after.get("technical"));

          Document foundDocument = collection.find(
                  Filters.elemMatch("kafedras", Filters.elemMatch("specialnosts", Filters.eq("id", after.get("spec_id"))))
          ).first();

          if (foundDocument != null) {
            System.out.println("\n\nНайденный объект: " + foundDocument.toJson());

            // Получаем массив kafedras из найденного документа
            List<Document> kafedras = foundDocument.getList("kafedras", Document.class);
            // Проверяем, есть ли хотя бы один элемент в массиве kafedras
            if (!kafedras.isEmpty()) {
              // Проходим по всем элементам массива kafedras
              for (Document kafedra_ : kafedras) {
                // Проверяем, совпадает ли поле "id" с полем "specialnost_id" из valueDoc
                List<Document> specialnosts = kafedra_.getList("specialnosts", Document.class);
                if (specialnosts != null) {
                  // Проходим по всем элементам массива specialnosts
                  for (Document specialnost_ : specialnosts) {
                    if (specialnost_.get("id").equals(after.get("spec_id").asInt32().getValue())) {
                      // Получаем или создаем массив disciplines в найденном элементе массива specialnosts
                      List<Document> disciplines = specialnost_.getList("disciplines", Document.class);
                      if (disciplines == null) {
                        disciplines = new ArrayList<>();
                        specialnost_.put("disciplines", disciplines);
                      }
                      // Добавляем valueDoc в массив disciplines
                      disciplines.add(new Document(new Document("id", after.get("id"))
                              .append("name", after.get("name"))
                              .append("check_type", after.get("check_type"))
                              .append("description", after.get("description"))
                              .append("technical", after.get("technical"))));

                      // Обновляем найденный документ, добавляя обновленный массив kafedras
                      collection.updateOne(
                              Filters.eq("_id", foundDocument.getObjectId("_id")), // Предполагаем, что _id является уникальным идентификатором документа
                              Updates.set("kafedras", kafedras) // Обновляем массив kafedras в найденном документе
                      );
                      // Если элемент найден и обновлен, выходим из цикла
                      break;
                    }
                  }
                }
              }
            } else {
              System.out.println("Элемент массива kafedras не найден.");
            }
          } else {
            System.out.println("Объект не найден.");
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
