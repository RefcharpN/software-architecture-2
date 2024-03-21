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

public class RdbmsUpdate implements CdcOperation {

  @Override
  public void perform(final SinkDocument doc, final MongoSinkTopicConfig config) {

    BsonDocument keyDoc =
        doc.getKeyDoc()
            .orElseThrow(
                () -> new DataException("Key document must not be missing for update operation"));

    BsonDocument valueDoc =
        doc.getValueDoc()
            .orElseThrow(
                () -> new DataException("Value document must not be missing for update operation"));
    final BsonDocument after = valueDoc.getDocument("after");
    final BsonDocument before = valueDoc.getDocument("before");

    try {
      MongoClient mongoClient = MongoClients.create("mongodb://root:2517Pass!Part@mongo:27017/");
      MongoDatabase database = mongoClient.getDatabase("db_mirea");
      MongoCollection<Document> collection = database.getCollection("smelkin");

      switch (config.getTopic()) {
        case "mirea.public.institute":
          System.out.println(
              "Update in \"universities\" record: "
                  + "\nbefore:\n"
                  + before.toJson()
                  + "\nafter:\n"
                  + after.toJson());

          collection.updateOne(
              Filters.eq("id", after.get("id")), Updates.set("name", after.get("name")));
          break;
        case "mirea.public.kafedra":
          System.out.println(
              "Update in \"institutes\" record: "
                  + "\nbefore:\n"
                  + before.toJson()
                  + "\nafter:\n"
                  + after.toJson());


          Document institute = collection.find(Filters.eq("kafedras.id", before.get("id"))).first();
          List<Document> kaf = institute.getList("kafedras", Document.class);
          List<Document> specialnosts = null;

          for (Document kaf_ : kaf) {
            if (kaf_.get("id").equals(before.get("id").asInt32().getValue())) {
              specialnosts = kaf_.getList("specialnosts", Document.class);
            }
          }
          if (specialnosts == null) specialnosts = new ArrayList<>();

          collection.updateOne(
              Filters.eq("id", before.get("institute_id")),
              Updates.pull("kafedras", Filters.eq("id", before.get("id"))));

          Document kafedra =
              new Document("id", after.get("id"))
                  .append("name", after.get("name"))
                  .append("specialnosts", specialnosts);

          collection.updateOne(
              Filters.eq("id", after.get("institute_id")), Updates.push("kafedras", kafedra));
          break;
        case "mirea.public.specialnost":
          System.out.println(
              "Update in \"departments\" record: "
                  + "\nbefore:\n"
                  + before.toJson()
                  + "\nafter:\n"
                  + after.toJson());

          Document institute_2 = collection.find(Filters.eq("kafedras.id", before.get("kafedra_id"))).first();
          List<Document> kaf_2 = institute_2.getList("kafedras", Document.class);
          List<Document> specialnosts_2 = null;
          List<Document> disciplines = null;

          for (Document kaf_ : kaf_2) {
            if (kaf_.get("id").equals(before.get("kafedra_id").asInt32().getValue())) {
              specialnosts_2 = kaf_.getList("specialnosts", Document.class);
              for (Document spec_ : specialnosts_2) {
                if (spec_.get("id").equals(before.get("id").asInt32().getValue())) {
                  disciplines = spec_.getList("disciplines", Document.class);
                  break;
                }
              }
            }
          }
          if (disciplines == null) disciplines = new ArrayList<>();

          collection.updateOne(
              Filters.and(
                  Filters.eq("kafedras.id", before.get("kafedra_id")),
                  Filters.eq("kafedras.specialnosts.id", before.get("id"))),
              Updates.pull("kafedras.$.specialnosts", Filters.eq("id", before.get("id"))));

          Document department =
              new Document("id", after.get("id"))
                  .append("name", after.get("name"))
                  .append("disciplines", disciplines);

          collection.updateOne(
              Filters.eq("kafedras.id", after.get("kafedra_id")),
              Updates.push("kafedras.$.specialnosts", department));
          break;

        case "mirea.public.disciplines":
          System.out.println(
                  "Update in \"departments\" record: "
                          + "\nbefore:\n"
                          + before.toJson()
                          + "\nafter:\n"
                          + after.toJson());

          //удаление
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

          //добавление
          Document foundDocument_ = collection.find(
                  Filters.elemMatch("kafedras", Filters.elemMatch("specialnosts", Filters.eq("id", after.get("spec_id"))))
          ).first();

          if (foundDocument_ != null) {
            System.out.println("\n\nНайденный объект: " + foundDocument_.toJson());

            // Получаем массив kafedras из найденного документа
            List<Document> kafedras = foundDocument_.getList("kafedras", Document.class);
            // Проверяем, есть ли хотя бы один элемент в массиве kafedras
            if (!kafedras.isEmpty()) {
              // Проходим по всем элементам массива kafedras
              for (Document kafedra_ : kafedras) {
                // Проверяем, совпадает ли поле "id" с полем "specialnost_id" из valueDoc
                List<Document> specialnosts_ = kafedra_.getList("specialnosts", Document.class);
                if (specialnosts_ != null) {
                  // Проходим по всем элементам массива specialnosts
                  for (Document specialnost_ : specialnosts_) {
                    if (specialnost_.get("id").equals(after.get("spec_id").asInt32().getValue())) {
                      // Получаем или создаем массив disciplines в найденном элементе массива specialnosts
                      List<Document> disciplines_ = specialnost_.getList("disciplines", Document.class);
                      if (disciplines_ == null) {
                        disciplines = new ArrayList<>();
                        specialnost_.put("disciplines", disciplines);
                      }
                      // Добавляем valueDoc в массив disciplines
                      disciplines_.add(new Document(new Document("id", after.get("id"))
                              .append("name", after.get("name"))
                              .append("check_type", after.get("check_type"))
                              .append("description", after.get("description"))
                              .append("technical", after.get("technical"))));

                      // Обновляем найденный документ, добавляя обновленный массив kafedras
                      collection.updateOne(
                              Filters.eq("_id", foundDocument_.getObjectId("_id")), // Предполагаем, что _id является уникальным идентификатором документа
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
