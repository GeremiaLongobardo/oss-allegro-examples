/*
 * Copyright 2019 Symphony Communication Services, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.symphony.s2.allegro.examples.calendar.mongo.allegro;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.symphony.oss.models.core.canon.facade.IEncryptedApplicationRecord;

/**
 * An example application which creates a ToDoItem, adding it to a current and absolute sequence.
 * 
 * @author Bruce Skingle
 *
 */
public class MongoAllegroCreateJsonItem extends AllegroMongoCalendarExample
{
  @Override
  public void run(CalendarContext context)
  {
    List<Document> databases = context.mongoClient_.listDatabases().into(new ArrayList<>());
    databases.forEach(db -> System.out.println(db.toJson()));


    System.out.println("db = " + context.db_.getName());
    System.out.println("collection " + context.todoItems_);
    
    String now = Instant.now().toString();
    String item = "{\n" + 
//        "  \"_type\":\"com.symphony.s2.model.calendar.ToDoItem\",\n" + 
//        "  \"_version\":\"1.0\",\n" + 
        "  \"description\":\"Since we are creating this item with a due date of Instant.now() we are already late!\",\n" + 
        "  \"due\":\"" + now + "\",\n" + 
        "  \"timeTaken\":\"" + new BigDecimal(1000.0 / 3.0) + "\",\n" + 
        "  \"title\":\"An example TODO Item\"\n" + 
        "}";
    String header = "{\n" + 
//        "  \"_type\":\"com.symphony.s2.model.object.AffectedUsers\",\n" + 
//        "  \"_version\":\"1.0\",\n" + 
        "  \"affectedUsers\":[\n" + 
        "    " + context.allegro2Api_.getUserId() + "\n" + 
        "  ],\n" + 
        "  \"effectiveDate\":\"" + now + "\",\n" + 
        "  \"requestingUser\":" + context.allegro2Api_.getUserId() + "\n" + 
        "}";
    
    System.out.println("About to create item " + item);
    System.out.println("with header " + header);
    
    // Create an encrypted StoredApplicationRecord.
    IEncryptedApplicationRecord toDoObject = context.allegro2Api_.newApplicationRecordBuilder()
        .withThreadId(context.threadId_)
        .withHeader(header)
        .withPayload(item)
      .build();
    
    System.out.println("About to store " + toDoObject);
    
    Document toDoDoc = Document.parse(toDoObject.toString());
    
    context.todoItems_.insertOne(toDoDoc);
  }
  
  /**
   * Main.
   * 
   * @param args Command line arguments.
   */
  public static void main(String[] args)
  {
    new MongoAllegroCreateJsonItem().run(args);
  }
}
