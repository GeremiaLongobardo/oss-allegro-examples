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

package com.symphony.s2.allegro.examples.calendar.mongo;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.symphony.oss.allegro.examples.calendar.canon.IToDoItem;
import com.symphony.oss.allegro.examples.calendar.canon.ToDoItem;
import com.symphony.oss.models.object.canon.AffectedUsers;
import com.symphony.oss.models.object.canon.IAffectedUsers;
import com.symphony.oss.models.object.canon.facade.IStoredApplicationRecord;

/**
 * An example application which creates a ToDoItem, adding it to a current and absolute sequence.
 * 
 * @author Bruce Skingle
 *
 */
public class CreateMongoToDoItem extends AllegroMongoCalendarExample
{
  @Override
  public void run(CalendarContext context)
  {
    List<Document> databases = context.mongoClient_.listDatabases().into(new ArrayList<>());
    databases.forEach(db -> System.out.println(db.toJson()));


    System.out.println("db = " + context.db_.getName());
    System.out.println("collection " + context.todoItems_);
    
//      FindIterable<Document> cursor = collection.find();
//      
//      for(Document doc : cursor)
//      {
//        System.out.println("doc = " + doc.getDouble("x"));
//      }
    
    // Create the payload
    IToDoItem toDoItem = new ToDoItem.Builder()
      .withDue(Instant.now())
      .withTimeTaken(new BigDecimal(1000.0 / 3.0))
      .withTitle("An example TODO Item")
      .withDescription("Since we are creating this item with a due date of Instant.now() we are already late!")
      .build();
    
    System.out.println("About to create item " + toDoItem);
    
    // Create the header
    IAffectedUsers affectedUsers = new AffectedUsers.Builder()
        .withRequestingUser(context.allegroPodApi_.getUserId())
        .withAffectedUsers(context.allegroPodApi_.getUserId())
        .withEffectiveDate(Instant.now())
        .build();
    
    
    // Create an encrypted StoredApplicationRecord.
    IStoredApplicationRecord toDoObject = context.allegroPodApi_.newApplicationRecordBuilder()
        .withThreadId(context.threadId_)
        .withHeader(affectedUsers)
        .withPayload(toDoItem)
      .build();
    
    System.out.println("About to store " + toDoObject);
    
    Document toDoDoc = Document.parse(toDoObject.toString());
    
    context.todoItems_.insertOne(toDoDoc);
    
    for(Document doc : context.todoItems_.find())
    {
      System.out.println("doc = " + doc);
    }
  }
  
  /**
   * Main.
   * 
   * @param args Command line arguments.
   */
  public static void main(String[] args)
  {
    new CreateMongoToDoItem().run(args);
  }
}
