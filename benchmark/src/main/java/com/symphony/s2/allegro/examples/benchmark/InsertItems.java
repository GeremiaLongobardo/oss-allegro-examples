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

package com.symphony.s2.allegro.examples.benchmark;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.bson.Document;

import com.mongodb.client.model.Indexes;
import com.symphony.oss.allegro.examples.calendar.canon.IPersonDetails;
import com.symphony.oss.allegro.examples.calendar.canon.IPersonIndex;
import com.symphony.oss.allegro2.api.AllegroConsumerManager;
import com.symphony.oss.canon.runtime.IEntity;
import com.symphony.oss.models.core.canon.facade.IApplicationRecord;
import com.symphony.oss.models.core.canon.facade.IEncryptedApplicationRecord;
import com.symphony.oss.models.object.canon.facade.IApplicationObjectHeader;
import com.symphony.oss.models.object.canon.facade.IApplicationObjectPayload;

/**
 * An example application which creates a ToDoItem, adding it to a current and absolute sequence.
 * 
 * @author Bruce Skingle
 *
 */
public class InsertItems extends AllegroMongoBenchmarkExample
{
  @Override
  public void run()
  {
    List<Document> databases = mongoClient_.listDatabases().into(new ArrayList<>());
    databases.forEach(db -> System.out.println(db.toJson()));


    System.out.println("db = " + db_.getName());
    System.out.println("collection " + items_);
    
    items_.createIndex(Indexes.ascending("familyName", "givenName"));
    
    Entities entities = new Entities(allegroPodApi_.getModelRegistry(), "/10people.json");
    
    long start = System.currentTimeMillis();
    
    List<Document> documents = new ArrayList<>(entities.getDocuments().size());
    
    for(IEntity entity : entities)
    {
      IApplicationRecord record = (IApplicationRecord)entity;
      
   // Create an encrypted StoredApplicationRecord.
      IEncryptedApplicationRecord storedRecord = allegroPodApi_.newApplicationRecordBuilder()
          .withThreadId(threadId_)
          .withHeader(record.getHeader())
          .withPayload(record.getPayload())
        .build();
      
      Document doc = Document.parse(storedRecord.toString());
      
      documents.add(doc);
    }
    long middle = System.currentTimeMillis();
    items_.insertMany(documents);
    long end = System.currentTimeMillis();
    
    System.out.println("Encrypted " + entities.getDocuments().size() + " items in " + (middle - start) + "ms");
    System.out.println("Inserted " + entities.getDocuments().size() + " items in " + (end - middle) + "ms");
    System.out.println("Created " + entities.getDocuments().size() + " items in " + (end - start) + "ms");
    
    IApplicationRecord record1 = (IApplicationRecord)entities.getDocuments().get(0);
    start = System.currentTimeMillis();
    
    
    
    // Create an encrypted StoredApplicationRecord.
    IEncryptedApplicationRecord storedRecord = allegroPodApi_.newApplicationRecordBuilder()
           .withThreadId(threadId_)
           .withHeader(record1.getHeader())
           .withPayload(record1.getPayload())
         .build();
       
       Document doc1 = Document.parse(storedRecord.toString());
       
       middle = System.currentTimeMillis();
       items_.insertOne(doc1);
       end = System.currentTimeMillis();
       
       System.out.println("Encrypted 1 in " + (middle - start) + "ms");
       System.out.println("Inserted 1 in " + (end - middle) + "ms");
       System.out.println("Created 1 items in " + (end - start) + "ms");
    
//      FindIterable<Document> cursor = collection.find();
//      
//      for(Document doc : cursor)
//      {
//        System.out.println("doc = " + doc.getDouble("x"));
//      }
    
//    // Create the payload
//    IToDoItem toDoItem = new ToDoItem.Builder()
//      .withDue(Instant.now())
//      .withTimeTaken(new BigDecimal(1000.0 / 3.0))
//      .withTitle("An example TODO Item")
//      .withDescription("Since we are creating this item with a due date of Instant.now() we are already late!")
//      .build();
//    
//    System.out.println("About to create item " + toDoItem);
//    
//    // Create the header
//    IAffectedUsers affectedUsers = new AffectedUsers.Builder()
//        .withRequestingUser(context.allegroPodApi_.getUserId())
//        .withAffectedUsers(context.allegroPodApi_.getUserId())
//        .withEffectiveDate(Instant.now())
//        .build();
//    
//    
//    // Create an encrypted StoredApplicationRecord.
//    IStoredApplicationRecord toDoObject = context.allegroPodApi_.newApplicationRecordBuilder()
//        .withThreadId(context.threadId_)
//        .withHeader(affectedUsers)
//        .withPayload(toDoItem)
//      .build();
//    
//    System.out.println("About to store " + toDoObject);
//    
//    Document toDoDoc = Document.parse(toDoObject.toString());
//    
//    context.todoItems_.insertOne(toDoDoc);
    
    List<IPersonIndex> readIndex = new LinkedList<>();
    List<IPersonDetails> readDetails = new LinkedList<>();
    
    AllegroConsumerManager consumerManager = allegroPodApi_.newConsumerManagerBuilder()
        .withConsumer(IPersonIndex.class, IPersonDetails.class, (record, header, payload) ->
        {
          readIndex.add(header);
          readDetails.add(payload);
        })
        .withConsumer(IApplicationObjectHeader.class, IApplicationObjectPayload.class, (record, header, payload) ->
        {
          System.out.println("StoredApplicationRecord:  " + record);
        })
        .build();
    
    start = System.currentTimeMillis();
    for(Document doc : items_.find())
    {
      //System.out.println("doc = " + doc);
      
      consumerManager.accept(doc.toJson());
    }
    
    end = System.currentTimeMillis();
    
    System.out.println("Read " + readIndex.size() + " items in " + (end - start) + "ms");
      

  }
  
  /**
   * Main.
   * 
   * @param args Command line arguments.
   */
  public static void main(String[] args)
  {
    new InsertItems().run(args);
  }
}
