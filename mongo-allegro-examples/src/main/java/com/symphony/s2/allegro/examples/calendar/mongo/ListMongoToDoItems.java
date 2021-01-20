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

import org.bson.Document;

import com.symphony.oss.allegro.api.StoredRecordConsumerManager;
import com.symphony.oss.allegro.examples.calendar.canon.IToDoItem;
import com.symphony.oss.models.object.canon.IAffectedUsers;
import com.symphony.oss.models.object.canon.facade.IApplicationObjectHeader;
import com.symphony.oss.models.object.canon.facade.IApplicationObjectPayload;

/**
 * An example application which creates a ToDoItem, adding it to a current and absolute sequence.
 * 
 * @author Bruce Skingle
 *
 */
public class ListMongoToDoItems extends AllegroMongoCalendarExample
{
  @Override
  public void run(CalendarContext context)
  {
    
    for(Document doc : context.todoItems_.find())
    {
      System.out.println("doc = " + doc);
      
//      IEncryptedApplicationPayload storedObject = context.allegroPodApi_.getModelRegistry().parseOne(new StringReader(doc.toJson()), EncryptedApplicationPayload.TYPE_ID, IEncryptedApplicationPayload.class);
//      
//      System.out.println("storedObject = " + storedObject);
//      
//      IApplicationObjectPayload applicationPayload = context.allegroPodApi_.decryptObject(storedObject);
//      
//      System.out.println("applicationPayload = " + applicationPayload);
    }
    
    StoredRecordConsumerManager consumerManager = context.allegroPodApi_.newConsumerManagerBuilder()
      .withConsumer(IAffectedUsers.class, IToDoItem.class, (record, header, payload) ->
      {
        System.out.println("Header: " + header);
        System.out.println("Payload: " + payload);
        System.out.println("StoredApplicationRecord: " + record);
      })
      .withConsumer(IApplicationObjectHeader.class, IApplicationObjectPayload.class, (record, header, payload) ->
      {
        System.out.println("StoredApplicationRecord:  " + record);
      })
      .build();
    
    for(Document doc : context.todoItems_.find())
    {
      System.out.println("doc = " + doc);
      
      consumerManager.accept(doc.toJson());
    }
  }

  /**
   * Main.
   * 
   * @param args Command line arguments.
   */
  public static void main(String[] args)
  {
    new ListMongoToDoItems().run(args);
  }
}
