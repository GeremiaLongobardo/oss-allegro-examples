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

import org.bson.Document;

import com.symphony.oss.allegro.examples.calendar.canon.IToDoHeader;
import com.symphony.oss.allegro.examples.calendar.canon.IToDoItem;
import com.symphony.oss.allegro2.api.AllegroConsumerManager;
import com.symphony.oss.models.core.canon.IApplicationPayload;

/**
 * An example application which creates a ToDoItem, adding it to a current and absolute sequence.
 * 
 * @author Bruce Skingle
 *
 */
public class MongoAllegroConsumeToDoItems extends AllegroMongoCalendarExample
{
  @Override
  public void run(CalendarContext context)
  {
    
    AllegroConsumerManager consumerManager = context.allegro2Api_.newConsumerManagerBuilder()
      .withConsumer(IToDoHeader.class, IToDoItem.class, (record, header, payload) ->
      {
        System.out.println("ToDoHeader: " + header);
        System.out.println("ToDoItem:   " + payload);
        System.out.println("EncryptedApplicationRecord: " + record);
      })
      .withConsumer(IApplicationPayload.class, IApplicationPayload.class, (record, header, payload) ->
      {
        System.out.println("Untyped Header:  " + header);
        System.out.println("Untyped Payload: " + payload);
        System.out.println("EncryptedApplicationRecord:  " + record);
      })
      .build();
    
    for(Document doc : context.todoItems_.find())
    {
      //System.out.println("doc = " + doc);
      
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
    new MongoAllegroConsumeToDoItems().run(args);
  }
}
