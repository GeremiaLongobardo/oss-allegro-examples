/*
 * Copyright 2021 Symphony Communication Services, LLC.
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

package com.symphony.oss.allegro.examples.mongo.fx;

import org.bson.Document;

import com.symphony.oss.allegro.examples.model.fx.canon.IQuote;
import com.symphony.oss.allegro.examples.model.fx.canon.IRfq;
import com.symphony.oss.models.core.canon.IApplicationPayload;
import com.symphony.oss.models.core.canon.facade.IApplicationRecord;

/**
 * An example application which processes FX items without using a consumer.
 * 
 * For each item a single line summary is output.
 * 
 * @author Bruce Skingle
 *
 */
public class MongoListFxItems extends MongoFxExample
{
  private MongoListFxItems(String[] args)
  {
    super(args);
  }

  private void run()
  {
    // Process all documents in the fxItems collection
    for(Document doc : fxItems_.find())
    {
      /* Call Allegro to decrypt the Document. The returned IApplicationRecord contains the header as well as the decrypted payload. */
      IApplicationRecord applicationRecord = allegro2MongoApi_.decrypt(doc);
      
      /* Get the decrypted payload. The returned type is IApplicationPayload however if the ModelRegistry contains factories for the
       * domain model (it is added in the contructor of MongoFxExample, our super class) then the returned object will be fully typed,
       * allowing us to do instanceof tests.
       * 
       * Note that a ConsumerManager would do all this for you.
       */
      IApplicationPayload payload = applicationRecord.getPayload();
      
      if(payload instanceof IRfq)
      {
        // Handle and RFQ
        printRfq((IRfq) payload);
      }
      else if(payload instanceof IQuote)
      {
        // Handle a Quote
        printQuote((IQuote) payload);
      }
      else
      {
        // Handle anything else
        System.out.println("header = " + applicationRecord.getHeader());
        System.out.println("payload = " + applicationRecord.getPayload());
      }
    }
  }

  /**
   * Main.
   * 
   * @param args Command line arguments.
   */
  public static void main(String[] args)
  {
    new MongoListFxItems(args).run();
  }
}
