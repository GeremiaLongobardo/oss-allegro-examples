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

import com.symphony.oss.allegro.examples.model.fx.canon.IFxHeader;
import com.symphony.oss.allegro.examples.model.fx.canon.IQuote;
import com.symphony.oss.allegro.examples.model.fx.canon.IRfq;
import com.symphony.oss.allegro2.mongo.api.AllegroMongoConsumerManager;
import com.symphony.oss.models.core.canon.IApplicationPayload;

/**
 * USe an Allegro consumer to process all objects in the Fx collection and print a one line summary of each one.
 * 
 * @author Bruce Skingle
 *
 */
public class MongoConsumeFxItems extends MongoFxExample
{
  private MongoConsumeFxItems(String[] args)
  {
    super(args);
  }

  private void run()
  {
    // A consumer manager is a container of consumers and routes each object to the consumer with the closest type to each actual record.
    AllegroMongoConsumerManager consumerManager = allegro2MongoApi_.newConsumerManagerBuilder()
      .withConsumer(IFxHeader.class, IRfq.class, (record, header, rfq) ->
      {
        // Consumer for RFQ objects, call the standard print routine.
        printRfq(rfq);
      })
      .withConsumer(IFxHeader.class, IQuote.class, (record, header, quote) ->
      {
        // Consumer for Quote objects, call the standard print routine.
        printQuote(quote);
      })
      .withConsumer(IApplicationPayload.class, IApplicationPayload.class, (record, header, payload) ->
      {
        /* Consumer for any other object type, although the consumer is defined as taking IApplicationPayload the passed objects
         * will actually be typed if the model registry has appropriate factories for the serialised types encountered, so
         * it would be reasonable to do instandeof(TypeName) checks, although it would probably be easier to register typed consumers.
         * 
         * There is an error consumer which will be called if any consumer throws a RuntimeException or if no appropriate consumer exists.
         * The default error consumer logs and error but different logic can be configured by calling withErrorConsumer()
         */
        System.out.println("Untyped Header:  " + header);
        System.out.println("Untyped Payload: " + payload);
        System.out.println("EncryptedApplicationRecord:  " + record);
      })
      .build();
    
    // Find all items in the collection and process them via the above consumers.
    consumerManager.accept(fxItems_.find());
  }

  /**
   * Main.
   * 
   * @param args Command line arguments.
   */
  public static void main(String[] args)
  {
    new MongoConsumeFxItems(args).run();
  }
}
