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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.symphony.oss.commons.dom.json.IJsonObject;
import com.symphony.oss.commons.dom.json.ImmutableJsonObject;
import com.symphony.oss.models.core.canon.facade.IApplicationRecord;

/**
 * An example application which processes FX items without using a Canon or a consumer.
 * 
 * @author Bruce Skingle
 *
 */
public class MongoListJsonFxItems extends MongoFxExample
{
  private static final Logger log_ = LoggerFactory.getLogger(MongoListJsonFxItems.class);

  private MongoListJsonFxItems(String[] args)
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
      
      /* Use methods on the JsonObject returned to find the payloadType attribute in the header.
       * We could call toString() on the applicationRecord or applicationRecord.getHeader() to get the full JSON as a String which
       * could be re-parsed if preferred.
       */
      String payloadType = applicationRecord.getHeader().getJsonObject().getString("payloadType", null);
      
      if(payloadType == null)
      {
        log_.error("Unknown payload type " + applicationRecord.getHeader());
      }
      else
      {
        // Get the payload object.
        ImmutableJsonObject payload = applicationRecord.getPayload().getJsonObject();
        
        // Get the CCY Pair from the payload.
        IJsonObject<?> ccyPairJson = payload.getObject("ccyPair");
        
        /* Construct the 6 character CCY Pair ID from the JSON object. It would be simpler to store the CCY pair as a string but
         * this illustrates how to navigate the JSON if you need to do that.
         */
        String ccyPair = ccyPairJson.getRequiredString("base") + ccyPairJson.getRequiredString("counter");
        
        switch(payloadType)
        {
          case "com.symphony.oss.allegro.examples.model.fx.Quote":
            // Handle a Quote
            printHeader();
            System.out.format("%-5.5s %s %8.5f %4s %-25.25s %s%n",
                "QUOTE", ccyPair, payload.getRequiredDouble("price"), "", payload.getRequiredString("id"), payload.getRequiredString("expires"));
            break;
            

          case "com.symphony.oss.allegro.examples.model.fx.Rfq":
            // Handle an RFQ
            printHeader();
            System.out.format("%-5.5s %s %8d %4d %-25.25s %s%n",
                "RFQ", ccyPair, payload.getRequiredLong("quantity"), payload.getRequiredInteger("streamFor"), payload.getRequiredString("id"),
                payload.getRequiredString("expires"));
            break;
          
          default:
            // Handle anything else
            log_.error("Unknown payload type " + applicationRecord.getHeader());
        }
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
    new MongoListJsonFxItems(args).run();
  }
}
