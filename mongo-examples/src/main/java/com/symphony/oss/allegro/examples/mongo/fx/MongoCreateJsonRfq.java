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

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.bson.Document;

/**
 * Create an RFQ using raw JSON.
 * 
 * @author Bruce Skingle
 *
 */
public class MongoCreateJsonRfq extends MongoFxExample
{
  private MongoCreateJsonRfq(String[] args)
  {
    super(args);
  }

  private void run()
  {
    /* The _type and _version attributes are added by Canon generated code and are discriminators to allow objects to be deserialised.
     * This JSON was generated from a serialised Canon model object but the type and version attributes have been removed to demonstrate 
     * the effects of using JSON generated without Canon support.
     * 
     * Of course callers wishing to avoid Canon would probably have some other model support, but for the purpose of this example
     * we will construct the Json by String concatenation.
     * 
     * The only restriction on the values passed as the header and payload are that they are valid JSON objects.
     */
    
    // We use a timestamp as an ID as a simple way of generating unique IDs, this is obviously not what one would do for a real application.
    String now = Instant.now().toString();
    
    // Create the RFQ Payload which will be encrypted.
    String item = "{\n" + 
//        "  \"_type\":\"com.symphony.oss.allegro.examples.model.fx.Rfq\",\n" + 
//        "  \"_version\":\"1.0\",\n" + 
        "  \"ccyPair\":{\n" + 
//        "    \"_type\":\"com.symphony.oss.allegro.examples.model.fx.CcyPair\",\n" + 
//        "    \"_version\":\"1.0\",\n" + 
        "    \"base\":\"GBP\",\n" + 
        "    \"counter\":\"USD\"\n" + 
        "  },\n" + 
        "  \"expires\":\"" + Instant.now().plus(30, ChronoUnit.SECONDS).toString() + "\",\n" +
        "  \"id\":\"" + now + "\",\n" + 
        "  \"quantity\":1000000,\n" + 
        "  \"streamFor\":10\n" + 
        "}"; 

    // Create the header which will be stored unencrypted.
    String header = "{\n" + 
//        "  \"_type\":\"com.symphony.oss.allegro.examples.model.fx.FxHeader\",\n" + 
//        "  \"_version\":\"1.0\",\n" + 
        "  \"ccyPair\":{\n" + 
//        "    \"_type\":\"com.symphony.oss.allegro.examples.model.fx.CcyPair\",\n" + 
//        "    \"_version\":\"1.0\",\n" + 
        "    \"base\":\"GBP\",\n" + 
        "    \"counter\":\"USD\"\n" + 
        "  },\n" + 
        "  \"id\":\"" + now + "\",\n" + 
        "  \"payloadType\":\"com.symphony.oss.allegro.examples.model.fx.Rfq\"\n" + 
        "}";
    
    System.out.println("About to create item " + item);
    System.out.println("with header " + header);
    
    // Create an encrypted BSON Document.
    Document toDoDoc = allegro2MongoApi_.newEncryptedDocumentBuilder()
        .withThreadId(threadId_)
        .withHeader(header)
        .withPayload(item)
      .build();
    
    System.out.println("About to store " + toDoDoc);
    
    fxItems_.insertOne(toDoDoc);
  }
  
  /**
   * Main.
   * 
   * @param args Command line arguments.
   */
  public static void main(String[] args)
  {
    new MongoCreateJsonRfq(args).run();
  }
}
