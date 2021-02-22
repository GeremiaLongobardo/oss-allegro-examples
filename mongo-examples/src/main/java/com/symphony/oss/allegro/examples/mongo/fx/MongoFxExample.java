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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.symphony.oss.allegro.examples.model.fx.canon.FxModel;
import com.symphony.oss.allegro.examples.model.fx.canon.IQuote;
import com.symphony.oss.allegro.examples.model.fx.canon.IRfq;

/**
 * Base class for examples using the FX model.
 * 
 * @author Bruce Skingle
 *
 */
abstract class MongoFxExample extends MongoExample
{
  /** Mongodb client */
  protected final MongoClient               mongoClient_;
  /** Mongodb database */
  protected final MongoDatabase             db_;
  /** Mongodb collection */
  protected final MongoCollection<Document> fxItems_;
  
  private boolean header_ = true;
  
  protected MongoFxExample(String[] args)
  {
    super(args);
    
    allegro2MongoApi_.getModelRegistry().withFactories(FxModel.FACTORIES);
  
    String connectionString = "mongodb+srv://" + mongoUser_ + ":" + mongoPassword_ + "@" + mongoHost_ + "/FX?retryWrites=true&w=majority";
  
    mongoClient_ = MongoClients.create(connectionString);
    db_ = mongoClient_.getDatabase("FX");
    fxItems_ = db_.getCollection("FxItems");
  }

  /**
   * Print the header row if it has not already been output.
   */
  protected void printHeader()
  {
    if(header_)
    {
      System.out.format("%-5.5s %6.6s %8.8s %4.4s %-25.25s %s%n",
          "TYPE", "CCY", "AMT/PRICE", "CNT", "ID", "EXPIRES");
      header_ = false;
    }
  }

  /**
   * Print the given RFQ as a single line of formatted output.
   * 
   * @param rfq The RFQ to be printed.
   */
  protected void printRfq(IRfq rfq)
  {
    printRfq(rfq, "");
  }

  /**
   * Print the given RFQ and the additional message as a single line of formatted output.
   * 
   * @param rfq The RFQ to be printed.
   * @param message An additional message to be printed.
   */
  protected void printRfq(IRfq rfq, String message)
  {
    printHeader();
    System.out.format("%-5.5s %s %8d %4d %-25.25s %s %s%n",
        "RFQ",
        rfq.getCcyPair().getCode(), rfq.getQuantity(), rfq.getStreamFor(), rfq.getId(), rfq.getExpires(), message);
  }
  /**
   * Print the given Quote as a single line of formatted output.
   * 
   * @param quote The Quote to be printed.
   */
  protected void printQuote(IQuote quote)
  {
    printHeader();
    System.out.format("%-5.5s %s %8.5f %4s %-25.25s %s%n",
        "QUOTE", quote.getCcyPair().getCode(), quote.getPrice(), "", quote.getId(), quote.getExpires());
  }
}
