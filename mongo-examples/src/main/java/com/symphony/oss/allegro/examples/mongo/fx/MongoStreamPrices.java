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

import static com.mongodb.client.model.Filters.eq;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.bson.Document;

import com.symphony.oss.allegro.examples.model.fx.canon.IFxHeader;
import com.symphony.oss.allegro.examples.model.fx.canon.IQuote;
import com.symphony.oss.allegro.examples.model.fx.canon.IRfq;
import com.symphony.oss.allegro.examples.model.fx.canon.Quote;
import com.symphony.oss.allegro.examples.model.fx.canon.Rfq;
import com.symphony.oss.allegro2.mongo.api.AllegroMongoConsumerManager;

/**
 * Example program which reads all RFQs from the FX collection and for each one which has not expired, streams prices
 * by creating Quote objects in the same collection.
 * 
 * These prices could actually be streamed to another process via a MongoDb change stream.
 * 
 * @author Bruce Skingle
 *
 */
public class MongoStreamPrices extends MongoFxExample
{
  private static final Random random_ = new Random();
  
  private MongoStreamPrices(String[] args)
  {
    super(args);
  }

  private void run()
  {
    ExecutorService executor = Executors.newCachedThreadPool();
    
    // Consumer to process RFQ objects.
    AllegroMongoConsumerManager consumerManager = allegro2MongoApi_.newConsumerManagerBuilder()
        .withConsumer(IFxHeader.class, IRfq.class, (record, header, payload) ->
        {
          // Simply compare the current system time to the expires timestamp on the RFQ and stream if appropriate.
          if(Instant.now().isAfter(payload.getExpires()))
          {
            printRfq(payload, "expired");
          }
          else
          {
            printRfq(payload, "STREAM");
            
            // Set off a thread to generate prices.
            executor.submit(new PriceStreamer(payload));
          }
        })
        .build();
      
      /* Here we are doing an indexed query on the FX collection to find only RFQ objects.
       * See MongoCreateIndexes to initialise the index.
       * 
       * We don't provide any consumers other than the RFQ one since we "know" that all the objects retrieved will be
       * RFQs, if it somehow turns out that some garbage is in the database the Error consumer will be called.  
       */
      consumerManager.accept(fxItems_.find(eq("header.payloadType", Rfq.TYPE_ID)));
      
      try
      {
        // Wait for all price generation threads to finish gracefully.
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
      }
      catch (InterruptedException e)
      {
        e.printStackTrace();
      }
  }
  
  /**
   * Class to stream prices.
   * 
   * @author Bruce Skingle
   *
   */
  class PriceStreamer implements Runnable
  {
    /** The last price sent */
    private double  price_ = 1.0;
    /** Number of further prices to send */
    private Integer streamFor_;
    /* The RFQ we are sending prices for */
    private IRfq    rfq_;
    
    /**
     * Constructor.
     * 
     * @param rfq The RFQ we are sending prices for
     */
    PriceStreamer(IRfq rfq)
    {
      // Initialise the price for a few ccy pairs, otherwise use 1, this is an Allegro example not a tutorial on FX trading.
      
      switch(rfq.getCcyPair().getCode())
      {
        case "USDGBP":
          price_ = 0.72;
          break;
          
        case "GBPUSD":
          price_ = 1.42;
          break;
          
        case "USDJPY":
          price_ = 104.73;
          break;
      }
      streamFor_ = rfq.getStreamFor();
      rfq_ = rfq;
    }

    @Override
    public void run()
    {
      while(streamFor_-- > 0)
      {
        // Jitter the price by a random amount.
        price_ += (random_.nextInt(200) - 100.0) / 1000.0;
        
        // Create a Quote
        IQuote quote = new Quote.Builder()
            .withCcyPair(rfq_.getCcyPair())
            .withId(rfq_.getId())
            .withExpires(Instant.now().plus(800, ChronoUnit.MILLIS))
            .withPrice(price_)
            .build();
        
        printQuote(quote);        
        
        // Create an encrypted Document from the Quote, note the header is generated from the Quote. 
        Document doc = allegro2MongoApi_.newEncryptedDocumentBuilder()
          .withThreadId(threadId_)
          .withHeader(quote.getHeader())
          .withPayload(quote)
        .build();
        
        // Store the document to Mongodb
        fxItems_.insertOne(doc);
        
        try
        {
          // pause....
          Thread.sleep(1000);
        }
        catch (InterruptedException e)
        {
          throw new RuntimeException(e);
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
    new MongoStreamPrices(args).run();
  }
}
