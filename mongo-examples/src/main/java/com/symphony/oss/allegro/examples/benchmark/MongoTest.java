/*
 * Copyright 2021 Symphony Communication Services, LLC.
 *
 * All Rights Reserved
 */

package com.symphony.oss.allegro.examples.benchmark;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.bson.Document;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.symphony.oss.allegro.examples.calendar.canon.CalendarModel;
import com.symphony.oss.allegro.examples.model.fx.canon.Ccy;
import com.symphony.oss.allegro.examples.model.fx.canon.FxModel;
import com.symphony.oss.allegro.examples.model.fx.canon.IFxHeader;
import com.symphony.oss.allegro.examples.model.fx.canon.IRfq;
import com.symphony.oss.allegro.examples.model.fx.canon.Rfq;
import com.symphony.oss.allegro.examples.model.fx.canon.facade.CcyPair;
import com.symphony.oss.allegro.examples.model.fx.canon.facade.ICcyPair;
import com.symphony.oss.allegro2.mongo.api.Allegro2MongoApi;
import com.symphony.oss.allegro2.mongo.api.AllegroMongoConsumerManager;
import com.symphony.oss.allegro2.mongo.api.IAllegro2MongoApi;
import com.symphony.oss.fugue.Fugue;
import com.symphony.oss.fugue.aws.config.S3Helper;
import com.symphony.oss.models.allegro.canon.facade.AllegroConfiguration;
import com.symphony.oss.models.core.canon.IApplicationPayload;
import com.symphony.oss.models.core.canon.facade.PodAndUserId;
import com.symphony.oss.models.core.canon.facade.ThreadId;

public class MongoTest extends MongoUtils implements RequestHandler<Map<String, String>, String>
{

  
  ExecutorService executor = Executors.newCachedThreadPool();
  
  String ss = "-----BEGIN RSA PRIVATE KEY-----\n"
      + "MIIJKQIBAAKCAgEAyfASpUvwBS8kUMmRufhw6YK/bK4IaCfwf4ErOJSU6fSSejbN\n"
      + "zNMCsVGGXfU2IE9r9eNDKjvgKlSGYsw/z6E04zMESBN6v1JFmbz6suFLCYMvJnRj\n"
      + "Yzu4GudgIkYEdL/8GIs5qqgqoU6J3pmG8PGlWFFAFZuEugCnhnDnhvrZTW6TUS9q\n"
      + "1ExlFDr5+l3qgybvwDFmSTDTtMuRToZyGgXT8iIMzJidn5C0+ZqnOFKu2jk1pu5y\n"
      + "nvUE7/Q5c48sKBpDPFhdBUnPn4gyvxX+YDd8hbB7SgKjiIAb2kyzPr+DnTQBqM42\n"
      + "8G7++TroOmngSQ9iD3HlzBuvoBGKd75YAvie5jxJ0nlKeTc2SexTgDJ7wAQHhUiE\n"
      + "WnxlTwFB701oz2/jdb+CU3nIjD3gfyCPBdsg+4Ga3Hu28eJKvnm/gTmg/3j85FZy\n"
      + "QeIH+I1+MZJ6T6LQZwfZCh8d8ANsSyWfoBoXR2Lo19Q3+RyFphb/jJhkVrVT689+\n"
      + "/6NzKfZ+3hhUO0R8evoRTDdjgfPQZwihvzkTjT3g5TEVz00c8iN/We1bWWGjU6QN\n"
      + "+zZrsmVsnCvCyc8REOmpDkFFabLEucElTLwkaFBXerrmg6N9N+IuzWV0g8QMUrZt\n"
      + "ASevLWkrZ/iKUUCRmp2lpXazYPRxm4mht6f8KT9MwG+UjgwJiF8de9iP14sCAwEA\n"
      + "AQKCAgAZMhQRfb0P2IbucYf2LTJhYEfkFn7ECB+wuPwcjiVaX4KbLxVUdguC9koO\n"
      + "lRbQxzgQMO/jaB8bCPrPfu6DSHjh1C63k36gOnKmsPaStRt4r16FrMXtzri02qOb\n"
      + "AyNxMbcRlOV4Do1rHPKEzKESlQPtVg3p5EKWNpiwZIvuwoO0bpSA76qjnHiMIqOo\n"
      + "AmnNPWFymHdLsITprPCPXKUF7C46qtbv/CbGjvaVoh81HtPkNHkmP+AbRzT7f3xf\n"
      + "l8CJrGUxNVE95Ky8GfFC+EqJ1MJ7p75qSJQA5wMSpLlEnf6c8a03U6t1KlQz4o6x\n"
      + "Ix6x3vJ8PNjQM7AhB69AK4atsDmbBVX8IUaIg7U3OUR5Hac5cNEk1uxxUxsp1AUY\n"
      + "PXc/DN6NYTvaWFY37OET6Hy51krkkpKRxpXhYFoxKiuahHFA5r6MKRhlY/3pT+R1\n"
      + "GGGhXcUErui2atLQYM0XN0w3YIDPdcFtDFLLaNhBFt+yssfD+cfI7wfKn+s7mxyG\n"
      + "N/dYTW/v3mpfZs9ZYHCTUhLN43Iajj67dt8RyPoVc/hClx7CnGeH3jQyl41waEyC\n"
      + "wPRJmmVcl7L83me9ZbWmKhViIDjVyROKrgYk5VxUDh/oNEcX1sK2Z2g8vRWd2B3V\n"
      + "9Cos4XUBgBcNSHljjK6/CAPGRD9rrWiKkrDic50vN4fxfs1UGQKCAQEA/re4Ez2d\n"
      + "6Xg0PGnZqdeZnU3+wJk6S47QAmTuKCIT9C8m/7N3K14CpDbISP3NpTRpDnYdCbAD\n"
      + "hKOxct22M98xUzWzrgIytfdwjzkiqEBHvDTiOXVWWyPa/2enwFLGiWd0CLCyHend\n"
      + "bS1tlm8ZT3c5tcWubghpgiqIgn76+j1NwiJ1++LRd2UNwzYjmus7ncNb0DsH/nlj\n"
      + "AZGUuMdgddmnch+ZCYrJCREbtEQu6dM+iyyU2+zKqGIYJdJNEspUAKQD3aCLwDS1\n"
      + "OUtSq+3MOv2GPLiSUn99BsCieKaG4cGKOSywOj6oobra5EdVs/ZS+68SPaYYiMt3\n"
      + "p+iAVqjQ8wD+PwKCAQEAyvRUt02jWZXLWTNuDSeLPamZ0GraE5TgwzmqgC1zgQn8\n"
      + "6ylSFBqyE9r3oWrDlxxJ5WksvARdd97LBQrIvFE2nsRQEgUZJaMaRGfHSfEXutPr\n"
      + "nypmS1+zCStNYd/JCy+/nHAZgpcgDEZ4taMHaokoGGPLxa/DsPUeG61x1RAwM1sT\n"
      + "CfIP5HGIo+CuWqSTgMPPnQHCmVBLW7wAxt12jxnzdOkf9CSvghoQxHojI3jvNUXp\n"
      + "0LMCt1S4SYJxMpyV0EpzGkreRlasVi64ZZ+C1cTpHu5nkVWryPJ1WIgyFL7J+7hZ\n"
      + "1uCy5hsuHl6QoqUHcGuF3vyoXSToSofxoDqHbvirtQKCAQEA3i/F2srBx1j6P8SZ\n"
      + "gIS307aoRLqkVipiGeSOteVcAIFtBFk411Ru+21p8fmqe7Qm+91d4QhvWTEs5aSu\n"
      + "fbrwHMRYUq5KK96Gifht2dQQfwSmXTaUOHYjXuy9MI9D7bGXTslK5bsNUmHxGOsa\n"
      + "ae33Ii8ow4SE/G07nJFFPilhfblvr2OgEgTNJ34/OrB81VYG7bHAfGLIUSL7Vt9b\n"
      + "rhFI9czIQsGUMVuCDhH31jFVejNOOjToZk8C/2LdpkllUxW+5YFYxjVz62Ff4OdS\n"
      + "YhaqFKdvw5S/q7c3QgUI3T5k4xCvPG9Ry5nhvITZJlz07+Q8BhiviAvr+URfRriF\n"
      + "Y6N3IwKCAQBfRdVi+zo8eror1J3L5Q1TVAOVuCNJX/EnFDH629tF7xdgmVQjheik\n"
      + "s3zdtI2+qRPzcq0CUhZCiD7LziUvafx3CXcBDo2ggnF5PTJrfpcCKCuK20+MAI++\n"
      + "NSqtUG1DKBYN7P28tQ7hVE+kDLUViO9ei7KDyZ/WuSp6GbC/MUs/QHiuiYh2V+mp\n"
      + "7HZMrMdlggY7ETF14SCPHrFKqXnXcXo9HpbWeEY/j9bhNOi75TB91KUPSIl04Htm\n"
      + "Xqqe9KzLS7e+1viDEnryNhpv8jJsAXTiBU5vBkWPHmqBteW0oV+WIBNkcLTqmkXL\n"
      + "Ed9Zypi0aHU+mWK73vCA1FJUQkWDjY6lAoIBAQC4ryJzbSmbsQRcZUxZW/j8JQ6r\n"
      + "N9GR0nZX8/hP+cNwJiXbtdtGZjP12gbY8faxJCCmBrBE2H6q/wzKEax2Z4hsl2Ka\n"
      + "FllYy9/n4FvoMQc18AAM/CcA1lGS8vFSVg5/YjYyxsmGNGWbZrNzqNb7iCQAoPC1\n"
      + "Ci2w4BGCPqyxWWyBKwSyNLSKNzM1C3t9/n/rBFoUWoE5xshK71zTOHa+nFQqLU3s\n"
      + "SUKHv1eGKbWGjjK9lRbqvlWh0gKiyMciCedgnHJ5kdx53gAd/IWY2UDJLK03yq54\n"
      + "KhugIveI3DsbgZ2c1n8o+UaXtFOuXTI8ntqbTFoh76niDvU9QOusDmCVEaob\n"
      + "-----END RSA PRIVATE KEY-----\n"
      + "";
  
  


  public static void main(String[] args)
  {
    AmazonS3 s3 = AmazonS3ClientBuilder
        .standard()
        .withRegion("eu-west-1")
        .build();
    
    HashMap<String, String> input = new HashMap<>();

    input.put("POD_URL", "https://psdev.symphony.com/ ");
    input.put("SERVICE_ACCOUNT", "allegroBot");
    input.put("THREAD_ID", "+ZNefHakKm9vK7WREHHLEn///pEOR60jdA==");
    input.put("MONGO_USER", "geremia");
    input.put("MONGO_HOST", "M30-Cluster.wqz4a.mongodb.net/brucedb?retryWrites=true&w=majority");
    input.put("MONGO_PASSWORD", "XXXXXXXX");
    input.put("CONSUMERS", 5+"");
    input.put("DIRECTORY","test/test");
    new MongoTest().handleRequest(input, null);
  }

  @Override
  public String handleRequest(Map<String, String> input, Context context)
  {

    System.out.println("***** "+(coldStart? "COLD": "WARM" )+" START *****");
    coldStart = false;
    
    int lambdaN_= init(input);
    
    System.out.println("WORKER STARTING FROM "+lambdaN_);
    
    String directory     = input.getOrDefault("DIRECTORY", "");
    
    ArrayList<RFQThread> tt = new ArrayList<>();

    for (long i = lambdaN_; i < CONSUMERS_ + lambdaN_; i++)
    {
      tt.add(new RFQThread(collection_ + i, true, i, i));
      executor.submit(tt.get(tt.size()-1));


      for (int j = 0; j < RATIO; j++)
      {
        tt.add(new RFQThread(collection_ + i, false, i, j));
        executor.submit(tt.get(tt.size()-1));
      }
    }
    
    waitForComplete(tt); 
    
    String content =  process(tt);  
    
    saveOnS3(content.toString(), directory, lambdaN_+".csv");

    return "Done";
  }
  
  private void waitForComplete(ArrayList<RFQThread> tt)
  {
    try
    {
      Thread.sleep(6 * 60 * 1000);
    }
    catch (InterruptedException e1)
    {
      e1.printStackTrace();
      throw e1;
    }
    int retry= 0;
    boolean done = false;
    
    while (!done && retry <  60)
    {
      try
      {
        Thread.sleep(1000);
      }
      catch (InterruptedException e)
      {
        e.printStackTrace();
        throw e;
      }
      done = true;
      for (RFQThread t : tt)
        done = done && t.closed.get();
      
      if(!done)
        retry++;
    }
    
    for (RFQThread t : tt)
      t.close();
  }

  private String process(ArrayList<RFQThread> tt)
  {
    
    ArrayList<Sample> all   = new ArrayList<>();
    ArrayList<Long> times   = new ArrayList<>();
    ArrayList<Double> rates = new ArrayList<>();
    
    for (RFQThread t : tt)
    {
      if (t.consumer_)
      {
        ArrayList<Long> ss = new ArrayList<>();
        rates.addAll(t.rates);
        for (Sample s : t.samples)
          ss.add(s.latency);

        all.addAll(t.samples);
        times.addAll(ss);

        System.out.println("CONSUMER " + t.id_ + " RECEIVED RECORDS " + t.samples.size() + " AVERAGE: " + MongoUtils.average(ss)
            + " PERCENTILE: " + MongoUtils.percentile(ss, 90) + " RATE: " + MongoUtils.average(t.rates, true) + " PERCENTILE RATE: "
            + MongoUtils.percentile(t.rates) + " MAX: " + MongoUtils.maximum(ss));
      }
    }

    System.out.println("TOTAL RATE: " + MongoUtils.average(rates, true) + " PERCENTILE RATE: " + MongoUtils.percentile(rates));
    System.out.println("TOTAL AVERAGE: " + MongoUtils.average(times) + " PERCENTILE: " + MongoUtils.percentile(times, 90));

    StringBuilder sb = new StringBuilder();

    for (Sample s : all)
      sb.append(s.receiveTime + "," + s.sendTime + "," + s.consumer + "," + s.producer + "," + s.latency + "," + s.index
          + "\n");
    
    return sb.toString();
    
  }


  public int init(Map<String, String> input) {
    
    podUrl_         = input.get(POD_URL);
    serviceAccount_ = input.get(SERVICE_ACCOUNT);
    mongoHost_      = input.get(MONGO_HOST);
    mongoPassword_  = input.get(MONGO_PASSWORD);
    mongoUser_      = input.get(MONGO_USER);
    String lambdaN_s = input.get(LAMBDA_N);

    int lambdaN_ = lambdaN_s == null? 0 : Integer.parseInt(lambdaN_s);
    
    threadId_       = ThreadId.newBuilder().build(input.get(THREAD_ID));
    
    CONSUMERS_ = Integer.parseInt(input.get(CONSUMERS));
    PRODUCERS_ = CONSUMERS_ * 5;

    allegro2MongoApi_ = new Allegro2MongoApi.Builder()        
        .withConfiguration(new AllegroConfiguration.Builder()
                .withPodUrl(podUrl_)
                .withUserName(serviceAccount_)  
                .withRsaPemCredential(ss)
                .build())
        .build();
    
    allegro2MongoApi_.getModelRegistry()
    .withFactories(FxModel.FACTORIES);
    
    ownerUserId_ = ownerId_ == null ? allegro2MongoApi_.getUserId() : PodAndUserId.newBuilder().build(ownerId_);
    return lambdaN_;
  }

  public class RFQThread implements Runnable
  {
    MongoDatabase                  db_;
    MongoCollection<Document>      todoItems_;
    String                         collection_;

    private boolean                consumer_;
    private AtomicBoolean          closed   = new AtomicBoolean(false);

    private int                    MESSAGES = 5 * 60 * 5;

    ArrayList<Long>                measures = new ArrayList<>();
    ArrayList<Sample>              samples  = new ArrayList<>();
    Instant                        last     = Instant.now();
    AtomicInteger                  count    = new AtomicInteger();
    ArrayList<Double>              rates    = new ArrayList<>();
    long                           last_m;
    Long                           id_;
    long                           start    = Instant.now().toEpochMilli();
    long                           pid_;
    TreeMap<Long, Integer>         indexes  = new TreeMap<>();
    ChangeStreamIterable<Document> changeStream;
    int                            lost = 0;

   public RFQThread(String collection, boolean consumer, Long id, long pid)
   {
     collection_ = collection;
     consumer_   = consumer;
     id_         = id;
     pid_        = pid;
     
   }
   
   public synchronized void close()
   {
     if (changeStream != null)
       changeStream.cursor().close();
      closed.set(true);
   }

   
   @Override
   public void run()
   {
     String connectionString = "mongodb+srv://" + mongoUser_ + ":" + mongoPassword_ + "@" + mongoHost_
         + "/Calendar?retryWrites=true&w=majority";

     try (MongoClient mongoClient = MongoClients.create(connectionString))
     {

       db_ = mongoClient.getDatabase("Calendar");

       todoItems_ = db_.getCollection(collection_);

       if (consumer_)
       {
         todoItems_.drop();
         System.out.println("CONSUMER " + id_ + " COLLECTION " + collection_);
         consume();
         // close();
       }
       else
       {
         try
         {
           Thread.sleep(5000);
         }
         catch (InterruptedException e)
         {
           e.printStackTrace();
           throw e;
         }
         System.out.println("PRODUCER " + id_ + " COLLECTION " + collection_);

         produce();
       }

     }
     catch (RuntimeException e)
     {
       e.printStackTrace();

       System.out.println((consumer_ ? "Consumer " : "Producer") + id_ + " " + (consumer_ ? "" : pid_)
           + " prematurely closed due to " + e.getMessage());

       close();

     }
   }
   
  public void consume()
  {
    changeStream = todoItems_.watch();  

     AllegroMongoConsumerManager consumerManager = allegro2MongoApi_.newConsumerManagerBuilder()
         .withConsumer(IFxHeader.class, IRfq.class, (record, header, rfq) ->
         {
           if (!closed.get())
             process(rfq);
           else {
             System.out.println("Consumer "+id_ +" stream closed, received "+header.toString());
           }
         })
         .withConsumer(IApplicationPayload.class, IApplicationPayload.class, (record, header, payload) ->
         {
           System.out.println("Untyped Header:  " + header);
           System.out.println("Untyped Payload: " + payload);
           System.out.println("EncryptedApplicationRecord:  " + record);

         })
         .build();

    Consumer<ChangeStreamDocument<Document>> consumer = (change) ->
    {
      consumerManager.accept(change.getFullDocument());
    };
    
    changeStream.forEach(consumer);

  }
  
  public void process(IRfq rfq)
  {
    long sent = rfq.getExpires().toEpochMilli();
    long now = Instant.now().toEpochMilli();
    long diff = (now - sent);
    
    if(sent < start)
      System.out.println("ERROR RECEIVING RECORD OLDER THAN CREATION : "+rfq.toString());

    if(Integer.parseInt(rfq.getId()) != id_.longValue())
      System.out.println("ERROR FILTER NOW WORKING, EXPECTED "+id_+ " GOT "+Integer.parseInt(rfq.getId()) );
    
    long producer =  rfq.getQuantity();
    long seq_n = rfq.getStreamFor();
    
    Sample s = new Sample(now, sent, id_, producer, diff, indexes.getOrDefault(producer, 0));
    
    if (s.index != seq_n)
    {
      System.out.println("ERROR SEQUENCE NUMBER, CONSUMER " + id_ + " EXPECTED " + s.index + " GOT " + seq_n);
      s.index = seq_n;
      lost+=(seq_n - s.index);
    } 
    indexes.put(producer, (int) seq_n + 1);

    samples.add(s);

    count.addAndGet(1);

    if (count.get() % 100 == 0)
    {
      long next = System.currentTimeMillis();
      double rate = (100.0 * 1000 / (next - last_m));

      rates.add(rate);
      last_m = next;
    }
    if(count.get() % 500 == 0) {
      System.out.println( "CONSUMER " + id_+ "RECEIVED "+count.get()+" STARTED AT "+Instant.ofEpochMilli(start));
    }
    
    if((count.get()+ lost) == MESSAGES * 5) {
      System.out.println( "CONSUMER " + id_+ " PROCESSING TERMINATED, RECEIVED "+count.get()+" STARTED AT "+Instant.ofEpochMilli(start)
      +", FIRST RECORD RECEIVED AT "+Instant.ofEpochMilli(samples.get(0).receiveTime)+", SENT AT "+Instant.ofEpochMilli(samples.get(0).sendTime)+
      ", LAST RECORD RECEIVED AT "+Instant.ofEpochMilli(samples.get(samples.size()-1).receiveTime)+", SENT AT "+Instant.ofEpochMilli(samples.get(samples.size()-1).sendTime));
      close();
      
      
    }
  }
  
  public void send() 
  {
    Instant now = Instant.now();

    ICcyPair ccyPair = new CcyPair.Builder()
        .withBase(Ccy.GBP)
        .withCounter(Ccy.USD)
        .build();

    IRfq rfqItem = new Rfq.Builder()
      .withCcyPair(ccyPair)
      .withId(id_+"")
      .withQuantity(pid_)
      .withStreamFor(count.getAndIncrement())
      .withExpires(now)
      .build();

     Document toDoObject = allegro2MongoApi_.newEncryptedDocumentBuilder()
        .withThreadId(threadId_)
        .withHeader(rfqItem.getHeader())
        .withPayload(rfqItem) 
      .build();
     
    todoItems_.insertOne(toDoObject);
    long took = System.currentTimeMillis() - now.toEpochMilli();
    if (took > period)
      System.out.println("TOOK " + took + (took < period ? "" : " WARNING, MAX IS " + period));

  }
  
  private void produce()
  {
    long next = System.currentTimeMillis() + period;

    Random r = new Random();
    try
    {
      Thread.sleep((int) (r.nextDouble() * 200 / 2));
    }
    catch (InterruptedException e1)
    {
      e1.printStackTrace();
      throw e1;
    }
    Instant start = Instant.now();
    
    while (!closed.get())
    {
      send();

      long sleeptime = next - System.currentTimeMillis();
      if (sleeptime > 0)
      {
        try
        {
          Thread.sleep(sleeptime);
        }
        catch (InterruptedException e)
        {
          close();
          e.printStackTrace();
        }

      }
      else
        System.out.println("FALLING BACK OF ms " + sleeptime);

      next = next + period;

      if (count.get() == MESSAGES)
        close();
    }
    System.out.println("PRODUCER " + id_ + "-" + pid_ + " PRODUCED RECORDS: " + count.get() + " START@END: " + start
        + "@" + Instant.now());
  }
}

public class Sample implements Comparable
{
  long     receiveTime;
  long     sendTime;
  String   producer;
  String   consumer;
  long     latency;
  long     index;

  public Sample(long now, long sent, Long id, long producer_, long diff, Integer index_)
  {
    
    receiveTime = now;
    sendTime = sent;
    latency = diff;
    consumer = id + "";
    producer = producer_ + "";
    index = index_;
  }

  @Override
  public int compareTo(Object o)
  {
    Sample obj = (Sample) o;
    return (int) (sendTime - obj.sendTime);
  }

  @Override
  public String toString()
  {
    return receiveTime + "," + sendTime + "," + consumer + "," + producer + "," + latency + "," + index;
  }

  @Override
  public boolean equals(Object o)
  {
    Sample obj = (Sample) o;
    return obj.hashCode() == hashCode() && this.receiveTime == obj.receiveTime && sendTime == obj.sendTime
        && producer.equals(obj.producer) && index == obj.index;
  }

}

}
