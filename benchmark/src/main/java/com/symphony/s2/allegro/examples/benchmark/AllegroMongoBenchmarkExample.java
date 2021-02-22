/*
 * Copyright 2021 Symphony Communication Services, LLC.
 *
 * All Rights Reserved
 */

package com.symphony.s2.allegro.examples.benchmark;

import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.symphony.oss.allegro.examples.calendar.canon.CalendarModel;
import com.symphony.oss.allegro2.api.Allegro2Api;
import com.symphony.oss.allegro2.api.IAllegro2Api;
import com.symphony.oss.fugue.cmd.CommandLineHandler;
import com.symphony.oss.models.allegro.canon.facade.Allegro2Configuration;
import com.symphony.oss.models.core.canon.facade.ThreadId;

public abstract class AllegroMongoBenchmarkExample extends CommandLineHandler implements Runnable
{
  private static final String ALLEGRO         = "ALLEGRO_";
  private static final String SERVICE_ACCOUNT = "SERVICE_ACCOUNT";
  private static final String POD_URL         = "POD_URL";
  private static final String MONGO_HOST      = "MONGO_HOST";
  private static final String MONGO_USER      = "MONGO_USER";
  private static final String MONGO_PASSWORD  = "MONGO_PASSWORD";
  private static final String CREDENTIAL_FILE = "CREDENTIAL_FILE";
  private static final String THREAD_ID       = "THREAD_ID";

  private String                      serviceAccount_;
  private String                      podUrl_;
  private String                      credentialFile_;

  protected ThreadId                  threadId_;
  protected IAllegro2Api            allegroPodApi_;
  protected String                    mongoHost_;
  protected String                    mongoUser_;
  protected String                    mongoPassword_;
  protected String                    connectionString_;
  protected MongoClient               mongoClient_;
  protected MongoDatabase             db_;
  protected MongoCollection<Document> items_;

  /**
   * Constructor.
   */
  public AllegroMongoBenchmarkExample()
  {
    withFlag('s',   SERVICE_ACCOUNT,  ALLEGRO + SERVICE_ACCOUNT,  String.class,   false, true,   (v) -> serviceAccount_       = v);
    withFlag('p',   POD_URL,          ALLEGRO + POD_URL,          String.class,   false, true,   (v) -> podUrl_               = v);
    withFlag('H',   MONGO_HOST,       ALLEGRO + MONGO_HOST,       String.class,   false, true,   (v) -> mongoHost_            = v);
    withFlag('U',   MONGO_USER,       ALLEGRO + MONGO_USER,       String.class,   false, true,   (v) -> mongoUser_            = v);
    withFlag('P',   MONGO_PASSWORD,   ALLEGRO + MONGO_PASSWORD,   String.class,   false, true,   (v) -> mongoPassword_        = v);
    withFlag('f',   CREDENTIAL_FILE,  ALLEGRO + CREDENTIAL_FILE,  String.class,   false, false,  (v) -> credentialFile_       = v);
    withFlag('t',   THREAD_ID,        ALLEGRO + THREAD_ID,        String.class,   false, true,   (v) -> threadId_             = ThreadId.newBuilder().build(v));
  }
  
  /**
   * Method to be called from Main().
   * 
   * @param args Command line arguments.
   */
  public void run(String[] args)
  {
    process(args);
    
    allegroPodApi_ = new Allegro2Api.Builder()
        .withFactories(CalendarModel.FACTORIES)
        .withConfiguration(new Allegro2Configuration.Builder()
                .withPodUrl(podUrl_)
                .withUserName(serviceAccount_)
                .withRsaPemCredentialFile(credentialFile_)
                .build())
        .build();
    
    connectionString_ = "mongodb+srv://" + mongoUser_ + ":" + mongoPassword_ + "@" + mongoHost_ + "/Benchmark?retryWrites=true&w=majority";
    
    try (MongoClient mongoClient = MongoClients.create(connectionString_))
    {
      mongoClient_ = mongoClient;
      db_ = mongoClient_.getDatabase("Benchmark");
      items_ = db_.getCollection("Items");
      run();
    }
    catch(RuntimeException e)
    {
      e.printStackTrace();
    }
  }
}
