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

package com.symphony.s2.allegro.examples.benchmark;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.symphony.oss.allegro.api.AllegroApi;
import com.symphony.oss.allegro.api.AsyncConsumerManager;
import com.symphony.oss.allegro.api.ConsumerManager;
import com.symphony.oss.allegro.api.IAllegroApi;
import com.symphony.oss.allegro.api.IAllegroQueryManager;
import com.symphony.oss.allegro.api.request.FetchPartitionObjectsRequest;
import com.symphony.oss.allegro.api.request.PartitionId;
import com.symphony.oss.allegro.api.request.PartitionQuery;
import com.symphony.oss.allegro.api.request.UpsertPartitionRequest;
import com.symphony.oss.allegro.examples.calendar.canon.IToDoHeader;
import com.symphony.oss.allegro.examples.calendar.canon.IToDoItem;
import com.symphony.oss.allegro.examples.calendar.canon.ToDoHeader;
import com.symphony.oss.allegro.examples.calendar.canon.ToDoItem;
import com.symphony.oss.canon.json.JsonParser;
import com.symphony.oss.canon.json.model.JsonDom;
import com.symphony.oss.canon.json.model.JsonObject;
import com.symphony.oss.canon.json.model.JsonObjectDom;
import com.symphony.oss.fugue.cmd.CommandLineHandler;
import com.symphony.oss.fugue.trace.ITraceContext;
import com.symphony.oss.fugue.trace.ITraceContextTransaction;
import com.symphony.oss.fugue.trace.ITraceContextTransactionFactory;
import com.symphony.oss.fugue.trace.log.LoggerTraceContextTransactionFactory;
import com.symphony.oss.models.allegro.canon.SslTrustStrategy;
import com.symphony.oss.models.allegro.canon.facade.AllegroConfiguration;
import com.symphony.oss.models.allegro.canon.facade.ConnectionSettings;
import com.symphony.oss.models.core.canon.facade.PodAndUserId;
import com.symphony.oss.models.core.canon.facade.ThreadId;
import com.symphony.oss.models.object.canon.facade.IPartition;
import com.symphony.oss.models.object.canon.facade.IStoredApplicationObject;

/**
 * An example application which creates a ToDoItem, adding it to a current and absolute sequence.
 * 
 * @author Bruce Skingle
 *
 */
public class Benchmark extends CommandLineHandler implements Runnable, RequestHandler<Map<String,String>, String>
{
  public static final Logger log_ = LoggerFactory.getLogger(Benchmark.class);
  
  public static final String PARTITION_NAME  = "Benchmark";
  public static final String FEED_NAME       = "BenchmarkFeed";
  
  private static final String ALLEGRO         = "ALLEGRO_";
  private static final String SERVICE_ACCOUNT = "SERVICE_ACCOUNT";
  private static final String POD_URL         = "POD_URL";
  private static final String OBJECT_STORE_URL = "OBJECT_STORE_URL";
  private static final String CREDENTIAL_FILE = "CREDENTIAL_FILE";
  private static final String PEM_CREDENTIAL = "PEM_CREDENTIAL";
  private static final String THREAD_ID       = "THREAD_ID";
  private static final String OWNER_USER_ID    = "OWNER_USER_ID";
  
  private String              serviceAccount_;
  private String              podUrl_;
  private String              objectStoreUrl_;
  private String              credentialFile_;
  private String              pemCredential_;
  private ThreadId            threadId_;
  private Long                ownerId_;
  
  private IAllegroApi         allegroApi_;

  /**
   * Constructor.
   */
  public Benchmark()
  {
    withFlag('s',   SERVICE_ACCOUNT,  ALLEGRO + SERVICE_ACCOUNT,  String.class,   true, true,   (v) -> serviceAccount_       = v);
    withFlag('p',   POD_URL,          ALLEGRO + POD_URL,          String.class,   true, true,   (v) -> podUrl_               = v);
    withFlag('o',   OBJECT_STORE_URL, ALLEGRO + OBJECT_STORE_URL, String.class,   true, true,   (v) -> objectStoreUrl_       = v);
    withFlag('f',   CREDENTIAL_FILE,  ALLEGRO + CREDENTIAL_FILE,  String.class,   true, false,  (v) -> credentialFile_       = v);
    withFlag('c',   PEM_CREDENTIAL,   ALLEGRO + PEM_CREDENTIAL,   String.class,   true, false,  (v) -> pemCredential_       = v);
    withFlag('t',   THREAD_ID,        ALLEGRO + THREAD_ID,        String.class,   true, true,   (v) -> threadId_             = ThreadId.newBuilder().build(v));
    withFlag('u',   OWNER_USER_ID,    ALLEGRO + OWNER_USER_ID,    Long.class,     true, false,  (v) -> ownerId_              = v);
  }
  
  @Override
  public void run()
  { 
    AllegroConfiguration.Builder configBuilder = new AllegroConfiguration.Builder()
    .withPodUrl(podUrl_)
    .withApiUrl(objectStoreUrl_)
    .withUserName(serviceAccount_)
    .withRsaPemCredentialFile(credentialFile_)
    .withApiConnectionSettings(new ConnectionSettings.Builder()
        .withSslTrustStrategy(SslTrustStrategy.TRUST_ALL_CERTS)
        .build());
    
    if(pemCredential_ != null)
      configBuilder.withRsaPemCredential(pemCredential_);
    
    ITraceContextTransactionFactory traceFactory = new LoggerTraceContextTransactionFactory();
    
    allegroApi_ = new AllegroApi.Builder()
            .withConfiguration(configBuilder.build())
            .withTraceFactory(traceFactory)
            .build();
    
    PodAndUserId ownerUserId = ownerId_ == null ? allegroApi_.getUserId() : PodAndUserId.newBuilder().build(ownerId_);
    
    try(ITraceContextTransaction t = traceFactory.createTransaction("Benchmark", "run1"))
    {
      ITraceContext trace = t.open();
      
      trace.trace("START");
      
    System.out.println("CallerId is " + allegroApi_.getUserId());
    System.out.println("OwnerId is " + ownerUserId);
    System.out.println("PodId is " + allegroApi_.getPodId());

    benchmark("10", 10);
    benchmark("100", 100);
    benchmark("1000", 1000);
    }
  }
  
  private void benchmark(String name, int cnt)
  {

    createPartition(name);
    int read = listItems(name);
    int more = cnt - read;
    
    if(more >0)
    {
      createItems(name, more);
    }
    
    listItems(name);
//    listItemsAsync(name);
  }

  private int listItems(String name)
  {
    List<IStoredApplicationObject> storedItems = new LinkedList<>();
    List<IToDoItem> todoItems = new LinkedList<>();
    
    long start = System.currentTimeMillis();
    
    allegroApi_.fetchPartitionObjects(new FetchPartitionObjectsRequest.Builder()
        .withQuery(new PartitionQuery.Builder()
            .withName(PARTITION_NAME + name)
            .build()
            )
          .withConsumerManager(new ConsumerManager.Builder()
//              .withConsumer(IToDoItem.class, (item, trace) ->
//              {
//                synchronized(todoItems)
//                {
//                  todoItems.add(item);
//                }
//              })
              .withConsumer(IStoredApplicationObject.class, (item, trace) ->
              {
                synchronized(storedItems)
                {
                  storedItems.add(item);
                }
              })
              .build()
              )
          .build()
          );
    
    long end = System.currentTimeMillis();
    long time = end - start;
    
    log_.info("Read " + todoItems.size() + " ToDoItems and " + storedItems.size() + " storedApplicationObjects in " + time + "ms.");
    
    return todoItems.size() + storedItems.size();
  }
  


  private int listItemsAsync(String name)
  {
    List<IStoredApplicationObject> storedItems = new LinkedList<>();
    List<IToDoItem> todoItems = new LinkedList<>();
    
    
    
    IAllegroQueryManager queryManager = allegroApi_.fetchPartitionObjects(new FetchPartitionObjectsRequest.Builder()
        .withQuery(new PartitionQuery.Builder()
            .withName(PARTITION_NAME + name)
            .build()
            )
          .withConsumerManager(new AsyncConsumerManager.Builder()
              .withConsumer(IToDoItem.class, (item, trace) ->
              {
                synchronized(todoItems)
                {
                  todoItems.add(item);
                }
              })
              .withConsumer(IStoredApplicationObject.class, (item, trace) ->
              {
                synchronized(storedItems)
                {
                  storedItems.add(item);
                }
              })
              .build()
              )
          .build()
          );
    
    long start = System.currentTimeMillis();
    queryManager.start();
    
    while(!queryManager.isIdle())
    {
      System.out.println("busy....");
      try
      {
        Thread.sleep(100);
      }
      catch (InterruptedException e)
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    long end = System.currentTimeMillis();
    long time = end - start;
    
    log_.info("Read Async " + todoItems.size() + " ToDoItems and " + storedItems.size() + " storedApplicationObjects in " + time + "ms.");
    
    return todoItems.size() + storedItems.size();
  }

  private void createItems(String name, int cnt)
  {
    List<IStoredApplicationObject> items = new LinkedList<>();
    
    long start = System.currentTimeMillis();
    
    for(int i=0 ; i<cnt ; i++)
    {
      IToDoItem toDoItem = new ToDoItem.Builder()
          .withDue(Instant.now())
          .withTimeTaken(new BigDecimal(1000.0 / 3.0))
          .withTitle("An example TODO Item " + cnt)
          .withDescription("Since we are creating this item with a due date of Instant.now() we are already late!")
          .build();
        
        IToDoHeader header = new ToDoHeader.Builder()
            .withRequestingUser(allegroApi_.getUserId())
            .withAffectedUsers(allegroApi_.getUserId())
            .withEffectiveDate(Instant.now())
            .build();
        
        IStoredApplicationObject toDoObject = allegroApi_.newApplicationObjectBuilder()
            .withThreadId(threadId_)
            .withHeader(header)
            .withPayload(toDoItem)
            .withPartition(new PartitionId.Builder()
                .withName(PARTITION_NAME + name)
                .build()
                )
            .withSortKey(toDoItem.getDue().toString() + i)
            .withPurgeDate(Instant.now().plusMillis(24 * 60 * 60000))
          .build();
        
        items.add(toDoObject);
    }
    
    long end = System.currentTimeMillis();
    long time = end - start;
    
    log_.info("Created " + items.size() + " storedApplicationObjects in " + time + "ms.");
    
    start = System.currentTimeMillis();
    
    for(IStoredApplicationObject item : items)
    {
      allegroApi_.store(item);
    }
    
    end = System.currentTimeMillis();
    time = end - start;
    
    log_.info("Stored " + items.size() + " storedApplicationObjects in " + time + "ms.");
  }

  private void createPartition(String name)
  {
    long start = System.currentTimeMillis();
    IPartition partition = allegroApi_.upsertPartition(new UpsertPartitionRequest.Builder()
          .withName(PARTITION_NAME + name)
          .build()
        );
    

    long end = System.currentTimeMillis();
    long time = end - start;
    
    log_.info("Created partition in " + time + "ms, " + partition.getId().getHash() + " " + partition);
  }

  /**
   * Main.
   * 
   * @param args Command line arguments.
   */
  public static void main(String[] args)
  {
    Benchmark program = new Benchmark();
    
    if(args.length == 0)
    { 
      JsonDom jsonDom = new JsonParser.Builder()
          .withInput(System.in)
          .build()
          .parse();
      
      if(jsonDom instanceof JsonObjectDom)
      {
        JsonObject json = ((JsonObjectDom)jsonDom).getObject();
        
        System.out.println(json);
        
        List<String> argv = new LinkedList<>();
        
        for(String name : json.getNames())
        {
          System.out.println(name + " = " + json.getRequiredString(name));
          argv.add("--" + name);
          argv.add(json.getRequiredString(name));
        }
        
        args = argv.toArray(new String[argv.size()]);
      }
      else
      {
        System.err.println("A Json object is required.");
        System.err.println(jsonDom.getErrors());
        
        return;
      }
    }
    
    program.process(args);
    
    try
    {
      program.run();
    }
    catch(RuntimeException e)
    {
      e.printStackTrace();
    }
    
    
  }

  @Override
  public String handleRequest(Map<String, String> input, Context context)
  {
    List<String> argv = new LinkedList<>();
    
    for(Entry<String, String> entry : input.entrySet())
    {
      System.out.println(entry.getKey() + " = " + entry.getValue());
      argv.add("--" + entry.getKey());
      argv.add(entry.getValue());
    }
    
    String[] args = argv.toArray(new String[argv.size()]);
    
    process(args);
    
    try
    {
      run();
    }
    catch(RuntimeException e)
    {
      e.printStackTrace();
    }
    return "Done";
  }
}
