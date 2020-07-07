/*
 *
 *
 * Copyright 2020 Symphony Communication Services, LLC.
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

package com.symphony.s2.object.timer.feed;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.symphony.oss.allegro.api.AllegroApi;
import com.symphony.oss.allegro.api.AsyncConsumerManager;
import com.symphony.oss.allegro.api.ConsumerManager;
import com.symphony.oss.allegro.api.IAllegroApi;
import com.symphony.oss.allegro.api.IAllegroQueryManager;
import com.symphony.oss.allegro.api.ResourcePermissions;
import com.symphony.oss.allegro.api.request.FeedQuery;
import com.symphony.oss.allegro.api.request.FetchFeedObjectsRequest;
import com.symphony.oss.allegro.api.request.FetchPartitionObjectsRequest;
import com.symphony.oss.allegro.api.request.PartitionId;
import com.symphony.oss.allegro.api.request.PartitionQuery;
import com.symphony.oss.allegro.api.request.UpsertFeedRequest;
import com.symphony.oss.allegro.examples.calendar.canon.CalendarModel;
import com.symphony.oss.allegro.examples.calendar.canon.IToDoHeader;
import com.symphony.oss.allegro.examples.calendar.canon.IToDoItem;
import com.symphony.oss.allegro.examples.calendar.canon.ToDoHeader;
import com.symphony.oss.allegro.examples.calendar.canon.ToDoItem;
import com.symphony.oss.canon.runtime.exception.CanonException;
import com.symphony.oss.commons.hash.Hash;
import com.symphony.oss.fugue.cmd.CommandLineHandler;
import com.symphony.oss.fugue.pipeline.ISimpleThreadSafeConsumer;
import com.symphony.oss.fugue.trace.ITraceContext;
import com.symphony.oss.fugue.trace.ITraceContextTransaction;
import com.symphony.oss.fugue.trace.ITraceContextTransactionFactory;
import com.symphony.oss.fugue.trace.log.LoggerTraceContextTransactionFactory;
import com.symphony.oss.models.allegro.canon.SslTrustStrategy;
import com.symphony.oss.models.allegro.canon.facade.AllegroConfiguration;
import com.symphony.oss.models.allegro.canon.facade.ConnectionSettings;
import com.symphony.oss.models.core.canon.facade.PodAndUserId;
import com.symphony.oss.models.core.canon.facade.ThreadId;
import com.symphony.oss.models.object.canon.DeletionType;
import com.symphony.oss.models.object.canon.IFeed;
import com.symphony.oss.models.object.canon.facade.IStoredApplicationObject;

/**
 * Test to time the feed service.
 * 
 * @author Bruce Skingle
 *
 */
public class FeedTimerAssembly extends CommandLineHandler
{
  private static final String FEED_NAME                  = "ApiIntegrationTestFeed";
  private static final String FILTER_FEED_NAME           = "ApiIntegrationTestFilterFeed";
  private static final String PARTITION_NAME             = "MyPartition";

  private static final String ALLEGRO                    = "ALLEGRO_";
  private static final String SERVICE_ACCOUNT            = "SERVICE_ACCOUNT";
  private static final String POD_URL                    = "POD_URL";
  private static final String OBJECT_STORE_URL           = "OBJECT_STORE_URL";
  private static final String CREDENTIAL_FILE            = "CREDENTIAL_FILE";
  private static final String CREATE_THREADS             = "CREATE_THREADS";
  private static final String THREAD_ID                  = "THREAD_ID";
  private static final String OBJECT_COUNT               = "OBJECT_COUNT";
  private static final String SESSION_TOKEN              = "SESSION_TOKEN";
  private static final String KEYMANAGER_TOKEN           = "KEYMANAGER_TOKEN";

  private static final Logger log_ = LoggerFactory.getLogger(FeedTimerAssembly.class);
  private static final String BANNER = "---------------------------------------------------------------------------------------------------------";
  
  private ITraceContextTransactionFactory  traceFactory_             = new LoggerTraceContextTransactionFactory();
  private String                           serviceAccount_;
  private String                           podUrl_;
  private String                           objectStoreUrl_;
  private String                           credentialFile_;
  private String                           sessionToken_;
  private String                           keymanagerToken_;
  private int                              objectCount_   = 50;
  private int                              createThreads_ = 10;

  private PodAndUserId                     userId_;
  private ThreadId                         threadId_;

  private IAllegroApi                      allegroApi_;

  private Thread                           mainThread_                = Thread.currentThread();

  private IFeed feed_;


  /**
   * Constructor.
   */
  public FeedTimerAssembly()
  {
    withFlag('s',   SERVICE_ACCOUNT,  ALLEGRO + SERVICE_ACCOUNT,  String.class,   false, false,  (v) -> serviceAccount_   = v);
    withFlag('p',   POD_URL,          ALLEGRO + POD_URL,          String.class,   false, true,  (v) -> podUrl_           = v);
    withFlag('o',   OBJECT_STORE_URL, ALLEGRO + OBJECT_STORE_URL, String.class,   false, true,  (v) -> objectStoreUrl_   = v);
    withFlag('f',   CREDENTIAL_FILE,  ALLEGRO + CREDENTIAL_FILE,  String.class,   false, false,  (v) -> credentialFile_   = v);
    withFlag('t',   THREAD_ID,        ALLEGRO + THREAD_ID,        String.class,   false, true,  (v) -> threadId_         = ThreadId.newBuilder().build(v));
    withFlag('c',   CREATE_THREADS,   ALLEGRO + CREATE_THREADS,   Integer.class,  false, false, (v) -> createThreads_    = v);
    withFlag('n',   OBJECT_COUNT,     ALLEGRO + OBJECT_COUNT,     Integer.class,  false, false, (v) -> objectCount_      = v);
    withFlag('S',   SESSION_TOKEN,    ALLEGRO + SESSION_TOKEN,    String.class,  false, false, (v) -> sessionToken_      = v);
    withFlag('K',   KEYMANAGER_TOKEN, ALLEGRO + KEYMANAGER_TOKEN, String.class,  false, false, (v) -> keymanagerToken_   = v);
  }
  
  /**
   * Main test method.
   * @return 
   */
  public String run()
  {     
  	allegroApi_ = new AllegroApi.Builder()
            .withConfiguration(new AllegroConfiguration.Builder()
                    .withPodUrl(podUrl_)
                    .withApiUrl(objectStoreUrl_)
                    .withUserName(serviceAccount_)
                    .withRsaPemCredentialFile(credentialFile_)
                    .withApiConnectionSettings(new ConnectionSettings.Builder()
                        .withSslTrustStrategy(SslTrustStrategy.TRUST_ALL_CERTS)
                        .build())
                    .build())
            .withFactories(CalendarModel.FACTORIES)
            .withTraceFactory(traceFactory_)
            .withKeymanagerToken(keymanagerToken_)
            .build();
      
    userId_         = allegroApi_.getUserId();
    
    log_.info("PodId is " + allegroApi_.getPodId());
    log_.info("UserId is " + userId_);

    if(credentialFile_ != null)
    {
      System.out.println("SessionToken " + allegroApi_.getSessionToken());
      System.out.println("KeyManagerToken " + allegroApi_.getKeyManagerToken());
    }
    
    try(ITraceContextTransaction txn = traceFactory_.createTransaction("FeedTimer", UUID.randomUUID().toString()))
    {
      ITraceContext trace = txn.open();
      //upsertFeed();
      
      trace.trace("ABOUT_TO_INIT_CRYPTO_LIB");
      // Initialize cryptolib
      encryptItem(0, trace);
      
      log_.info("Delete partition objects before start...");
      deletePartitionObjects();
      
      log_.info("Consume feed items before start...");
      // discard anything in the feed queue
      fetchAllFeedItems();
      
      log_.info(BANNER);
      log_.info("Create " + objectCount_ + " objects with " + createThreads_ + " threads.");
      log_.info(BANNER);
      
      Map<Hash, List<ReceivedObject>> resultMap = new HashMap<>();
      CountDownLatch latch = new CountDownLatch(objectCount_);
      
      
      log_.info("Start feed...");
      
      IAllegroQueryManager queryManager = allegroApi_.fetchFeedObjects(new FetchFeedObjectsRequest.Builder()
          .withQuery(new FeedQuery.Builder()
              .withName(FEED_NAME)
              .build())
          .withConsumerManager(new AsyncConsumerManager.Builder()
              .withSubscriberThreadPoolSize(10)
              .withHandlerThreadPoolSize(90)
              .withConsumer(IStoredApplicationObject.class, (item, allegroTrace) ->
              {
                ReceivedObject receivedObject = new ReceivedObject(item);
                
                allegroTrace.trace("RECEIVED_OBJECT", item);
                
                synchronized(resultMap)
                {
                  List<ReceivedObject> resultList = resultMap.get(item.getAbsoluteHash());
                  
                  if(resultList == null)
                  {
                    resultList = new LinkedList<>();
                    resultMap.put(item.getAbsoluteHash(), resultList);
                  }
                  else
                  {
                    log_.warn("Duplicate notification for " + item.getAbsoluteHash());
                  }
                  resultList.add(receivedObject);
                  log_.info("Received " + resultMap.size() + " objects so far...");
                }

                latch.countDown();
              })
              .build()
              )
          .build()
          );
      
      queryManager.start();
      
      ExecutorService createExecutor = Executors.newFixedThreadPool(createThreads_);
      
      log_.info("Create " + objectCount_ + " objects...");
    
      for(int i=0 ; i<objectCount_ ; i++)
      {
        final int n = i;
        
        
        Runnable task = new Runnable()
        {
          @Override
          public void run()
          {
            try(ITraceContextTransaction subTraceTxn = traceFactory_.createTransaction("CreateObject", String.valueOf(n)))
            {
              final ITraceContext subTrace = subTraceTxn.open();

              subTrace.trace("ABOUT_TO_CREATE", "OBJECT", String.valueOf(n));
              IStoredApplicationObject item = createItem(n, subTrace);
            }
            catch(RuntimeException e)
            {
              e.printStackTrace();
            }
          }
        };
        createExecutor.submit(task);
        
//        try
//        {
//          Thread.sleep(1000L);
//        }
//        catch (InterruptedException e)
//        {
//          // TODO Auto-generated catch block
//          e.printStackTrace();
//        }
      }
      
      createExecutor.shutdown();
      
      
      
      while(getResultSize(resultMap) < objectCount_)
      {
        
        
        try
        {
          latch.await();
        }
        catch (InterruptedException e)
        {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
      
      log_.info(BANNER);
      log_.info("Received " + resultMap.size() + " objects.");
      
      log_.info("Deleting objects...");
      deletePartitionObjects();
      
      queryManager.stop();
    
      
      log_.info("Processed " + objectCount_ + " objects with " + createThreads_ + " create threads");
      long totalTime = 0L;
      long maxTime = 0L;
      
      for(List<ReceivedObject> resultList : resultMap.values())
      {
        StringBuilder s = null;
        
        for(ReceivedObject receivedObject : resultList)
        {
          ToDoHeader header = ((ToDoHeader)receivedObject.object.getHeader());
          Instant effectiveDate = header.getEffectiveDate();
          long elapsedTime = Duration.between(effectiveDate, receivedObject.received).toMillis();
          
          if(s == null)
          {
            totalTime += elapsedTime;
            maxTime = Math.max(maxTime, elapsedTime);
            
            s = new StringBuilder();
            s.append(receivedObject.object.getAbsoluteHash() + " | " + 
              effectiveDate);
          }
          s.append(" | " + elapsedTime + " ms.");
        }
        log_.info(s.toString());
      }
      
      log_.info(BANNER);
      log_.info("Max time " + maxTime);
      log_.info("Average time " + (totalTime / objectCount_));
      log_.info(BANNER);
      
      return "Max time " + maxTime + ", Average time " + (totalTime / objectCount_);
    }
    catch(RuntimeException e)
    {
      log_.error("Failed to run", e);
      throw e;
    }
    finally
    {
      log_.info("Cleaning up...");
      deletePartitionObjects();
    }
  }
  
  private int getResultSize(Map<?,?> resultList)
  {
    synchronized(resultList)
    {
      return resultList.size();
    }
  }

  class ReceivedObject
  {
    IStoredApplicationObject  object;
    Instant                   received;
    
    public ReceivedObject(IStoredApplicationObject object)
    {
      this.object = object;
      this.received = Instant.now();
    }
  }

  

  private void deletePartitionObjects()
  {
    allegroApi_.fetchPartitionObjects(new FetchPartitionObjectsRequest.Builder()
        .withQuery(new PartitionQuery.Builder()
            .withName(PARTITION_NAME)
            .build()
            )
        .withConsumerManager(new ConsumerManager.Builder()
            .withConsumer(IStoredApplicationObject.class, (item, trace) ->
            {
              log_.info("Delete " + item.getBaseHash());
              
              allegroApi_.delete(item, DeletionType.PHYSICAL);
            })
            .build()
            )
        .build()
        );
  }

  

  private void fetchAllFeedItems()
  {
    VersionConsumer consumer = null;
    
    do
    {
      consumer = new VersionConsumer(null, false);
      
      allegroApi_.fetchFeedObjects(new FetchFeedObjectsRequest.Builder()
          .withQuery(new FeedQuery.Builder()
              .withName(FEED_NAME)
              .build())
          .withConsumerManager(new ConsumerManager.Builder()
              .withConsumer(IStoredApplicationObject.class, consumer)
              .build())
          .build()
          );
      
      log_.info("fetchFeedObjects returned " + consumer.count_ + " items.");
    }while(consumer.count_ > 0);
  }

  private void upsertFeed()
  {
    ResourcePermissions permissions = null;
    
    UpsertFeedRequest.Builder builder = new UpsertFeedRequest.Builder()
        .withName(FEED_NAME)
        .withPermissions(permissions)
        .withPartitionIds(
            new PartitionId.Builder()
            .withName(PARTITION_NAME)
            .build()
            )
        ;
    
    feed_ = allegroApi_.upsertFeed(builder.build());
    
    log_.info("Feed is " + feed_);
  }

  
  private class VersionConsumer implements ISimpleThreadSafeConsumer<IStoredApplicationObject>
  {
    int count_;
    private Hash expectedBaseHash_;
    private boolean async_;

    public VersionConsumer(Hash baseHash, boolean async)
    {
      expectedBaseHash_ = baseHash;
      async_ = async;
    }

    @Override
    public synchronized void consume(IStoredApplicationObject item, ITraceContext trace)
    {
      if(expectedBaseHash_ != null && !item.getBaseHash().equals(expectedBaseHash_))
        throw new IllegalStateException("Expected baseHash " + expectedBaseHash_ + " but received " + item.getBaseHash());
      
      if(async_)
      {
        if(mainThread_ == Thread.currentThread())
          throw new IllegalStateException("Expected to be called from a pool thread but actually called from " + Thread.currentThread());
      }
      else
      {
        if(mainThread_ != Thread.currentThread())
          throw new IllegalStateException("Expected to be called from main thread but actually called from " + Thread.currentThread());
      }

      count_++;
      
      System.out.format("  %-50s %-50s %-50s %-50s %s%n",
          item.getBaseHash(),
          item.getAbsoluteHash(),
          item.getPartitionHash(),
          item.getSortKey(),
          item.getCreatedDate()
          );
    }
  }

  private IStoredApplicationObject createItem(int n, ITraceContext trace)
  {
    IStoredApplicationObject toDoObject = encryptItem(n, trace);
    
    try
    {
      allegroApi_.store(toDoObject);
    }
    catch(CanonException e)
    {
      System.err.println("HERE " + toDoObject.getAbsoluteHash());
      e.printStackTrace();
      System.err.println("HERE " + toDoObject.getAbsoluteHash());
    }
    trace.trace("STORED", toDoObject);
    
    return toDoObject;
  }

  private IStoredApplicationObject encryptItem(int n, ITraceContext trace)
  {

    IToDoItem item = new ToDoItem.Builder()
        .withDue(Instant.now())
        .withTitle("A test Item")
        .withDescription("Item " + n)
        .build();
    
    //log_.info("About to create item " + item);
    log_.info("create " + n);
    
    IToDoHeader testHeader = new ToDoHeader.Builder()
        .withRequestingUser(allegroApi_.getUserId())
        .withAffectedUsers(allegroApi_.getUserId())
        .withEffectiveDate(Instant.now())
        .build();
    
    IStoredApplicationObject toDoObject = allegroApi_.newApplicationObjectBuilder()
        .withThreadId(threadId_)
        .withHeader(testHeader)
        .withPayload(item)
        .withPartition(new PartitionId.Builder()
            .withName(PARTITION_NAME)
            .build()
            )
        .withSortKey(String.valueOf(n))
      .build();

    trace.trace("CREATED", toDoObject);
    
    return toDoObject;
  }
}
