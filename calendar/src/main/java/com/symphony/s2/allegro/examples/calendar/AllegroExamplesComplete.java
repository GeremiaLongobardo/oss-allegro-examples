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

package com.symphony.s2.allegro.examples.calendar;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;

import com.symphony.oss.allegro.api.AllegroApi;
import com.symphony.oss.allegro.api.ConsumerManager;
import com.symphony.oss.allegro.api.IAllegroApi;
import com.symphony.oss.allegro.api.Permission;
import com.symphony.oss.allegro.api.ResourcePermissions;
import com.symphony.oss.allegro.api.request.FeedId;
import com.symphony.oss.allegro.api.request.FeedQuery;
import com.symphony.oss.allegro.api.request.FetchFeedObjectsRequest;
import com.symphony.oss.allegro.api.request.FetchObjectVersionsRequest;
import com.symphony.oss.allegro.api.request.FetchPartitionObjectsRequest;
import com.symphony.oss.allegro.api.request.PartitionId;
import com.symphony.oss.allegro.api.request.PartitionQuery;
import com.symphony.oss.allegro.api.request.UpsertFeedRequest;
import com.symphony.oss.allegro.api.request.UpsertPartitionRequest;
import com.symphony.oss.allegro.api.request.VersionQuery;
import com.symphony.oss.allegro.examples.calendar.canon.IToDoHeader;
import com.symphony.oss.allegro.examples.calendar.canon.IToDoItem;
import com.symphony.oss.allegro.examples.calendar.canon.ToDoHeader;
import com.symphony.oss.allegro.examples.calendar.canon.ToDoItem;
import com.symphony.oss.canon.runtime.exception.NotFoundException;
import com.symphony.oss.fugue.cmd.CommandLineHandler;
import com.symphony.oss.models.allegro.canon.SslTrustStrategy;
import com.symphony.oss.models.allegro.canon.facade.AllegroConfiguration;
import com.symphony.oss.models.allegro.canon.facade.ConnectionSettings;
import com.symphony.oss.models.core.canon.facade.PodAndUserId;
import com.symphony.oss.models.core.canon.facade.ThreadId;
import com.symphony.oss.models.object.canon.DeletionType;
import com.symphony.oss.models.object.canon.IFeed;
import com.symphony.oss.models.object.canon.facade.IPartition;
import com.symphony.oss.models.object.canon.facade.IStoredApplicationObject;

/**
 * An example application which creates a ToDoItem, adding it to a current and absolute sequence.
 * 
 * @author Bruce Skingle
 *
 */
public class AllegroExamplesComplete extends CommandLineHandler implements Runnable
{
  private static final String ALLEGRO         = "ALLEGRO_";
  private static final String SERVICE_ACCOUNT = "SERVICE_ACCOUNT";
  private static final String POD_URL         = "POD_URL";
  private static final String OBJECT_STORE_URL = "OBJECT_STORE_URL";
  private static final String CREDENTIAL_FILE = "CREDENTIAL_FILE";
  private static final String THREAD_ID       = "THREAD_ID";
  private static final String OWNER_USER_ID    = "OWNER_USER_ID";
  private static final String OTHER_USER_ID    = "OTHER_USER_ID";
  private static final String SORT_KEY_PREFIX  = "SORT_KEY_PREFIX";
  
  private String              serviceAccount_;
  private String              podUrl_;
  private String              objectStoreUrl_;
  private String              credentialFile_;
  private ThreadId            threadId_;
  private Long                ownerId_;
  private PodAndUserId        otherUserId_;
  private String              sortKeyPrefix_;
  
  private IAllegroApi         allegroApi_;

  /**
   * Constructor.
   */
  public AllegroExamplesComplete()
  {
    withFlag('s',   SERVICE_ACCOUNT,  ALLEGRO + SERVICE_ACCOUNT,  String.class,   false, true,   (v) -> serviceAccount_       = v);
    withFlag('p',   POD_URL,          ALLEGRO + POD_URL,          String.class,   false, true,   (v) -> podUrl_               = v);
    withFlag('o',   OBJECT_STORE_URL, ALLEGRO + OBJECT_STORE_URL, String.class,   false, true,   (v) -> objectStoreUrl_       = v);
    withFlag('f',   CREDENTIAL_FILE,  ALLEGRO + CREDENTIAL_FILE,  String.class,   false, false,  (v) -> credentialFile_       = v);
    withFlag('t',   THREAD_ID,        ALLEGRO + THREAD_ID,        String.class,   false, true,   (v) -> threadId_             = ThreadId.newBuilder().build(v));
    withFlag('u',   OWNER_USER_ID,    ALLEGRO + OWNER_USER_ID,    Long.class,     false, false,  (v) -> ownerId_              = v);
    withFlag(null,  OTHER_USER_ID,    ALLEGRO + OTHER_USER_ID,    Long.class,     false, false,  (v) -> otherUserId_          = PodAndUserId.newBuilder().build(v));
    withFlag('k',   SORT_KEY_PREFIX,  ALLEGRO + SORT_KEY_PREFIX,  String.class,   false, false,  (v) -> sortKeyPrefix_        = v);
  }
  
  @Override
  public void run()
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
            .build();
    
    PodAndUserId ownerUserId = ownerId_ == null ? allegroApi_.getUserId() : PodAndUserId.newBuilder().build(ownerId_);
    
    System.out.println("CallerId is " + allegroApi_.getUserId());
    System.out.println("OwnerId is " + ownerUserId);
    System.out.println("PodId is " + allegroApi_.getPodId());
    
    ResourcePermissions permissions = null;
    
    if(otherUserId_ != null)
    {
      permissions = new ResourcePermissions.Builder()
          .withUser(otherUserId_, Permission.Read, Permission.Write)
          .build();
    }
    
    
    upsertPartition(permissions);
    
    IFeed feed = upsertFeed(permissions, ownerUserId);

    String sortKey = sortKeyPrefix_ + "-" + Instant.now();
    
    IStoredApplicationObject object = storeObject(ownerUserId, sortKey);
    
    object = updateItem(object);
      
    fetchVersions(object);
    
    fetchFeed(ownerUserId);
    
    fetchPartitionObjects(ownerUserId, sortKey);
    
    deleteItem(object);
    
    deleteFeed(ownerUserId, feed);
       
  }
  
  private void deleteFeed(PodAndUserId ownerUserId, IFeed feed)
  {
    allegroApi_.deleteFeed(new FeedId.Builder()
        .withId(feed.getId())
        .build()
        );
    
    try 
    {
          allegroApi_.fetchFeedObjects(new FetchFeedObjectsRequest.Builder()
                  .withQuery(new FeedQuery.Builder()
                      .withName(CalendarApp.FEED_NAME)
                      .withOwner(ownerUserId)
                      .withMaxItems(10)
                      .build())
                  .withConsumerManager(new ConsumerManager.Builder()
                      .withConsumer(Object.class, (object, trace) ->
                      {
                        System.out.println("FEED 1 - RECEIVED --> "+object);
                        System.out.println("**** FEED DELETED ***");
                        throw new IllegalStateException("The feed should have been already deleted!");
                        
                      })
                      .build())
                  .build()
                  );
                }
          catch (NotFoundException e)
          {
            System.out.println("OK, FEED NOT FOUND, SINCE IT WAS DELETED");
          }
    
  }

  private void deleteItem(IStoredApplicationObject object)
  {
    allegroApi_.delete(object, DeletionType.LOGICAL);
    
    ArrayList<Object> items = new ArrayList<>();
    
    allegroApi_.fetchPartitionObjects(new FetchPartitionObjectsRequest.Builder()
        .withQuery(new PartitionQuery.Builder()
            .withMaxItems(10)
            .withName(CalendarApp.PARTITION_NAME)
            .withOwner(object.getOwner())
            .withSortKeyPrefix(object.getSortKey().asString())
            .build()
            )
          .withConsumerManager(new ConsumerManager.Builder()
              .withConsumer(Object.class, (item, trace) ->
              {
                System.out.println("**** OBJECT RETRIEVED ***");
                items.add(item);
              })
              .build()
              )
          .build()
          );
    
    
    if(items.size() != 0)
      throw new IllegalStateException("The item should have been deleted");
    
  }

  private void fetchPartitionObjects(PodAndUserId ownerUserId, String sortKey)
  {
    ArrayList<Object> items = new ArrayList<>();
    
    allegroApi_.fetchPartitionObjects(new FetchPartitionObjectsRequest.Builder()
        .withQuery(new PartitionQuery.Builder()
            .withMaxItems(10)
            .withName(CalendarApp.PARTITION_NAME)
            .withOwner(ownerUserId)
            .withSortKeyPrefix(sortKey)
            .build()
            )
          .withConsumerManager(new ConsumerManager.Builder()
              .withConsumer(Object.class, (item, trace) ->
              {
                System.out.println("**** OBJECT RETRIEVED ***");
                items.add(item);
              })
              .build()
              )
          .build()
          );
    
    
    if(items.size() == 0)
      throw new IllegalStateException("No Item not retrieved from partition");
    
  }

  private void fetchFeed(PodAndUserId ownerUserId)
  {
  ArrayList<Object> list = new ArrayList<>();
    
    allegroApi_.fetchFeedObjects(new FetchFeedObjectsRequest.Builder()
        .withQuery(new FeedQuery.Builder()
            .withName(CalendarApp.FEED_NAME)
            .withOwner(ownerUserId)
            .withMaxItems(10)
            .build())
        .withConsumerManager(new ConsumerManager.Builder()
            .withConsumer(Object.class, (object, trace) ->
            {
              System.out.println("**** FEED OBJECT RETRIEVED ***");
              list.add(object);
            })
            .build())
        .build()
        );
    
    if(list.size() == 0)
      throw new IllegalStateException("No Object not retrieved from feed");
   
    
  }

  private void fetchVersions(IStoredApplicationObject object)
  {
    
    ArrayList<IStoredApplicationObject> list = new ArrayList<>();
    
    allegroApi_.fetchObjectVersions(new FetchObjectVersionsRequest.Builder()
        .withQuery(new VersionQuery.Builder()
            .withMaxItems(1)
            .withBaseHash(object.getBaseHash())
            .build()
            )
        .withConsumerManager(new ConsumerManager.Builder()
            .withConsumer(IStoredApplicationObject.class, (vitem, vtrace) ->
            {
              System.out.format("V %-50s %-50s %-50s %-50s %s%n",
                  vitem.getBaseHash(),
                  vitem.getAbsoluteHash(),
                  vitem.getPartitionHash(),
                  vitem.getSortKey(),
                  vitem.getCreatedDate()
                  );
              list.add(vitem);
              
            })
            .build()
            )
        .build()
        );
    
    if(list.size() == 0)
      throw new IllegalStateException("No Object not retrieved from base hash");
   
  }

  private IStoredApplicationObject storeObject(PodAndUserId ownerUserId, String sortKey)
  {
    IToDoItem toDoItem = new ToDoItem.Builder()
        .withDue(Instant.now())
        .withTimeTaken(new BigDecimal(1000.0 / 3.0))
        .withTitle("An example TODO Item")
        .withDescription("Since we are creating this item with a due date of Instant.now() we are already late!")
        .build();
      
      //System.out.println("About to create item " + toDoItem);
      
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
              .withName(CalendarApp.PARTITION_NAME)
              .withOwner(ownerUserId)
              .build()
              )
          .withSortKey(sortKey)
          .withPurgeDate(Instant.now().plusMillis(60000))
        .build();
      
      
      allegroApi_.store(toDoObject);
      
      System.out.println("**** OBJECT STORED ***");
      System.out.println("absoluteHash " + toDoObject.getAbsoluteHash());
      
      return toDoObject;
    
  }

  private IFeed upsertFeed(ResourcePermissions permissions, PodAndUserId ownerUserId)
  {
    UpsertFeedRequest.Builder builder = new UpsertFeedRequest.Builder()
        .withName(CalendarApp.FEED_NAME)
        .withPermissions(permissions)
        .withPartitionIds(
            new PartitionId.Builder()
            .withName(CalendarApp.PARTITION_NAME)
            .withOwner(ownerUserId)
            .build()
            )
        ;
    
    IFeed feed = allegroApi_.upsertFeed(builder.build());

   // System.out.println("Feed is " + feed);
    System.out.println("Feed hash is " + feed.getId().getHash());    
    System.out.println("**** FEED CREATED ***");
    
    return feed;
    
  }

  private void upsertPartition(ResourcePermissions permissions)
  {
    IPartition partition = allegroApi_.upsertPartition(new UpsertPartitionRequest.Builder()
          .withName(CalendarApp.PARTITION_NAME)
          .withPermissions(permissions)
          .build()
        );
    
   // System.out.println("partition is " + partition);  
    System.out.println("partition hash is " + partition.getId().getHash());   
    System.out.println("**** PARTITION CREATED ***");
    
  }

  /**
   * Main.
   * 
   * @param args Command line arguments.
   */
  public static void main(String[] args)
  {
    AllegroExamplesComplete program = new AllegroExamplesComplete();
    
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
  
  private IStoredApplicationObject updateItem(IStoredApplicationObject object) {
    
    ArrayList<IStoredApplicationObject> list = new ArrayList<>();
    
    IToDoItem toDoItem = new ToDoItem.Builder()
        .withDue(Instant.now())
        .withTimeTaken(new BigDecimal(1000.0 / 3.0))
        .withTitle("An example UPDATED TODO Item")
        .withDescription("Updated at " + Instant.now() + ", item ")
        .build();
      
   // System.out.println("About to update item " + toDoItem);
    
    IStoredApplicationObject toDoObject = allegroApi_.newApplicationObjectUpdater(object)
        .withPayload(toDoItem)
      .build();
    
    allegroApi_.store(toDoObject);
    
    allegroApi_.fetchPartitionObjects(new FetchPartitionObjectsRequest.Builder()
        .withQuery(new PartitionQuery.Builder()
            .withMaxItems(10)
            .withName(CalendarApp.PARTITION_NAME)
            .withOwner(object.getOwner())
            .withSortKeyPrefix(object.getSortKey().asString())
            .build()
            )
          .withConsumerManager(new ConsumerManager.Builder()
              .withConsumer(IStoredApplicationObject.class, (item, trace) ->
              {
                  list.add(item);
              })
              .build()
              )
          .build()
          );
    if(list.size() == 0)
      throw new IllegalStateException("No Object not retrieved from feed");
    return list.get(0);
   
  }
  
}
