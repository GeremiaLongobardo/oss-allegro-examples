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

import com.symphony.oss.allegro.api.AllegroApi;
import com.symphony.oss.allegro.api.IAllegroApi;
import com.symphony.oss.allegro.api.request.PartitionId;
import com.symphony.oss.allegro.examples.calendar.canon.IToDoHeader;
import com.symphony.oss.allegro.examples.calendar.canon.IToDoItem;
import com.symphony.oss.allegro.examples.calendar.canon.ToDoHeader;
import com.symphony.oss.allegro.examples.calendar.canon.ToDoItem;
import com.symphony.oss.fugue.cmd.CommandLineHandler;
import com.symphony.oss.models.allegro.canon.SslTrustStrategy;
import com.symphony.oss.models.allegro.canon.facade.AllegroConfiguration;
import com.symphony.oss.models.allegro.canon.facade.ConnectionSettings;
import com.symphony.oss.models.core.canon.facade.PodAndUserId;
import com.symphony.oss.models.core.canon.facade.ThreadId;
import com.symphony.oss.models.object.canon.facade.IStoredApplicationObject;

/**
 * An example application which creates a ToDoItem, adding it to a current and absolute sequence.
 * 
 * @author Bruce Skingle
 *
 */
public class CreateToDoItem extends CommandLineHandler implements Runnable
{
  private static final String ALLEGRO         = "ALLEGRO_";
  private static final String SERVICE_ACCOUNT = "SERVICE_ACCOUNT";
  private static final String POD_URL         = "POD_URL";
  private static final String OBJECT_STORE_URL = "OBJECT_STORE_URL";
  private static final String CREDENTIAL_FILE = "CREDENTIAL_FILE";
  private static final String THREAD_ID       = "THREAD_ID";
  private static final String OWNER_USER_ID    = "OWNER_USER_ID";
  
  private String              serviceAccount_;
  private String              podUrl_;
  private String              objectStoreUrl_;
  private String              credentialFile_;
  private ThreadId            threadId_;
  private Long                ownerId_;
  
  private IAllegroApi         allegroApi_;

  /**
   * Constructor.
   */
  public CreateToDoItem()
  {
    withFlag('s',   SERVICE_ACCOUNT,  ALLEGRO + SERVICE_ACCOUNT,  String.class,   false, true,   (v) -> serviceAccount_       = v);
    withFlag('p',   POD_URL,          ALLEGRO + POD_URL,          String.class,   false, true,   (v) -> podUrl_               = v);
    withFlag('o',   OBJECT_STORE_URL, ALLEGRO + OBJECT_STORE_URL, String.class,   false, true,   (v) -> objectStoreUrl_       = v);
    withFlag('f',   CREDENTIAL_FILE,  ALLEGRO + CREDENTIAL_FILE,  String.class,   false, false,  (v) -> credentialFile_       = v);
    withFlag('t',   THREAD_ID,        ALLEGRO + THREAD_ID,        String.class,   false, true,   (v) -> threadId_             = ThreadId.newBuilder().build(v));
    withFlag('u',   OWNER_USER_ID,    ALLEGRO + OWNER_USER_ID,    Long.class,     false, false,  (v) -> ownerId_              = v);
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
    

    
    IToDoItem toDoItem = new ToDoItem.Builder()
      .withDue(Instant.now())
      .withTimeTaken(new BigDecimal(1000.0 / 3.0))
      .withTitle("An example TODO Item")
      .withDescription("Since we are creating this item with a due date of Instant.now() we are already late!")
      .build();
    
    System.out.println("About to create item " + toDoItem);
    
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
        .withSortKey(toDoItem.getDue().toString())
//        .withPurgeDate(Instant.now().plusMillis(60000))
      .build();
    
    allegroApi_.store(toDoObject);
    
    System.out.println("Created " + toDoObject);
    System.out.println("absoluteHash " + toDoObject.getAbsoluteHash());
  }
  
  /**
   * Main.
   * 
   * @param args Command line arguments.
   */
  public static void main(String[] args)
  {
    CreateToDoItem program = new CreateToDoItem();
    
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
}
