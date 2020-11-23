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
import com.symphony.oss.allegro.api.AllegroMultiTenantApi;
import com.symphony.oss.allegro.api.IAllegroApi;
import com.symphony.oss.allegro.api.IAllegroMultiTenantApi;
import com.symphony.oss.allegro.api.Permission;
import com.symphony.oss.allegro.api.ResourcePermissions;
import com.symphony.oss.allegro.api.request.PartitionId;
import com.symphony.oss.allegro.api.request.UpsertPartitionRequest;
import com.symphony.oss.allegro.examples.calendar.canon.IToDoItem;
import com.symphony.oss.allegro.examples.calendar.canon.ToDoItem;
import com.symphony.oss.fugue.cmd.CommandLineHandler;
import com.symphony.oss.models.allegro.canon.SslTrustStrategy;
import com.symphony.oss.models.allegro.canon.facade.AllegroConfiguration;
import com.symphony.oss.models.allegro.canon.facade.AllegroMultiTenantConfiguration;
import com.symphony.oss.models.allegro.canon.facade.ConnectionSettings;
import com.symphony.oss.models.core.canon.facade.PodAndUserId;
import com.symphony.oss.models.core.canon.facade.ThreadId;
import com.symphony.oss.models.object.canon.AffectedUsers;
import com.symphony.oss.models.object.canon.IAffectedUsers;
import com.symphony.oss.models.object.canon.IEncryptedApplicationPayloadAndHeader;
import com.symphony.oss.models.object.canon.facade.IPartition;
import com.symphony.oss.models.object.canon.facade.IStoredApplicationObject;

/**
 * An example application which creates a ToDoItem, adding it to a current and absolute sequence.
 * 
 * @author Bruce Skingle
 *
 */
public class CreateToDoItemInTwoStages extends CommandLineHandler implements Runnable
{
  private static final String    ALLEGRO          = "ALLEGRO_";
  private static final String    SERVICE_ACCOUNT  = "SERVICE_ACCOUNT";
  private static final String    POD_URL          = "POD_URL";
  private static final String    OBJECT_STORE_URL = "OBJECT_STORE_URL";
  private static final String    CREDENTIAL_FILE  = "CREDENTIAL_FILE";
  private static final String    MT_CREDENTIAL    = "MT_CREDENTIAL_FILE";
  private static final String    THREAD_ID        = "THREAD_ID";
  private static final String    OWNER_USER_ID    = "OWNER_USER_ID";
  private static final String    OTHER_USER_ID    = "OTHER_USER_ID";

  private String                 serviceAccount_;
  private String                 podUrl_;
  private String                 objectStoreUrl_;
  private String                 credentialFile_;
  private String                 mtCredentialFile_;
  private ThreadId               threadId_;
  private Long                   ownerId_;
  private PodAndUserId           otherUserId_;
  private PodAndUserId           multiTenantUserId_;

  private IAllegroApi            allegroApi_;
  private IAllegroMultiTenantApi multiTenantApi_;

  /**
   * Constructor.
   */
  public CreateToDoItemInTwoStages()
  {
    withFlag('s',   SERVICE_ACCOUNT,  ALLEGRO + SERVICE_ACCOUNT,  String.class,   false, true,   (v) -> serviceAccount_       = v);
    withFlag('p',   POD_URL,          ALLEGRO + POD_URL,          String.class,   false, true,   (v) -> podUrl_               = v);
    withFlag('o',   OBJECT_STORE_URL, ALLEGRO + OBJECT_STORE_URL, String.class,   false, true,   (v) -> objectStoreUrl_       = v);
    withFlag('f',   CREDENTIAL_FILE,  ALLEGRO + CREDENTIAL_FILE,  String.class,   false, false,  (v) -> credentialFile_       = v);
    withFlag('m',   MT_CREDENTIAL,    ALLEGRO + MT_CREDENTIAL,    String.class,   false, true,   (v) -> mtCredentialFile_ = v);
    withFlag('t',   THREAD_ID,        ALLEGRO + THREAD_ID,        String.class,   false, true,   (v) -> threadId_             = ThreadId.newBuilder().build(v));
    withFlag('u',   OWNER_USER_ID,    ALLEGRO + OWNER_USER_ID,    Long.class,     false, false,  (v) -> ownerId_              = v);
    withFlag(null,  OTHER_USER_ID,    ALLEGRO + OTHER_USER_ID,    Long.class,     false, false,  (v) -> otherUserId_          = PodAndUserId.newBuilder().build(v));
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
	    
    
    multiTenantApi_ = new AllegroMultiTenantApi.Builder()
    		 .withConfiguration(new AllegroMultiTenantConfiguration.Builder()
	                    .withApiUrl(objectStoreUrl_)
	                    .withPrincipalCredentialFile(mtCredentialFile_)
	                    .withApiConnectionSettings(new ConnectionSettings.Builder()
	                        .withSslTrustStrategy(SslTrustStrategy.TRUST_ALL_CERTS)
	                        .build())
	                    .build())
        //.withFactories(ObjectStoreTestModel.FACTORIES)
        .build();
    
    multiTenantUserId_  = multiTenantApi_.getUserId();
    
    PodAndUserId ownerUserId = ownerId_ == null ? allegroApi_.getUserId() : PodAndUserId.newBuilder().build(ownerId_);
    
    System.out.println("CallerId is " + allegroApi_.getUserId());
    System.out.println("OwnerId is " + ownerUserId);
    System.out.println("PodId is " + allegroApi_.getPodId());
    System.out.println("multiTenantUserId is " + multiTenantUserId_);
    
    ResourcePermissions.Builder permissions = new ResourcePermissions.Builder()
        .withUser(multiTenantUserId_, Permission.Read, Permission.Write)
        ;
    
    if(otherUserId_ != null)
      permissions.withUser(otherUserId_, Permission.Read, Permission.Write);
    
    IPartition partition = allegroApi_.upsertPartition(new UpsertPartitionRequest.Builder()
          .withName(CalendarApp.PARTITION_NAME)
          .withPermissions(permissions.build())
          .build()
        );
    
    System.out.println("partition is " + partition);
    
    // Create the payload
    IToDoItem toDoItem = new ToDoItem.Builder()
      .withDue(Instant.now())
      .withTimeTaken(new BigDecimal(1000.0 / 3.0))
      .withTitle("An example TODO Item")
      .withDescription("Since we are creating this item with a due date of Instant.now() we are already late!")
      .build();
    
    System.out.println("About to create item " + toDoItem);
    
    // Create the header
    IAffectedUsers affectedUsers = new AffectedUsers.Builder()
        .withRequestingUser(allegroApi_.getUserId())
        .withAffectedUsers(allegroApi_.getUserId())
        .withEffectiveDate(Instant.now())
        .build();
    
    // Create an encrypted StoredApplicationObject in a single step.
    IStoredApplicationObject toDoObject = allegroApi_.newApplicationObjectBuilder()
        .withThreadId(threadId_)
        .withHeader(affectedUsers)
        .withPayload(toDoItem)
        .withPartition(new PartitionId.Builder()
            .withName(CalendarApp.PARTITION_NAME)
            .withOwner(ownerUserId)
            .build()
            )
        .withSortKey(toDoItem.getDue().toString())
      .build();
    
    /*
     * If for some reason you wanted to encrypt the object on the client and then send it to the server for a process
     * there to decide how to store it, you would create an IEncryptedApplicationPayload or an IEncryptedApplicationPayloadAndHeader
     * on the client like this....
     */
    IEncryptedApplicationPayloadAndHeader toDoPayload = allegroApi_.newEncryptedApplicationPayloadAndHeaderBuilder()
        .withThreadId(threadId_)
        .withHeader(affectedUsers)
        .withPayload(toDoItem)
      .build();
    
    /*
     * Then on the server you could create a StoredApplicationObject from the given encrypted payload like this:
     */
    IStoredApplicationObject toDoObject2 = multiTenantApi_.newEncryptedApplicationObjectBuilder()
        .withEncryptedPayloadAndHeader(toDoPayload)
        .withPartition(new PartitionId.Builder()
          .withName(CalendarApp.PARTITION_NAME)
          .build()
          .getHash(ownerUserId)
          )
        .withSortKey(toDoItem.getDue().toString())
      .build();
    
    System.out.println();
    System.out.println("These two objects should be the same except for the encrypted payload (which differ because there is a");
    System.out.println("random initialization vector in the encryption process):");
    System.out.println();
    System.out.println(toDoObject);
    System.out.println();
    System.out.println(toDoObject2);
    System.out.println();
    
    multiTenantApi_.store(toDoObject2);
    
    System.out.println("Created " + toDoObject);
  }
  
  /**
   * Main.
   * 
   * @param args Command line arguments.
   */
  public static void main(String[] args)
  {
    CreateToDoItemInTwoStages program = new CreateToDoItemInTwoStages();
    
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
