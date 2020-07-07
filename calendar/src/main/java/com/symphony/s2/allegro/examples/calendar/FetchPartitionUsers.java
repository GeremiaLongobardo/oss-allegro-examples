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

import com.symphony.oss.allegro.api.AllegroApi;
import com.symphony.oss.allegro.api.IAllegroApi;
import com.symphony.oss.allegro.api.Permission;
import com.symphony.oss.allegro.api.ResourcePermissions;
import com.symphony.oss.allegro.api.request.PartitionQuery;
import com.symphony.oss.allegro.api.request.UpsertPartitionRequest;
import com.symphony.oss.commons.hash.Hash;
import com.symphony.oss.fugue.cmd.CommandLineHandler;
import com.symphony.oss.models.allegro.canon.SslTrustStrategy;
import com.symphony.oss.models.allegro.canon.facade.AllegroConfiguration;
import com.symphony.oss.models.allegro.canon.facade.ConnectionSettings;
import com.symphony.oss.models.core.canon.facade.PodAndUserId;
import com.symphony.oss.models.object.canon.IPageOfUserPermissions;
import com.symphony.oss.models.object.canon.IUserPermissionsRequest;
import com.symphony.oss.models.object.canon.facade.IPartition;

/**
 * An example application which creates a ToDoItem, adding it to a current and absolute sequence.
 * 
 * @author Bruce Skingle
 *
 */
public class FetchPartitionUsers extends CommandLineHandler implements Runnable
{
  private static final String ALLEGRO         = "ALLEGRO_";
  private static final String SERVICE_ACCOUNT = "SERVICE_ACCOUNT";
  private static final String POD_URL         = "POD_URL";
  private static final String OBJECT_STORE_URL = "OBJECT_STORE_URL";
  private static final String CREDENTIAL_FILE = "CREDENTIAL_FILE";
  private static final String OWNER_USER_ID    = "OWNER_USER_ID";
  private static final String OTHER_USER_ID    = "OTHER_USER_ID";
  
  private String              serviceAccount_;
  private String              podUrl_;
  private String              objectStoreUrl_;
  private String              credentialFile_;
  private PodAndUserId        otherUserId_;
  private Long                ownerId_;
  
  private IAllegroApi         allegroApi_;

  /**
   * Constructor.
   */
  public FetchPartitionUsers()
  {
    withFlag('s',   SERVICE_ACCOUNT,  ALLEGRO + SERVICE_ACCOUNT,  String.class,   false, true,   (v) -> serviceAccount_       = v);
    withFlag('p',   POD_URL,          ALLEGRO + POD_URL,          String.class,   false, true,   (v) -> podUrl_               = v);
    withFlag('o',   OBJECT_STORE_URL, ALLEGRO + OBJECT_STORE_URL, String.class,   false, true,   (v) -> objectStoreUrl_       = v);
    withFlag('f',   CREDENTIAL_FILE,  ALLEGRO + CREDENTIAL_FILE,  String.class,   false, false,  (v) -> credentialFile_       = v);
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
    
    System.out.println("PodId is " + allegroApi_.getPodId());
    
    ResourcePermissions permissions = null;
    
    if(otherUserId_ != null)
    {
      permissions = new ResourcePermissions.Builder()
          .withUser(otherUserId_, Permission.Read, Permission.Write)
          .build();
    }
       
    
    IPartition partition = allegroApi_.upsertPartition(new UpsertPartitionRequest.Builder()
          .withName(CalendarApp.PARTITION_NAME)
          .withPermissions(permissions)
          .build()
        );
    
    System.out.println("partition is " + partition);
    
    System.out.println("partition hash is " + partition.getId().getHash());
    
    IPageOfUserPermissions requests = allegroApi_.fetchPartitionUsers(new PartitionQuery.Builder()
            	.withHash(Hash.newInstance(partition.getId().getHash().toString())
                ).build()
            );
    for(IUserPermissionsRequest r : requests.getData())
    	System.out.println("UserPermission is " + r);
  }
  
  /**
   * Main.
   * 
   * @param args Command line arguments.
   */
  public static void main(String[] args)
  {
    FetchPartitionUsers program = new FetchPartitionUsers();
    
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
