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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.symphony.oss.allegro.api.AllegroApi;
import com.symphony.oss.allegro.api.IAllegroApi;
import com.symphony.oss.allegro.api.Permission;
import com.symphony.oss.allegro.api.ResourcePermissions;
import com.symphony.oss.allegro.api.request.PartitionId;
import com.symphony.oss.allegro.api.request.UpsertFeedRequest;
import com.symphony.oss.allegro.examples.calendar.canon.CalendarModel;
import com.symphony.oss.fugue.cmd.CommandLineHandler;
import com.symphony.oss.models.allegro.canon.SslTrustStrategy;
import com.symphony.oss.models.allegro.canon.facade.AllegroConfiguration;
import com.symphony.oss.models.allegro.canon.facade.ConnectionSettings;
import com.symphony.oss.models.core.canon.facade.PodAndUserId;
import com.symphony.oss.models.object.canon.IFeed;

/**
 * Retrieve all objects on the given Sequence.
 * 
 * @author Bruce Skingle
 *
 */
public class CreateFeed extends CommandLineHandler implements Runnable
{
  private static final Logger log_ = LoggerFactory.getLogger(CreateFeed.class);
  
  private static final String ALLEGRO          = "ALLEGRO_";
  private static final String SERVICE_ACCOUNT  = "SERVICE_ACCOUNT";
  private static final String POD_URL          = "POD_URL";
  private static final String OBJECT_STORE_URL = "OBJECT_STORE_URL";
  private static final String CREDENTIAL_FILE  = "CREDENTIAL_FILE";
  private static final String OWNER_USER_ID    = "OWNER_USER_ID";
  private static final String OTHER_USER_ID    = "OTHER_USER_ID";
  
  private String              serviceAccount_;
  private String              podUrl_;
  private String              objectStoreUrl_;
  private String              credentialFile_;
  private PodAndUserId        ownerId_;
  private PodAndUserId        otherUserId_;
  
  private IAllegroApi         allegroApi_;

  /**
   * Constructor.
   */
  public CreateFeed()
  {
    withFlag('s',   SERVICE_ACCOUNT,  ALLEGRO + SERVICE_ACCOUNT,  String.class,   false, true,   (v) -> serviceAccount_       = v);
    withFlag('p',   POD_URL,          ALLEGRO + POD_URL,          String.class,   false, true,   (v) -> podUrl_               = v);
    withFlag('o',   OBJECT_STORE_URL, ALLEGRO + OBJECT_STORE_URL, String.class,   false, true,   (v) -> objectStoreUrl_       = v);
    withFlag('f',   CREDENTIAL_FILE,  ALLEGRO + CREDENTIAL_FILE,  String.class,   false, true,   (v) -> credentialFile_       = v);
    withFlag('u',   OWNER_USER_ID,    ALLEGRO + OWNER_USER_ID,    Long.class,     false, false,  (v) -> ownerId_              = PodAndUserId.newBuilder().build(v));
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
            .withFactories(CalendarModel.FACTORIES)
            .build();
    
    System.out.println("CallerId is " + allegroApi_.getUserId());
    System.out.println("OwnerId is " + otherUserId_);
    System.out.println("OtherUserId is " + otherUserId_);
    
    ResourcePermissions permissions = null;
    
    if(otherUserId_ != null)
    {
      permissions = new ResourcePermissions.Builder()
          .withUser(otherUserId_, Permission.Read)
          .build()
          ;
    }
    
    UpsertFeedRequest.Builder builder = new UpsertFeedRequest.Builder()
        .withName(CalendarApp.FEED_NAME)
        .withPermissions(permissions)
        .withPartitionIds(
            new PartitionId.Builder()
            .withName(CalendarApp.PARTITION_NAME)
            .withOwner(ownerId_)
            .build()
            )
        ;
    
    IFeed feed = allegroApi_.upsertFeed(builder.build());

    log_.info("Feed hash is " + feed.getId().getHash());
    log_.info("Feed is " + feed);
  }
  
  /**
   * Main.
   * 
   * @param args Command line arguments.
   */
  public static void main(String[] args)
  {
    CreateFeed program = new CreateFeed();
    
    program.process(args);
    
    program.run();
  }
}
