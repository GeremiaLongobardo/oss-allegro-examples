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
import com.symphony.oss.allegro.api.ConsumerManager;
import com.symphony.oss.allegro.api.IAllegroApi;
import com.symphony.oss.allegro.api.request.FetchPartitionObjectsRequest;
import com.symphony.oss.allegro.api.request.PartitionQuery;
import com.symphony.oss.allegro.examples.calendar.canon.CalendarModel;
import com.symphony.oss.allegro.examples.calendar.canon.IToDoItem;
import com.symphony.oss.fugue.cmd.CommandLineHandler;
import com.symphony.oss.models.allegro.canon.SslTrustStrategy;
import com.symphony.oss.models.allegro.canon.facade.AllegroConfiguration;
import com.symphony.oss.models.allegro.canon.facade.ConnectionSettings;
import com.symphony.oss.models.core.canon.facade.PodAndUserId;
import com.symphony.oss.models.object.canon.facade.IStoredApplicationObject;

/**
 * Retrieve all objects on the given Sequence.
 * 
 * @author Bruce Skingle
 *
 */
public class ListItems extends CommandLineHandler implements Runnable
{
  private static final String ALLEGRO          = "ALLEGRO_";
  private static final String SERVICE_ACCOUNT  = "SERVICE_ACCOUNT";
  private static final String POD_URL          = "POD_URL";
  private static final String OBJECT_STORE_URL = "OBJECT_STORE_URL";
  private static final String CREDENTIAL_FILE  = "CREDENTIAL_FILE";
  private static final String OWNER_USER_ID    = "OWNER_USER_ID";
  private static final String SORT_KEY_PREFIX  = "SORT_KEY_PREFIX";
  
  private String              serviceAccount_;
  private String              podUrl_;
  private String              objectStoreUrl_;
  private String              credentialFile_;
  private String              sortKeyPrefix_;
  private Long                ownerId_;
  
  private IAllegroApi         allegroApi_;

  /**
   * Constructor.
   */
  public ListItems()
  {
    withFlag('s',   SERVICE_ACCOUNT,  ALLEGRO + SERVICE_ACCOUNT,  String.class,   false, true,   (v) -> serviceAccount_       = v);
    withFlag('p',   POD_URL,          ALLEGRO + POD_URL,          String.class,   false, true,   (v) -> podUrl_               = v);
    withFlag('o',   OBJECT_STORE_URL, ALLEGRO + OBJECT_STORE_URL, String.class,   false, true,   (v) -> objectStoreUrl_       = v);
    withFlag('f',   CREDENTIAL_FILE,  ALLEGRO + CREDENTIAL_FILE,  String.class,   false, true,   (v) -> credentialFile_       = v);
    withFlag('u',   OWNER_USER_ID,    ALLEGRO + OWNER_USER_ID,    Long.class,     false, false,  (v) -> ownerId_              = v);
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
	            .withFactories(CalendarModel.FACTORIES)
	            .build();
    
    System.out.println("Allegro configuration = " + allegroApi_.getConfiguration());
    
    // Verify obsolete deprecated methods still work
    allegroApi_ = new AllegroApi.Builder()
        .withPodUrl(podUrl_)
        .withObjectStoreUrl(objectStoreUrl_)
        .withUserName(serviceAccount_)
        .withRsaPemCredentialFile(credentialFile_)
        .withFactories(CalendarModel.FACTORIES)
        .withTrustAllSslCerts()
        .build();
    
    System.out.println("Obsolete configuration = " + allegroApi_.getConfiguration());
    
    PodAndUserId ownerUserId = ownerId_ == null ? allegroApi_.getUserId() : PodAndUserId.newBuilder().build(ownerId_);
    
    System.out.println("CallerId is " + allegroApi_.getUserId());
    System.out.println("OwnerId is " + ownerUserId);
    
    allegroApi_.fetchPartitionObjects(new FetchPartitionObjectsRequest.Builder()
        .withQuery(new PartitionQuery.Builder()
            .withMaxItems(10)
            .withName(CalendarApp.PARTITION_NAME)
            .withOwner(ownerUserId)
            .withSortKeyPrefix(sortKeyPrefix_)
            .build()
            )
          .withConsumerManager(new ConsumerManager.Builder()
              .withConsumer(IToDoItem.class, (item, trace) ->
              {
                System.out.println("Header:  " + item.getStoredApplicationObject().getHeader());
                System.out.println("Payload: " + item);
              })
              .withConsumer(IStoredApplicationObject.class, (item, trace) ->
              {
                System.out.println("StoredApplicationObject:  " + item);
              })
              .build()
              )
          .build()
          );
  }
  
  /**
   * Main.
   * 
   * @param args Command line arguments.
   */
  public static void main(String[] args)
  {
    ListItems program = new ListItems();
    
    program.process(args);
    
    program.run();
  }
}
