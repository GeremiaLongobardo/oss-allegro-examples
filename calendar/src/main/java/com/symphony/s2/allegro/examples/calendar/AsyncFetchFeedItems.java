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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.symphony.oss.allegro.api.AllegroApi;
import com.symphony.oss.allegro.api.AsyncConsumerManager;
import com.symphony.oss.allegro.api.IAllegroApi;
import com.symphony.oss.allegro.api.IAllegroQueryManager;
import com.symphony.oss.allegro.api.request.FeedQuery;
import com.symphony.oss.allegro.api.request.FetchFeedObjectsRequest;
import com.symphony.oss.allegro.examples.calendar.canon.CalendarModel;
import com.symphony.oss.allegro.examples.calendar.canon.IToDoItem;
import com.symphony.oss.fugue.cmd.CommandLineHandler;
import com.symphony.oss.models.allegro.canon.SslTrustStrategy;
import com.symphony.oss.models.allegro.canon.facade.AllegroConfiguration;
import com.symphony.oss.models.allegro.canon.facade.ConnectionSettings;
import com.symphony.oss.models.core.canon.facade.PodAndUserId;

/**
 * Retrieve all objects on the given Sequence.
 * 
 * @author Bruce Skingle
 *
 */
public class AsyncFetchFeedItems extends CommandLineHandler implements Runnable
{
  private static final Logger log_ = LoggerFactory.getLogger(AsyncFetchFeedItems.class);
  
  private static final String ALLEGRO          = "ALLEGRO_";
  private static final String SERVICE_ACCOUNT  = "SERVICE_ACCOUNT";
  private static final String POD_URL          = "POD_URL";
  private static final String OBJECT_STORE_URL = "OBJECT_STORE_URL";
  private static final String CREDENTIAL_FILE  = "CREDENTIAL_FILE";
  private static final String OWNER_USER_ID    = "OWNER_USER_ID";
  
  private String              serviceAccount_;
  private String              podUrl_;
  private String              objectStoreUrl_;
  private String              credentialFile_;
  private Long                ownerId_;
  
  private IAllegroApi         allegroApi_;

  /**
   * Constructor.
   */
  public AsyncFetchFeedItems()
  {
    withFlag('s',   SERVICE_ACCOUNT,  ALLEGRO + SERVICE_ACCOUNT,  String.class,   false, true,   (v) -> serviceAccount_       = v);
    withFlag('p',   POD_URL,          ALLEGRO + POD_URL,          String.class,   false, true,   (v) -> podUrl_               = v);
    withFlag('o',   OBJECT_STORE_URL, ALLEGRO + OBJECT_STORE_URL, String.class,   false, true,   (v) -> objectStoreUrl_       = v);
    withFlag('f',   CREDENTIAL_FILE,  ALLEGRO + CREDENTIAL_FILE,  String.class,   false, true,   (v) -> credentialFile_       = v);
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
	      .withFactories(CalendarModel.FACTORIES)
	      .build();

    
    PodAndUserId ownerUserId = ownerId_ == null ? allegroApi_.getUserId() : PodAndUserId.newBuilder().build(ownerId_);
    
    System.out.println("CallerId is " + allegroApi_.getUserId());
    System.out.println("OwnerId is " + ownerUserId);
      
    IAllegroQueryManager subscriber = allegroApi_.fetchFeedObjects(new FetchFeedObjectsRequest.Builder()
        .withQuery(new FeedQuery.Builder()
            .withName(CalendarApp.FEED_NAME)
            .withOwner(ownerUserId)
            .build()
            )
        .withConsumerManager(new AsyncConsumerManager.Builder()
            .withSubscriberThreadPoolSize(10)
            .withHandlerThreadPoolSize(90)
            .withConsumer(IToDoItem.class, (message, traceContext) ->
            {
              log_.info(message.toString());
            })
            .withUnprocessableMessageConsumer((item, trace, message, cause) ->
            {
              log_.error("Failed to consume message: " + message + "\nPayload:" + item, cause);
            })
            .build()
        )
      .build()
    );

    log_.info("Subscriber state: " + subscriber.getLifecycleState());
    subscriber.start();
    
    
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    
    log_.info("Subscriber state: " + subscriber.getLifecycleState());
    
    System.err.println();
    System.err.println("Press RETURN to quit");
    try
    {
      in.readLine();
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    
    log_.info("Stopping...");
    subscriber.stop();
    log_.info("Subscriber state: " + subscriber.getLifecycleState());
  }
  
  /**
   * Main.
   * 
   * @param args Command line arguments.
   */
  public static void main(String[] args)
  {
    AsyncFetchFeedItems program = new AsyncFetchFeedItems();
    
    program.process(args);
    
    program.run();
  }
}
