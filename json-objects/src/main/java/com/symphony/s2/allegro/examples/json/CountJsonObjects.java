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

package com.symphony.s2.allegro.examples.json;

import com.symphony.oss.allegro.api.AllegroApi;
import com.symphony.oss.allegro.api.ConsumerManager;
import com.symphony.oss.allegro.api.IAllegroApi;
import com.symphony.oss.allegro.api.request.FetchPartitionObjectsRequest;
import com.symphony.oss.allegro.api.request.PartitionQuery;
import com.symphony.oss.fugue.cmd.CommandLineHandler;
import com.symphony.oss.models.allegro.canon.SslTrustStrategy;
import com.symphony.oss.models.allegro.canon.facade.AllegroConfiguration;
import com.symphony.oss.models.allegro.canon.facade.ConnectionSettings;
import com.symphony.oss.models.core.canon.facade.PodAndUserId;
import com.symphony.oss.models.object.canon.facade.IApplicationObjectPayload;
import com.symphony.oss.models.pod.canon.IUserV2;

/**
 * Retrieve all objects on the given Sequence.
 * 
 * @author Bruce Skingle
 *
 */
public class CountJsonObjects extends CommandLineHandler implements JsonObjectExample
{
  private static final String ALLEGRO          = "ALLEGRO_";
  private static final String SERVICE_ACCOUNT  = "SERVICE_ACCOUNT";
  private static final String POD_URL          = "POD_URL";
  private static final String OBJECT_STORE_URL = "OBJECT_STORE_URL";
  private static final String CREDENTIAL_FILE  = "CREDENTIAL_FILE";
  private static final String OWNER_USER_ID    = "OWNER_USER_ID";
  private static final String CERT_FILE        = "CERT_FILE";
  private static final String CERT_PASSWORD    = "CERT_PASSWORD";
  private static final String SESSION_AUTH_URL = "SESSION_AUTH_URL";
  private static final String KEY_AUTH_URL     = "KEY_AUTH_URL";
  private static final String PROXY_URL        = "PROXY_URL";
  
  private String              serviceAccount_;
  private String              podUrl_;
  private String              objectStoreUrl_;
  private String              credentialFile_;
  
  private IAllegroApi         allegroApi_;
  private String              certFile_;
  private String              certPassword_;
  private String              sessionAuthUrl_;
  private String              keyAuthUrl_;
  private String              proxyUrl_;
  private Long                ownerId_;
  private int messageCount_;

  /**
   * Constructor.
   */
  public CountJsonObjects()
  {
    withFlag('s',   SERVICE_ACCOUNT,  ALLEGRO + SERVICE_ACCOUNT,  String.class,   false, false,  (v) -> serviceAccount_       = v);
    withFlag('p',   POD_URL,          ALLEGRO + POD_URL,          String.class,   false, true,   (v) -> podUrl_               = v);
    withFlag('o',   OBJECT_STORE_URL, ALLEGRO + OBJECT_STORE_URL, String.class,   false, true,   (v) -> objectStoreUrl_       = v);
    withFlag('f',   CREDENTIAL_FILE,  ALLEGRO + CREDENTIAL_FILE,  String.class,   false, false,  (v) -> credentialFile_       = v);
    withFlag('u',   OWNER_USER_ID,    ALLEGRO + OWNER_USER_ID,    Long.class,     false, false,  (v) -> ownerId_              = v);
    withFlag(null,  CERT_FILE,        ALLEGRO + CERT_FILE,        String.class,   false, false,  (v) -> certFile_             = v);
    withFlag(null,  CERT_PASSWORD,    ALLEGRO + CERT_PASSWORD,    String.class,   false, false,  (v) -> certPassword_         = v);
    withFlag(null,  SESSION_AUTH_URL, ALLEGRO + SESSION_AUTH_URL, String.class,   false, false,  (v) -> sessionAuthUrl_       = v);
    withFlag(null,  KEY_AUTH_URL,     ALLEGRO + KEY_AUTH_URL,     String.class,   false, false,  (v) -> keyAuthUrl_           = v);
    withFlag(null,  PROXY_URL,        ALLEGRO + PROXY_URL,        String.class,   false, false,  (v) -> proxyUrl_           = v);
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
          .withAuthCertFile(certFile_)
          .withAuthCertFilePassword(certPassword_)
          .withSessionAuthUrl(sessionAuthUrl_)
          .withKeyAuthUrl(keyAuthUrl_)
          .withDefaultConnectionSettings(new ConnectionSettings.Builder()
              .withSslTrustStrategy(SslTrustStrategy.TRUST_ALL_CERTS)
              .withProxyUrl(proxyUrl_)
              .build())
          .build())
      .build();
    
   System.out.println("Allegro configuration = " + allegroApi_.getConfiguration());
   
   PodAndUserId ownerUserId = ownerId_ == null ? allegroApi_.getUserId() : PodAndUserId.newBuilder().build(ownerId_);
   System.out.println("CallerId is " + allegroApi_.getUserId());
   System.out.println("OwnerId is " + ownerUserId);

   while(true)
   {
     IUserV2 sessionInfo = allegroApi_.getSessioninfo();
     
     System.out.println("sessionInfo=" + sessionInfo);
     
     messageCount_=0;
     
     allegroApi_.fetchPartitionObjects(new FetchPartitionObjectsRequest.Builder()
        .withQuery(new PartitionQuery.Builder()
            .withName(PARTITION_NAME)
            .withOwner(ownerUserId)
            .withMaxItems(10)
            .build()
            )
        .withConsumerManager(new ConsumerManager.Builder()
            .withConsumer(IApplicationObjectPayload.class, (item, trace) ->
            {
              System.out.println("Header:  " + item.getStoredApplicationObject().getHeader());
              System.out.println("Payload: " + item);
              messageCount_++;
            })
            .build()
            )
        .build()
        );
    
      System.out.println("Got " + messageCount_ + " objects, Sleeping...");
      try
      {
        Thread.sleep(15000);
      }
      catch (InterruptedException e)
      {
        throw new IllegalStateException(e);
      }
    }
  }
  
  /**
   * Main.
   * 
   * @param args Command line arguments.
   */
  public static void main(String[] args)
  {
    CountJsonObjects program = new CountJsonObjects();
    
    program.process(args);
    
    program.run();
  }
}
