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

import java.time.Instant;

import com.symphony.oss.allegro.api.AllegroApi;
import com.symphony.oss.allegro.api.IAllegroApi;
import com.symphony.oss.allegro.api.request.UpsertPartitionRequest;
import com.symphony.oss.fugue.cmd.CommandLineHandler;
import com.symphony.oss.models.allegro.canon.SslTrustStrategy;
import com.symphony.oss.models.allegro.canon.facade.AllegroConfiguration;
import com.symphony.oss.models.allegro.canon.facade.ConnectionSettings;
import com.symphony.oss.models.core.canon.facade.ThreadId;
import com.symphony.oss.models.object.canon.facade.ApplicationObjectPayload;
import com.symphony.oss.models.object.canon.facade.IApplicationObjectPayload;
import com.symphony.oss.models.object.canon.facade.IPartition;
import com.symphony.oss.models.object.canon.facade.IStoredApplicationObject;

public class CreateJsonObject extends CommandLineHandler implements JsonObjectExample
{
  private static final String ALLEGRO         = "ALLEGRO_";
  private static final String SERVICE_ACCOUNT = "SERVICE_ACCOUNT";
  private static final String POD_URL         = "POD_URL";
  private static final String OBJECT_STORE_URL = "OBJECT_STORE_URL";
  private static final String CREDENTIAL_FILE = "CREDENTIAL_FILE";
  private static final String THREAD_ID       = "THREAD_ID";
  
  private String              serviceAccount_;
  private String              podUrl_;
  private String              objectStoreUrl_;
  private String              credentialFile_;
  private ThreadId            threadId_;
  
  private IAllegroApi         allegroApi_;

  /**
   * Constructor.
   */
  public CreateJsonObject()
  {
    withFlag('s',   SERVICE_ACCOUNT,  ALLEGRO + SERVICE_ACCOUNT,  String.class,   false, true,   (v) -> serviceAccount_       = v);
    withFlag('p',   POD_URL,          ALLEGRO + POD_URL,          String.class,   false, true,   (v) -> podUrl_               = v);
    withFlag('o',   OBJECT_STORE_URL, ALLEGRO + OBJECT_STORE_URL, String.class,   false, true,   (v) -> objectStoreUrl_       = v);
    withFlag('f',   CREDENTIAL_FILE,  ALLEGRO + CREDENTIAL_FILE,  String.class,   false, false,  (v) -> credentialFile_       = v);
    withFlag('t',   THREAD_ID,        ALLEGRO + THREAD_ID,        String.class,   false, true,   (v) -> threadId_             = ThreadId.newBuilder().build(v));
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
	    
    String now = Instant.now().toString();
    String json = String.format("{ \"name\": \"Some JSON Object\", \"date\": \"%s\" }", now);
    
    IPartition partition = allegroApi_.upsertPartition(new UpsertPartitionRequest.Builder()
        .withName(PARTITION_NAME)
        .build()
      );
  
    System.out.println("partition is " + partition);
    
    System.out.println("About to create item " + json);
    
    
    IApplicationObjectPayload item = new ApplicationObjectPayload(json);
    
    IStoredApplicationObject toDoObject = allegroApi_.newApplicationObjectBuilder()
        .withThreadId(threadId_)
        .withPayload(item)
        .withPartition(partition)
        .withSortKey(now)
      .build();
    
    allegroApi_.store(toDoObject);
    
    System.out.println("Created " + toDoObject);
  }
  
  /**
   * Main.
   * 
   * @param args Command line arguments.
   */
  public static void main(String[] args)
  {
    CreateJsonObject program = new CreateJsonObject();
    
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
