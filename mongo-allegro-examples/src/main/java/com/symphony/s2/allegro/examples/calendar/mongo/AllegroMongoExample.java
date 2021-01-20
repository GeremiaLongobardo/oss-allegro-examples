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

package com.symphony.s2.allegro.examples.calendar.mongo;

import com.symphony.oss.allegro.api.AllegroPodApi;
import com.symphony.oss.allegro.api.IAllegroPodApi;
import com.symphony.oss.fugue.cmd.CommandLineHandler;
import com.symphony.oss.models.allegro.canon.facade.AllegroPodConfiguration;
import com.symphony.oss.models.core.canon.facade.PodAndUserId;
import com.symphony.oss.models.core.canon.facade.ThreadId;

/**
 * An example application which creates a ToDoItem, adding it to a current and absolute sequence.
 * 
 * @author Bruce Skingle
 *
 */
public abstract class AllegroMongoExample extends CommandLineHandler
{
  private static final String    ALLEGRO         = "ALLEGRO_";
  private static final String    SERVICE_ACCOUNT = "SERVICE_ACCOUNT";
  private static final String    POD_URL         = "POD_URL";
  private static final String    MONGO_HOST      = "MONGO_HOST";
  private static final String    MONGO_USER      = "MONGO_USER";
  private static final String    MONGO_PASSWORD  = "MONGO_PASSWORD";
  private static final String    CREDENTIAL_FILE = "CREDENTIAL_FILE";
  private static final String    THREAD_ID       = "THREAD_ID";

  private String                 serviceAccount_;
  private String                 podUrl_;
  private String                 credentialFile_;
  private ThreadId               threadId_;
  private Long                   ownerId_;


  private IAllegroPodApi         allegroPodApi_;
  
  private String                 mongoHost_;
  private String mongoUser_;
  private String mongoPassword_;

  /**
   * Constructor.
   */
  public AllegroMongoExample()
  {
    withFlag('s',   SERVICE_ACCOUNT,  ALLEGRO + SERVICE_ACCOUNT,  String.class,   false, true,   (v) -> serviceAccount_       = v);
    withFlag('p',   POD_URL,          ALLEGRO + POD_URL,          String.class,   false, true,   (v) -> podUrl_               = v);
    withFlag('H',   MONGO_HOST,       ALLEGRO + MONGO_HOST,       String.class,   false, true,   (v) -> mongoHost_            = v);
    withFlag('U',   MONGO_USER,       ALLEGRO + MONGO_USER,       String.class,   false, true,   (v) -> mongoUser_            = v);
    withFlag('P',   MONGO_PASSWORD,   ALLEGRO + MONGO_PASSWORD,   String.class,   false, true,   (v) -> mongoPassword_        = v);
    withFlag('f',   CREDENTIAL_FILE,  ALLEGRO + CREDENTIAL_FILE,  String.class,   false, false,  (v) -> credentialFile_       = v);
    withFlag('t',   THREAD_ID,        ALLEGRO + THREAD_ID,        String.class,   false, true,   (v) -> threadId_             = ThreadId.newBuilder().build(v));
  }
  
  /**
   * Method to be called from Main().
   * 
   * @param args Command line arguments.
   */
  public void run(String[] args)
  {
    process(args);
    
    allegroPodApi_ = new AllegroPodApi.Builder()
        .withConfiguration(new AllegroPodConfiguration.Builder()
                .withPodUrl(podUrl_)
                .withUserName(serviceAccount_)
                .withRsaPemCredentialFile(credentialFile_)
                .build())
        .build();
    
    PodAndUserId ownerUserId = ownerId_ == null ? allegroPodApi_.getUserId() : PodAndUserId.newBuilder().build(ownerId_);
    
    try
    {
      run(mongoUser_, mongoPassword_, mongoHost_, ownerUserId, allegroPodApi_, threadId_);
    }
    catch(RuntimeException e)
    {
      e.printStackTrace();
    }
  }
  
  protected abstract void run(String mongoUser, String mongoPassword, String mongoHost, PodAndUserId ownerUserId, IAllegroPodApi allegroPodApi, ThreadId threadId);
}
