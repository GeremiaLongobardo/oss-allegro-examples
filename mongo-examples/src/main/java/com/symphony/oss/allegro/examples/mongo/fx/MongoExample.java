/*
 * Copyright 2021 Symphony Communication Services, LLC.
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

package com.symphony.oss.allegro.examples.mongo.fx;

import com.symphony.oss.allegro2.mongo.api.Allegro2MongoApi;
import com.symphony.oss.allegro2.mongo.api.IAllegro2MongoApi;
import com.symphony.oss.models.allegro.canon.facade.Allegro2Configuration;
import com.symphony.oss.models.core.canon.facade.ThreadId;

/**
 * Base class for Mongo examples.
 * 
 * This class contains protected members containing all the configuration parameters used by the various examples,
 * as well as the Allegro Mongo client.
 * 
 * @author Bruce Skingle
 *
 */
abstract class MongoExample
{
  /** Name of the Symphony service account to login to the pod as */
  protected final String            serviceAccount_;
  /** URL of the Symphony pod */
  protected final String            podUrl_;
  /* Path to a file containing the service account RSA private key to authenticate with */
  protected final String            credentialFile_;
  /* Thread (conversation) ID whose content key should be used to encrypt new objects */
  protected final ThreadId          threadId_;
  /** Host name for the Mongodb cluster */
  protected final String            mongoHost_;
  /** Mongodb user name */
  protected final String            mongoUser_;
  /** Mongodb password */
  protected final String            mongoPassword_;
  
  /** Allegro client */
  protected final IAllegro2MongoApi allegro2MongoApi_;

  /**
   * Constructor.
   * 
   * @param args Command line arguments.
   */
  MongoExample(String[] args)
  {
    MongoExampleCommandLineHandler handler = new MongoExampleCommandLineHandler();
    
    handler.process(args);

    serviceAccount_ = handler.serviceAccount_;
    podUrl_         = handler.podUrl_;
    credentialFile_ = handler.credentialFile_;
    threadId_       = handler.threadId_;
    mongoHost_      = handler.mongoHost_;
    mongoUser_      = handler.mongoUser_;
    mongoPassword_  = handler.mongoPassword_;
  
    
    allegro2MongoApi_ = new Allegro2MongoApi.Builder()
        .withConfiguration(new Allegro2Configuration.Builder()
                .withPodUrl(podUrl_)
                .withUserName(serviceAccount_)
                .withRsaPemCredentialFile(credentialFile_)
                .build())
        .build();
  }
}
