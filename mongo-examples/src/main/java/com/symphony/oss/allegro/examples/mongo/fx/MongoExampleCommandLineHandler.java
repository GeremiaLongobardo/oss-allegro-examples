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

import com.symphony.oss.fugue.cmd.CommandLineHandler;
import com.symphony.oss.models.core.canon.facade.ThreadId;

/**
 * Class to gather configuration variables from the command line, system properties or environment variables.
 * See MongoExample for a description of each value.
 * 
 * @author Bruce Skingle
 *
 */
class MongoExampleCommandLineHandler extends CommandLineHandler
{
  private static final String ALLEGRO         = "ALLEGRO_";
  private static final String SERVICE_ACCOUNT = "SERVICE_ACCOUNT";
  private static final String POD_URL         = "POD_URL";
  private static final String MONGO_HOST      = "MONGO_HOST";
  private static final String MONGO_USER      = "MONGO_USER";
  private static final String MONGO_PASSWORD  = "MONGO_PASSWORD";
  private static final String CREDENTIAL_FILE = "CREDENTIAL_FILE";
  private static final String THREAD_ID       = "THREAD_ID";

  String              serviceAccount_;
  String              podUrl_;
  String              credentialFile_;
  ThreadId            threadId_;

  String              mongoHost_;
  String              mongoUser_;
  String              mongoPassword_;

  /**
   * Constructor.
   */
  MongoExampleCommandLineHandler()
  {
    withFlag('s',   SERVICE_ACCOUNT,  ALLEGRO + SERVICE_ACCOUNT,  String.class,   false, true,   (v) -> serviceAccount_       = v);
    withFlag('p',   POD_URL,          ALLEGRO + POD_URL,          String.class,   false, true,   (v) -> podUrl_               = v);
    withFlag('H',   MONGO_HOST,       ALLEGRO + MONGO_HOST,       String.class,   false, true,   (v) -> mongoHost_            = v);
    withFlag('U',   MONGO_USER,       ALLEGRO + MONGO_USER,       String.class,   false, true,   (v) -> mongoUser_            = v);
    withFlag('P',   MONGO_PASSWORD,   ALLEGRO + MONGO_PASSWORD,   String.class,   false, true,   (v) -> mongoPassword_        = v);
    withFlag('f',   CREDENTIAL_FILE,  ALLEGRO + CREDENTIAL_FILE,  String.class,   false, false,  (v) -> credentialFile_       = v);
    withFlag('t',   THREAD_ID,        ALLEGRO + THREAD_ID,        String.class,   false, true,   (v) -> threadId_             = ThreadId.newBuilder().build(v));
  }
}
