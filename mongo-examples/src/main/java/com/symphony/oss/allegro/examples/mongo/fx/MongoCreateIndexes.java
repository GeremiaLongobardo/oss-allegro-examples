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

import com.mongodb.client.model.Indexes;

/**
 * Create indexes on the FX collection. The stream prices example does a query which will use the  header.payloadType index.
 * 
 * @author Bruce Skingle
 *
 */
public class MongoCreateIndexes extends MongoFxExample
{
  private MongoCreateIndexes(String[] args)
  {
    super(args);
  }

  private void run()
  {
    fxItems_.createIndex(Indexes.ascending("header.payloadType"));
  }
  
  /**
   * Main.
   * 
   * @param args Command line arguments.
   */
  public static void main(String[] args)
  {
    new MongoCreateIndexes(args).run();
  }
}
