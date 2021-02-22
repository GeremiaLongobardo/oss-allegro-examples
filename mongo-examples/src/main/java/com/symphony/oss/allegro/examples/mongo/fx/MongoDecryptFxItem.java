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

import com.symphony.oss.models.core.canon.facade.IApplicationRecord;

/**
 * An example application which decrypts a single FX item.
 * 
 * @author Bruce Skingle
 *
 */
public class MongoDecryptFxItem extends MongoFxExample
{
  private MongoDecryptFxItem(String[] args)
  {
    super(args);
  }

  private void run()
  {
    String json = "{\n" + 
        "  \"_id\":{\n" + 
        "    \"$oid\":\"6023bf49117f470fb1a8ba33\"\n" + 
        "  },\n" + 
        "  \"_type\":\"com.symphony.s2.model.core.EncryptedApplicationRecord\",\n" + 
        "  \"_version\":\"1.0\",\n" + 
        "  \"cipherSuiteId\":\"RSA2048_AES256\",\n" + 
        "  \"encryptedPayload\":\"QNszVba7aZbLTM9tXSEK48nwgCbLhNHhFmRNfaFGjtBoLS7/NA8Zn1n7FmxlCPjZGIaQ3sB/aP7jfZX2SCEHajIYqLZgMKJBEh/ksnIrwIDsZlT3yVTJEUiGftHR11BtrLP/bPzWKyETm1PZUAb1h3YcnSLMpP4O8y1knYgYwzMTuV+JZl1Vlfj8Q58mgMqlmWNn2t+oCagpjxEl+FJisF/Xea0XskkOjJruOSBSJX5n9KGYYRt69T9/K02bkEUulfSuXiFKJyc6p/RpysE++PK8YmPRaS0Rt1ZKhlWPvWRXsbeGJoUCkuvJpDvmtUAZRLodvmPlYjbW5R2ApZaTBr55A8Qq/uP6G35/Jd61O2GhYaaUEL2PSm71D9dK6Qn9FlJ2sgli1RojkAGz/q6p5JYIA2ZcyKhasltkYhwsfQempZShQQPaw4D+eo6AlG4w21WBwPdpATCVYd6TNnrbpDm7STxG/Y8YeZ4RgH7vrobQcdMjZQtj8p2HVvDQfxxHHQUaEHl0jQ==\",\n" + 
        "  \"header\":{\n" + 
        "    \"_type\":\"com.symphony.oss.allegro.examples.model.fx.FxHeader\",\n" + 
        "    \"_version\":\"1.0\",\n" + 
        "    \"ccyPair\":{\n" + 
        "      \"_type\":\"com.symphony.oss.allegro.examples.model.fx.CcyPair\",\n" + 
        "      \"_version\":\"1.0\",\n" + 
        "      \"base\":\"GBP\",\n" + 
        "      \"counter\":\"USD\"\n" + 
        "    },\n" + 
        "    \"id\":\"2021-02-10T11:11:03.905Z\",\n" + 
        "    \"payloadType\":\"com.symphony.oss.allegro.examples.model.fx.Rfq\"\n" + 
        "  },\n" + 
        "  \"rotationId\":0,\n" + 
        "  \"threadId\":\"ccb3dVjJaRpNtCkOaN0KYn///o1rbTtZdA==\"\n" + 
        "}\n";
    
    IApplicationRecord applicationRecord = allegro2MongoApi_.decrypt(json);
      
    
    System.out.println("header = " + applicationRecord.getHeader());
    System.out.println("payload = " + applicationRecord.getPayload());
  }

  /**
   * Main.
   * 
   * @param args Command line arguments.
   */
  public static void main(String[] args)
  {
    new MongoDecryptFxItem(args).run();
  }
}
