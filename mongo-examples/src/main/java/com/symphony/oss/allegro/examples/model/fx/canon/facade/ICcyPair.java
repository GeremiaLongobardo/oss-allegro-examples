/**
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
 *
 *----------------------------------------------------------------------------------------------------
 * Proforma generated from
 *		Template groupId		 org.symphonyoss.s2.canon
 *           artifactId canon-template-java
 *		Template name		   proforma/java/Object/I_.java.ftl
 *		Template version	   1.0
 *  At                  2021-02-09 16:00:35 GMT
 *----------------------------------------------------------------------------------------------------
 */

package com.symphony.oss.allegro.examples.model.fx.canon.facade;

import javax.annotation.concurrent.Immutable;

import com.symphony.oss.allegro.examples.model.fx.canon.ICcyPairEntity;

/**
 * Facade for Object ObjectSchema(CcyPair)
 *
 * A Currency Pair.
 * Generated from ObjectSchema(CcyPair) at #/components/schemas/CcyPair
 * 
 * In this example application a currency pair is represented as a base and counter currency object.
 * You might prefer to actually store a currency pair as a 6 character string, this example simply
 * serves to demonstrate how to a Canon facade can be used to add a derived attribute.
 */
@Immutable
public interface ICcyPair
  extends ICcyPairEntity
{
  /**
   * Return the 6 character code for this ccy pair.
   * 
   * @return the 6 character code for this ccy pair.
   */
  String getCode();
}
/*----------------------------------------------------------------------------------------------------
 * End of template proforma/java/Object/I_.java.ftl
 * End of code generation
 *------------------------------------------------------------------------------------------------- */