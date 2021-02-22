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
 *  At                  2021-02-10 11:27:52 GMT
 *----------------------------------------------------------------------------------------------------
 */

package com.symphony.oss.allegro.examples.model.fx.canon.facade;

import javax.annotation.concurrent.Immutable;

import com.symphony.oss.allegro.examples.model.fx.canon.IFxBaseEntity;
import com.symphony.oss.allegro.examples.model.fx.canon.IFxHeader;
import com.symphony.oss.models.core.canon.IApplicationPayload;

/**
 * Facade for Object ObjectSchema(FxBase)
 *
 * Base of FX objects.
 * Generated from ObjectSchema(FxBase) at #/components/schemas/FxBase
 * 
 * In this FX example all objects have a common header object format, this facade shows how hand written code can
 * be safely overlaid on the Canon generated code.
 * 
 */
@Immutable
public interface IFxBase
  extends IApplicationPayload, IFxBaseEntity
{
  /**
   * Return the header for this object.
   * 
   * @return the header for this object.
   */
  IFxHeader getHeader();
}
/*----------------------------------------------------------------------------------------------------
 * End of template proforma/java/Object/I_.java.ftl
 * End of code generation
 *------------------------------------------------------------------------------------------------- */