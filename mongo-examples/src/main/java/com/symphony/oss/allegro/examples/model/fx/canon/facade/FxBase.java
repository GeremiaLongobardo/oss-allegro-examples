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
 *		Template name		   proforma/java/Object/_.java.ftl
 *		Template version	   1.0
 *  At                  2021-02-10 11:27:52 GMT
 *----------------------------------------------------------------------------------------------------
 */

package com.symphony.oss.allegro.examples.model.fx.canon.facade;

import javax.annotation.concurrent.Immutable;

import com.symphony.oss.allegro.examples.model.fx.canon.FxBaseEntity;
import com.symphony.oss.allegro.examples.model.fx.canon.FxHeader;
import com.symphony.oss.allegro.examples.model.fx.canon.IFxHeader;
import com.symphony.oss.canon.runtime.IModelRegistry;
import com.symphony.oss.commons.dom.json.ImmutableJsonObject;
import com.symphony.oss.commons.dom.json.MutableJsonObject;

/**
 * Facade for Object ObjectSchema(FxBase)
 *
 * Base of FX objects.
 * Generated from ObjectSchema(FxBase) at #/components/schemas/FxBase
 * 
 * Everything in this file was generated as a proforma by Canon except for the getHeader() method
 * which was hand written.
 */
@Immutable
public class FxBase extends FxBaseEntity implements IFxBase
{
  /**
   * Constructor from builder.
   * 
   * @param builder A mutable builder containing all values.
   */
  public FxBase(AbstractFxBaseBuilder<?,?> builder)
  {
    super(builder);
  }
  
  /**
   * Constructor from serialised form.
   * 
   * @param jsonObject An immutable JSON object containing the serialized form of the object.
   * @param modelRegistry A model registry to use to deserialize any nested objects.
   */
  public FxBase(ImmutableJsonObject jsonObject, IModelRegistry modelRegistry)
  {
    super(jsonObject, modelRegistry);
  }
  
  /**
   * Constructor from mutable JSON object.
   * 
   * @param mutableJsonObject A mutable JSON object containing the serialized form of the object.
   * @param modelRegistry A model registry to use to deserialize any nested objects.
   */
  public FxBase(MutableJsonObject mutableJsonObject, IModelRegistry modelRegistry)
  {
    super(mutableJsonObject, modelRegistry);
  }
   
  /**
   * Copy constructor.
   * 
   * @param other Another instance from which all attributes are to be copied.
   */
  public FxBase(IFxBase other)
  {
    super(other);
  }
  
  @Override
  public IFxHeader getHeader()
  {
    return new FxHeader.Builder()
        .withCcyPair(getCcyPair())
        .withId(getId())
        .withPayloadType(getCanonType())
        .build();
  }
}
/*----------------------------------------------------------------------------------------------------
 * End of template proforma/java/Object/_.java.ftl
 * End of code generation
 *------------------------------------------------------------------------------------------------- */