/*
 * Copyright 2021 Symphony Communication Services, LLC.
 *
 * All Rights Reserved
 */

package com.symphony.s2.allegro.examples.benchmark;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.bson.Document;

import com.symphony.oss.canon.runtime.IEntity;
import com.symphony.oss.canon.runtime.ModelRegistry;

public class Entities implements Iterable<IEntity> //Closeable
{
  private List<IEntity> entities_ = new LinkedList<>();
//  private InputStream in_;
//  private BufferedReader reader_;
//  private ModelRegistry modelRegistry_;
  int index_ = 0;

  public Entities(ModelRegistry modelRegistry, String name)
  {
//    modelRegistry_ = modelRegistry;
    try(InputStream in = getClass().getResourceAsStream(name))
    {

      //      reader_ = new BufferedReader(new InputStreamReader(in_));
      modelRegistry.parseStream(in, (modelObject) ->
      {
        entities_.add(modelObject);
      });
    }
    catch (IOException e)
    {
      System.err.println("Failed to close input");
      e.printStackTrace();
    }
  }
  
//  public IEntity next()
//  {
//    if(index_ >= entities_.size())
//      return null;
//    
//    return entities_.get(index_++);
//  }

  public List<IEntity> getDocuments()
  {
    return entities_;
  }

  @Override
  public Iterator<IEntity> iterator()
  {
    return entities_.iterator();
  }

//  @Override
//  public void close() throws IOException
//  {
//    in_.close();
//  }
}
