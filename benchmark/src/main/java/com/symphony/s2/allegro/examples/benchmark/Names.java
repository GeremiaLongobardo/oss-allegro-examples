/*
 * Copyright 2021 Symphony Communication Services, LLC.
 *
 * All Rights Reserved
 */

package com.symphony.s2.allegro.examples.benchmark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;

public class Names
{
  static SecureRandom random_ = new SecureRandom();
  static long         now_ = System.currentTimeMillis();
  
  List<String> names_ = new LinkedList<>();
  
  public Names(String name)
  {
    try(InputStream in = getClass().getResourceAsStream(name))
    {
      BufferedReader r = new BufferedReader(new InputStreamReader(in));
      String line;
      
      while((line = r.readLine()) != null)
      {
        names_.add(line);
      }
    }
    catch (IOException e)
    {
      throw new IllegalStateException(e);
    }
  }
  
  public String next()
  {
    return names_.get(random_.nextInt(names_.size() - 1));
  }
  
  public static Instant nextInstant()
  {
    return Instant.ofEpochMilli(random_.nextLong() % now_);
  }
}
