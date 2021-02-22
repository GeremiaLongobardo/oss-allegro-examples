/*
 * Copyright 2021 Symphony Communication Services, LLC.
 *
 * All Rights Reserved
 */

package com.symphony.s2.allegro.examples.benchmark;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

public class LoadGivenNames
{
  public static void main(String[] argv) throws IOException
  {

    PrintWriter out = new PrintWriter("givenNames.txt");
    
    File f = new File("/Users/bruce/yob2019.txt");
    FileReader fis = new FileReader(f);
    BufferedReader r = new BufferedReader(fis);
    String line;
    String name = null;
    
    r.readLine();
    
    while((line = r.readLine()) != null)
    {
      if(name != null)
        out.println(name);
      
      String n = line.split(",")[0];
    
      name = n.charAt(0) + n.substring(1).toLowerCase();
    }
    
  }
}
