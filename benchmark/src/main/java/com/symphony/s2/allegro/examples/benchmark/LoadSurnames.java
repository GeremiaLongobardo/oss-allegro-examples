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

public class LoadSurnames
{
  public static void main(String[] argv) throws IOException
  {

    PrintWriter out = new PrintWriter("familyNames.txt");
    
    File f = new File("/Users/bruce/Names_2010Census.csv");
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
