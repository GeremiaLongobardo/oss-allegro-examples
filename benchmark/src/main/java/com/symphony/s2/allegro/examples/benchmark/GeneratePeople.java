/*
 * Copyright 2021 Symphony Communication Services, LLC.
 *
 * All Rights Reserved
 */

package com.symphony.s2.allegro.examples.benchmark;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;

import com.symphony.oss.allegro.examples.calendar.canon.IPersonDetails;
import com.symphony.oss.allegro.examples.calendar.canon.IPersonIndex;
import com.symphony.oss.allegro.examples.calendar.canon.PersonDetails;
import com.symphony.oss.allegro.examples.calendar.canon.PersonIndex;
import com.symphony.oss.models.core.canon.facade.ApplicationRecord;
import com.symphony.oss.models.core.canon.facade.IApplicationRecord;

public class GeneratePeople
{
  long           count_;
  Names  givenNames_ = new Names("/givenNames.txt");
  Names  familyNames_ = new Names("/familyNames.txt");
  
  public GeneratePeople(int count)
  {
    count_ = count;
  }

  public void run() throws FileNotFoundException
  {
//    PrintStream out = System.out;
    
    PrintWriter out = new PrintWriter(count_ + "people.json"); 
    
    out.println("[");
    
    boolean first = true;
    
    for(long i=1 ; i<=count_ ; i++)
    {
      if(first)
        first = false;
      else
        out.println(",");
      
      IPersonIndex personIndex = new PersonIndex.Builder()
          .withGivenName(givenNames_.next())
          .withFamilyName(familyNames_.next())
          .build();
      
      IPersonDetails personDetails = new PersonDetails.Builder()
          .withId(i)
          .withDob(Names.nextInstant())
          .build();
      
   // Create an encrypted StoredApplicationRecord.
      IApplicationRecord record = new ApplicationRecord.Builder()
          .withHeader(personIndex)
          .withPayload(personDetails)
        .build();
      
      out.print(record.toString().replaceAll("\\n *", ""));
    }
    out.println();
    out.println("]");
    out.close();
  }

  public static void main(String[] argv) throws IOException
  {
    new GeneratePeople(1000).run();
  }
}
