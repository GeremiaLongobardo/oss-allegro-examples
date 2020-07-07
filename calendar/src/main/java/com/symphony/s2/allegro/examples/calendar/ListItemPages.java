/*
 * Copyright 2019 Symphony Communication Services, LLC.
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

package com.symphony.s2.allegro.examples.calendar;

import com.symphony.oss.allegro.api.AllegroApi;
import com.symphony.oss.allegro.api.IAllegroApi;
import com.symphony.oss.allegro.api.IObjectPage;
import com.symphony.oss.allegro.api.request.PartitionQuery;
import com.symphony.oss.allegro.examples.calendar.canon.CalendarModel;
import com.symphony.oss.fugue.cmd.CommandLineHandler;
import com.symphony.oss.models.allegro.canon.SslTrustStrategy;
import com.symphony.oss.models.allegro.canon.facade.AllegroConfiguration;
import com.symphony.oss.models.allegro.canon.facade.ConnectionSettings;
import com.symphony.oss.models.core.canon.facade.PodAndUserId;
import com.symphony.oss.models.object.canon.facade.IApplicationObjectPayload;
import com.symphony.oss.models.object.canon.facade.IStoredApplicationObject;

/**
 * Retrieve all objects on the given Sequence.
 * 
 * @author Bruce Skingle
 *
 */
public class ListItemPages extends CommandLineHandler implements Runnable
{
  private static final boolean EASY_WAY        = true;
  
  private static final String ALLEGRO          = "ALLEGRO_";
  private static final String SERVICE_ACCOUNT  = "SERVICE_ACCOUNT";
  private static final String POD_URL          = "POD_URL";
  private static final String OBJECT_STORE_URL = "OBJECT_STORE_URL";
  private static final String CREDENTIAL_FILE  = "CREDENTIAL_FILE";
  private static final String OWNER_USER_ID    = "OWNER_USER_ID";
  private static final String SORT_KEY_PREFIX  = "SORT_KEY_PREFIX";
  private static final String PAGE_SIZE        = "PAGE_SIZE";
  
  private String              serviceAccount_;
  private String              podUrl_;
  private String              objectStoreUrl_;
  private String              credentialFile_;
  private String              sortKeyPrefix_;
  private Long                ownerId_;
  private Integer             pageSize_ = 4;
  
  private IAllegroApi         allegroApi_;

  /**
   * Constructor.
   */
  public ListItemPages()
  {
    withFlag('s',   SERVICE_ACCOUNT,  ALLEGRO + SERVICE_ACCOUNT,  String.class,   false, true,   (v) -> serviceAccount_       = v);
    withFlag('p',   POD_URL,          ALLEGRO + POD_URL,          String.class,   false, true,   (v) -> podUrl_               = v);
    withFlag('o',   OBJECT_STORE_URL, ALLEGRO + OBJECT_STORE_URL, String.class,   false, true,   (v) -> objectStoreUrl_       = v);
    withFlag('f',   CREDENTIAL_FILE,  ALLEGRO + CREDENTIAL_FILE,  String.class,   false, true,   (v) -> credentialFile_       = v);
    withFlag('u',   OWNER_USER_ID,    ALLEGRO + OWNER_USER_ID,    Long.class,     false, false,  (v) -> ownerId_              = v);
    withFlag('k',   SORT_KEY_PREFIX,  ALLEGRO + SORT_KEY_PREFIX,  String.class,   false, false,  (v) -> sortKeyPrefix_        = v);
    withFlag('n',   PAGE_SIZE,        ALLEGRO + PAGE_SIZE,        Integer.class,  false, false,  (v) -> pageSize_             = v);
  }
  
  @Override
  public void run()
  {
	allegroApi_ = new AllegroApi.Builder()
	            .withConfiguration(new AllegroConfiguration.Builder()
	                    .withPodUrl(podUrl_)
	                    .withApiUrl(objectStoreUrl_)
	                    .withUserName(serviceAccount_)
	                    .withRsaPemCredentialFile(credentialFile_)
	                    .withApiConnectionSettings(new ConnectionSettings.Builder()
	                        .withSslTrustStrategy(SslTrustStrategy.TRUST_ALL_CERTS)
	                        .build())
	                    .build())
	            .withFactories(CalendarModel.FACTORIES)
	            .build();
    
    PodAndUserId ownerUserId = ownerId_ == null ? allegroApi_.getUserId() : PodAndUserId.newBuilder().build(ownerId_);
    
    System.out.println("CallerId is " + allegroApi_.getUserId());
    System.out.println("OwnerId is " + ownerUserId);
    
    IObjectPage page = allegroApi_.fetchPartitionObjectPage(new PartitionQuery.Builder()
            .withMaxItems(pageSize_)
            .withName(CalendarApp.PARTITION_NAME)
            .withOwner(ownerUserId)
            .withSortKeyPrefix(sortKeyPrefix_)
            .build()
            );
    
    IObjectPage lastPage;
    
    do
    {
      System.out.println();
      System.out.println("PAGE --------------------------------");
      
      for(IStoredApplicationObject item : page.getData())
      {
        IApplicationObjectPayload payload = allegroApi_.decryptObject(item);
        
        System.out.println(item.getAbsoluteHash() + " " + item.getCanonType() + " " + payload.getCanonType());
      }
      
      lastPage = page;
      
      if(EASY_WAY)
      {
        page = page.fetchNextPage();
      }
      else
      {
        if(page.getAfter() == null)
        {
          page = null;
        }
        else
        {
          page = allegroApi_.fetchPartitionObjectPage(new PartitionQuery.Builder()
              .withMaxItems(pageSize_)
              .withName(CalendarApp.PARTITION_NAME)
              .withOwner(ownerUserId)
              .withSortKeyPrefix(sortKeyPrefix_)
              .withAfter(page.getAfter())
              .build()
              );
        }
      }
    }while(page != null);
    

    System.out.println();
    System.out.println("PENULTIMATE PAGE --------------------------------");
    
    if(EASY_WAY)
    {
      page = lastPage.fetchPrevPage();
    }
    else
    {
      if(lastPage.getBefore() == null)
      {
        page = null;
      }
      else
      {
        page = allegroApi_.fetchPartitionObjectPage(new PartitionQuery.Builder()
            .withMaxItems(pageSize_)
            .withName(CalendarApp.PARTITION_NAME)
            .withOwner(ownerUserId)
            .withSortKeyPrefix(sortKeyPrefix_)
            .withAfter(lastPage.getBefore())
            .withScanForwards(false)
            .build()
            );
      }
    }
    
    for(IStoredApplicationObject item : page.getData())
    {
      IApplicationObjectPayload payload = allegroApi_.decryptObject(item);
      
      System.out.println(item.getAbsoluteHash() + " " + item.getCanonType() + " " + payload.getCanonType());
    }
  }
  
  /**
   * Main.
   * 
   * @param args Command line arguments.
   */
  public static void main(String[] args)
  {
    ListItemPages program = new ListItemPages();
    
    program.process(args);
    
    program.run();
  }
}
