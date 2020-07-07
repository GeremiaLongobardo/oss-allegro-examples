/*
 * Copyright 2019 Symphony Communication Services, LLC.
 *
 * All Rights Reserved
 */

package com.symphony.s2.allegro.examples.getmessage;

import java.util.List;

import com.symphony.oss.allegro.api.AllegroApi;
import com.symphony.oss.allegro.api.ConsumerManager;
import com.symphony.oss.allegro.api.IAllegroApi;
import com.symphony.oss.allegro.api.request.FetchFeedMessagesRequest;
import com.symphony.oss.fugue.cmd.CommandLineHandler;
import com.symphony.oss.models.allegro.canon.facade.AllegroConfiguration;
import com.symphony.oss.models.allegro.canon.facade.ChatMessage;
import com.symphony.oss.models.allegro.canon.facade.IReceivedChatMessage;
import com.symphony.oss.models.allegro.canon.facade.IReceivedSocialMessage;
import com.symphony.oss.models.internal.pod.canon.AckId;
import com.symphony.oss.models.internal.pod.canon.FeedId;

/**
 * Create a feed for the caller's default feed. 
 * 
 * @author Bruce Skingle
 *
 */


/*
 * Temporarily withdrawn 
 */


public class FetchFeedMessages extends CommandLineHandler implements Runnable
{
  private static final String   ALLEGRO          = "ALLEGRO_";
  private static final String   SERVICE_ACCOUNT  = "SERVICE_ACCOUNT";
  private static final String   POD_URL          = "POD_URL";
  private static final String   OBJECT_STORE_URL = "OBJECT_STORE_URL";
  private static final String   CREDENTIAL_FILE  = "CREDENTIAL_FILE";
  private static final String   TRUST            = "TRUST";
  
  private String              serviceAccount_;
  private String              podUrl_;
  private String              objectStoreUrl_;
  private String              credentialFile_;
  private String              trust_;
  
  private IAllegroApi         allegroApi_;

  /**
   * Constructor.
   */
  public FetchFeedMessages()
  {
    withFlag('s',   SERVICE_ACCOUNT,  ALLEGRO + SERVICE_ACCOUNT,  String.class,   false, true,   (v) -> serviceAccount_       = v);
    withFlag('p',   POD_URL,          ALLEGRO + POD_URL,          String.class,   false, true,   (v) -> podUrl_               = v);
    withFlag('o',   OBJECT_STORE_URL, ALLEGRO + OBJECT_STORE_URL, String.class,   false, true,   (v) -> objectStoreUrl_       = v);
    withFlag('f',   CREDENTIAL_FILE,  ALLEGRO + CREDENTIAL_FILE,  String.class,   false, true,   (v) -> credentialFile_       = v);
    withFlag('T',   TRUST,            ALLEGRO + TRUST,            String.class,   false, false,  (v) -> trust_                = v);
  }

  @Override
  public void run()
  {
	AllegroApi.Builder builder = new AllegroApi.Builder()
	            .withConfiguration(new AllegroConfiguration.Builder()
	                    .withPodUrl(podUrl_)
	                    .withApiUrl(objectStoreUrl_)
	                    .withUserName(serviceAccount_)
	                    .withRsaPemCredentialFile(credentialFile_)
	                    .build()
	                    );

    if(trust_ != null)
    {
      switch(trust_.toLowerCase())
      {
        case "symphony":
          builder.withTrustedSslCertResources(IAllegroApi.SYMPHONY_DEV_QA_ROOT_CERT);
          break;
        
        case "all":
          builder.withTrustAllSslCerts();
          break;
        
        default:
          throw new IllegalArgumentException("Invalid trust value, expected \"symphony\" or \"all\"");
      }
    }
    
    allegroApi_ = builder.build();
    
    List<FeedId> feedIds = allegroApi_.listMessageFeeds();
    
    FeedId feedId;
    
    if(feedIds.isEmpty())
    {
      feedId = allegroApi_.createMessageFeed();
    }
    else
    {
      feedId = feedIds.get(0);
    }
    
    System.out.println("FeedId is " + feedId);
    
    AckId ackId = null;
    
    while(true)
    {
      System.out.println("fetchFeedMessages...");
      ackId = allegroApi_.fetchFeedMessages(new FetchFeedMessagesRequest.Builder()
          .withFeedId(feedId)
          .withAckId(ackId)
          .withConsumerManager(new ConsumerManager.Builder()
            .withConsumer(IReceivedChatMessage.class, (item, trace) ->
            {
              System.out.println(item.getPresentationML());
            })
            .withConsumer(IReceivedSocialMessage.class, (item, trace) ->
            {
              System.out.println(item.getPresentationML());
              
              if(item.isMentioned(allegroApi_.getUserId()))
              {
                String messageML     = "<messageML><p>You said <i>" + 
                    item.getText() + 
                    "</i></p></messageML>";
                
                allegroApi_.sendMessage(new ChatMessage.Builder()
                    .withMessageML(messageML)
                    .withThreadId(item.getThreadId())
                    .build()
                    );
                
                System.out.println("Replied.");
              }
            })
            .build()
          )
          .build()
          );
    }
  }
  
  /**
   * Main.
   * 
   * @param args Command line arguments.
   */
  public static void main(String[] args)
  {
    FetchFeedMessages program = new FetchFeedMessages();
    
    program.process(args);
    
    program.run();
  }
}
