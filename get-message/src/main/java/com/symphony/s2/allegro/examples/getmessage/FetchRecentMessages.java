/*
 * Copyright 2019-2020 Symphony Communication Services, LLC.
 *
 * All Rights Reserved
 */

package com.symphony.s2.allegro.examples.getmessage;

import com.symphony.oss.allegro.api.AllegroApi;
import com.symphony.oss.allegro.api.ConsumerManager;
import com.symphony.oss.allegro.api.IAllegroApi;
import com.symphony.oss.allegro.api.request.FetchRecentMessagesRequest;
import com.symphony.oss.fugue.cmd.CommandLineHandler;
import com.symphony.oss.models.allegro.canon.SslTrustStrategy;
import com.symphony.oss.models.allegro.canon.facade.AllegroConfiguration;
import com.symphony.oss.models.allegro.canon.facade.ConnectionSettings;
import com.symphony.oss.models.allegro.canon.facade.IReceivedChatMessage;
import com.symphony.oss.models.allegro.canon.facade.IReceivedSocialMessage;
import com.symphony.oss.models.core.canon.facade.ThreadId;

/**
 * Fetch the 5 most recent messages from the given conversation.
 * 
 * @author Bruce Skingle
 *
 */
public class FetchRecentMessages extends CommandLineHandler implements Runnable
{
  private static final String ALLEGRO             = "ALLEGRO_";
  private static final String SERVICE_ACCOUNT     = "SERVICE_ACCOUNT";
  private static final String POD_URL             = "POD_URL";
  private static final String OBJECT_STORE_URL    = "OBJECT_STORE_URL";
  private static final String CREDENTIAL_FILE     = "CREDENTIAL_FILE";
  private static final String THREAD_ID           = "THREAD_ID";
  private static final String MAX_MESSAGES        = "MAX_MESSAGES";
  
  private String              serviceAccount_;
  private String              podUrl_;
  private String              objectStoreUrl_;
  private String              credentialFile_;
  private ThreadId            threadId_;
  private int                 maxMessages_ = 5;
  
  private IAllegroApi         allegroApi_;

  /**
   * Constructor.
   */
  public FetchRecentMessages()
  {
    withFlag('s',   SERVICE_ACCOUNT,      ALLEGRO + SERVICE_ACCOUNT,      String.class,   false, true,   (v) -> serviceAccount_       = v);
    withFlag('p',   POD_URL,              ALLEGRO + POD_URL,              String.class,   false, true,   (v) -> podUrl_               = v);
    withFlag('o',   OBJECT_STORE_URL,     ALLEGRO + OBJECT_STORE_URL,     String.class,   false, true,   (v) -> objectStoreUrl_       = v);
    withFlag('f',   CREDENTIAL_FILE,      ALLEGRO + CREDENTIAL_FILE,      String.class,   false, true,   (v) -> credentialFile_       = v);
    withFlag('t',   THREAD_ID,            ALLEGRO + THREAD_ID,            String.class,   false, true,   (v) -> threadId_             = ThreadId.newBuilder().build(v));
    withFlag('m',   MAX_MESSAGES   ,      ALLEGRO + MAX_MESSAGES,         Integer.class,  false, false,  (v) -> maxMessages_          = v);
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
	            .build();
	    
//      .withTrustedSslCertResources(IAllegroApi.SYMPHONY_DEV_QA_ROOT_CERT)

    System.out.println("Fetch messages from pod...");
    allegroApi_.fetchRecentMessagesFromPod(
        new FetchRecentMessagesRequest.Builder()
          .withThreadId(threadId_)
          .withMaxItems(maxMessages_)
          .withConsumerManager(new ConsumerManager.Builder()
              .withConsumer(IReceivedChatMessage.class, (message, trace) ->
              {
                System.out.println("M " + message.getMessageId() + " " + message.getText());
              }
              )
              .withConsumer(IReceivedSocialMessage.class, (message, trace) ->
              {
                System.out.println("S " + message.getMessageId() + " " + message.getText() + " " + message.getMentions());
                System.out.println("S " + message.getPresentationML());
              }
              )
              .build()
          )
          .build()
        );
  }
  
  /**
   * Main.
   * 
   * @param args Command line arguments.
   */
  public static void main(String[] args)
  {
    FetchRecentMessages program = new FetchRecentMessages();
    
    program.process(args);
    
    program.run();
  }
}
