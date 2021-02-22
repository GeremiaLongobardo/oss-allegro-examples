/*
 * Copyright 2019 Symphony Communication Services, LLC.
 *
 * All Rights Reserved
 */

package com.symphony.s2.allegro.examples.getmessage;

import com.symphony.oss.allegro2.api.Allegro2Api;
import com.symphony.oss.allegro2.api.IAllegro2Api;
import com.symphony.oss.fugue.cmd.CommandLineHandler;
import com.symphony.oss.models.allegro.canon.facade.Allegro2Configuration;
import com.symphony.oss.models.allegro.canon.facade.IChatMessage;
import com.symphony.oss.models.core.canon.facade.ThreadId;

/**
 * Send a message containing an at-mention using the email address shorthand tag.
 * 
 * @author Bruce Skingle
 *
 */
public class AtMention extends CommandLineHandler implements Runnable
{
  private static final String ALLEGRO         = "ALLEGRO_";
  private static final String SERVICE_ACCOUNT = "SERVICE_ACCOUNT";
  private static final String POD_URL         = "POD_URL";
  private static final String CREDENTIAL_FILE = "CREDENTIAL_FILE";
  private static final String THREAD_ID       = "THREAD_ID";
  
  private String              serviceAccount_;
  private String              podUrl_;
  private String              credentialFile_;
  private ThreadId            threadId_;
  
  private IAllegro2Api         allegroApi_;

  /**
   * Constructor.
   */
  public AtMention()
  {
    withFlag('s',   SERVICE_ACCOUNT,  ALLEGRO + SERVICE_ACCOUNT,  String.class,   false, true,   (v) -> serviceAccount_       = v);
    withFlag('p',   POD_URL,          ALLEGRO + POD_URL,          String.class,   false, true,   (v) -> podUrl_               = v);
    withFlag('f',   CREDENTIAL_FILE,  ALLEGRO + CREDENTIAL_FILE,  String.class,   false, true,   (v) -> credentialFile_       = v);
    withFlag('t',   THREAD_ID,        ALLEGRO + THREAD_ID,        String.class,   false, true,   (v) -> threadId_             = ThreadId.newBuilder().build(v));
  }
  
  @Override
  public void run()
  {
    allegroApi_ = new Allegro2Api.Builder()
      .withConfiguration(new Allegro2Configuration.Builder()
        .withPodUrl(podUrl_)
        .withUserName(serviceAccount_)
        .withRsaPemCredentialFile(credentialFile_)
        .build()
      )
      .build();
    
    String messageML     = "<messageML><p>I am <mention email=\"" + allegroApi_.getUserInfo().getEmailAddress() + "\"/></p></messageML>";

    IChatMessage chatMessage = allegroApi_.newChatMessageBuilder()
        .withThreadId(threadId_)
        .withMessageML(messageML)
        .build();
    
    allegroApi_.sendMessage(chatMessage);
  }
  
  /**
   * Main.
   * 
   * @param args Command line arguments.
   */
  public static void main(String[] args)
  {
    AtMention program = new AtMention();
    
    program.process(args);
    
    program.run();
  }
}
