/*
 * Copyright 2019 Symphony Communication Services, LLC.
 *
 * All Rights Reserved
 */

package com.symphony.s2.allegro.examples.getmessage;

import com.symphony.oss.allegro2.api.Allegro2Api;
import com.symphony.oss.allegro2.api.IAllegro2Api;
import com.symphony.oss.fugue.cmd.CommandLineHandler;
import com.symphony.oss.models.allegro.canon.SslTrustStrategy;
import com.symphony.oss.models.allegro.canon.facade.AllegroConfiguration;
import com.symphony.oss.models.allegro.canon.facade.ConnectionSettings;
import com.symphony.oss.models.allegro.canon.facade.IChatMessage;
import com.symphony.oss.models.core.canon.facade.ThreadId;

/**
 * Send a message containing an at-mention using the email address shorthand tag.
 * 
 * @author Bruce Skingle
 *
 */
public class SendEntityJson extends CommandLineHandler implements Runnable
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

  private IAllegro2Api        allegroApi_;

  /**
   * Constructor.
   */
  public SendEntityJson()
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
	            .withConfiguration(new AllegroConfiguration.Builder()
	                    .withPodUrl(podUrl_)
	                    .withUserName(serviceAccount_)
	                    .withRsaPemCredentialFile(credentialFile_)
	                    .withApiConnectionSettings(new ConnectionSettings.Builder()
	                        .withSslTrustStrategy(SslTrustStrategy.TRUST_ALL_CERTS)
	                        .build())
	                    .build())
	            .build();
	    
//      .withTrustedSslCertResources(IAllegroApi.SYMPHONY_DEV_QA_ROOT_CERT)
    
    String presentationML     = "<div data-format=\"PresentationML\" data-version=\"2.0\" class=\"wysiwyg\"><p>This was sent as PresentationML + EntityJSON. <span class=\"entity\" data-entity-id=\"0\">@Bruce Skingle</span> <span class=\"entity\" data-entity-id=\"1\">@Bruce Bot</span> </p></div>";

    String entityJson = "{\n" + 
        "  \"0\":{\n" + 
        "    \"id\":[\n" + 
        "      {\n" + 
        "        \"type\":\"com.symphony.user.userId\",\n" + 
        "        \"value\":\"11476152617525\"\n" + 
        "      }\n" + 
        "    ],\n" + 
        "    \"type\":\"com.symphony.user.mention\"\n" + 
        "  },\n" + 
        "  \"1\":{\n" + 
        "    \"id\":[\n" + 
        "      {\n" + 
        "        \"type\":\"com.symphony.user.userId\",\n" + 
        "        \"value\":\"11407433183256\"\n" + 
        "      }\n" + 
        "    ],\n" + 
        "    \"type\":\"com.symphony.user.mention\"\n" + 
        "  },\n" + 
        "  \"_type\":\"com.symphony.s2.model.allegro.EntityJson\"\n" + 
        "}";
    
    IChatMessage chatMessage = allegroApi_.newChatMessageBuilder()
        .withThreadId(threadId_)
        .withPresentationML(presentationML)
        .withEntityJson(entityJson)
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
    SendEntityJson program = new SendEntityJson();
    
    program.process(args);
    
    program.run();
  }
}
