/*
 * Copyright 2021 Symphony Communication Services, LLC.
 *
 * All Rights Reserved
 */

package com.symphony.oss.allegro.examples.benchmark;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.lambda.model.LogType;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.symphony.oss.fugue.Fugue;

public class MongoTestMain extends MongoUtils implements RequestHandler<Map<String, String>, String>
{
  private static final String    MONGO_HOST      = "MONGO_HOST";
  private static final String    MONGO_USER      = "MONGO_USER";
  private static final String    MONGO_PASSWORD  = "MONGO_PASSWORD";
  
  private AWSLambda lambdaClient_ =
      AWSLambdaClientBuilder.standard()
      .withRegion("eu-west-1")
      .build();
  
  AmazonS3 s3 = AmazonS3ClientBuilder
      .standard()
      .withRegion("eu-west-1")
      .build();
  private int startup_; 
  


  @Override
  public String handleRequest(Map<String, String> input, Context context)
  {
    int CONSUMERS          = Integer.parseInt(input.get("CONSUMERS"));
    int LAMBDAS            = Integer.parseInt(input.get("LAMBDAS"));
    mongoHost_      = input.get(MONGO_HOST);
    mongoPassword_  = input.get(MONGO_PASSWORD);
    mongoUser_      = input.get(MONGO_USER);
    startup_               = Integer.parseInt(input.get("STARTUP_TIME"));
    String directory       = input.get("DIRECTORY");
    boolean parse       = Boolean.parseBoolean(input.get("PARSE_ONLY"));
    
    if (!parse)
    {
       directory = "tests/" + directory + "-TEST-/Cx" + CONSUMERS + "-Lx" + LAMBDAS + "/"
          + Instant.now();
       
       launchWorkers(LAMBDAS, CONSUMERS, directory);
    }
    
    TreeMap<Integer, ArrayList<Long[]>> samples = loadDataFromS3(LAMBDAS, directory, CONSUMERS);

    TreeMap<Integer, ArrayList<Long>[]> ss = split(samples);

     String result = aggregate(ss);
     
     ObjectMetadata metadata = new ObjectMetadata();
     metadata.setContentType("text/plain");
     metadata.setContentLength(result.length());

     PutObjectRequest request = new PutObjectRequest(bucket, directory + "/" + "result.txt",
         new ByteArrayInputStream(result.getBytes()), metadata);

     s3.putObject(request);

     return "Done";
   }

  private String aggregate(TreeMap<Integer, ArrayList<Long>[]> ss)
  {
    ArrayList<Long> all_startup     = new ArrayList<>();
    ArrayList<Long> all_operational = new ArrayList<>();
    StringBuilder sb = new StringBuilder();
    
    for (Entry<Integer, ArrayList<Long>[]> e : ss.entrySet())
    {
      String out = "CONSUMER " + e.getKey() + " RECEIVED RECORDS " + (e.getValue()[0].size() + e.getValue()[1].size())
          + " AVERAGE: ("  + average(e.getValue()[0])+")"+ average(e.getValue()[1]) 
          + " 90thPERC: (" + percentile(e.getValue()[0], 90) +") "+percentile(e.getValue()[1], 90) 
          + " 95thPERC: (" + percentile(e.getValue()[0], 95) +") "+percentile(e.getValue()[1], 95)
          + " 98thPERC: (" + percentile(e.getValue()[0], 98) +") "+percentile(e.getValue()[1], 98)
          + " MAX: (" + maximum(e.getValue()[0])+") "+ maximum(e.getValue()[1]);
      sb.append(out + "\n");

      System.out.println(out);

      all_startup.addAll(e.getValue()[0]);
      all_operational.addAll(e.getValue()[1]);
    }

    String out =            "TOTAL AVERAGE STARTUP: "+ average(all_operational) 
    + " 90thPERC: "+percentile(all_startup, 90) 
    + " 95thPERC: "+percentile(all_startup, 95)
    + " 98thPERC: "+percentile(all_startup, 98)
    + " MAX: "+ maximum(all_startup);
    
    String out2 =            "TOTAL AVERAGE: ("  + average(all_startup)+")"+ average(all_operational) 
    + " 90thPERC: "+percentile(all_operational, 90) 
    + " 95thPERC: "+percentile(all_operational, 95)
    + " 98thPERC: "+percentile(all_operational, 98)
    + " MAX: "+ maximum(all_operational);
    
    sb.append(out + "\n");
    sb.append(out2 + "\n");
    System.out.println(out);
    System.out.println(out2);
    return sb.toString();
  }

  private void launchWorkers(int LAMBDAS, int CONSUMERS, String dir)
  {
    for (int i = 0; i < LAMBDAS; i++)
    {
      int k = i;
     new Thread(() -> invokeLambda(mongoUser_, mongoHost_, mongoPassword_, CONSUMERS, k, dir)).start();

    }

    try
    {
      Thread.sleep(1000 * 60 * 8);
    }
    catch (InterruptedException e1)
    {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    
  }

  private TreeMap<Integer, ArrayList<Long>[]> split(TreeMap<Integer, ArrayList<Long[]>> samples)
  {
    TreeMap<Integer, ArrayList<Long>[]> ret = new TreeMap<>();

    for (Entry<Integer, ArrayList<Long[]>> e : samples.entrySet())
    {

      ArrayList<Long[]> latencies = e.getValue();
      long limit = latencies.get(0)[0] + startup_ * 1000;

      ArrayList<Long> startup = new ArrayList<>();
      ArrayList<Long> operational = new ArrayList<>();

      for (Long[] l : latencies)
      {
        if (l[0] <= limit)
        {
          startup.add(l[1]);
        }
        else
          operational.add(l[1]);

      }
      ArrayList<Long>[] tmp = new ArrayList[] { startup, operational };
      ret.put(e.getKey(), tmp);
    }

    return ret;
  }

  private void invokeLambda(String mongoUser_, String mongoHost_, String mongoPassword_, int CONSUMERS, int i, String directory)
  {
    InvokeResult invokeResult = lambdaClient_.invoke(new InvokeRequest()
        .withFunctionName("allegro-mongo-benchmark-test")
        .withPayload("{\n"
            + "  \"POD_URL\": \"https://psdev.symphony.com/ \",\n"
            + "  \"SERVICE_ACCOUNT\": \"allegroBot\",\n"
            + "  \"THREAD_ID\": \"+ZNefHakKm9vK7WREHHLEn///pEOR60jdA==\",\n"
            + "  \"MONGO_USER\": \""+mongoUser_+"\",\n"
            + "  \"MONGO_HOST\": \""+mongoHost_+"\",\n"
            + "  \"MONGO_PASSWORD\": \""+mongoPassword_+"\",\n"
            + "  \"CONSUMERS\": \""+CONSUMERS+"\",\n"
            + "  \"LAMBDA_N\": \""+(i)*CONSUMERS+"\",\n"
            + "  \"CLEANUP\": \"false\",\n"
            + "  \"DIRECTORY\":\""+directory+"\"\n"
            + "}")
        .withLogType(LogType.Tail)
        .withInvocationType(InvocationType.Event)
//        .withSdkRequestTimeout(7 * 60 * 1000)
//        .withSdkClientExecutionTimeout(7 * 60 * 1000)
        );
  }
  
private TreeMap<Integer, ArrayList<Long[]>> loadDataFromS3(int LAMBDAS, String directory, int CONSUMERS)
  { 
  TreeMap<Integer,  ArrayList<Long[]>> samples = new TreeMap<>();
  StringBuilder sb =  new StringBuilder();
  
  for (int i = 0; i < LAMBDAS; i++)
  {
    System.out.println("Loading data from "+bucket+"/"+directory + "/" + i * CONSUMERS + ".csv");
    boolean done = false;

    S3Object object = null;
    while (!done)
    {
      try
      {
        object = s3.getObject(new GetObjectRequest(bucket, directory + "/" + i * CONSUMERS + ".csv"));
        done = true;
      }
      catch (AmazonS3Exception e)
      {
        System.out.println("The file "+directory + "/" + i * CONSUMERS + ".txt has not been written yet .... RETRYING in 30s");
        try
        {
          Thread.sleep(30 * 1000);
        }
        catch (InterruptedException e1)
        {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }
      }

    }
    try(InputStream objectData = object.getObjectContent()){
    // Process the objectData stream.
    BufferedReader r = new BufferedReader(new InputStreamReader(objectData));
    String line;
    while ((line = r.readLine()) != null)
    {
      sb.append(line + "\n");
      String[] ss = line.split(",");
      Integer consumer = Integer.parseInt(ss[2]);
      Long receive = Long.parseLong(ss[0]);
      Long latency = Long.parseLong(ss[4]);
      ArrayList<Long[]> tmp = samples.get(consumer);
      if (tmp == null)
        samples.put(consumer, tmp = new ArrayList<>());
      tmp.add(new Long[] {receive, latency});
    }

  }
  catch (IOException e)
  {
    e.printStackTrace();
    throw e;
  }

}
saveOnS3(sb.toString(), directory, "all.csv");

return samples;
}

}
