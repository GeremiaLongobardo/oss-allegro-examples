/*
 * Copyright 2021 Symphony Communication Services, LLC.
 *
 * All Rights Reserved
 */

package com.symphony.oss.allegro.examples.benchmark;

import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.symphony.oss.allegro2.mongo.api.IAllegro2MongoApi;
import com.symphony.oss.fugue.Fugue;
import com.symphony.oss.models.core.canon.facade.PodAndUserId;
import com.symphony.oss.models.core.canon.facade.ThreadId;

public class MongoUtils
{
  String      collection_ = "TodoItems";
   int CONSUMERS_;

   int PRODUCERS_;
  
   static final String SERVICE_ACCOUNT = "SERVICE_ACCOUNT";
   static final String POD_URL        = "POD_URL";
   static final String MONGO_HOST     = "MONGO_HOST";
   static final String MONGO_USER     = "MONGO_USER";
   static final String MONGO_PASSWORD = "MONGO_PASSWORD";
   static final String THREAD_ID      = "THREAD_ID";
   static final String LAMBDA_N       = "LAMBDA_N";
   static final String CONSUMERS      = "CONSUMERS";
  
   String                 serviceAccount_;
   String                 podUrl_;
   ThreadId               threadId_;
   Long                   ownerId_;

   Long               period          = 1000L / 5;
   static final int   RATIO           = 5;

   IAllegro2MongoApi         allegro2MongoApi_;
  
   String                 mongoHost_;
   String mongoUser_;
   String mongoPassword_;
  
   PodAndUserId ownerUserId_;
   
   static String bucket =  "allegro-mongo-benchmark";
  
  boolean coldStart = true;
  
  public static long maximum(List<Long> latencies)
  {
    Collections.sort(latencies);
    return latencies.size() == 0 ? 0 : latencies.get(latencies.size() - 1);
  }
  
public static long percentile(List<Long> latencies, double percentile)
{
  Collections.sort(latencies);
  int index = (int) Math.ceil(percentile / 100.0 * latencies.size());
  return latencies.size() == 0 ? 0 : latencies.get(index - 1);
}

public static double percentile(List<Double> latencies)
{
  Collections.sort(latencies);
  int index = (int) Math.ceil(90.0 / 100.0 * latencies.size());
  return latencies.size() == 0 ? 0 : latencies.get(index - 1);
}

public static long average(List<Long> marks)
{
  double sum = 0;
  if (!marks.isEmpty())
  {
    for (Long mark : marks)
      sum += mark;
    return Math.round(sum / marks.size());
  }
  return 0L;
}

public static long average(List<Double> marks, boolean b)
{
  double sum = 0;
  if (!marks.isEmpty())
  {
    for (Double mark : marks)
      sum += mark;
    
    return Math.round(sum / marks.size());
  }
  return 0L;
}
 void saveOnS3(String s, String directory, String name)
{
  
  System.out.println("WRITING FILE "+directory + "/" + name + ".txt");
  Map<String, String> tags = new HashMap<>();

  tags.put(Fugue.TAG_FUGUE_ITEM, "allegro-mongo-benchmark");

  AmazonS3 s3 = AmazonS3ClientBuilder
      .standard()
      .withRegion("eu-west-1")
      .build();

  ObjectMetadata metadata = new ObjectMetadata();
  metadata.setContentType("text/plain");
  metadata.setContentLength(s.length());

  PutObjectRequest request = new PutObjectRequest(bucket, directory + "/" + name,
      new ByteArrayInputStream(s.getBytes()), metadata);

  s3.putObject(request);
}
}
