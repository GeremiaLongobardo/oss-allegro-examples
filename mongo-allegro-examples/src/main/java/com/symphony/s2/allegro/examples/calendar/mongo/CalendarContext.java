/*
 * Copyright 2021 Symphony Communication Services, LLC.
 *
 * All Rights Reserved
 */

package com.symphony.s2.allegro.examples.calendar.mongo;

import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.symphony.oss.allegro.api.IAllegroPodApi;
import com.symphony.oss.models.core.canon.facade.PodAndUserId;
import com.symphony.oss.models.core.canon.facade.ThreadId;

public class CalendarContext
{
  MongoClient               mongoClient_;
  PodAndUserId              ownerUserId_;
  IAllegroPodApi            allegroPodApi_;
  ThreadId                  threadId_;
  MongoDatabase             db_;
  MongoCollection<Document> todoItems_;

  CalendarContext(MongoClient mongoClient, PodAndUserId ownerUserId, IAllegroPodApi allegroPodApi, ThreadId threadId)
  {
    mongoClient_ = mongoClient;
    ownerUserId_ = ownerUserId;
    allegroPodApi_ = allegroPodApi;
    threadId_ = threadId;
    db_ = mongoClient.getDatabase("Calendar");
    todoItems_ = db_.getCollection("TodoItems");
  }

}
