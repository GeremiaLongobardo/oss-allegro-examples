---
nav_order: 58
parent: ToDo List Example
---
# Create Feed

The Create Feed example creates a Feed, which it subscribes to the partition containing our ToDoItems.

A Feed is a queue, which provides at least once delivery of objects which are created or updated in the Object Store.
A Feed can be subscribed to one or more Partitions, which means that whenever an object is created which
is a member of one of those Partitions, the new object will be placed onto the Feed to be read.

The first part of the program is almost identical to the [Hello World Example](/HelloWorld.html), see the description
there for more details.

In this case we also need to provide the Object Store URL, since these examples use that API.

```java
public class CreateFeed extends CommandLineHandler implements Runnable
{
  private static final Logger log_ = LoggerFactory.getLogger(CreateFeed.class);
  
  private static final String ALLEGRO          = "ALLEGRO_";
  private static final String SERVICE_ACCOUNT  = "SERVICE_ACCOUNT";
  private static final String POD_URL          = "POD_URL";
  private static final String OBJECT_STORE_URL = "OBJECT_STORE_URL";
  private static final String CREDENTIAL_FILE  = "CREDENTIAL_FILE";
  private static final String OWNER_USER_ID    = "OWNER_USER_ID";
  private static final String OTHER_USER_ID    = "OTHER_USER_ID";
  
  private String              serviceAccount_;
  private String              podUrl_;
  private String              objectStoreUrl_;
  private String              credentialFile_;
  private PodAndUserId        ownerId_;
  private PodAndUserId        otherUserId_;
  
  private IAllegroApi         allegroApi_;

  /**
   * Constructor.
   */
  public CreateFeed()
  {
    withFlag('s',   SERVICE_ACCOUNT,  ALLEGRO + SERVICE_ACCOUNT,  String.class,   false, true,   (v) -> serviceAccount_       = v);
    withFlag('p',   POD_URL,          ALLEGRO + POD_URL,          String.class,   false, true,   (v) -> podUrl_               = v);
    withFlag('o',   OBJECT_STORE_URL, ALLEGRO + OBJECT_STORE_URL, String.class,   false, true,   (v) -> objectStoreUrl_       = v);
    withFlag('f',   CREDENTIAL_FILE,  ALLEGRO + CREDENTIAL_FILE,  String.class,   false, true,   (v) -> credentialFile_       = v);
    withFlag('u',   OWNER_USER_ID,    ALLEGRO + OWNER_USER_ID,    Long.class,     false, false,  (v) -> ownerId_              = PodAndUserId.newBuilder().build(v));
    withFlag(null,  OTHER_USER_ID,    ALLEGRO + OTHER_USER_ID,    Long.class,     false, false,  (v) -> otherUserId_          = PodAndUserId.newBuilder().build(v));
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
    
    System.out.println("CallerId is " + allegroApi_.getUserId());
    System.out.println("OwnerId is " + otherUserId_);
    System.out.println("OtherUserId is " + otherUserId_);
```

In the next part of the program we create a **ResourcePermissions** object if the optional OTHER\_USER\_ID parameter is provided
to grant that user read access to the feed:

```java
    ResourcePermissions permissions = null;
    
    if(otherUserId_ != null)
    {
      permissions = new ResourcePermissions.Builder()
          .withUser(otherUserId_, Permission.Read)
          .build()
          ;
    }
```

Next we call the __upsertFeed__ method to create the feed and its subscription to the Partition. As
this is an upsert operation:

```java  
    UpsertFeedRequest.Builder builder = new UpsertFeedRequest.Builder()
        .withName("myCalendarFeed")
        .withPermissions(permissions)
        .withPartitionIds(
            new PartitionId.Builder()
            .withName(CalendarApp.PARTITION_NAME)
            .withOwner(ownerId_)
            .build()
            )
        ;
    
    IFeed feed = allegroApi_.upsertFeed(builder.build());
    
    log_.info("Feed is " + feed);
```

When we run the program the first thing we see is some preliminary log messages:


```
2020-06-09T10:55:38.085000000Z [main               ] INFO  com.symphony.oss.allegro.api.AllegroApi - AllegroApi constructor start
2020-06-09T10:55:38.305000000Z [main               ] INFO  com.symphony.oss.allegro.api.AllegroApi - sbe auth....
2020-06-09T10:55:39.029000000Z [main               ] INFO  com.symphony.oss.allegro.api.AllegroApi - fetch podInfo_....
2020-06-09T10:55:39.150000000Z [main               ] INFO  com.symphony.oss.allegro.api.AllegroApi - keymanager auth....
2020-06-09T10:55:39.313000000Z [main               ] INFO  com.symphony.oss.allegro.api.AllegroApi - getAccountInfo....
2020-06-09T10:55:39.622000000Z [main               ] INFO  com.symphony.oss.allegro.api.AllegroApi - userId_ = 351775001412007
2020-06-09T10:55:39.626000000Z [main               ] INFO  com.symphony.oss.allegro.api.AllegroApi - allegroApi constructor done.
CallerId is 351775001412007
OwnerId is null
OtherUserId is null
```
Then we see the feed meta data:

```
2020-06-09T10:55:40.540000000Z [main               ] INFO  com.symphony.s2.allegro.examples.calendar.CreateFeed - Feed is {
  "_type":"com.symphony.s2.model.object.Feed",
  "_version":"1.0",
  "hashType":1,
  "id":{
    "_type":"com.symphony.s2.model.object.NamedUserIdObject",
    "_version":"1.0",
    "hashType":1,
    "name":"myCalendarFeed",
    "userId":351775001412007
  },
  "queueName":"sym-s2-dev-s2smoke2-object-feed-cMUuB1d1Px8pmrIc8EB7ZX"
}