---
nav_order: 30
parent: ToDo List Example
---
# List Items

The List Items example lists all the items in the Partition.

The first part of the program is almost identical to the [Hello World Example](/HelloWorld.html), see the description
there for more details.

In this case we also need to provide the Object Store URL, since these examples use that API.

The optional OWNER\_USER\_ID parameter allows you to test this program as an additional user of the Partition rather than
the user who created (and therefore owns) the Partition.

```java
public class ListItems extends CommandLineHandler implements Runnable
{
  private static final String ALLEGRO          = "ALLEGRO_";
  private static final String SERVICE_ACCOUNT  = "SERVICE_ACCOUNT";
  private static final String POD_URL          = "POD_URL";
  private static final String OBJECT_STORE_URL = "OBJECT_STORE_URL";
  private static final String CREDENTIAL_FILE  = "CREDENTIAL_FILE";
  private static final String OWNER_USER_ID    = "OWNER_USER_ID";
  private static final String SORT_KEY_PREFIX  = "SORT_KEY_PREFIX";
  
  private String              serviceAccount_;
  private String              podUrl_;
  private String              objectStoreUrl_;
  private String              credentialFile_;
  private String              sortKeyPrefix_;
  private Long                ownerId_;
  
  private IAllegroApi         allegroApi_;

  /**
   * Constructor.
   */
  public ListItems()
  {
    withFlag('s',   SERVICE_ACCOUNT,  ALLEGRO + SERVICE_ACCOUNT,  String.class,   false, true,   (v) -> serviceAccount_       = v);
    withFlag('p',   POD_URL,          ALLEGRO + POD_URL,          String.class,   false, true,   (v) -> podUrl_               = v);
    withFlag('o',   OBJECT_STORE_URL, ALLEGRO + OBJECT_STORE_URL, String.class,   false, true,   (v) -> objectStoreUrl_       = v);
    withFlag('f',   CREDENTIAL_FILE,  ALLEGRO + CREDENTIAL_FILE,  String.class,   false, true,   (v) -> credentialFile_       = v);
    withFlag('u',   OWNER_USER_ID,    ALLEGRO + OWNER_USER_ID,    Long.class,     false, false,  (v) -> ownerId_              = v);
    withFlag('k',   SORT_KEY_PREFIX,  ALLEGRO + SORT_KEY_PREFIX,  String.class,   false, false,  (v) -> sortKeyPrefix_        = v);
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
```

In the next part of the program we call the __fetchPartitionObjects__ method to retrieve the objects in the Partition. We pass a limit of
10 as a safety precaution in case the Partition is large.

This method takes a ConsumerManager to process the retrieved objects, in this example we provide
Consumers for **IToDoITem** and **IStoredApplicationObject**.

When Allegro returns objects the ConsumerManager finds the Consumer with a closest match to the actual object retrieved, decrypting
it if necessary. Since we know that all objects in this Partition are **ToDoItem**s, we expect that the **StoredApplicationObject** 
Consumer will never be called. If the program were run as a user which has access to the Partition but is not a member of the 
thread (conversation) with which the objects are encrypted, then Allegro would be unable to fetch keys to decrypt the objects
and in this case they would be returned as (encrypted) **StoredApplicationObject**s.


```java
    allegroApi_.fetchPartitionObjects(new FetchPartitionObjectsRequest.Builder()
        .withQuery(new PartitionQuery.Builder()
            .withMaxItems(10)
            .withName(CalendarApp.PARTITION_NAME)
            .withOwner(ownerUserId)
            .withSortKeyPrefix(sortKeyPrefix_)
            .build()
            )
          .withConsumerManager(new ConsumerManager.Builder()
              .withConsumer(IToDoItem.class, (item, trace) ->
              {
                System.out.println("Header:  " + item.getStoredApplicationObject().getHeader());
                System.out.println("Payload: " + item);
              })
              .withConsumer(IStoredApplicationObject.class, (item, trace) ->
              {
                System.out.println("StoredApplicationObject:  " + item);
              })
              .build()
              )
          .build()
          );
  }
```

When we run the program the first thig we see is some preliminary log messages:


```
2020-06-09T10:17:47.480000000Z [main               ] INFO  com.symphony.oss.allegro.api.AllegroApi - AllegroApi constructor start
2020-06-09T10:17:47.700000000Z [main               ] INFO  com.symphony.oss.allegro.api.AllegroApi - sbe auth....
2020-06-09T10:17:48.451000000Z [main               ] INFO  com.symphony.oss.allegro.api.AllegroApi - fetch podInfo_....
2020-06-09T10:17:48.570000000Z [main               ] INFO  com.symphony.oss.allegro.api.AllegroApi - keymanager auth....
2020-06-09T10:17:48.731000000Z [main               ] INFO  com.symphony.oss.allegro.api.AllegroApi - getAccountInfo....
2020-06-09T10:17:49.044000000Z [main               ] INFO  com.symphony.oss.allegro.api.AllegroApi - userId_ = 351775001412007
2020-06-09T10:17:49.048000000Z [main               ] INFO  com.symphony.oss.allegro.api.AllegroApi - allegroApi constructor done.
CallerId is 351775001412007
OwnerId is 351775001412007
2020-06-09T10:17:50.294000000Z [main               ] INFO  com.gs.ti.wpt.lc.security.cryptolib.Utils - Arch: mac os x/x86_64
2020-06-09T10:17:50.317000000Z [main               ] INFO  com.symphony.security.utils.DynamicLibraryLoader - java.library.path=/var/folders/4k/0sfqq6v1605547b2gjcldk2h0000gn/T:/Users/bruce/Library/Java/Extensions:/Library/Java/Extensions:/Network/Library/Java/Extensions:/System/Library/Java/Extensions:/usr/lib/java:.
2020-06-09T10:17:50.550000000Z [main               ] INFO  com.gs.ti.wpt.lc.security.cryptolib.Utils - Arch: mac os x/x86_64
```
Then we see the contents of the Partition with both the Header and (decrypted) Payload shown:

```
Header:  {
  "_type":"com.symphony.s2.model.calendar.ToDoHeader",
  "_version":"1.0",
  "affectedUsers":[
    351775001412007
  ],
  "effectiveDate":"2020-05-13T14:47:15.341Z",
  "requestingUser":351775001412007
}

Payload: {
  "_type":"com.symphony.s2.model.calendar.ToDoItem",
  "_version":"1.0",
  "description":"Since we are creating this item with a due date of Instant.now() we are already late!",
  "due":"2020-05-13T14:47:15.309Z",
  "timeTaken":"333.33333333333331438552704639732837677001953125",
  "title":"An example TODO Item"
}

Header:  {
  "_type":"com.symphony.s2.model.calendar.ToDoHeader",
  "_version":"1.0",
  "affectedUsers":[
    351775001412007
  ],
  "effectiveDate":"2020-06-01T13:32:52.605Z",
  "requestingUser":351775001412007
}

Payload: {
  "_type":"com.symphony.s2.model.calendar.ToDoItem",
  "_version":"1.0",
  "description":"Since we are creating this item with a due date of Instant.now() we are already late!",
  "due":"2020-06-01T13:32:52.585Z",
  "timeTaken":"333.33333333333331438552704639732837677001953125",
  "title":"An example TODO Item"
}

Header:  {
  "_type":"com.symphony.s2.model.calendar.ToDoHeader",
  "_version":"1.0",
  "affectedUsers":[
    351775001412007
  ],
  "effectiveDate":"2020-06-09T10:08:19.112Z",
  "requestingUser":351775001412007
}

Payload: {
  "_type":"com.symphony.s2.model.calendar.ToDoItem",
  "_version":"1.0",
  "description":"Since we are creating this item with a due date of Instant.now() we are already late!",
  "due":"2020-06-09T10:08:19.092Z",
  "timeTaken":"333.33333333333331438552704639732837677001953125",
  "title":"An example TODO Item"
}

```
