---
nav_order: 20
parent: ToDo List Example
---
# Create An Item

The Create an Item example creates a new ToDo item.

The first part of the program is almost identical to the [Hello World Example](/HelloWorld.html), see the description
there for more details.

In this case we also need to provide the Object Store URL, since these examples use that API.

```java
public class CreateToDoItem extends CommandLineHandler implements Runnable
{
  private static final String ALLEGRO         = "ALLEGRO_";
  private static final String SERVICE_ACCOUNT = "SERVICE_ACCOUNT";
  private static final String POD_URL         = "POD_URL";
  private static final String OBJECT_STORE_URL = "OBJECT_STORE_URL";
  private static final String CREDENTIAL_FILE = "CREDENTIAL_FILE";
  private static final String THREAD_ID       = "THREAD_ID";
  private static final String OWNER_USER_ID    = "OWNER_USER_ID";
  
  private String              serviceAccount_;
  private String              podUrl_;
  private String              objectStoreUrl_;
  private String              credentialFile_;
  private ThreadId            threadId_;
  private Long                ownerId_;
  
  private IAllegroApi         allegroApi_;

  /**
   * Constructor.
   */
  public CreateToDoItem()
  {
    withFlag('s',   SERVICE_ACCOUNT,  ALLEGRO + SERVICE_ACCOUNT,  String.class,   false, true,   (v) -> serviceAccount_       = v);
    withFlag('p',   POD_URL,          ALLEGRO + POD_URL,          String.class,   false, true,   (v) -> podUrl_               = v);
    withFlag('o',   OBJECT_STORE_URL, ALLEGRO + OBJECT_STORE_URL, String.class,   false, true,   (v) -> objectStoreUrl_       = v);
    withFlag('f',   CREDENTIAL_FILE,  ALLEGRO + CREDENTIAL_FILE,  String.class,   false, false,  (v) -> credentialFile_       = v);
    withFlag('t',   THREAD_ID,        ALLEGRO + THREAD_ID,        String.class,   false, true,   (v) -> threadId_             = ThreadId.newBuilder().build(v));
    withFlag('u',   OWNER_USER_ID,    ALLEGRO + OWNER_USER_ID,    Long.class,     false, false,  (v) -> ownerId_              = v);
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
    
    PodAndUserId ownerUserId = ownerId_ == null ? allegroApi_.getUserId() : PodAndUserId.newBuilder().build(ownerId_);
    
    System.out.println("CallerId is " + allegroApi_.getUserId());
    System.out.println("OwnerId is " + ownerUserId);
    System.out.println("PodId is " + allegroApi_.getPodId());
```

In the next part of the program we create a ToDo item, setting the due date to the current time and the title and description to 
hard coded values. 

```java
    IToDoItem toDoItem = new ToDoItem.Builder()
      .withDue(Instant.now())
      .withTimeTaken(new BigDecimal(1000.0 / 3.0))
      .withTitle("An example TODO Item")
      .withDescription("Since we are creating this item with a due date of Instant.now() we are already late!")
      .build();
    
    System.out.println("About to create item " + toDoItem);
```

Next we create the header object, which will be stored unencrypted in the cloud. This object should only be used if
you need to process objects server side and should only contain timestamps, IDs and public information.

```java
    IToDoHeader header = new ToDoHeader.Builder()
        .withRequestingUser(allegroApi_.getUserId())
        .withAffectedUsers(allegroApi_.getUserId())
        .withEffectiveDate(Instant.now())
        .build();
```

The ToDo object is the unencrypted payload which we wish to store. Before we can store it we need to encrypt
it, which we do by creating a StoredApplicationObject using the __ApplicationObjectBuilder__ class.

```java
    IStoredApplicationObject toDoObject = allegroApi_.newApplicationObjectBuilder()
        .withThreadId(threadId_)
        .withHeader(header)
        .withPayload(toDoItem)
        .withPartition(new PartitionId.Builder()
            .withName(CalendarApp.PARTITION_NAME)
            .withOwner(ownerUserId)
            .build()
            )
        .withSortKey(toDoItem.getDue().toString())
//        .withPurgeDate(Instant.now().plusMillis(60000))
      .build();
    
    allegroApi_.store(toDoObject);
    
    System.out.println("Created " + toDoObject);
```
To create the encrypted StoredApplicationObject we pass the threadId of the conversation with which the object is
to be associated. This is used to obtain the content encryption key with which the payload will be encrypted.
Note that any member of the conversation (chat room of instant message conversation) would be able to obtain
the content key for that conversation and could read the object from the object store.

We also provide the payload and header created above, and the partition to which the object should be added. The SortKey
determines the position of the new object in the Partition and must be unique within the partition. If you wish to
use a time stamp as the SortKey as this example does, then you either need to be certain that the time stamp of all
records in a Partition will be unique or append some additional attribute to avoid collisions.

If you attempt to create a second object with a duplicate SortKey the call will fail, updates to the underlying data store
are performed transactionally to ensure this consistency.

When we call the __build()__ method the payload is encrypted and the StoredApplicationObject is returned which
can then be safely stored in the cloud.

The StoredApplicationObject signed as well as being encrypted, and this signature is verified before the object is
accepted by the server for storage.

When we run this example we see the following output:

```
2020-06-09T10:08:17.520000000Z [main               ] INFO  com.symphony.oss.allegro.api.AllegroApi - AllegroApi constructor start
2020-06-09T10:08:17.735000000Z [main               ] INFO  com.symphony.oss.allegro.api.AllegroApi - sbe auth....
2020-06-09T10:08:18.401000000Z [main               ] INFO  com.symphony.oss.allegro.api.AllegroApi - fetch podInfo_....
2020-06-09T10:08:18.556000000Z [main               ] INFO  com.symphony.oss.allegro.api.AllegroApi - keymanager auth....
2020-06-09T10:08:18.736000000Z [main               ] INFO  com.symphony.oss.allegro.api.AllegroApi - getAccountInfo....
2020-06-09T10:08:19.082000000Z [main               ] INFO  com.symphony.oss.allegro.api.AllegroApi - userId_ = 351775001412007
2020-06-09T10:08:19.086000000Z [main               ] INFO  com.symphony.oss.allegro.api.AllegroApi - allegroApi constructor done.
CallerId is 351775001412007
OwnerId is 351775001412007
PodId is 5119
```

After some preliminary log output we see the ToDo item payload: 

```
About to create item {
  "_type":"com.symphony.s2.model.calendar.ToDoItem",
  "_version":"1.0",
  "description":"Since we are creating this item with a due date of Instant.now() we are already late!",
  "due":"2020-06-09T10:08:19.092Z",
  "timeTaken":"333.33333333333331438552704639732837677001953125",
  "title":"An example TODO Item"
}

```

And finally the encrypted and signed StoredApplicationObject as stored in the cloud:

```
Created {
  "_type":"com.symphony.s2.model.object.StoredApplicationObject",
  "_version":"1.0",
  "cipherSuiteId":"RSA2048_AES256",
  "createdDate":"2020-06-09T10:08:19.858Z",
  "encryptedPayload":"FkSqD5xCqpKMexH1nZw0pkGyuBMsxBdbhdo+yWwFdt1hCbheoqbryndChFUMkp0AWwGDpmftXotm7Vl+qLSAlsAVRbyVDMBUPGBNvFCgkKQ7wwDm8FeNQ9Uuw5O7aFxkmzKt602PMmJVKNeC9vh6EXT/fC3mtRCRuZPGia/2JttPUF+y3kQxEUNbE0wnTGRCfxrb84GskKq0f7uj36G6g52Tm/EL8GxC28juBb6xVBQXUXcZd7YfF2bqu8oIBQSlWkvkhOCC6JdOQ/9AvoZn3OEUowuwkOmdaX3jwNWO2MCrSqjmMEGO76XvyE6CgM67Wa+cVB8g51LTfh2z1A2sWco9C5/LM60ANSAr94n8yu3Yo3u/Vw2yeeYOpqdJ5qj2nLjh3Agnz6gN5KbgiBkW+RbbSXxyRc2nJs129X8MXeSTHEZ7Lmy2WZxtIdB6FEbinkcOcFX3004bz6ObkIrl43DXcN+VdRdz10S5Hmw=",
  "hashType":1,
  "header":{
    "_type":"com.symphony.s2.model.calendar.ToDoHeader",
    "_version":"1.0",
    "affectedUsers":[
      351775001412007
    ],
    "effectiveDate":"2020-06-09T10:08:19.112Z",
    "requestingUser":351775001412007
  },
  "owner":351775001412007,
  "partitionHash":"j/vNnRtcln7uKIixRBWuqCjimkm5VxO4WLYKJVUT00oBAQ==",
  "rotationId":0,
  "sortKey":"2020-06-09T10:08:19.092Z",
  "threadId":"+ZNefHakKm9vK7WREHHLEn///pEOR60jdA=="
}
```

