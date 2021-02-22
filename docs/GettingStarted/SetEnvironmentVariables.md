---
nav_order: 30
parent: Getting Started
---
# Set Environment Variable

The example programs can accept parameters on the command line, via Java System Properties, or via Environment
Variables. For this example set the following Environment Variables:

```sh
$ export ALLEGRO_CREDENTIAL_FILE=~/keys/myKey.pem 
$ export ALLEGRO_POD_URL=https://yourpod.symphony.com
$ export ALLEGRO_SERVICE_ACCOUNT=allegroBot
$ 
```

If you chose a different name for the service account, or a different location for the credential
file, then substitute the appropriate values.