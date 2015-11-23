# So I hear you like GC* demos ...

A working end to end demo to show how we can pull data files from somewhere on the internet, load them into Google Cloud Storaage, format and load them into Google Big Query, then finally query our table a get results back.

This project is written in python and makes use of Google's custom python libraries for interacting with their API. You'll also need a Google Cloud account, you can sign up for free and get USD 300 credit just for trying it out. Nice eh?


## Running the awesome project

Clone the repo. We need google's custom API libraries for python (I'm running 2.7).


    sudo pip install google-api-python-client
    sudo pip install gcs-oauth2-boto-plugin

You should now be able to run

## TODO

1.need to automate loading of raw text data sets into GCS
