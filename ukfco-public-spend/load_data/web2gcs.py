#!/usr/bin/python
__author__ = 'Pete'

import boto
import gcs_oauth2_boto_plugin
import os
import shutil
import StringIO
import tempfile
import time


def main():

    # URI scheme for Google Cloud Storage.
    GOOGLE_STORAGE = 'gs'
    # URI scheme for accessing local files.
    LOCAL_FILE = 'file'

    # Fallback logic. In https://console.developers.google.com
    # under Credentials, create a new client ID for an installed application.
    # Required only if you have not configured client ID/secret in
    # the .boto file or as environment variables.
    # TODO: Set client id and secrete in .boto file
    CLIENT_ID = '703112408605-8thdjhqmra70c1p14io4tqejf9890t55.apps.googleusercontent.com'
    CLIENT_SECRET = 'xOVhYe0viljid5c5EwywpZKw'
    gcs_oauth2_boto_plugin.SetFallbackClientIdAndSecret(CLIENT_ID, CLIENT_SECRET)


    now = time.time()
    CATS_BUCKET = 'cats-%d' % now
    DOGS_BUCKET = 'dogs-%d' % now

    # Your project ID can be found at https://console.developers.google.com/
    # If there is no domain for your project, then project_id = 'YOUR_PROJECT'
    project_id = '703112408605'

    for name in (CATS_BUCKET, DOGS_BUCKET):
        # Instantiate a BucketStorageUri object.
        uri = boto.storage_uri(name, GOOGLE_STORAGE)
        # Try to create the bucket.
        try:
            # If the default project is defined,
            # you do not need the headers.
            # Just call: uri.create_bucket()
            header_values = {"x-goog-project-id": project_id}
            uri.create_bucket(headers=header_values)
            # TODO: Errors here

            print 'Successfully created bucket "%s"' % name
        except boto.exception.StorageCreateError, e:
            print 'Failed to create bucket:', e


    # Make some temporary files.
    temp_dir = tempfile.mkdtemp(prefix='googlestorage')
    tempfiles = {
        'labrador.txt': 'Who wants to play fetch? Me!',
        'collie.txt': 'Timmy fell down the well!'}
    for filename, contents in tempfiles.iteritems():
        with open(os.path.join(temp_dir, filename), 'w') as fh:
            fh.write(contents)

    # Upload these files to DOGS_BUCKET.
    for filename in tempfiles:
        with open(os.path.join(temp_dir, filename), 'r') as localfile:

            dst_uri = boto.storage_uri(
                DOGS_BUCKET + '/' + filename, GOOGLE_STORAGE)
            # The key-related functions are a consequence of boto's
            # interoperability with Amazon S3 (which employs the
            # concept of a key mapping to localfile).
            dst_uri.new_key().set_contents_from_file(localfile)
        print 'Successfully created "%s/%s"' % (
            dst_uri.bucket_name, dst_uri.object_name)

    shutil.rmtree(temp_dir)  # Don't forget to clean up!

if __name__ == "__main__":
    main()

