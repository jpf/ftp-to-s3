FTP to AWS webhook server
-------------------------

An FTP server that uploads every file it receives to S3

After the file is uploaded to S3, a webhook containing the S3 URL is made to the configured endpoint.

Setup
-----

    $ virtualenv --distribute venv
    $ pip install -r requirements.txt
    $ cp .env.sample .env
    $ emacs .env # other text editors can be used too

Running
-------

    $ python server.py

