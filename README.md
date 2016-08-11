

## Examples

To analyze the git repository 'linux', from origin repo
http://github.com/torvalds/linux using 'linux--store.db' for
storing raw data, 'linux-processed.db' for processed data, and
'linux-uploaded.db' for tracking uploaded items, and Uploading
results to an ElasticSearch instance in
https://user:passwd@es.instance.io. Logging level 'info'.

```sh
python3 blame_analysis.py --repodir linux \
 --store linux-store --processed linux-processed --uploaded linux-uploaded \
 http://github.com/torvalds/linux -l info \
 --es_url https://user:passwd@es.instance.io
 ```

Same, but assuming processing did finish, and all the processed data
is in 'linux-processed.db':

```sh
python3 blame_analysis.py --repodir linux \
 --store linux-store --processed linux-processed --uploaded linux-uploaded \
 http://github.com/torvalds/linux -l info --assume_processed \
 --es_url https://user:passwd@es.instance.io
 ```

## Notes and comments

You can run 'blame_analysis.py' on the full history of the Linux kernel.

http://www.padator.org/linux.php

The git repo can be obtained from the Internet Archive: https://archive.org/details/git-history-of-linux

A version of it that points to the current Linux git repo, and therefore can be updated as of today:

https://landley.net/kdocs/fullhist/

https://landley.net/kdocs/local/linux-fullhist.tar.bz2

Get that last one and:

```sh
tar xvjf linux-fullhist.tar.bz2
cd linux
git checkout -f
git clean -f
git fetch
git rebase
```

Then, run 'blame_analysis.py' as commented above.
