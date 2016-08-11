

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
