# Postgres growth

**You see**: the database's disk filling, "Days kept past retention" on the Overview, or slow pages.

The spool, the inbox of webhook events and the logs are kept in day partitions, and a day is dropped once its
retention has passed and nothing needs it any more. A day is kept past retention only because a delivery or a webhook
event of it is still open.

1. **Read the Overview's "Days kept past retention".** Each kept day names the delivery or event that holds it.
2. **Settle what holds it**: retry or skip the failed delivery, decide the held one, or dismiss the quarantined records.
   The day goes at the next maintenance run once nothing needs it.
3. **Look at what is big**:

    ```sql
    select relname, pg_size_pretty(pg_total_relation_size(relid)) as size
    from pg_catalog.pg_statio_user_tables where schemaname = 'feather'
    order by pg_total_relation_size(relid) desc limit 15;
    ```

4. **Shorten retention** for an environment if the days kept are more than you need, with `settings:` in its
   environment file and `feather config import`.
5. **Give it room**: the *small* deployment is sized for 5 million records a day (NFR-03). More than that needs a bigger
   disk, and the spool's chunks are compressed already.

Don't delete rows by hand: a version a route hasn't applied yet would be lost to it.
