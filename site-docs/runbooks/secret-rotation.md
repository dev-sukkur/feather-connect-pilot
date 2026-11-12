# Secret rotation

Two things rotate: a **secret** (a vendor's password or token), and the environment's **master key**, which seals
every secret in the database.

## A secret

1. Get the new value from the vendor, while the old one still works if the vendor allows two.
2. Set it: `feather secret set zakya/api-key --reason "quarterly rotation"`, reading the value from stdin, or the
   Replace button on the Connections page. The value is sealed at once; nothing reads it back.
3. Check the connection: the Connections page's Check button. Flows pick the new value up on their next task.
4. Revoke the old value at the vendor.

## The master key

1. Make a new key: `feather secret new-key`.
2. Give it to every server and worker as `FEATHER_MASTER_KEY_PREVIOUS`, and restart them: every process can now open
   secrets under either key.
3. Swap them: the new key as `FEATHER_MASTER_KEY`, the old one as `FEATHER_MASTER_KEY_PREVIOUS`, and restart.
4. Seal every secret again under the new key: `feather secret rotate-key --reason "yearly rotation"`. It runs in one
   transaction; if another rotation overtook it, it fails and can be run again.
5. Remove `FEATHER_MASTER_KEY_PREVIOUS`, and restart.
6. Keep the old key with the backups it sealed, for as long as one of them may be restored.

A process whose key isn't the active one can open secrets but not set them, and is answered 409.
