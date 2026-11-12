# Failed deliveries

**You see**: failed deliveries on the Overview or the Failures page, or an alert `delivery_failed`.

Brief faults are retried by themselves, so a failed delivery needs a person. Until it is handled, its route holds its
later deliveries of the same partition, so they stay in order. Other routes and partitions keep going.

1. **Group them.** On Failures, the deliveries are grouped by error. One cause usually explains a group: a destination
   down, a credential expired, a field the destination refuses.
2. **Read one.** Open a delivery: its error, its attempts, and the records it held. A refusal of single records puts
   them in Quarantine instead, with the destination's reason.
3. **Fix the cause** outside Feather: bring the destination back, set the secret again
   (`feather secret set NAME`), or fix the data at the source.
4. **Check the connection**: the Connections page's Check button, or the API's `:check`.
5. **Retry** the group from Failures. You see the impact first; give the reason. Retried deliveries resend the same
   chunks with the same idempotency keys, so a destination that took part of a chunk drops the repeat.
6. **Or skip** what can't be delivered and doesn't need to be: the route moves on to the next version. In a protected
   environment, a skip waits on the Holds page for a second person.

If the same failure keeps coming back, pause the route's flow (`Pause` on its page) until the destination is fixed,
and tell its owner. Trace one record end to end with `feather trace DATASET KEY`.
