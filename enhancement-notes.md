- We can improve performance in two ways : Make the distsys component better and/or make the "CRDT" part better
- is the seperation of these two actually proper in code? 
- TODO make sure these two things are actually separate
- Could handle sending to different nodes differently, reduce "send event" count by one

- We could modify our checker to check for eventual consistency by not just equating the reads but also doing some sort of subset, superset checks or if deletes are interleaved then incorporating for that as well. Not sure how that would look like. The incorrectness bug that we observed was because our checker is hard checking for equality without giving much time for convergence. 