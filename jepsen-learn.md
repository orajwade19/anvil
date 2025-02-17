# Notes
- Jepsen uses the linux network emulation (tc-netem) to emulate traffic delays, packet reordering etc. wtf is this


https://github.com/trvedata/crdt-isabelle



in my implementation, using a set for both deduplication AND as a set. is this decoupling possible? Obviously possible, but the way to approach it puzzles me right now.

Alright - some ideas : 
-> add a type field to the event log  ("add" and "delete" )
-> For "delete" -> we will have to iterate through receivedMessages and some part of the log, and add a unique ID (older) to the delete. For now, I think the ID can be equal to msgId of the add. we ignore unique ids of delete events when scanning through. 