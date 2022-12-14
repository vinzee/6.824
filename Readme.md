# 6.824: Distributed Systems (MIT)

![Master Build](https://github.com/github/docs/actions/workflows/main.yml/badge.svg)

## Links
- https://pdos.csail.mit.edu/6.824/
- https://pdos.csail.mit.edu/6.824/labs/guidance.html
- Debugging distributed systems: https://blog.josejg.com/debugging-pretty/
- Youtube lectures: https://www.youtube.com/playlist?list=PLrw6a1wE39_tb2fErI4-WkMbsvGQk9_UB
- https://github.com/nsiregar/mit-go

## Course Prerequisites
- http://web.mit.edu/6.033/www/index.shtml (operating systems, networking, distributed systems, and security)
- https://pdos.csail.mit.edu/6.828/2022/schedule.html (Operating Systems Engineering)

## Lab 1: Map Reduce
- https://pdos.csail.mit.edu/6.824/labs/lab-mr.html


## Lab 2: Raft
- Raft Paper: <https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf> or <https://raft.github.io/raft.pdf>
- <https://pdos.csail.mit.edu/6.824/labs/lab-raft.html>
- <https://thesquareplanet.com/blog/students-guide-to-raft>
- <https://thesquareplanet.com/blog/instructors-guide-to-raft>
    - Figure 2 is, in reality, a formal specification, where every clause is a MUST, not a SHOULD.
- Reference Implementations:
    - Raftscope: https://github.com/ongardie/raftscope/blob/master/raft.js
    - 8.624: https://github.com/Sorosliu1029/6.824/blob/master/src/raft/raft.go
    - etcd: https://github.com/etcd-io/etcd/blob/main/server/etcdserver/raft.go
    - hashicorp: https://github.com/hashicorp/raft
- Best Raft Blogs
    - https://eli.thegreenplace.net/2020/implementing-raft-part-0-introduction/
    - https://eli.thegreenplace.net/2020/implementing-raft-part-3-persistence-and-optimizations/
- https://groups.google.com/g/raft-dev/c/Ezijjiolr_A?pli=1

### Behavior during Network Partition
Assume the network is divided into two parts. The first has two nodes with term 2, and the second has three nodes with term 1. So the second has the majority and will commit log entries. When the network recovered (the partition is healed) and the leader hears about a higher term it will step down, but another election will simply elect one of the three nodes on the majority side of the partition. The leader shouldn't simply ignore a higher term.

However, in most implementations these days, a *pre-vote protocol* is used. That would ensure that the nodes on the smaller side of the partition never even transition to candidate and increment the term since they can't win an election, and the leader would never hear of a higher term and have to step down.

### What happens to replicated but uncommited logs ??
- Supposed a 3-member raft cluster a[master],b,c. Client sends command to a, a replicate it to b and c, a apply the log to the status machine and response to client, then crash before replicate the committed state to b and c.
- Short Ans: The next leader will commit those entries.
- Long Ans: When b becomes the leader it will commit the entry for which it never received the updated commitIndex from the prior leader. Indeed, part of the new leader's responsibilities when it becomes the leader is to ensure all entries it logged prior to the start of its term are stored on a majority of servers and then commit them. That means server B sends an AppendEntries RPC to C, verifies that C has all the entries prior to the start of leader B's term, then increases the commitIndex beyond the start of its term (usually by committing a no-op entry) and applies the entries left over from the prior term to its state machine.
- https://groups.google.com/g/raft-dev/c/n8YledqIrUs

```
# enable debug logs
DEBUG=true go test -run 2A

# test with race condition checker
go test -run 2A -race

# test with time
time go test -run 2A

# test multiple times
$ for i in {0..10}; do go test -run 2A; done
```

## Go-Lang
- Race Detection: https://www.sohamkamani.com/golang/data-races/
- https://go.dev/doc/articles/race_detector
- Lecture 5: Go, Threads, and Raft: https://www.youtube.com/watch?v=UzzcUS2OHqo
```
	cond := sync.NewCond(&mutex)
    mutex.Lock()
    ... do work ...
    cond.Broadcast()
    mutex.Unlock()

	rf.mu.Lock()
	defer rf.mu.Unlock()

	for wait-condition {
		cond.Wait()
    }
```