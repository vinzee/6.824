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
- Blogs
    - https://eli.thegreenplace.net/2020/implementing-raft-part-0-introduction/
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