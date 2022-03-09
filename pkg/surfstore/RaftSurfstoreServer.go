package surfstore

import (
	context "context"
	"fmt"
	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"log"
	"sync"
	"time"
)

type RaftSurfstore struct {
	isLeader  bool
	term      int64
	log       []*UpdateOperation
	metaStore *MetaStore

	// customized fields
	commitIndex int64
	lastApplied int64
	serverId    int64
	serversIP   []string
	// fields for leader, reinitialize after election
	nextIndex  []int64
	matchIndex []int64

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	notCrashedCond *sync.Cond

	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	ch := make(chan string, len(s.serversIP))
	for idx := 0; idx < len(s.serversIP); idx++ {
		if idx == int(s.serverId) {
			continue
		}
		go func(i int, ch chan string) {
			conn, err := grpc.Dial(s.serversIP[i], grpc.WithInsecure())
			defer conn.Close()
			if err != nil {
				log.Printf("[Goroutine] UpdateFile: Err occurred when dialing followers, err=%v", err)
				return
			}
			c := NewRaftSurfstoreClient(conn)
			for {
				// if current server is not leader, stop retrying
				if !s.isLeader {
					ch <- "false"
					return
				}
				if s.isCrashed {
					ch <- "crash"
					return
				}
				appendEntryInput := &AppendEntryInput{
					Term:         s.term,
					LeaderCommit: s.commitIndex,
					PrevLogIndex: s.nextIndex[i] - 1,
					Entries:      s.log[s.nextIndex[i]-1:],
				}
				if s.nextIndex[i] > 1 {
					appendEntryInput.PrevLogTerm = s.log[s.nextIndex[i]-2].Term
				}
				b, err := c.AppendEntries(ctx, appendEntryInput)
				if err != nil {
					log.Printf("Err occurred when appending entries to followers, err=%v", err)
					time.Sleep(100 * time.Millisecond)
					continue
				}
				if !b.Success {
					if b.GetTerm() > s.term {
						ch <- "false"
						return
					}
					s.nextIndex[i] -= 1
					continue
				} else {
					s.nextIndex[i] = b.GetMatchedIndex() + 1
					s.matchIndex[i] = b.GetMatchedIndex()
					ch <- "true"
					return
				}
			}
		}(idx, ch)
	}
	done := 1
	for i := 0; i < len(s.serversIP)-1; i++ {
		res := <-ch
		if res == "true" {
			done += 1
			if done > len(s.serversIP)/2 {
				// apply to state machine
				infoMap, err := s.metaStore.GetFileInfoMap(ctx, &emptypb.Empty{})
				if err != nil {
					log.Printf("Err occurred when applying to state machine, err=%v", err)
					return nil, err
				}
				return infoMap, nil
			}
		} else if res == "false" {
			s.isLeader = false
			return nil, ERR_NOT_LEADER
		} else if res == "crash" {
			return nil, ERR_SERVER_CRASHED
		}
	}

	return nil, ERR_MAJORITY_SERVER_CRASHED
}

func (s *RaftSurfstore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	//fmt.Printf("[GetBlockStoreAddr]%v", s.nextIndex)
	//fmt.Printf("[GetBlockStoreAddr] request received, len(s.serversIP)=%v\n", len(s.serversIP))
	ch := make(chan string, len(s.serversIP))
	for idx := 0; idx < len(s.serversIP); idx++ {
		if idx == int(s.serverId) {
			continue
		}
		//fmt.Printf("[GetBlockStoreAddr] dailing No.%v server \n", idx)
		go func(i int, ch chan string) {
			conn, err := grpc.Dial(s.serversIP[i], grpc.WithInsecure())
			defer conn.Close()
			if err != nil {
				log.Printf("[Goroutine] UpdateFile: Err occurred when dialing followers, err=%v", err)
				return
			}
			c := NewRaftSurfstoreClient(conn)
			for {
				// if current server is not leader, stop retrying
				if !s.isLeader {
					ch <- "false"
					return
				}
				if s.isCrashed {
					ch <- "crash"
					return
				}
				appendEntryInput := &AppendEntryInput{
					Term:         s.term,
					LeaderCommit: s.commitIndex,
					PrevLogIndex: s.nextIndex[i] - 1,
					Entries:      s.log[s.nextIndex[i]-1:],
				}
				if s.nextIndex[i] > 1 {
					appendEntryInput.PrevLogTerm = s.log[s.nextIndex[i]-2].Term
				}
				//fmt.Printf("[GetBlockStoreAddr] nextIndex=%v,len(entries)=%v,idx=%v\n", s.nextIndex[i], len(appendEntryInput.Entries), i)
				b, err := c.AppendEntries(ctx, appendEntryInput)
				//fmt.Printf("[GetBlockStoreAddr] ae=%v,i=%v\n", b.GetSuccess(), i)
				if err != nil {
					log.Printf("Err occurred when appending entries to followers, err=%v", err)
					time.Sleep(100 * time.Millisecond)
					continue
				}
				if !b.Success {
					if b.GetTerm() > s.term {
						ch <- "false"
						return
					}
					s.nextIndex[i] -= 1
					continue
				} else {
					s.nextIndex[i] = b.GetMatchedIndex() + 1
					s.matchIndex[i] = b.GetMatchedIndex()
					ch <- "true"
					return
				}
			}
		}(idx, ch)
	}
	done := 1
	for i := 0; i < len(s.serversIP)-1; i++ {
		res := <-ch
		if res == "true" {
			done += 1
			//fmt.Printf("[GetBlockStoreAddr] Get 1 more done!done=%v\n", done)
			if done > len(s.serversIP)/2 {
				// apply to state machine
				addr, err := s.metaStore.GetBlockStoreAddr(ctx, &emptypb.Empty{})
				if err != nil {
					log.Printf("Err occurred when applying to state machine, err=%v", err)
					return nil, err
				}
				fmt.Printf("[GetBlockStoreAddr] I'm %v,Get block addr succeed=%v\n", s.serverId, addr)
				return addr, nil
			}
		} else if res == "false" {
			s.isLeader = false
			return nil, ERR_NOT_LEADER
		} else if res == "crash" {
			//fmt.Printf("[GetBlockStoreAddr] Get crash signal\n")
			return nil, ERR_SERVER_CRASHED
		}
	}

	return nil, ERR_MAJORITY_SERVER_CRASHED
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	ch := make(chan string, len(s.serversIP))
	s.log = append(s.log, &UpdateOperation{Term: s.term, FileMetaData: filemeta})
	curIdx := len(s.log)
	for idx := 0; idx < len(s.serversIP); idx++ {
		if idx == int(s.serverId) {
			continue
		}
		go func(i int, ch chan string) {
			conn, err := grpc.Dial(s.serversIP[i], grpc.WithInsecure())
			defer conn.Close()
			if err != nil {
				log.Printf("[Goroutine] UpdateFile: Err occurred when dialing followers, err=%v", err)
				return
			}
			c := NewRaftSurfstoreClient(conn)
			for {
				// if current server is not leader, stop retrying
				if !s.isLeader {
					ch <- "false"
					return
				}
				if s.isCrashed {
					ch <- "crash"
					return
				}
				appendEntryInput := &AppendEntryInput{
					Term:         s.term,
					LeaderCommit: s.commitIndex,
					PrevLogIndex: s.nextIndex[i] - 1,
					Entries:      s.log[s.nextIndex[i]-1:],
				}
				if s.nextIndex[i] > 1 {
					appendEntryInput.PrevLogTerm = s.log[s.nextIndex[i]-2].Term
				}
				b, err := c.AppendEntries(ctx, appendEntryInput)
				if err != nil {
					log.Printf("Err occurred when appending entries to followers, err=%v", err)
					time.Sleep(100 * time.Millisecond)
					continue
				}
				if !b.Success {
					if b.GetTerm() > s.term {
						ch <- "false"
						return
					}
					s.nextIndex[i] -= 1
					continue
				} else {
					s.nextIndex[i] = b.GetMatchedIndex() + 1
					s.matchIndex[i] = b.GetMatchedIndex()
					ch <- "true"
					return
				}
			}
		}(idx, ch)
	}
	done := 1
	for i := 0; i < len(s.serversIP)-1; i++ {
		res := <-ch
		if res == "true" {
			done += 1
			if done > len(s.serversIP)/2 {
				// apply to state machine
				// TODO 是否需要执行lastapplied-commitIndex之间的？
				s.commitIndex = int64(curIdx)
				ver, err := s.metaStore.UpdateFile(ctx, filemeta)
				if err != nil {
					log.Printf("Err occurred when applying to state machine, err=%v", err)
					return nil, err
				}
				s.lastApplied = int64(curIdx)
				//fmt.Println(s.commitIndex, ver.GetVersion())
				return ver, nil
			}
		} else if res == "false" {
			s.isLeader = false
			return nil, ERR_NOT_LEADER
		} else if res == "crash" {
			return nil, ERR_SERVER_CRASHED
		}
	}

	return nil, ERR_MAJORITY_SERVER_CRASHED
}

// AppendEntries 1. Reply false if term < currentTerm (§5.1)
//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
//matches prevLogTerm (§5.3)
//3. If an existing entry conflicts with a new one (same index but different
//terms), delete the existing entry and all that follow it (§5.3)
//4. Append any new entries not already in the log
//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
//of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	if input.Term > s.term {
		// RfS2
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower
		s.term = input.Term
		if s.isLeader {
			s.isLeader = false
		}
	}
	if input.GetTerm() < s.term {
		//1.Reply false if term < currentTerm
		return &AppendEntryOutput{Success: false, Term: s.term, ServerId: s.serverId}, nil
	}
	// index number starts from 1
	if (int(input.GetPrevLogIndex()) > 0) && (len(s.log) < int(input.GetPrevLogIndex()) || s.log[input.GetPrevLogIndex()-1].GetTerm() != s.term) {
		// 2. return false
		return &AppendEntryOutput{Success: false, Term: s.term, ServerId: s.serverId}, nil
	} else {
		// 3. delete the existing entry and all that follow it
		// 4. Append any new entries not already in the log
		// TODO check correctness
		s.log = s.log[:input.GetPrevLogIndex()]
		s.log = append(s.log, input.GetEntries()...)
	}
	//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
	//of last new entry)
	if input.GetLeaderCommit() > s.commitIndex {
		s.commitIndex = min(input.GetLeaderCommit(), int64(len(s.log)))
	}
	s.applyToStateMachine(ctx, s.lastApplied, s.commitIndex)
	return &AppendEntryOutput{ServerId: s.serverId, Term: s.term, Success: true, MatchedIndex: int64(len(s.log))}, nil
}

// SetLeader This should set the leader status and any related variables as if the node has just won an election
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	s.term += 1
	s.isLeader = true
	// Volatile state on leaders
	s.nextIndex = make([]int64, len(s.serversIP))
	for i := 0; i < len(s.nextIndex); i++ {
		s.nextIndex[i] = int64(len(s.log) + 1)
	}
	s.matchIndex = make([]int64, len(s.serversIP))
	return &Success{Flag: true}, nil
}

// SendHeartbeat Send a "Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	if !s.isLeader {
		return &Success{Flag: false}, nil
	}
	for idx, ip := range s.serversIP {
		if idx == int(s.serverId) {
			// skip itself
			continue
		}
		conn, err := grpc.Dial(ip, grpc.WithInsecure())
		if err != nil {
			log.Printf("Err occurred when dialing followers, err=%v", err)
			return nil, err
		}
		l := s.sendHeartbeat(conn, idx, ctx)
		if !l {
			s.isLeader = false
			return &Success{Flag: false}, nil
		}
	}
	//update commitIdx Id
	newcommitIdx := 0
	for N := 1; N <= len(s.log); N++ {
		count := 0
		for i := 0; i < len(s.serversIP); i++ {
			if N > int(s.matchIndex[i]) {
				count += 1
			}
		}
		if count > len(s.serversIP)/2 && s.log[N-1].GetTerm() == s.term {
			newcommitIdx = N
		}
	}
	s.commitIndex = int64(newcommitIdx)
	s.applyToStateMachine(ctx, s.lastApplied, s.commitIndex)
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) sendHeartbeat(conn *grpc.ClientConn, idx int, ctx context.Context) bool {
	c := NewRaftSurfstoreClient(conn)
	defer conn.Close()
	for {
		appendEntryInput := &AppendEntryInput{
			Term:         s.term,
			LeaderCommit: s.commitIndex,
			PrevLogIndex: s.nextIndex[idx] - 1,
			Entries:      s.log[s.nextIndex[idx]-1:],
		}
		if s.nextIndex[idx] > 1 {
			appendEntryInput.PrevLogTerm = s.log[s.nextIndex[idx]-2].Term
		}
		b, err := c.AppendEntries(ctx, appendEntryInput)
		if err != nil {
			log.Printf("Err occurred when send heartbeat to followers, err=%v", err)
			return true
		}
		if !b.Success {
			if b.GetTerm() > s.term {
				return false
			}
			s.nextIndex[idx] -= 1
			continue

		} else {
			s.nextIndex[idx] = b.GetMatchedIndex() + 1
			s.matchIndex[idx] = b.GetMatchedIndex()
			return true
		}
	}
}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.notCrashedCond.Broadcast()
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) IsCrashed(ctx context.Context, _ *emptypb.Empty) (*CrashedState, error) {
	return &CrashedState{IsCrashed: s.isCrashed}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	return &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)

func max(n1 int64, n2 int64) int64 {
	if n1 > n2 {
		return n1
	} else {
		return n2
	}
}
func min(n1 int64, n2 int64) int64 {
	if n1 > n2 {
		return n2
	} else {
		return n1
	}
}

func (s *RaftSurfstore) applyToStateMachine(ctx context.Context, lastApplied int64, commitIndex int64) {
	if commitIndex > lastApplied {
		// RfS1
		// apply log[lastApplied] to state machine
		for i := s.lastApplied; i < s.commitIndex; i++ {
			// apply s.log[i] to state machine
			_, err := s.metaStore.UpdateFile(ctx, s.log[i].GetFileMetaData())
			if err != nil {
				log.Printf("Err occurred when applying s.log[i] to state machine, err=%v", err)
			}
		}
		s.lastApplied = s.commitIndex
	}
}
