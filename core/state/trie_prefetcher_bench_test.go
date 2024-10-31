package state

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb"
)

type testCase struct {
	trieSize       int    // Number of nodes in trie
	prefetchKeys   int    // Number of keys to prefetch
	maxConcurrency int    // Maximum number of concurrent prefetches
	name           string // Test case name
}

func BenchmarkTriePrefetcher(b *testing.B) {
	var cases []testCase
	for _, concurrency := range []int{1, 4, 16, 64} {
		for _, trieSize := range []int{1_000, 100_000, 10_000_000} {
			for _, prefetchKeys := range []int{10, 100, 1000, 10_000} {
				cases = append(cases, testCase{
					trieSize:       trieSize,
					prefetchKeys:   prefetchKeys,
					maxConcurrency: concurrency,
					name:           fmt.Sprintf("trieSize=%d,prefetchKeys=%d,maxConcurrency=%d", trieSize, prefetchKeys, concurrency),
				})
			}
		}
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			dir := b.TempDir()
			db, err := rawdb.NewLevelDBDatabase(dir, 128, 128, "", false)
			if err != nil {
				b.Fatalf("cannot create temporary database: %v", err)
			}
			statedb := NewDatabase(triedb.NewDatabase(db, nil), nil)

			// Generate random addresses and accounts
			keys := generateBytes(tc.trieSize)
			values := generateBytes(tc.trieSize)

			// Create state trie and insert accounts
			root := types.EmptyRootHash
			tr, _ := trie.New(trie.TrieID(root), statedb.TrieDB())
			for i := 0; i < tc.trieSize; i++ {
				tr.Update(keys[i], values[i])
			}

			// Commit trie changes
			root, nodes := tr.Commit(false)
			statedb.TrieDB().Update(root, types.EmptyRootHash, 0, trienode.NewWithNodeSet(nodes), nil)
			statedb.TrieDB().Commit(root, false)

			// Select random keys to prefetch
			prefetchKeys := make([][]byte, tc.prefetchKeys)
			for i := 0; i < tc.prefetchKeys; i++ {
				idx, err := rand.Int(rand.Reader, big.NewInt(int64(tc.trieSize)))
				if err != nil {
					panic(err)
				}
				prefetchKeys[i] = keys[idx.Int64()]
			}

			db.Close()

			for i := 0; i < b.N; i++ {
				func() {
					// Create new database for each iteration to purge the cache from the setup phase and previous iterations
					db, err := rawdb.NewLevelDBDatabase(dir, 128, 128, "", false)
					if err != nil {
						b.Fatalf("cannot create temporary database: %v", err)
					}
					defer db.Close()

					statedb := NewDatabase(triedb.NewDatabase(db, nil), nil)

					b.ResetTimer()
					b.StartTimer()

					// Create new prefetcher for each iteration
					prefetcher := newTriePrefetcher(statedb, root, "test", false, tc.maxConcurrency)

					prefetcher.prefetch(common.Hash{}, root, common.Address{}, prefetchKeys, true)

					prefetcher.terminate(false)

					// Wait for prefetching to complete
					prefetcher.trie(common.Hash{}, root)

					b.StopTimer()
				}()
			}
		})
	}
}

func generateBytes(n int) [][]byte {
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = make([]byte, 32)
		rand.Read(keys[i])
	}
	return keys
}
