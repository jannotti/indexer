package importer

import (
	"archive/tar"
	"compress/bzip2"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/algorand/go-algorand-sdk/encoding/json"
	log "github.com/sirupsen/logrus"

	"github.com/algorand/indexer/accounting"
	"github.com/algorand/indexer/idb"
	"github.com/algorand/indexer/types"
)

// NewImportHelper builds an ImportHelper
func NewImportHelper(genesisJSONPath string, numRoundsLimit, blockFileLimite int, l *log.Logger) (*ImportHelper) {
	return &ImportHelper{
		GenesisJSONPath: genesisJSONPath,
		NumRoundsLimit:  numRoundsLimit,
		BlockFileLimit:  blockFileLimite,
		Log:             l,
	}
}

// ImportHelper glues together a directory full of block files and an Importer objects.
type ImportHelper struct {
	// GenesisJSONPath is the location of the genesis file
	GenesisJSONPath string

	// NumRoundsLimit is the number of rounds to process, if 0 import continues forever.
	NumRoundsLimit int

	// BlockFileLimit is the number of block files to process.
	BlockFileLimit int

	Log *log.Logger
}

// Import is the main ImportHelper function that glues together a directory full of block files and an Importer objects.
func (h *ImportHelper) Import(db idb.IndexerDb, args []string) {
	err := ImportProto(db)
	maybeFail(err, h.Log, "import proto, %v", err)

	imp := NewDBImporter(db)
	blocks := 0
	txCount := 0
	start := time.Now()
	for _, fname := range args {
		matches, err := filepath.Glob(fname)
		if err == nil {
			pathsSorted := blockTarPaths(matches)
			sort.Sort(&pathsSorted)
			if h.BlockFileLimit != 0 && len(pathsSorted) > h.BlockFileLimit {
				pathsSorted = pathsSorted[:h.BlockFileLimit]
			}
			for _, gfname := range pathsSorted {
				fb, ft := importFile(db, imp, gfname, h.Log)
				blocks += fb
				txCount += ft
			}
		} else {
			// try without passing throug glob
			fb, ft := importFile(db, imp, fname, h.Log)
			blocks += fb
			txCount += ft
		}
	}
	blockdone := time.Now()
	if blocks > 0 {
		dt := blockdone.Sub(start)
		h.Log.Infof("%d blocks in %s, %.0f/s, %d txn, %.0f/s", blocks, dt.String(), float64(time.Second)*float64(blocks)/float64(dt), txCount, float64(time.Second)*float64(txCount)/float64(dt))
	}

	initialImport := InitialImport(db, h.GenesisJSONPath, h.Log)
	maybeFail(err, h.Log, "problem getting the max round")
	filter := idb.UpdateFilter{
		StartRound: 0,
	}
	if h.NumRoundsLimit != 0 {
		filter.RoundLimit = &h.NumRoundsLimit
	}
	if !initialImport {
		state, err := db.GetImportState()
		maybeFail(err, h.Log, "problem getting the import state")
		filter.StartRound = state.AccountRound
	}
	accountingRounds, txnCount := updateAccounting(db, filter, h.Log)
	if initialImport {
		accountingRounds++
	}

	accountingdone := time.Now()
	if accountingRounds > 0 {
		dt := accountingdone.Sub(blockdone)
		h.Log.Infof("%d rounds accounting in %s, %.1f/s (%d txns, %.1f/s)", accountingRounds, dt.String(), float64(time.Second)*float64(accountingRounds)/float64(dt), txnCount, float64(time.Second)*float64(txnCount)/float64(dt))
	}

	dt := accountingdone.Sub(start)
	h.Log.Infof(
		"%d blocks loaded (%.1f/s) and %d rounds accounting in %s, %.1f/s (%d txns, %.1f/s)",
		blocks,
		float64(time.Second)*float64(blocks)/float64(dt),
		accountingRounds,
		dt.String(),
		float64(time.Second)*float64(accountingRounds)/float64(dt),
		txnCount,
		float64(time.Second)*float64(txnCount)/float64(dt),
	)
}

func maybeFail(err error, l *log.Logger, errfmt string, params ...interface{}) {
	if err == nil {
		return
	}
	l.WithError(err).Errorf(errfmt, params...)
	os.Exit(1)
}

func importTar(imp Importer, tarfile io.Reader, l *log.Logger) (blocks, txCount int, err error) {
	lastlog := time.Now()
	blocks = 0
	prevBlocks := 0
	tf := tar.NewReader(tarfile)
	var header *tar.Header
	header, err = tf.Next()
	txCount = 0
	var btxns int
	for err == nil {
		if header.Typeflag != tar.TypeReg {
			err = fmt.Errorf("cannot deal with non-regular-file tar entry %#v", header.Name)
			return
		}
		blockbytes := make([]byte, header.Size)
		_, err = io.ReadFull(tf, blockbytes)
		if err != nil {
			err = fmt.Errorf("error reading tar entry %#v: %v", header.Name, err)
			return
		}
		btxns, err = imp.ImportBlock(blockbytes)
		if err != nil {
			err = fmt.Errorf("error importing tar entry %#v: %v", header.Name, err)
			return
		}
		txCount += btxns
		blocks++
		now := time.Now()
		dt := now.Sub(lastlog)
		if dt > (5 * time.Second) {
			dblocks := blocks - prevBlocks
			l.Infof("loaded from tar %v, %.1f/s", header.Name, ((float64(dblocks) * float64(time.Second)) / float64(dt)))
			lastlog = now
			prevBlocks = blocks
		}
		header, err = tf.Next()
	}
	if err == io.EOF {
		err = nil
	}
	return
}

func importFile(db idb.IndexerDb, imp Importer, fname string, l *log.Logger) (blocks, txCount int) {
	blocks = 0
	txCount = 0
	var btxns int
	imported, err := db.AlreadyImported(fname)
	maybeFail(err, l, "%s: %v", fname, err)
	if imported {
		return
	}
	l.Infof("importing %s ...", fname)
	if strings.HasSuffix(fname, ".tar") {
		fin, err := os.Open(fname)
		maybeFail(err, l, "%s: %v", fname, err)
		defer fin.Close()
		tblocks, btxns, err := importTar(imp, fin, l)
		maybeFail(err, l, "%s: %v", fname, err)
		blocks += tblocks
		txCount += btxns
	} else if strings.HasSuffix(fname, ".tar.bz2") {
		fin, err := os.Open(fname)
		maybeFail(err, l, "%s: %v", fname, err)
		defer fin.Close()
		bzin := bzip2.NewReader(fin)
		tblocks, btxns, err := importTar(imp, bzin, l)
		maybeFail(err, l, "%s: %v", fname, err)
		blocks += tblocks
		txCount += btxns
	} else {
		// assume a standalone block msgpack blob
		blockbytes, err := ioutil.ReadFile(fname)
		maybeFail(err, l, "%s: could not read, %v", fname, err)
		btxns, err = imp.ImportBlock(blockbytes)
		maybeFail(err, l, "%s: could not import, %v", fname, err)
		blocks++
		txCount += btxns
	}
	err = db.MarkImported(fname)
	maybeFail(err, l, "%s: %v", fname, err)
	return
}

func loadGenesis(db idb.IndexerDb, in io.Reader) (err error) {
	var genesis types.Genesis
	gbytes, err := ioutil.ReadAll(in)
	if err != nil {
		return fmt.Errorf("error reading genesis, %v", err)
	}
	err = json.Decode(gbytes, &genesis)
	if err != nil {
		return fmt.Errorf("error decoding genesis, %v", err)
	}

	return db.LoadGenesis(genesis)
}

// InitialImport imports the genesis block if needed. Returns true if the initial import occurred.
func InitialImport(db idb.IndexerDb, genesisJSONPath string, l *log.Logger) bool {
	stateJSONStr, err := db.GetMetastate("state")
	maybeFail(err, l, "getting import state, %v", err)

	if stateJSONStr == "" {
		if genesisJSONPath != "" {
			l.Infof("loading genesis %s", genesisJSONPath)
			// if we're given no previous state and we're given a genesis file, import it as initial account state
			gf, err := os.Open(genesisJSONPath)
			maybeFail(err, l, "%s: %v", genesisJSONPath, err)
			err = loadGenesis(db, gf)
			maybeFail(err, l, "%s: could not load genesis json, %v", genesisJSONPath, err)
			return true
		}
		l.Errorf("no import state recorded; need --genesis genesis.json file to get started")
		os.Exit(1)
		return false
	}
	return false
}

// allTransactionsFor is a helper to iterate through all of the transactions
func allTransactionsFor(db idb.IndexerDb, filter idb.UpdateFilter) <-chan idb.TxnRow {
	if filter.Address == nil {
		return db.YieldTxns(context.Background(), filter.StartRound - 1)
	}

	result := make(chan idb.TxnRow)
	go func() {
		done := false
		var next = ""
		for !done {
			order := idb.OrderAsc
			txns := db.Transactions(context.Background(), idb.TransactionFilter{
				Address: filter.Address[:],
				MinRound: uint64(filter.StartRound),
				NextToken: next,
				ResultOrder: &order,
			})

			// Forward transactions to the response channel, save next token.
			count := 0
			for txrow := range txns {
				result <- txrow
				next = txrow.Next()
				count++
			}
			done = count == 0
		}
		close(result)
	}()
	return result
}

// UpdateAccounting triggers an accounting update.
func UpdateAccounting(db idb.IndexerDb, filter idb.UpdateFilter, l *log.Logger) (rounds, txnCount int) {
	return updateAccounting(db, filter, l)
}

type changes struct {
	changes map[uint64][]change
	balance map[uint64]int64
}

type change struct {
	delta int64
	aca   int64
	balance int64
	close bool
	round uint64
}


func sideCheck(updates idb.RoundUpdates, round uint64, filter idb.UpdateFilter, changes *changes) {
	if round == 6674634 || round == 6674608 {
		fmt.Println("We have arrived...")
	}

	var addrb [32] byte
	copy(addrb[:], filter.Address[:])

	for _, subround := range updates.AssetUpdates {
		if vals, ok := subround[addrb]; ok {
			for _, val := range vals {
				if _, ok := changes.balance[val.AssetID]; !ok {
					changes.balance[val.AssetID] = 0
				}
				changes.changes[val.AssetID] = append(changes.changes[val.AssetID], change{
					delta:   val.Delta.Int64(),
					round:   round,
					balance: changes.balance[val.AssetID],
				})
				changes.balance[val.AssetID] = changes.balance[val.AssetID] + val.Delta.Int64()
			}

			// Check for close
			// Note: this only tracks when the account is closed, not when something is closed into the account...
			if len(vals) > 0 {
				val := vals[len(vals)-1]
				if val.Closed != nil && addrb == *filter.Address { //v[len(v)-1].Closed.Sender == *filter.Address {
					if _, ok := changes.balance[val.AssetID]; !ok {
						changes.balance[val.AssetID] = 0
					}
					changes.changes[val.AssetID] = append(changes.changes[val.AssetID], change{
						close:   true,
						delta:   changes.balance[val.AssetID] * -1,
						aca:     changes.balance[val.AssetID],
						round:   round,
						balance: changes.balance[val.AssetID],
					})
					changes.balance[val.AssetID] = 0
				}
			}
		}

		/*
			for _, val := range updates.AssetCloses {
				if val.Sender == *filter.Address {
					if _, ok := changes.balance[val.AssetID]; !ok {
						changes.balance[val.AssetID] = 0
					}
					changes.changes[val.AssetID] = append(changes.changes[val.AssetID], change{
						close:   true,
						delta:   changes.balance[val.AssetID] * -1,
						aca:     changes.balance[val.AssetID],
						round:   round,
						balance: changes.balance[val.AssetID],
					})
					changes.balance[val.AssetID] = 0
				}
			}
		*/
	}
}

func dumpChanges(changes *changes) {
	for k, v := range changes.changes {
		fmt.Printf("Asset #%d\n", k)
		fmt.Printf("| %11s | %5s | %11s | %11s | %11s |\n", "round", "close", "balance", "delta", "aca")
		fmt.Printf("| ----------- | ----- | ----------- | ----------- | ----------- |\n")
		for _, change := range v {
			fmt.Printf("| %11d | %5t | %11d | %11d | %11d |\n", change.round, change.close, change.balance, change.delta, change.aca)
		}
	}
}

func updateAccounting(db idb.IndexerDb, filter idb.UpdateFilter, l *log.Logger) (rounds, txnCount int) {
	l.Infof("will start from round %d", filter.StartRound)

	rounds = 0
	txnCount = 0
	lastlog := time.Now()
	act := accounting.New()
	txns := allTransactionsFor(db, filter)
	currentRound := uint64(0)
	roundsSeen := 0
	lastRoundsSeen := roundsSeen
	txnForRound := 0
	var blockPtr *types.Block = nil
	var changes = &changes{
		changes: make(map[uint64][]change),
		balance: make(map[uint64]int64),
	}
	for txn := range txns {
		maybeFail(txn.Error, l, "updateAccounting txn fetch, %v", txn.Error)
		if txn.Round != currentRound {
			// TODO: commit rounds with no transactions to avoid a special case to update the db metastate.
			if blockPtr != nil && txnForRound > 0 {
				sideCheck(act.RoundUpdates, currentRound, filter, changes)
				//dumpChanges(changes)
				err := db.CommitRoundAccounting(act.RoundUpdates.Filter(filter), currentRound, blockPtr.RewardsLevel)
				maybeFail(err, l, "failed to commit round accounting")
			}

			// initialize accounting for next round
			txnForRound = 0
			prevRound := currentRound
			roundsSeen++
			currentRound = txn.Round
			block, err := db.GetBlock(currentRound)
			maybeFail(err, l, "problem fetching next round (%d)", currentRound)
			blockPtr = &block
			act.InitRound(block)

			// Exit if limit reached
			if (filter.RoundLimit != nil) && (roundsSeen > *filter.RoundLimit) {
				l.Infof("hit rounds limit %d > %d", roundsSeen, filter.RoundLimit)
				break
			}

			// Log progress
			now := time.Now()
			dt := now.Sub(lastlog)
			//if dt > (5 * time.Second) {
				drounds := roundsSeen - lastRoundsSeen
				l.Infof("accounting through %d, %.1f/s", prevRound, ((float64(drounds) * float64(time.Second)) / float64(dt)))
				lastlog = now
				lastRoundsSeen = roundsSeen
			//}

			if currentRound == 6674634 || currentRound == 6674608 {
				fmt.Println("We have arrived...")
			}
		}
		err := act.AddTransaction(&txn)
		maybeFail(err, l, "txn accounting r=%d i=%d, %v", txn.Round, txn.Intra, err)
		txnCount++
		txnForRound++
	}

	// Commit the final round
	// TODO: commit rounds with empty paysets to avoid a special case to update the db metastate.
	if blockPtr != nil && txnForRound > 0 {
		sideCheck(act.RoundUpdates, currentRound, filter, changes)
		err := db.CommitRoundAccounting(act.RoundUpdates, currentRound, blockPtr.RewardsLevel)
		maybeFail(err, l, "failed to commit round accounting")
	}
	dumpChanges(changes)

	rounds += roundsSeen
	if rounds > 0 {
		l.Infof("accounting updated through round %d", currentRound)
	}
	return
}

type blockTarPaths []string

// Len is part of sort.Interface
func (paths *blockTarPaths) Len() int {
	return len(*paths)
}

func pathNameStartInt(x string) int64 {
	x = filepath.Base(x)
	underscorePos := strings.IndexRune(x, '_')
	if underscorePos == -1 {
		// try converting the whole string, might be a plain block
		v, err := strconv.ParseInt(x, 10, 64)
		if err == nil {
			return v
		}
		return -1
	}
	v, err := strconv.ParseInt(x[:underscorePos], 10, 64)
	if err != nil {
		return -1
	}
	return v
}

// Less is part of sort.Interface
func (paths *blockTarPaths) Less(i, j int) bool {
	return pathNameStartInt((*paths)[i]) < pathNameStartInt((*paths)[j])
}

// Swap is part of sort.Interface
func (paths *blockTarPaths) Swap(i, j int) {
	t := (*paths)[i]
	(*paths)[i] = (*paths)[j]
	(*paths)[j] = t
}
