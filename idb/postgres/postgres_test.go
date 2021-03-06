package postgres

import (
	"fmt"
	"testing"
	"time"

	"github.com/algorand/go-algorand-sdk/encoding/msgpack"

	"github.com/algorand/indexer/idb"
	"github.com/algorand/indexer/types"
	"github.com/stretchr/testify/require"
)

func TestAllMigrations(t *testing.T) {
	for idx, m := range migrations {
		t.Run(fmt.Sprintf("Test migration %d", idx), func(t *testing.T) {
			db := MakeMockDB([]*MockStmt{
				// "state"
				MakeMockStmt(
					1,
					[]string{"v"},
					[][]interface{}{
						{`{"account_round": 9000000}`},
					}),
				// "migration"
				MakeMockStmt(
					1,
					[]string{"v"},
					[][]interface{}{
						{fmt.Sprintf(`{"next": %d}`, idx)},
					}),
			})

			// This automatically runs migrations
			pdb, err := openPostgres(db, &idb.IndexerDbOptions{
				ReadOnly: false,
			}, nil)
			require.NoError(t, err)

			// Just need a moment for the go routine to get started
			time.Sleep(100 * time.Millisecond)

			h, err := pdb.Health()
			fmt.Printf("%v\n", h)
			// Health attempts to get num rows...
			require.Error(t, err, "not enough statements loaded into mock driver")

			// There should be an error because I'm not attempting to mock the migration code.
			//require.Contains(t, err.Error(), fmt.Sprintf("error during migration %d", idx))
			str := fmt.Sprintf("error during migration %d (%s)", idx, m.description)
			require.Contains(t, h.Error, str)
			require.Contains(t, (*h.Data)["migration-status"], str)
		})
	}
}

func TestNoMigrationsNeeded(t *testing.T) {
	db := MakeMockDB([]*MockStmt{
		// "state"
		MakeMockStmt(
			1,
			[]string{"v"},
			[][]interface{}{
				{`{"account_round": 9000000}`},
			}),
		// "migration"
		MakeMockStmt(
			1,
			[]string{"v"},
			[][]interface{}{
				{fmt.Sprintf(`{"next": %d}`, len(migrations)+1)},
			}),
	})

	// This automatically runs migraions
	pdb, err := openPostgres(db, &idb.IndexerDbOptions{
		ReadOnly: false,
	}, nil)

	// Just need a moment for the go routine to get started
	time.Sleep(100 * time.Millisecond)

	h, err := pdb.Health()
	// Health attempts to get num rows...
	require.Error(t, err, "not enough statements loaded into mock driver")

	require.Equal(t, (*h.Data)["migration-status"], "Migrations Complete")
}

func TestTealKeyValue(t *testing.T) {
	a := require.New(t)

	k1 := []byte("key1")
	k2 := []byte("key2")

	var tkv TealKeyValue
	_, ok := tkv.get(k1)
	a.False(ok)

	tkv.put(k1, TealValue{})
	_, ok = tkv.get(k1)
	a.True(ok)

	tkv.put(k2, TealValue{})
	_, ok = tkv.get(k2)
	a.True(ok)

	tkv.delete(k1)
	_, ok = tkv.get(k1)
	a.False(ok)

	tkv.delete(k2)
	_, ok = tkv.get(k2)
	a.False(ok)
}

type txnMsgpackJSON struct {
	msgpack []byte
	json    string
}

var testTxns = []txnMsgpackJSON{
	{
		[]uint8{0x83, 0xa2, 0x64, 0x74, 0x81, 0xa2, 0x67, 0x64, 0x81, 0xa9, 0xfe, 0xfe, 0xff, 0xef, 0x0, 0x0, 0x11, 0x22, 0x33, 0x82, 0xa2, 0x61, 0x74, 0x1, 0xa2, 0x62, 0x73, 0xc4, 0x3, 0x78, 0x78, 0x78, 0xa3, 0x73, 0x69, 0x67, 0xc4, 0x40, 0x51, 0xca, 0x9f, 0x32, 0xca, 0x9d, 0x66, 0x4b, 0xde, 0xa0, 0x98, 0xd9, 0x1b, 0xd, 0xe, 0x4d, 0x39, 0xca, 0x2, 0x4c, 0x4e, 0xc4, 0xba, 0x88, 0x1a, 0xb6, 0xa, 0x63, 0xff, 0xb0, 0x95, 0xc6, 0xb6, 0x7d, 0x0, 0xb4, 0xdc, 0xef, 0x41, 0xe6, 0x3b, 0xc3, 0x43, 0x3e, 0xb5, 0xa2, 0xa0, 0x27, 0xad, 0x9c, 0xc0, 0x57, 0x93, 0x5c, 0x4e, 0xcd, 0x18, 0xea, 0xb0, 0x6b, 0xe3, 0x97, 0x17, 0x3, 0xa3, 0x74, 0x78, 0x6e, 0x8b, 0xa4, 0x61, 0x70, 0x61, 0x61, 0x91, 0xc4, 0x3, 0x78, 0x78, 0x78, 0xa4, 0x61, 0x70, 0x61, 0x70, 0xc4, 0x17, 0x2, 0x20, 0x1, 0x1, 0x26, 0x1, 0x9, 0xfe, 0xfe, 0xff, 0xef, 0x0, 0x0, 0x11, 0x22, 0x33, 0x28, 0x36, 0x1a, 0x0, 0x67, 0x22, 0x43, 0xa4, 0x61, 0x70, 0x67, 0x73, 0x81, 0xa3, 0x6e, 0x62, 0x73, 0x1, 0xa4, 0x61, 0x70, 0x73, 0x75, 0xc4, 0x5, 0x2, 0x20, 0x1, 0x1, 0x22, 0xa3, 0x66, 0x65, 0x65, 0xcd, 0x3, 0xe8, 0xa2, 0x66, 0x76, 0x4, 0xa2, 0x67, 0x68, 0xc4, 0x20, 0x8a, 0xae, 0xf2, 0xee, 0x8f, 0x3, 0x93, 0xb9, 0xa5, 0x47, 0x41, 0x35, 0x3b, 0x97, 0x96, 0xf3, 0xd, 0xcc, 0x52, 0x10, 0x9d, 0x21, 0x15, 0x9a, 0x64, 0xe8, 0x47, 0x52, 0xb2, 0xcc, 0x90, 0x6a, 0xa2, 0x6c, 0x76, 0xcd, 0x3, 0xec, 0xa4, 0x6e, 0x6f, 0x74, 0x65, 0xc4, 0x8, 0x13, 0xfa, 0x3c, 0x55, 0xe8, 0x7b, 0x23, 0xea, 0xa3, 0x73, 0x6e, 0x64, 0xc4, 0x20, 0x4a, 0x82, 0x63, 0xeb, 0xc0, 0xd2, 0xee, 0xed, 0xac, 0x73, 0xdb, 0xb9, 0xd0, 0x27, 0xa1, 0xb2, 0x32, 0x99, 0x7a, 0xed, 0xc5, 0xde, 0xa2, 0x25, 0x7f, 0x7f, 0x2c, 0x8b, 0xcd, 0x42, 0x5f, 0x1a, 0xa4, 0x74, 0x79, 0x70, 0x65, 0xa4, 0x61, 0x70, 0x70, 0x6c},
		"{\"dt\":{\"gd\":{\"/v7/7wAAESIz\":{\"at\":1,\"bs\":\"eHh4\"}}},\"sig\":\"UcqfMsqdZkveoJjZGw0OTTnKAkxOxLqIGrYKY/+wlca2fQC03O9B5jvDQz61oqAnrZzAV5NcTs0Y6rBr45cXAw==\",\"txn\":{\"apaa\":[\"eHh4\"],\"apap\":\"AiABASYBCf7+/+8AABEiMyg2GgBnIkM=\",\"apgs\":{\"nbs\":1},\"apsu\":\"AiABASI=\",\"fee\":1000,\"fv\":4,\"gh\":\"iq7y7o8Dk7mlR0E1O5eW8w3MUhCdIRWaZOhHUrLMkGo=\",\"lv\":1004,\"note\":\"E/o8Veh7I+o=\",\"snd\":\"SoJj68DS7u2sc9u50CehsjKZeu3F3qIlf38si81CXxo=\",\"type\":\"appl\"}}",
	},
	{
		[]byte{0x83, 0xa2, 0x64, 0x74, 0x82, 0xa2, 0x67, 0x64, 0x82, 0xa3, 0x67, 0x6b, 0x62, 0x82, 0xa2, 0x61, 0x74, 0x1, 0xa2, 0x62, 0x73, 0xc4, 0x4, 0x74, 0x65, 0x73, 0x74, 0xa3, 0x67, 0x6b, 0x69, 0x82, 0xa2, 0x61, 0x74, 0x2, 0xa2, 0x75, 0x69, 0x64, 0xa2, 0x6c, 0x64, 0x81, 0x0, 0x82, 0xa3, 0x6c, 0x6b, 0x62, 0x82, 0xa2, 0x61, 0x74, 0x1, 0xa2, 0x62, 0x73, 0xc4, 0xb, 0x61, 0x6e, 0x6f, 0x74, 0x68, 0x65, 0x72, 0x74, 0x65, 0x73, 0x74, 0xa3, 0x6c, 0x6b, 0x69, 0x82, 0xa2, 0x61, 0x74, 0x2, 0xa2, 0x75, 0x69, 0xcc, 0xc8, 0xa3, 0x73, 0x69, 0x67, 0xc4, 0x40, 0xc9, 0x25, 0xb2, 0xa, 0x42, 0xda, 0x15, 0xbe, 0x74, 0x16, 0x1d, 0x45, 0xc9, 0x3b, 0xf, 0xa4, 0xcc, 0xdd, 0x86, 0xbd, 0xa, 0x53, 0x1e, 0x43, 0xb3, 0x7e, 0xf9, 0xcc, 0xaf, 0x44, 0x38, 0xce, 0x35, 0xa5, 0xaa, 0xb, 0x96, 0x28, 0x79, 0x6, 0xf8, 0xe1, 0xfb, 0x96, 0xe3, 0x79, 0x9b, 0x27, 0xfa, 0xa4, 0x51, 0x10, 0xc7, 0xb1, 0x84, 0x79, 0x46, 0xf8, 0xd8, 0x6a, 0x6c, 0x96, 0x93, 0x6, 0xa3, 0x74, 0x78, 0x6e, 0x8a, 0xa4, 0x61, 0x70, 0x61, 0x61, 0x91, 0xc4, 0x5, 0x66, 0x69, 0x72, 0x73, 0x74, 0xa4, 0x61, 0x70, 0x61, 0x6e, 0x1, 0xa4, 0x61, 0x70, 0x69, 0x64, 0x23, 0xa3, 0x66, 0x65, 0x65, 0xcd, 0x3, 0xe8, 0xa2, 0x66, 0x76, 0x7, 0xa2, 0x67, 0x68, 0xc4, 0x20, 0x8a, 0xae, 0xf2, 0xee, 0x8f, 0x3, 0x93, 0xb9, 0xa5, 0x47, 0x41, 0x35, 0x3b, 0x97, 0x96, 0xf3, 0xd, 0xcc, 0x52, 0x10, 0x9d, 0x21, 0x15, 0x9a, 0x64, 0xe8, 0x47, 0x52, 0xb2, 0xcc, 0x90, 0x6a, 0xa2, 0x6c, 0x76, 0xcd, 0x3, 0xef, 0xa4, 0x6e, 0x6f, 0x74, 0x65, 0xc4, 0x8, 0xc9, 0x83, 0x5, 0x5f, 0x20, 0x45, 0x8f, 0x98, 0xa3, 0x73, 0x6e, 0x64, 0xc4, 0x20, 0x32, 0xf8, 0xa1, 0x14, 0x66, 0x60, 0x7, 0xb7, 0xfe, 0x8, 0xd2, 0x48, 0x83, 0xdf, 0x28, 0x86, 0x16, 0x74, 0xa3, 0xb2, 0x5, 0x48, 0x1d, 0x4a, 0x45, 0x8e, 0x50, 0xb4, 0xba, 0x2f, 0x34, 0xde, 0xa4, 0x74, 0x79, 0x70, 0x65, 0xa4, 0x61, 0x70, 0x70, 0x6c},
		"{\"dt\":{\"gd\":{\"/v7/7wAAESIz\":{\"at\":1,\"bs\":\"eHh4\"},\"Z2ti\":{\"at\":1,\"bs\":\"dGVzdA==\"},\"Z2tp\":{\"at\":2,\"ui\":100}},\"ld\":{\"0\":{\"bGti\":{\"at\":1,\"bs\":\"YW5vdGhlcnRlc3Q=\"},\"bGtp\":{\"at\":2,\"ui\":200}}}},\"sig\":\"ySWyCkLaFb50Fh1FyTsPpMzdhr0KUx5Ds375zK9EOM41paoLlih5Bvjh+5bjeZsn+qRREMexhHlG+NhqbJaTBg==\",\"txn\":{\"apaa\":[\"Zmlyc3Q=\"],\"apan\":1,\"apap\":\"AiABASYBCf7+/+8AABEiMyg2GgBnIkM=\",\"apgs\":{\"nbs\":1},\"apid\":35,\"apsu\":\"AiABASI=\",\"fee\":1000,\"fv\":7,\"gh\":\"iq7y7o8Dk7mlR0E1O5eW8w3MUhCdIRWaZOhHUrLMkGo=\",\"lv\":1007,\"note\":\"yYMFXyBFj5g=\",\"snd\":\"MvihFGZgB7f+CNJIg98ohhZ0o7IFSB1KRY5QtLovNN4=\",\"type\":\"appl\"}}",
	},
}

func TestStxnJSON(t *testing.T) {
	var stxn types.SignedTxnWithAD
	for i, mt := range testTxns {
		t.Run(fmt.Sprintf("i=%d", i), func(t *testing.T) {
			msgpack.Decode(mt.msgpack, &stxn)
			js := stxnToJSON(stxn)
			require.Equal(t, mt.json, string(js))
		})
	}
}

func TestStxnJSONSynthetic(t *testing.T) {
	nonutf8b := []byte{254, 254, 255, 239, 0, 0, 17, 34, 51}
	nonutf8 := string(nonutf8b)
	var stxn types.SignedTxnWithAD
	stxn.EvalDelta.GlobalDelta = make(map[string]types.ValueDelta)
	stxn.EvalDelta.GlobalDelta[nonutf8] = types.ValueDelta{
		Action: types.SetBytesAction,
		Bytes:  nonutf8b,
	}
	stxn.EvalDelta.LocalDeltas = make(map[uint64]types.StateDelta, 1)
	ld := make(map[string]types.ValueDelta)
	ld[nonutf8] = types.ValueDelta{
		Action: types.SetBytesAction,
		Bytes:  nonutf8b,
	}
	stxn.EvalDelta.LocalDeltas[1] = ld
	js := stxnToJSON(stxn)
	require.Equal(t, "{\"dt\":{\"gd\":{\"/v7/7wAAESIz\":{\"at\":1,\"bs\":\"/v7/7wAAESIz\"}},\"ld\":{\"1\":{\"/v7/7wAAESIz\":{\"at\":1,\"bs\":\"/v7/7wAAESIz\"}}}}}", string(js))
}
