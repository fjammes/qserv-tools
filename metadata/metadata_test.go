/*
* LSST Data Management System
* See COPYRIGHT file at the top of the source tree.
*
* This product includes software developed by the
* LSST Project (http://www.lsst.org/).
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU General Public License for more details.
*
* You should have received a copy of the LSST License Statement and
* the GNU General Public License along with this program. If not,
* see <http://www.lsstcorp.org/LegalNotices/>.
 */

// Generate dbbench.ini file from Qserv integration tests's datasets
// See qserv/itest_src/datasets/case<ID>/queries
// Exemple to run it:
// go run itest/examples/dbbench.go && cat /tmp/dbbench.ini

package metadata

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

// Testfiletype check return values for metadata.filetype()
func TestFiletype(t *testing.T) {

	chunkId := 61271
	filename := "chunk_61271_overlap.txt"
	res, id, err := filetype(filename)
	if res != Overlap || err != nil || id != chunkId {
		t.Fatalf("Unrecognized file %s, res %d, err %v", filename, res, err)
	}

	filename = "chunk_61271.txt"
	res, id, err = filetype(filename)
	if res != Chunk || err != nil || id != chunkId {
		t.Fatalf("Unrecognized file %s, res %d, err %v", filename, res, err)
	}

	filename = "random.json"
	res, id, err = filetype(filename)
	if res != Json || err != nil {
		t.Fatalf("Unrecognized file %s, res %d, err %v", filename, res, err)
	}
}

// TestAppendMetadata check return values for metadata.appendMetadata()
func TestAppendMetadata(t *testing.T) {
	tables := make(map[string]table)
	_ = appendMetadata(tables, "RubinTable", "chunkdatadir", "chunk_61271.txt", Chunk, 61271)

	expectedTables := make(map[string]table)
	dataList := make(map[string]data)

	dataList["chunkdatadir"] = data{
		Chunks:   []int{61271},
		Overlaps: nil,
		Files:    nil,
	}

	expectedTables["RubinTable"] = table{
		Schema:   "RubinTable.json",
		Indexes:  []string(nil),
		DataList: dataList,
	}
	assert.Equal(t, expectedTables, tables, "The two table maps should be the same.")
}

// TestWalkDirs check return values for metadata.walkDirs()
func TestWalkDirs(t *testing.T) {

	_, filename, _, _ := runtime.Caller(0)
	srcDir := filepath.Dir(filepath.Dir(filename))
	testDir := filepath.Join(srcDir, "itest", "case01")
	log.Debug().Msgf("Test data directory %s", testDir)
	cfg := Config{
		DbJsonFile:    "database.json",
		OrderedTables: []string{},
		IdxDir:        filepath.Join(testDir, "idx"),
	}
	tables := walkDirs(testDir, cfg)
	log.Debug().Msgf("RefSrcMatch indexes %v", tables["RefSrcMatch"].Indexes)
	idx := []string{"idx_RefSrcMatchRandomXXX.json", "idx_RefSrcMatch_RandomYYY.json"}
	assert.Equal(t, idx, tables["RefSrcMatch"].Indexes, "The two index lists should be the same.")
	idx = []string{"idx_sdqa_Metric_id.json"}
	assert.Equal(t, idx, tables["sdqa_Metric"].Indexes, "The two index lists should be the same.")
	assert.Equal(t, []string(nil), tables["LeapSeconds"].Indexes, "The two index lists should be the same.")
}
