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
	"testing"
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
