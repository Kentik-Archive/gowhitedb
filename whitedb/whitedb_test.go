// Copyright (C) 2013  gowhitedb authors.
// Use of this source code is governed by a GPLv3
// license that can be found in the LICENSE file.

package whitedb_test

import (
	"testing"
	"whitedb"
)

func TestOpenClose(t *testing.T) {

	db, err := whitedb.AttachDatabase("1000", 2000000)
	defer db.DetatchDatabase()

	if err != nil {
		t.Fatal(err)
	}
}

func TestDelete(t *testing.T) {

	db, err := whitedb.AttachDatabase("1000", 2000000)

	if err != nil {
		t.Fatal(err)
	}

	db.DetatchDatabase()
	if err = whitedb.DeleteDatabase("1000"); err != nil {
		t.Fatal(err)
	}
}
