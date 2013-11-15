// Copyright (C) 2013  gowhitedb authors.
// Use of this source code is governed by a GPLv3
// license that can be found in the LICENSE file.

package whitedb

/*
#cgo pkg-config: whitedb
#include "gowhitedb.h"
#include <whitedb/dbapi.h>
*/
import "C"

import (
	"unsafe"
)

// WDBError is used for errors using the whitedb library.  It implements the
// builtin error interface.
type WDBError string

func (err WDBError) Error() string {
	return string(err)
}

// Wrapper for our db pointer
type WhiteDB struct {
	db unsafe.Pointer
}

// Attatches to a DB. Creates the db if it does not yet exist.
func AttachDatabase(address string, size int) (*WhiteDB, error) {
	cAdr := C.CString(address)
	defer C.free(unsafe.Pointer(cAdr))
	cSiz := C.int(size)
	d := &WhiteDB{db: C.wg_attach_database(cAdr, cSiz)}
	if d.db == nil {
		return nil, WDBError("Could not create database")
	}
	return d, nil
}

// Actually deletes the db.
func DeleteDatabase(address string) error {
	cAdr := C.CString(address)
	defer C.free(unsafe.Pointer(cAdr))
	if C.wg_delete_database(cAdr) != 0 {
		return WDBError("Could not delete database")
	}
	return nil
}

// Detatch the db from the current process but does not delete it.
func (d *WhiteDB) DetatchDatabase() {
	if d.db != nil {
		C.wg_detach_database(d.db)
	}
}
