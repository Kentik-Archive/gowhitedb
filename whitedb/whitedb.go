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

// Export some record types to Go.
const (
	NULLTYPE       = C.WG_NULLTYPE
	RECORDTYPE     = C.WG_RECORDTYPE
	INTTYPE        = C.WG_INTTYPE
	DOUBLETYPE     = C.WG_DOUBLETYPE
	STRTYPE        = C.WG_STRTYPE
	XMLLITERALTYPE = C.WG_XMLLITERALTYPE
	URITYPE        = C.WG_URITYPE
	BLOBTYPE       = C.WG_BLOBTYPE
	CHARTYPE       = C.WG_CHARTYPE
	FIXPOINTTYPE   = C.WG_FIXPOINTTYPE
	DATETYPE       = C.WG_DATETYPE
	TIMETYPE       = C.WG_TIMETYPE
	ANONCONSTTYPE  = C.WG_ANONCONSTTYPE
	VARTYPE        = C.WG_VARTYPE
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

// A record pointer
type Record struct {
	rec unsafe.Pointer
}

//// Work with Databases.

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

//// Generate/Delete/Find Records.

// Generate a record.
func (d *WhiteDB) CreateRecord(length int64) (*Record, error) {
	r := &Record{rec: C.wg_create_record(d.db, C.wg_int(length))}
	if r.rec == nil {
		return nil, WDBError("Could not create record")
	}
	return r, nil
}

// Same as above, but does not create index.
func (d *WhiteDB) CreateRawRecord(length int64) (*Record, error) {
	r := &Record{rec: C.wg_create_raw_record(d.db, C.wg_int(length))}
	if r.rec == nil {
		return nil, WDBError("Could not create record")
	}
	return r, nil
}

// Removes the record.
func (d *WhiteDB) DeleteRecord(r *Record) error {
	if C.wg_delete_record(d.db, r.rec) != 0 {
		return WDBError("Could not delete record")
	}
	return nil
}

// Seeks to the first record in a db.
func (d *WhiteDB) GetFirstRecord() (*Record, error) {
	r := &Record{rec: C.wg_get_first_record(d.db)}
	if r.rec == nil {
		return nil, WDBError("Done With DB")
	}
	return r, nil
}

// Increments to next record in the db.
func (d *WhiteDB) GetNextRecord(r *Record) (*Record, error) {
	nr := &Record{rec: C.wg_get_next_record(d.db, r.rec)}
	if nr.rec == nil {
		return nil, WDBError("Done With DB")
	}
	return nr, nil
}

//// Get set values in records.

// Sets the already encoded value
func (r *Record) SetField(d *WhiteDB, number uint16, data int64) error {
	if C.wg_set_field(d.db, r.rec, C.wg_int(number), C.wg_int(data)) != 0 {
		return WDBError("Could not set field")
	}
	return nil
}

// Same as above, but errors if the field has been set before.
func (r *Record) SetNewField(d *WhiteDB, number uint16, data int64) error {
	if C.wg_set_new_field(d.db, r.rec, C.wg_int(number), C.wg_int(data)) != 0 {
		return WDBError("Could not set field")
	}
	return nil
}

// Convenience function, does the encoding for you.
func (r *Record) SetInt64Field(d *WhiteDB, number uint16, data int64) error {
	if C.wg_set_int_field(d.db, r.rec, C.wg_int(number), C.wg_int(data)) != 0 {
		return WDBError("Could not set field")
	}
	return nil
}

// Takes Doubles
func (r *Record) SetFloat64Field(d *WhiteDB, number uint16, data float64) error {
	if C.wg_set_double_field(d.db, r.rec, C.wg_int(number), C.double(data)) != 0 {
		return WDBError("Could not set field")
	}
	return nil
}

func (r *Record) SetByteField(d *WhiteDB, number uint16, data []byte) error {
	if C.wg_set_str_field(d.db, r.rec, C.wg_int(number), (*C.char)(unsafe.Pointer(&data[0]))) != 0 {
		return WDBError("Could not set field")
	}
	return nil
}

///// Getters

// Returns 0 when there was an error.
// @TODO -- wrap this in better handling?
func (r *Record) GetField(d *WhiteDB, number uint16) int64 {
	return int64(C.wg_get_field(d.db, r.rec, C.wg_int(number)))
}

func (r *Record) GetFieldType(d *WhiteDB, number uint16) int64 {
	return int64(C.wg_get_field_type(d.db, r.rec, C.wg_int(number)))
}

func (r *Record) GetInt64Field(d *WhiteDB, number uint16) (int64, error) {
	if r.GetFieldType(d, number) != INTTYPE {
		return 0, WDBError("Not an int valued field")
	}
	return int64(C.wg_decode_int(d.db, C.wg_get_field(d.db, r.rec, C.wg_int(number)))), nil
}

func (r *Record) GetFloat64Field(d *WhiteDB, number uint16) (float64, error) {
	if r.GetFieldType(d, number) != DOUBLETYPE {
		return 0, WDBError("Not an double valued field")
	}
	return float64(C.wg_decode_double(d.db, C.wg_get_field(d.db, r.rec, C.wg_int(number)))), nil
}

/**
func (r *Record) GetByteField(d *WhiteDB, number uint16) ([]byte, error) {
	if r.GetFieldType(d, number) != STRINGTYPE {
		return 0, WDBError("Not an string valued field")
	}

	return int64(C.wg_decode_int(d.db, C.wg_get_field(d.db, r.rec, C.wg_int(number)))), nil
}
*/

/**


*/
