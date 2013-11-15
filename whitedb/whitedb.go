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
	"reflect"
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

	ILLEGAL = C.WG_ILLEGAL

	/* Query "arglist" parameters */
	EQUAL     = C.WG_COND_EQUAL     /** = */
	NOT_EQUAL = C.WG_COND_NOT_EQUAL /** != */
	LESSTHAN  = C.WG_COND_LESSTHAN  /** < */
	GREATER   = C.WG_COND_GREATER   /** > */
	LTEQUAL   = C.WG_COND_LTEQUAL   /** <= */
	GTEQUAL   = C.WG_COND_GTEQUAL   /** >= */

	/* Query types. Python extension module uses the API and needs these. */
	QTYPE_TTREE    = C.WG_QTYPE_TTREE
	QTYPE_HASH     = C.WG_QTYPE_HASH
	QTYPE_SCAN     = C.WG_QTYPE_SCAN
	QTYPE_PREFETCH = C.WG_QTYPE_PREFETCH
)

// WDBError is used for errors using the whitedb library.  It implements the
// builtin error interface.
type WDBError string

func (err WDBError) Error() string {
	return string(err)
}

// Wrapper for our db pointer
type Db struct {
	db unsafe.Pointer
}

// A record pointer
type Record struct {
	rec unsafe.Pointer
}

//// Work with Databases.

// Attatches to a DB. Creates the db if it does not yet exist.
func AttachDatabase(address string, size int) (*Db, error) {
	cAdr := C.CString(address)
	defer C.free(unsafe.Pointer(cAdr))
	cSiz := C.int(size)
	d := &Db{db: C.wg_attach_database(cAdr, cSiz)}
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
func (d *Db) DetatchDatabase() {
	if d.db != nil {
		C.wg_detach_database(d.db)
	}
}

//// Generate/Delete/Find Records.

// Generate a record.
func (d *Db) CreateRecord(length int64) (*Record, error) {
	r := &Record{rec: C.wg_create_record(d.db, C.wg_int(length))}
	if r.rec == nil {
		return nil, WDBError("Could not create record")
	}
	return r, nil
}

// Same as above, but does not create index.
func (d *Db) CreateRawRecord(length int64) (*Record, error) {
	r := &Record{rec: C.wg_create_raw_record(d.db, C.wg_int(length))}
	if r.rec == nil {
		return nil, WDBError("Could not create record")
	}
	return r, nil
}

// Removes the record.
func (d *Db) DeleteRecord(r *Record) error {
	if C.wg_delete_record(d.db, r.rec) != 0 {
		return WDBError("Could not delete record")
	}
	return nil
}

// Seeks to the first record in a db.
func (d *Db) GetFirstRecord() (*Record, error) {
	r := &Record{rec: C.wg_get_first_record(d.db)}
	if r.rec == nil {
		return nil, WDBError("Done With DB")
	}
	return r, nil
}

// Increments to next record in the db.
func (d *Db) GetNextRecord(r *Record) (*Record, error) {
	nr := &Record{rec: C.wg_get_next_record(d.db, r.rec)}
	if nr.rec == nil {
		return nil, WDBError("Done With DB")
	}
	return nr, nil
}

//// Search for a given record
func (d *Db) FindRecordInt(field uint16, condition uint16, data int, lr *Record) (*Record, error) {
	nr := &Record{rec: nil}
	if lr == nil {
		nr.rec = C.wg_find_record_int(d.db, C.wg_int(field), C.wg_int(condition), C.int(data), nil)
	} else {
		nr.rec = C.wg_find_record_int(d.db, C.wg_int(field), C.wg_int(condition), C.int(data), lr.rec)
	}

	if nr.rec == nil {
		return nil, WDBError("Done With DB")
	}
	return nr, nil
}

func (d *Db) FindRecordBytes(field uint16, condition uint16, data []byte, lr *Record) (*Record, error) {
	nr := &Record{rec: nil}
	if lr == nil {
		nr.rec = C.wg_find_record_str(d.db, C.wg_int(field), C.wg_int(condition), (*C.char)(unsafe.Pointer(&data[0])), nil)
	} else {
		nr.rec = C.wg_find_record_str(d.db, C.wg_int(field), C.wg_int(condition), (*C.char)(unsafe.Pointer(&data[0])), lr.rec)
	}

	if nr.rec == nil {
		return nil, WDBError("Done With DB")
	}
	return nr, nil
}

//// Get set values in records.

// Sets the already encoded value
func (r *Record) SetField(d *Db, field uint16, data int64) error {
	if C.wg_set_field(d.db, r.rec, C.wg_int(field), C.wg_int(data)) != 0 {
		return WDBError("Could not set field")
	}
	return nil
}

// Same as above, but errors if the field has been set before.
func (r *Record) SetNewField(d *Db, number uint16, data int64) error {
	if C.wg_set_new_field(d.db, r.rec, C.wg_int(number), C.wg_int(data)) != 0 {
		return WDBError("Could not set field")
	}
	return nil
}

// Convenience function, does the encoding for you.
func (r *Record) SetInt64Field(d *Db, number uint16, data int64) error {
	if C.wg_set_int_field(d.db, r.rec, C.wg_int(number), C.wg_int(data)) != 0 {
		return WDBError("Could not set field")
	}
	return nil
}

// Takes Doubles
func (r *Record) SetFloat64Field(d *Db, number uint16, data float64) error {
	if C.wg_set_double_field(d.db, r.rec, C.wg_int(number), C.double(data)) != 0 {
		return WDBError("Could not set field")
	}
	return nil
}

func (r *Record) SetBytesField(d *Db, number uint16, data []byte) error {
	if C.wg_set_str_field(d.db, r.rec, C.wg_int(number), (*C.char)(unsafe.Pointer(&data[0]))) != 0 {
		return WDBError("Could not set field")
	}
	return nil
}

///// Getters

// Returns 0 when there was an error.
// @TODO -- wrap this in better handling?
func (r *Record) GetField(d *Db, number uint16) int64 {
	return int64(C.wg_get_field(d.db, r.rec, C.wg_int(number)))
}

func (r *Record) GetFieldType(d *Db, number uint16) int64 {
	return int64(C.wg_get_field_type(d.db, r.rec, C.wg_int(number)))
}

func (r *Record) GetInt64Field(d *Db, number uint16) (int64, error) {
	if r.GetFieldType(d, number) != INTTYPE {
		return 0, WDBError("Not an int valued field")
	}
	return int64(C.wg_decode_int(d.db, C.wg_get_field(d.db, r.rec, C.wg_int(number)))), nil
}

func (r *Record) GetFloat64Field(d *Db, number uint16) (float64, error) {
	if r.GetFieldType(d, number) != DOUBLETYPE {
		return 0, WDBError("Not an double valued field")
	}
	return float64(C.wg_decode_double(d.db, C.wg_get_field(d.db, r.rec, C.wg_int(number)))), nil
}

// 0 Copy return.
// @TODO -- make sure doesn't leak
func (r *Record) GetBytesField(d *Db, number uint16) ([]byte, error) {
	if r.GetFieldType(d, number) != STRTYPE {
		return nil, WDBError("Not an string valued field")
	}

	enc := C.wg_get_field(d.db, r.rec, C.wg_int(number))
	slen := int(C.wg_decode_str_len(d.db, enc))
	sval := C.wg_decode_str(d.db, enc)

	var goSlice []byte
	sliceHeader := (*reflect.SliceHeader)((unsafe.Pointer(&goSlice)))
	sliceHeader.Cap = slen
	sliceHeader.Len = slen
	sliceHeader.Data = uintptr(unsafe.Pointer(sval))
	return goSlice, nil
}

////// Work with Transactions

func (d *Db) GetWriteLock() int64 {
	return int64(C.wg_start_write(d.db))
}

func (d *Db) EndWriteLock(lock int64) int64 {
	return int64(C.wg_end_write(d.db, C.wg_int(lock)))
}

func (d *Db) GetReadLock() int64 {
	return int64(C.wg_start_read(d.db))
}

func (d *Db) EndReadLock(lock int64) int64 {
	return int64(C.wg_end_read(d.db, C.wg_int(lock)))
}

/**
wg_encode_blob -- for bytes
*/
