// Copyright (C) 2013  gowhitedb authors.
// Use of this source code is governed by a GPLv3
// license that can be found in the LICENSE file.

package whitedb_test

import (
	"testing"
	"whitedb"
)

const (
	DB_SIZE = 2000000
	DB_NAME = "6700"
)

type testkv struct {
	Key   string
	Value int
}

func TestOpenClose(t *testing.T) {

	db, err := whitedb.AttachDatabase(DB_NAME, DB_SIZE)
	if err != nil {
		t.Fatal(err)
	}

	defer db.DetatchDatabase()
}

func TestDelete(t *testing.T) {

	db, err := whitedb.AttachDatabase(DB_NAME, DB_SIZE)
	if err != nil {
		t.Fatal(err)
	}

	db.DetatchDatabase()
	if err = whitedb.DeleteDatabase(DB_NAME); err != nil {
		t.Fatal(err)
	}
}

func TestRecordCreateDelete(t *testing.T) {
	db, err := whitedb.AttachDatabase(DB_NAME, DB_SIZE)
	if err != nil {
		t.Fatal(err)
	}

	defer db.DetatchDatabase()
	recs := make([]*whitedb.Record, 0, 0)

	if r, err := db.CreateRecord(2); err != nil {
		t.Fatal(err)
	} else {
		recs = append(recs, r)
	}

	if r, err := db.CreateRawRecord(2); err != nil {
		t.Fatal(err)
	} else {
		recs = append(recs, r)
	}

	for _, r := range recs {
		err = db.DeleteRecord(r)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestRecordSet(t *testing.T) {
	db, err := whitedb.AttachDatabase(DB_NAME, DB_SIZE)
	if err != nil {
		t.Fatal(err)
	}
	defer db.DetatchDatabase()

	rec, err := db.CreateRecord(5)
	if err != nil {
		t.Fatal(err)
	}

	if err := rec.SetField(db, 0, 0); err != nil {
		t.Fatal(err)
	}

	if err := rec.SetNewField(db, 1, 1); err != nil {
		t.Fatal(err)
	}

	if err := rec.SetInt64Field(db, 2, 2); err != nil {
		t.Fatal(err)
	}

	if err := rec.SetFloat64Field(db, 3, 3.0); err != nil {
		t.Fatal(err)
	}

	if err := rec.SetBytesField(db, 4, []byte("Test")); err != nil {
		t.Fatal(err)
	}

	if err := rec.SetNewField(db, 1, 1); err == nil {
		t.Errorf("Set new field faled. Expected an error, got nothing.")
	}
}

func TestIntGetSet(t *testing.T) {
	db, err := whitedb.AttachDatabase(DB_NAME, DB_SIZE)
	if err != nil {
		t.Fatal(err)
	}
	defer db.DetatchDatabase()
	vals := []int64{10, 3242, 23423423}

	rec, err := db.CreateRecord(int64(len(vals)))
	if err != nil {
		t.Fatal(err)
	}

	for i, v := range vals {
		if err := rec.SetInt64Field(db, uint16(i), v); err != nil {
			t.Fatal(err)
		}
	}

	for i, v := range vals {
		if vsave, err := rec.GetInt64Field(db, uint16(i)); err != nil {
			t.Fatal(err)
		} else if v != vsave {
			t.Errorf("Execting %d, got %d", v, vsave)
		}
	}
}

func TestFloat64GetSet(t *testing.T) {
	db, err := whitedb.AttachDatabase(DB_NAME, DB_SIZE)
	if err != nil {
		t.Fatal(err)
	}
	defer db.DetatchDatabase()
	vals := []float64{10.324, 3242.23423, 23423423.234234}

	rec, err := db.CreateRecord(int64(len(vals)))
	if err != nil {
		t.Fatal(err)
	}

	for i, v := range vals {
		if err := rec.SetFloat64Field(db, uint16(i), v); err != nil {
			t.Fatal(err)
		}
	}

	for i, v := range vals {
		if vsave, err := rec.GetFloat64Field(db, uint16(i)); err != nil {
			t.Fatal(err)
		} else if v != vsave {
			t.Errorf("Execting %d, got %d", v, vsave)
		}
	}
}

func TestSeek(t *testing.T) {
	whitedb.DeleteDatabase(DB_NAME)
	db, err := whitedb.AttachDatabase(DB_NAME, DB_SIZE)
	if err != nil {
		t.Fatal(err)
	}
	defer db.DetatchDatabase()
	vals := [][]byte{[]byte("dsfsdf"), []byte("234234sdfsdf"), []byte("sdfsdfsdfsdf")}

	for i, v := range vals {
		rec, err := db.CreateRecord(2)
		if err != nil {
			t.Fatal(err)
		}

		if err := rec.SetInt64Field(db, 0, int64(i)); err != nil {
			t.Fatal(err)
		}

		if err := rec.SetBytesField(db, 1, v); err != nil {
			t.Fatal(err)
		}
	}

	// Seek all
	count := 0
	rec, err := db.GetFirstRecord()
	for err == nil {
		count++
		rec, err = db.GetNextRecord(rec)
	}

	if count != len(vals) {
		t.Errorf("Execting %d, got %d", len(vals), count)
	}

	// Test out search
	count = 0
	rec, err = db.FindRecordInt(0, whitedb.EQUAL, 0, nil)
	for err == nil {
		count++
		rec, err = db.FindRecordInt(0, whitedb.EQUAL, 0, rec)
	}

	if count != 1 {
		t.Errorf("Execting %d, got %d", 1, count)
	}

	// Test out search ([]byte)
	count = 0
	rec, err = db.FindRecordBytes(1, whitedb.EQUAL, vals[2], nil)
	for err == nil {
		count++
		rec, err = db.FindRecordBytes(1, whitedb.EQUAL, vals[2], rec)
	}

	if count != 1 {
		t.Errorf("Execting %d, got %d", 1, count)
	}
}

func TestBytesGetSet(t *testing.T) {
	db, err := whitedb.AttachDatabase(DB_NAME, DB_SIZE)
	if err != nil {
		t.Fatal(err)
	}
	defer db.DetatchDatabase()
	vals := [][]byte{[]byte("dsfsdf"), []byte("234234sdfsdf"), []byte("sdfsdfsdfsdf")}

	rec, err := db.CreateRecord(int64(len(vals)))
	if err != nil {
		t.Fatal(err)
	}

	for i, v := range vals {
		if err := rec.SetBytesField(db, uint16(i), v); err != nil {
			t.Fatal(err)
		}
	}

	for i, v := range vals {
		if vsave, err := rec.GetBytesField(db, uint16(i)); err != nil {
			t.Fatal(err)
		} else if string(v) != string(vsave) {
			t.Errorf("Execting %s, got %s", string(v), string(vsave))
		}
	}
}

func TestGobGetSet(t *testing.T) {
	db, err := whitedb.AttachDatabase(DB_NAME, DB_SIZE)
	if err != nil {
		t.Fatal(err)
	}
	defer db.DetatchDatabase()
	vals := []testkv{testkv{"sdfsd", 1}, testkv{"sfswergdcxvxcv", 2}, testkv{"345345345345sdfsd", 3}}

	rec, err := db.CreateRecord(int64(len(vals)))
	if err != nil {
		t.Fatal(err)
	}

	for i, v := range vals {
		if err := rec.SetGobField(db, uint16(i), &v); err != nil {
			t.Fatal(err)
		}
	}

	for i, v := range vals {
		tk := testkv{}
		if err := rec.GetGobField(db, uint16(i), &tk); err != nil {
			t.Fatal(err)
		} else if v.Value != tk.Value {
			t.Errorf("Execting %s, got %s", v.Value, tk.Value)
		}
	}
}

// @TODO -- why can I not pass a pointer to a db into a new go routine?
func TestTransactions(t *testing.T) {

	whitedb.DeleteDatabase(DB_NAME)
	db, err := whitedb.AttachDatabase(DB_NAME, DB_SIZE)
	if err != nil {
		t.Fatal(err)
	}
	defer db.DetatchDatabase()

	// Make sure there's at least one record.
	rec, err := db.CreateRecord(1)
	rec.SetInt64Field(db, 0, 0)
	done := make(chan bool, 0)
	values := []int64{1, 2}

	update := func(c int64) {
		da, err := whitedb.AttachDatabase(DB_NAME, DB_SIZE)
		if err != nil {
			t.Fatal(err)
		}
		defer da.DetatchDatabase()

		wlock := da.GetWriteLock()
		r, _ := da.GetFirstRecord()
		if r != nil {
			val, _ := r.GetInt64Field(da, 0)
			r.SetInt64Field(da, 0, c+val)
		}
		da.EndWriteLock(wlock)
		done <- true
	}

	sum := int64(0)
	for _, v := range values {
		go update(v)
		sum += v
	}

	c := 0
	for _ = range done {
		c++
		if c >= 2 {
			break
		}
	}

	rlock := db.GetReadLock()
	val, _ := rec.GetInt64Field(db, 0)
	db.EndReadLock(rlock)

	if val != 3 {
		t.Errorf("Execting %s, got %s", sum, val)
	}
}

func BenchmarkIntSet(b *testing.B) {
	db, err := whitedb.AttachDatabase(DB_NAME, DB_SIZE)
	if err != nil {
		b.Fatal(err)
	}

	rec, err := db.CreateRecord(1)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		if err := rec.SetInt64Field(db, 0, int64(i)); err != nil {
			b.Fatal(err)
		}
	}

	db.DetatchDatabase()
	whitedb.DeleteDatabase(DB_NAME)
}

func BenchmarkBytesSet(b *testing.B) {
	db, err := whitedb.AttachDatabase(DB_NAME, DB_SIZE)
	if err != nil {
		b.Fatal(err)
	}

	rec, err := db.CreateRecord(1)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		if err := rec.SetBytesField(db, 0, []byte("testa")); err != nil {
			b.Fatal(err)
		}
	}

	db.DetatchDatabase()
	whitedb.DeleteDatabase(DB_NAME)
}
