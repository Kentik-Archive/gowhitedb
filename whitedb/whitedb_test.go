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
	if err != nil {
		t.Fatal(err)
	}

	defer db.DetatchDatabase()
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

func TestRecordCreateDelete(t *testing.T) {
	db, err := whitedb.AttachDatabase("1000", 2000000)
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
	db, err := whitedb.AttachDatabase("1000", 2000000)
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

	if err := rec.SetByteField(db, 4, []byte("Test")); err != nil {
		t.Fatal(err)
	}

	if err := rec.SetNewField(db, 1, 1); err == nil {
		t.Errorf("Set new field faled. Expected an error, got nothing.")
	}
}

func TestIntGetSet(t *testing.T) {
	db, err := whitedb.AttachDatabase("1000", 2000000)
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
	db, err := whitedb.AttachDatabase("1000", 2000000)
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

func TestByteGetSet(t *testing.T) {
	db, err := whitedb.AttachDatabase("1000", 2000000)
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
		if err := rec.SetByteField(db, uint16(i), v); err != nil {
			t.Fatal(err)
		}
	}

	for i, v := range vals {
		if vsave, err := rec.GetByteField(db, uint16(i)); err != nil {
			t.Fatal(err)
		} else if string(v) != string(vsave) {
			t.Errorf("Execting %s, got %s", string(v), string(vsave))
		}

	}
}
