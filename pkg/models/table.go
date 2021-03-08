// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.
package models

const MaxTableNum  int = 9999

type Table struct {
	Id		int			`json:"id"`
	Name	string		`json:"name"`
	MaxSlotMum int		`json:"max_slot_mum"`
	Auth    string		`json:"auth,omitempty"`
	IsBlocked bool		`json:"is_blocked,omitempty"`
}

type TableMeta struct {
	Id 		int 		`json:"id"`
}

func (t *TableMeta) Encode() []byte {
	return jsonEncode(t)
}

func (t *Table) Encode() []byte {
	return jsonEncode(t)
}

