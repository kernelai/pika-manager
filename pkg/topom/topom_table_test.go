package topom

import (
	"github.com/CodisLabs/codis/pkg/utils/assert"
	"testing"
)

func TestCreateTable(x *testing.T) {
	t := openTopom()
	defer t.Close()

	var name = "table1"
	const num = 128
	var id = -1
	var auth = "abc"

	assert.MustNoError(t.CreateTable(name, num, id, auth))

}
