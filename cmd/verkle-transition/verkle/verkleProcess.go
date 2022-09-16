package verkle

import "context"

type OptionsCfg struct {
	Ctx             context.Context
	VerkleDb        string
	StateDb         string
	WorkersCount    uint
	Tmpdir          string
	DisabledLookups bool
}
