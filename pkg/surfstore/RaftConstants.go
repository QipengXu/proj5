package surfstore

import (
	"fmt"
)

var ERR_SERVER_CRASHED = fmt.Errorf("Server is crashed.")
var ERR_NOT_LEADER = fmt.Errorf("Server is not the leader")
var ERR_MAJORITY_SERVER_CRASHED = fmt.Errorf("Majortity servers are crashed")

const SURF_CLIENT string = "[Surfstore RPCClient]:"
const SURF_SERVER string = "[Surfstore Server]:"
