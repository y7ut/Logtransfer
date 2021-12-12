package transfer

import(
	"github.com/y7ut/logtransfer/plugin"
)

var pluginsBoard = map[string]plugin.Handler{
	"Dump":   &plugin.Dump{},
	"Edit":   &plugin.Edit{},
	"SaveES": &plugin.SaveES{},
	"Alarm":  &plugin.Alarm{},
}
