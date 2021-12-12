package plugin

var RegistedPlugins = map[string]Handler{
	"Dump":   &Dump{},
	"Edit":   &Edit{},
	"SaveES": &SaveES{},
	"Alarm":  &Alarm{},
}
