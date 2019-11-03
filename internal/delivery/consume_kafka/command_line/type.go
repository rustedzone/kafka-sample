package commandline

type (
	flagAttr struct {
		broker    string
		topic     string
		groupName string
		esHost    string
		esType    string
		esIndex   string
		saramalog bool
	}
)
