package data

type vector struct {
	rawData []byte
}

func (v *vector) GetRawBytes() []byte {
	return v.rawData
}

type nullByteMap struct {
	byteMap []byte
}

func (n *nullByteMap) IsNull(index uint) bool {
	return n.byteMap[index] == 0
}

func (n *nullByteMap) GetNullBytemap() []byte {
	return n.byteMap
}
