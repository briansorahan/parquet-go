package marshal

import (
	"errors"
	"reflect"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/layout"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/types"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

//ss is []string
func MarshalJSON(ss []interface{}, schemaHandler *schema.SchemaHandler) (tb *map[string]*layout.Table, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("unknown error")
			}
		}
	}()

	res := make(map[string]*layout.Table)
	pathMap := schemaHandler.PathMap
	nodeBuf := NewNodeBuf(1)

	for i := 0; i < len(schemaHandler.SchemaElements); i++ {
		schema := schemaHandler.SchemaElements[i]
		pathStr := schemaHandler.IndexMap[int32(i)]
		numChildren := schema.GetNumChildren()
		if numChildren == 0 {
			res[pathStr] = layout.NewEmptyTable()
			res[pathStr].Path = common.StrToPath(pathStr)
			res[pathStr].MaxDefinitionLevel, _ = schemaHandler.MaxDefinitionLevel(res[pathStr].Path)
			res[pathStr].MaxRepetitionLevel, _ = schemaHandler.MaxRepetitionLevel(res[pathStr].Path)
			res[pathStr].RepetitionType = schema.GetRepetitionType()
			res[pathStr].Schema = schemaHandler.SchemaElements[schemaHandler.MapIndex[pathStr]]
			res[pathStr].Info = schemaHandler.Infos[i]
		}
	}
	for i := 0; i < len(ss); i++ {
		var stack []*Node

		nodeBuf.Reset()

		node := nodeBuf.GetNode()
		var ui interface{}

		// `useNumber`causes the Decoder to unmarshal a number into an interface{} as a Number instead of as a float64.
		d := json.NewDecoder(strings.NewReader(ss[i].(string)))
		d.UseNumber()
		d.Decode(&ui)

		node.Val = reflect.ValueOf(ui)
		node.PathMap = pathMap

		stack = append(stack, node)

		for len(stack) > 0 {
			ln := len(stack)
			node = stack[ln-1]
			stack = stack[:ln-1]

			newStack, err := marshalNode(res, schemaHandler, stack, nodeBuf, node)
			if err != nil {
				return nil, err
			}
			stack = newStack
		}
	}

	return &res, nil

}

func marshalNode(res map[string]*layout.Table, schemaHandler *schema.SchemaHandler, stack []*Node, nodeBuf *NodeBufType, node *Node) ([]*Node, error) {
	var (
		pathStr         = node.PathMap.Path
		schemaIndex, ok = schemaHandler.MapIndex[pathStr]
	)
	// no schema item will be ignored
	if !ok {
		return stack, nil
	}
	schema := schemaHandler.SchemaElements[schemaIndex]

	switch node.Val.Type().Kind() {
	case reflect.Map:
		return marshalMap(res, schemaHandler, stack, nodeBuf, node)
	case reflect.Slice:
		return marshalSlice(res, schemaHandler, stack, nodeBuf, node)
	default:
		table := res[node.PathMap.Path]
		pT, cT := schema.Type, schema.ConvertedType
		val := types.JSONTypeToParquetType(node.Val, pT, cT, int(schema.GetTypeLength()), int(schema.GetScale()))

		table.Values = append(table.Values, val)
		table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
		table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
	}
	return stack, nil
}

func marshalMap(res map[string]*layout.Table, schemaHandler *schema.SchemaHandler, stack []*Node, nodeBuf *NodeBufType, node *Node) ([]*Node, error) {
	var (
		err             error
		keys            = node.Val.MapKeys()
		pathStr         = node.PathMap.Path
		schemaIndex, ok = schemaHandler.MapIndex[pathStr]
	)
	// no schema item will be ignored
	if !ok {
		return stack, nil
	}
	schema := schemaHandler.SchemaElements[schemaIndex]

	if schema.GetConvertedType() == parquet.ConvertedType_MAP { //real map
		pathStr = pathStr + ".Key_value"
		if len(keys) <= 0 {
			for key, table := range res {
				if strings.HasPrefix(key, node.PathMap.Path) &&
					(len(key) == len(node.PathMap.Path) || key[len(node.PathMap.Path)] == '.') {
					table.Values = append(table.Values, nil)
					table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
					table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
				}
			}
		}

		rlNow, _ := schemaHandler.MaxRepetitionLevel(common.StrToPath(pathStr))
		for j := len(keys) - 1; j >= 0; j-- {
			key := keys[j]
			value := node.Val.MapIndex(key).Elem()

			newNode := nodeBuf.GetNode()
			newNode.PathMap = node.PathMap.Children["Key_value"].Children["Key"]
			newNode.Val = key
			newNode.DL = node.DL + 1
			if j == 0 {
				newNode.RL = node.RL
			} else {
				newNode.RL = rlNow
			}
			stack = append(stack, newNode)

			newNode = nodeBuf.GetNode()
			newNode.PathMap = node.PathMap.Children["Key_value"].Children["Value"]
			newNode.Val = value
			newNode.DL = node.DL + 1
			newPathStr := newNode.PathMap.Path // check again
			newSchemaIndex := schemaHandler.MapIndex[newPathStr]
			newSchema := schemaHandler.SchemaElements[newSchemaIndex]
			if newSchema.GetRepetitionType() == parquet.FieldRepetitionType_OPTIONAL { //map value only be :optional or required
				newNode.DL++
			}

			if j == 0 {
				newNode.RL = node.RL
			} else {
				newNode.RL = rlNow
			}
			stack = append(stack, newNode)
		}

	} else { //struct
		keysMap := make(map[string]int)
		for j := 0; j < len(keys); j++ {
			//ExName to InName
			keysMap[common.StringToVariableName(keys[j].String())] = j
		}
		for key := range node.PathMap.Children { // mapiternext
			stack, err = marshalStruct(res, schemaHandler, stack, nodeBuf, node, keysMap, key, keys)
		}
	}
	return stack, err
}

func marshalStruct(res map[string]*layout.Table, schemaHandler *schema.SchemaHandler, stack []*Node, nodeBuf *NodeBufType, node *Node, keysMap map[string]int, key string, keys []reflect.Value) ([]*Node, error) {
	ki, ok := keysMap[key]

	if ok && node.Val.MapIndex(keys[ki]).Elem().IsValid() {
		newNode := nodeBuf.GetNode()
		newNode.PathMap = node.PathMap.Children[key]
		newNode.Val = node.Val.MapIndex(keys[ki]).Elem()
		newNode.RL = node.RL
		newNode.DL = node.DL
		newPathStr := newNode.PathMap.Path
		newSchemaIndex := schemaHandler.MapIndex[newPathStr]
		newSchema := schemaHandler.SchemaElements[newSchemaIndex]
		if newSchema.GetRepetitionType() == parquet.FieldRepetitionType_OPTIONAL {
			newNode.DL++
		}
		stack = append(stack, newNode)

	} else {
		addToTable(res, node, key)
	}
	return stack, nil
}

func addToTable(res map[string]*layout.Table, node *Node, key string) {
	newPathStr := node.PathMap.Children[key].Path

	// Early return if we get an exact match.
	// This improves performance.
	if table, ok := res[newPathStr]; ok {
		table.Values = append(table.Values, nil)
		table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
		table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
		return
	}
	for path, table := range res {
		if strings.HasPrefix(path, newPathStr) && (len(path) == len(newPathStr) || path[len(newPathStr)] == '.') {
			table.Values = append(table.Values, nil)
			table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
			table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
		}
	}
}

func marshalSlice(res map[string]*layout.Table, schemaHandler *schema.SchemaHandler, stack []*Node, nodeBuf *NodeBufType, node *Node) ([]*Node, error) {
	var (
		pathStr         = node.PathMap.Path
		schemaIndex, ok = schemaHandler.MapIndex[pathStr]
	)
	// no schema item will be ignored
	if !ok {
		return stack, nil
	}
	schema := schemaHandler.SchemaElements[schemaIndex]

	ln := node.Val.Len()

	if schema.GetConvertedType() == parquet.ConvertedType_LIST { // real LIST
		pathStr = pathStr + ".List" + ".Element"
		if ln <= 0 {
			for key, table := range res {
				if strings.HasPrefix(key, node.PathMap.Path) &&
					(len(key) == len(node.PathMap.Path) || key[len(node.PathMap.Path)] == '.') {
					table.Values = append(table.Values, nil)
					table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
					table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
				}
			}
		}
		rlNow, _ := schemaHandler.MaxRepetitionLevel(common.StrToPath(pathStr))

		for j := ln - 1; j >= 0; j-- {
			newNode := nodeBuf.GetNode()
			newNode.PathMap = node.PathMap.Children["List"].Children["Element"]
			newNode.Val = node.Val.Index(j).Elem()
			if j == 0 {
				newNode.RL = node.RL
			} else {
				newNode.RL = rlNow
			}
			newNode.DL = node.DL + 1

			newPathStr := newNode.PathMap.Path
			newSchemaIndex := schemaHandler.MapIndex[newPathStr]
			newSchema := schemaHandler.SchemaElements[newSchemaIndex]
			if newSchema.GetRepetitionType() == parquet.FieldRepetitionType_OPTIONAL { //element of LIST can only be optional or required
				newNode.DL++
			}

			stack = append(stack, newNode)
		}

	} else { //Repeated
		if ln <= 0 {
			for key, table := range res {
				if strings.HasPrefix(key, node.PathMap.Path) &&
					(len(key) == len(node.PathMap.Path) || key[len(node.PathMap.Path)] == '.') {
					table.Values = append(table.Values, nil)
					table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
					table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
				}
			}
		}
		rlNow, _ := schemaHandler.MaxRepetitionLevel(common.StrToPath(pathStr))

		for j := ln - 1; j >= 0; j-- {
			newNode := nodeBuf.GetNode()
			newNode.PathMap = node.PathMap
			newNode.Val = node.Val.Index(j).Elem()
			if j == 0 {
				newNode.RL = node.RL
			} else {
				newNode.RL = rlNow
			}
			newNode.DL = node.DL + 1
			stack = append(stack, newNode)
		}
	}
	return stack, nil
}
