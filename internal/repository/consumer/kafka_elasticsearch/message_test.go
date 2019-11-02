package kafkaelasticsearch

import (
	"encoding/json"
	"reflect"
	"testing"
)

func Test_prepareDoc(t *testing.T) {
	type args struct {
		msg []byte
	}
	tests := []struct {
		name       string
		args       args
		wantDoc    json.RawMessage
		wantAction string
		wantDocID  string
		wantErr    bool
	}{
		{
			name: "positive case 1: get action,id and document from message",
			args: args{
				msg: []byte(`{"header":{"action":"update","target":"","key":"15235258"},"body":{"id":15235258,"is_preorder":false}}`),
			},
			wantDoc:    json.RawMessage(`{"id":15235258,"is_preorder":false}`),
			wantAction: "update",
			wantDocID:  "15235258",
		},
		{
			name: "negative case 1: invalid key",
			args: args{
				msg: []byte(`{"header":{"action":"update","target":"","key":""},"body":{"id":15235258,"is_preorder":false}}`),
			},
			wantErr: true,
		},
		{
			name: "negative case 1: invalid action",
			args: args{
				msg: []byte(`{"header":{"action":"","target":"","key":"15235258"},"body":{"id":15235258,"is_preorder":false}}`),
			},
			wantErr: true,
		},
		{
			name: "negative case 1: invalid message",
			args: args{
				msg: []byte(`{"header"}`),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDoc, gotAction, gotDocID, err := prepareDoc(tt.args.msg)
			if (err != nil) != tt.wantErr {
				t.Errorf("prepareDoc() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotDoc, tt.wantDoc) {
				t.Errorf("prepareDoc() gotDoc = %v, want %v", gotDoc, tt.wantDoc)
			}
			if gotAction != tt.wantAction {
				t.Errorf("prepareDoc() gotAction = %v, want %v", gotAction, tt.wantAction)
			}
			if gotDocID != tt.wantDocID {
				t.Errorf("prepareDoc() gotDocID = %v, want %v", gotDocID, tt.wantDocID)
			}
		})
	}
}
