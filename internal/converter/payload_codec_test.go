package converter

import (
	"testing"

	workflowactivation "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/workflowactivation"
	workflowcommands "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/workflowcommands"
	workflowcompletion "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/workflowcompletion"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"google.golang.org/protobuf/proto"
)

type markingPayloadCodec struct {
	encodeBatchSizes []int
	decodeBatchSizes []int
}

func (c *markingPayloadCodec) Encode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	c.encodeBatchSizes = append(c.encodeBatchSizes, len(payloads))
	return markPayloads(payloads, "encoded"), nil
}

func (c *markingPayloadCodec) Decode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	c.decodeBatchSizes = append(c.decodeBatchSizes, len(payloads))
	return markPayloads(payloads, "decoded"), nil
}

func markPayloads(payloads []*commonpb.Payload, value string) []*commonpb.Payload {
	result := make([]*commonpb.Payload, len(payloads))
	for i, payload := range payloads {
		result[i] = proto.Clone(payload).(*commonpb.Payload)
		if result[i].Metadata == nil {
			result[i].Metadata = make(map[string][]byte)
		}
		result[i].Metadata["codec"] = []byte(value)
	}
	return result
}

func TestDecodePayloadsTransformsPayloadFieldsButNotSearchAttributes(t *testing.T) {
	codec := &markingPayloadCodec{}
	activation := &workflowactivation.WorkflowActivation{
		Jobs: []*workflowactivation.WorkflowActivationJob{{
			Variant: &workflowactivation.WorkflowActivationJob_InitializeWorkflow{
				InitializeWorkflow: &workflowactivation.InitializeWorkflow{
					Arguments: []*commonpb.Payload{payload("one"), payload("two")},
					Headers:   map[string]*commonpb.Payload{"trace": payload("header")},
					Memo: &commonpb.Memo{Fields: map[string]*commonpb.Payload{
						"note": payload("memo"),
					}},
					LastCompletionResult: &commonpb.Payloads{Payloads: []*commonpb.Payload{
						payload("previous result"),
					}},
					SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
						"Keyword": payload("search"),
					}},
				},
			},
		}},
	}

	if err := DecodePayloads(activation, codec); err != nil {
		t.Fatalf("DecodePayloads failed: %v", err)
	}
	initialize := activation.Jobs[0].GetInitializeWorkflow()
	for _, converted := range append(
		initialize.Arguments,
		initialize.Headers["trace"],
		initialize.Memo.Fields["note"],
		initialize.LastCompletionResult.Payloads[0],
	) {
		if got := string(converted.Metadata["codec"]); got != "decoded" {
			t.Fatalf("decoded marker = %q, want decoded", got)
		}
	}
	if got := initialize.SearchAttributes.IndexedFields["Keyword"].Metadata["codec"]; got != nil {
		t.Fatalf("search attribute was codec transformed: %q", got)
	}
	if len(codec.decodeBatchSizes) != 4 || !containsBatch(codec.decodeBatchSizes, 2) {
		t.Fatalf("decode batch sizes = %v, want arguments together and map payloads separately", codec.decodeBatchSizes)
	}
}

func TestDecodePayloadsTransformsSignalInputAndHeaders(t *testing.T) {
	codec := &markingPayloadCodec{}
	activation := &workflowactivation.WorkflowActivation{
		Jobs: []*workflowactivation.WorkflowActivationJob{{
			Variant: &workflowactivation.WorkflowActivationJob_SignalWorkflow{
				SignalWorkflow: &workflowactivation.SignalWorkflow{
					SignalName: "greet",
					Input:      []*commonpb.Payload{payload("one"), payload("two")},
					Headers:    map[string]*commonpb.Payload{"trace": payload("header")},
				},
			},
		}},
	}

	if err := DecodePayloads(activation, codec); err != nil {
		t.Fatalf("DecodePayloads failed: %v", err)
	}
	signal := activation.Jobs[0].GetSignalWorkflow()
	for _, converted := range append(signal.Input, signal.Headers["trace"]) {
		if got := string(converted.Metadata["codec"]); got != "decoded" {
			t.Fatalf("decoded marker = %q, want decoded", got)
		}
	}
	if len(codec.decodeBatchSizes) != 2 || !containsBatch(codec.decodeBatchSizes, 2) {
		t.Fatalf("decode batch sizes = %v, want signal inputs together and header separately", codec.decodeBatchSizes)
	}
}

func TestEncodePayloadsTransformsCompletionPayloadFieldsButNotSearchAttributes(t *testing.T) {
	codec := &markingPayloadCodec{}
	schedule := &workflowcommands.ScheduleActivity{
		Arguments: []*commonpb.Payload{payload("one"), payload("two")},
		Headers:   map[string]*commonpb.Payload{"trace": payload("header")},
	}
	search := &workflowcommands.UpsertWorkflowSearchAttributes{
		SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
			"Keyword": payload("search"),
		}},
	}
	complete := &workflowcommands.CompleteWorkflowExecution{Result: payload("result")}
	completion := &workflowcompletion.WorkflowActivationCompletion{
		Status: &workflowcompletion.WorkflowActivationCompletion_Successful{
			Successful: &workflowcompletion.Success{Commands: []*workflowcommands.WorkflowCommand{
				{Variant: &workflowcommands.WorkflowCommand_ScheduleActivity{ScheduleActivity: schedule}},
				{Variant: &workflowcommands.WorkflowCommand_UpsertWorkflowSearchAttributes{UpsertWorkflowSearchAttributes: search}},
				{Variant: &workflowcommands.WorkflowCommand_CompleteWorkflowExecution{CompleteWorkflowExecution: complete}},
			}},
		},
	}

	if err := EncodePayloads(completion, codec); err != nil {
		t.Fatalf("EncodePayloads failed: %v", err)
	}
	for _, converted := range append(schedule.Arguments, schedule.Headers["trace"]) {
		if got := string(converted.Metadata["codec"]); got != "encoded" {
			t.Fatalf("encoded marker = %q, want encoded", got)
		}
	}
	if got := search.SearchAttributes.IndexedFields["Keyword"].Metadata["codec"]; got != nil {
		t.Fatalf("search attribute was codec transformed: %q", got)
	}
	if got := string(complete.Result.Metadata["codec"]); got != "encoded" {
		t.Fatalf("singular result marker = %q, want encoded", got)
	}
	if len(codec.encodeBatchSizes) != 3 || !containsBatch(codec.encodeBatchSizes, 2) {
		t.Fatalf("encode batch sizes = %v, want arguments together and map payloads separately", codec.encodeBatchSizes)
	}
}

func TestEncodePayloadsTransformsFailureEncodedAttributes(t *testing.T) {
	codec := &markingPayloadCodec{}
	completion := &workflowcompletion.WorkflowActivationCompletion{
		Status: &workflowcompletion.WorkflowActivationCompletion_Failed{
			Failed: &workflowcompletion.Failure{Failure: &failurepb.Failure{
				EncodedAttributes: payload("failure attributes"),
			}},
		},
	}

	if err := EncodePayloads(completion, codec); err != nil {
		t.Fatalf("EncodePayloads failed: %v", err)
	}
	encodedAttributes := completion.GetFailed().Failure.EncodedAttributes
	if got := string(encodedAttributes.Metadata["codec"]); got != "encoded" {
		t.Fatalf("failure encoded-attributes marker = %q, want encoded", got)
	}
}

func containsBatch(sizes []int, want int) bool {
	for _, size := range sizes {
		if size == want {
			return true
		}
	}
	return false
}

func payload(data string) *commonpb.Payload {
	return &commonpb.Payload{Data: []byte(data)}
}
