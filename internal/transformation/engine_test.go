package transformation

import (
	"testing"

	"github.com/IBM/sarama"
)

func TestEngine_AddRule(t *testing.T) {
	engine := NewEngine()

	rule := &Rule{
		ID:      "test-rule-1",
		Name:    "Test Rule",
		Topic:   "test-topic",
		Enabled: true,
	}

	err := engine.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Try to add duplicate
	err = engine.AddRule(rule)
	if err == nil {
		t.Fatal("Expected error for duplicate rule ID")
	}
}

func TestEngine_RemoveRule(t *testing.T) {
	engine := NewEngine()

	rule := &Rule{
		ID:      "test-rule-1",
		Name:    "Test Rule",
		Topic:   "test-topic",
		Enabled: true,
	}

	_ = engine.AddRule(rule)

	err := engine.RemoveRule("test-topic", "test-rule-1")
	if err != nil {
		t.Fatalf("Failed to remove rule: %v", err)
	}

	// Try to remove non-existent rule
	err = engine.RemoveRule("test-topic", "non-existent")
	if err == nil {
		t.Fatal("Expected error for non-existent rule")
	}
}

func TestEngine_Transform_SetField(t *testing.T) {
	engine := NewEngine()

	rule := &Rule{
		ID:      "set-field-rule",
		Name:    "Set Field Rule",
		Topic:   "test-topic",
		Enabled: true,
		Actions: []Action{
			{
				Type:  "set_field",
				Field: "new_field",
				Value: "new_value",
			},
		},
	}

	_ = engine.AddRule(rule)

	msg := &sarama.ProducerMessage{
		Topic: "test-topic",
		Value: sarama.StringEncoder(`{"existing":"value"}`),
	}

	result, err := engine.Transform(msg)
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	if !result.Transformed {
		t.Fatal("Expected message to be transformed")
	}

	if len(result.RulesApplied) != 1 {
		t.Fatalf("Expected 1 rule applied, got %d", len(result.RulesApplied))
	}
}

func TestEngine_Transform_MaskField(t *testing.T) {
	engine := NewEngine()

	rule := &Rule{
		ID:      "mask-field-rule",
		Name:    "Mask Field Rule",
		Topic:   "test-topic",
		Enabled: true,
		Actions: []Action{
			{
				Type:  "mask_field",
				Field: "password",
				Params: map[string]interface{}{
					"mask_char": "*",
				},
			},
		},
	}

	_ = engine.AddRule(rule)

	msg := &sarama.ProducerMessage{
		Topic: "test-topic",
		Value: sarama.StringEncoder(`{"password":"secret123"}`),
	}

	result, err := engine.Transform(msg)
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	if !result.Transformed {
		t.Fatal("Expected message to be transformed")
	}
}

func TestEngine_Transform_Conditions(t *testing.T) {
	engine := NewEngine()

	rule := &Rule{
		ID:      "conditional-rule",
		Name:    "Conditional Rule",
		Topic:   "test-topic",
		Enabled: true,
		Conditions: []Condition{
			{
				Type:     "field_equals",
				Field:    "status",
				Operator: "equals",
				Value:    "active",
			},
		},
		Actions: []Action{
			{
				Type:  "set_field",
				Field: "processed",
				Value: true,
			},
		},
	}

	_ = engine.AddRule(rule)

	// Message that matches condition
	msg1 := &sarama.ProducerMessage{
		Topic: "test-topic",
		Value: sarama.StringEncoder(`{"status":"active"}`),
	}

	result1, err := engine.Transform(msg1)
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	if !result1.Transformed {
		t.Fatal("Expected message to be transformed")
	}

	// Message that doesn't match condition
	msg2 := &sarama.ProducerMessage{
		Topic: "test-topic",
		Value: sarama.StringEncoder(`{"status":"inactive"}`),
	}

	result2, err := engine.Transform(msg2)
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	if result2.Transformed {
		t.Fatal("Expected message NOT to be transformed")
	}
}

func TestEngine_GetRules(t *testing.T) {
	engine := NewEngine()

	rule1 := &Rule{ID: "rule1", Topic: "topic1", Enabled: true}
	rule2 := &Rule{ID: "rule2", Topic: "topic1", Enabled: true}
	rule3 := &Rule{ID: "rule3", Topic: "topic2", Enabled: true}

	_ = engine.AddRule(rule1)
	_ = engine.AddRule(rule2)
	_ = engine.AddRule(rule3)

	topic1Rules := engine.GetRules("topic1")
	if len(topic1Rules) != 2 {
		t.Fatalf("Expected 2 rules for topic1, got %d", len(topic1Rules))
	}

	topic2Rules := engine.GetRules("topic2")
	if len(topic2Rules) != 1 {
		t.Fatalf("Expected 1 rule for topic2, got %d", len(topic2Rules))
	}
}

func TestEngine_ClearRules(t *testing.T) {
	engine := NewEngine()

	rule := &Rule{ID: "rule1", Topic: "topic1", Enabled: true}
	_ = engine.AddRule(rule)

	engine.ClearRules()

	allRules := engine.GetAllRules()
	if len(allRules) != 0 {
		t.Fatal("Expected all rules to be cleared")
	}
}
