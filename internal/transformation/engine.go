package transformation

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sync"

	"github.com/IBM/sarama"
	"go.uber.org/zap"

	"github.com/serkank2/kafkatwin/internal/monitoring"
)

// Engine handles message transformations
type Engine struct {
	rules map[string][]*Rule // topic -> rules
	mu    sync.RWMutex
}

// Rule represents a transformation rule
type Rule struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	Topic       string          `json:"topic"`
	Enabled     bool            `json:"enabled"`
	Conditions  []Condition     `json:"conditions"`
	Actions     []Action        `json:"actions"`
	Priority    int             `json:"priority"`
}

// Condition represents a condition for applying a rule
type Condition struct {
	Type     string `json:"type"`      // field_equals, field_contains, field_regex, header_exists
	Field    string `json:"field"`     // JSON path or header key
	Operator string `json:"operator"`  // equals, not_equals, contains, regex, exists
	Value    string `json:"value"`
}

// Action represents a transformation action
type Action struct {
	Type   string                 `json:"type"`   // set_field, remove_field, rename_field, set_header, mask_field
	Field  string                 `json:"field"`  // Target field
	Value  interface{}            `json:"value"`  // New value (for set operations)
	Params map[string]interface{} `json:"params"` // Additional parameters
}

// TransformResult represents the result of a transformation
type TransformResult struct {
	Message     *sarama.ProducerMessage
	Transformed bool
	RulesApplied []string
}

// NewEngine creates a new transformation engine
func NewEngine() *Engine {
	return &Engine{
		rules: make(map[string][]*Rule),
	}
}

// AddRule adds a transformation rule
func (e *Engine) AddRule(rule *Rule) error {
	if rule.Topic == "" {
		return fmt.Errorf("topic is required")
	}
	if rule.ID == "" {
		return fmt.Errorf("rule ID is required")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.rules[rule.Topic] == nil {
		e.rules[rule.Topic] = make([]*Rule, 0)
	}

	// Check for duplicate ID
	for _, existingRule := range e.rules[rule.Topic] {
		if existingRule.ID == rule.ID {
			return fmt.Errorf("rule with ID %s already exists", rule.ID)
		}
	}

	e.rules[rule.Topic] = append(e.rules[rule.Topic], rule)

	monitoring.Info("Transformation rule added",
		zap.String("rule_id", rule.ID),
		zap.String("topic", rule.Topic),
	)

	return nil
}

// RemoveRule removes a transformation rule
func (e *Engine) RemoveRule(topic string, ruleID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	rules, exists := e.rules[topic]
	if !exists {
		return fmt.Errorf("no rules for topic %s", topic)
	}

	for i, rule := range rules {
		if rule.ID == ruleID {
			e.rules[topic] = append(rules[:i], rules[i+1:]...)
			monitoring.Info("Transformation rule removed",
				zap.String("rule_id", ruleID),
				zap.String("topic", topic),
			)
			return nil
		}
	}

	return fmt.Errorf("rule %s not found", ruleID)
}

// Transform applies transformation rules to a message
func (e *Engine) Transform(msg *sarama.ProducerMessage) (*TransformResult, error) {
	e.mu.RLock()
	rules := e.rules[msg.Topic]
	e.mu.RUnlock()

	if len(rules) == 0 {
		return &TransformResult{
			Message:     msg,
			Transformed: false,
		}, nil
	}

	result := &TransformResult{
		Message:      msg,
		Transformed:  false,
		RulesApplied: make([]string, 0),
	}

	// Parse message value as JSON
	var data map[string]interface{}
	if msg.Value != nil {
		valueBytes, err := msg.Value.Encode()
		if err != nil {
			return nil, fmt.Errorf("failed to encode message value: %w", err)
		}

		if err := json.Unmarshal(valueBytes, &data); err != nil {
			// Not JSON, skip transformation
			return result, nil
		}
	} else {
		data = make(map[string]interface{})
	}

	// Apply rules in priority order
	for _, rule := range rules {
		if !rule.Enabled {
			continue
		}

		// Check conditions
		if !e.evaluateConditions(rule.Conditions, data, msg) {
			continue
		}

		// Apply actions
		for _, action := range rule.Actions {
			if err := e.applyAction(action, data, msg); err != nil {
				monitoring.Warn("Failed to apply transformation action",
					zap.String("rule_id", rule.ID),
					zap.String("action_type", action.Type),
					zap.Error(err),
				)
				continue
			}
		}

		result.Transformed = true
		result.RulesApplied = append(result.RulesApplied, rule.ID)

		monitoring.Debug("Transformation rule applied",
			zap.String("rule_id", rule.ID),
			zap.String("topic", msg.Topic),
		)
	}

	// Update message value if transformed
	if result.Transformed {
		newValue, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal transformed data: %w", err)
		}
		msg.Value = sarama.ByteEncoder(newValue)
	}

	return result, nil
}

// evaluateConditions checks if all conditions are met
func (e *Engine) evaluateConditions(conditions []Condition, data map[string]interface{}, msg *sarama.ProducerMessage) bool {
	if len(conditions) == 0 {
		return true
	}

	for _, condition := range conditions {
		if !e.evaluateCondition(condition, data, msg) {
			return false
		}
	}

	return true
}

// evaluateCondition checks if a single condition is met
func (e *Engine) evaluateCondition(condition Condition, data map[string]interface{}, msg *sarama.ProducerMessage) bool {
	switch condition.Type {
	case "field_equals":
		value, exists := data[condition.Field]
		if !exists {
			return false
		}
		return fmt.Sprintf("%v", value) == condition.Value

	case "field_contains":
		value, exists := data[condition.Field]
		if !exists {
			return false
		}
		strValue := fmt.Sprintf("%v", value)
		matched, _ := regexp.MatchString(condition.Value, strValue)
		return matched

	case "field_regex":
		value, exists := data[condition.Field]
		if !exists {
			return false
		}
		strValue := fmt.Sprintf("%v", value)
		matched, _ := regexp.MatchString(condition.Value, strValue)
		return matched

	case "header_exists":
		for _, header := range msg.Headers {
			if string(header.Key) == condition.Field {
				return true
			}
		}
		return false

	default:
		return false
	}
}

// applyAction applies a transformation action
func (e *Engine) applyAction(action Action, data map[string]interface{}, msg *sarama.ProducerMessage) error {
	switch action.Type {
	case "set_field":
		data[action.Field] = action.Value
		return nil

	case "remove_field":
		delete(data, action.Field)
		return nil

	case "rename_field":
		if newName, ok := action.Params["new_name"].(string); ok {
			if value, exists := data[action.Field]; exists {
				data[newName] = value
				delete(data, action.Field)
			}
		}
		return nil

	case "set_header":
		if value, ok := action.Value.(string); ok {
			msg.Headers = append(msg.Headers, sarama.RecordHeader{
				Key:   []byte(action.Field),
				Value: []byte(value),
			})
		}
		return nil

	case "mask_field":
		if _, exists := data[action.Field]; exists {
			maskChar := "*"
			if mc, ok := action.Params["mask_char"].(string); ok {
				maskChar = mc
			}

			value := fmt.Sprintf("%v", data[action.Field])
			masked := ""
			for i := 0; i < len(value); i++ {
				masked += maskChar
			}
			data[action.Field] = masked
		}
		return nil

	case "uppercase_field":
		if value, exists := data[action.Field]; exists {
			strValue := fmt.Sprintf("%v", value)
			data[action.Field] = regexp.MustCompile(".").ReplaceAllStringFunc(strValue, func(s string) string {
				if s >= "a" && s <= "z" {
					return string(s[0] - 32)
				}
				return s
			})
		}
		return nil

	case "lowercase_field":
		if value, exists := data[action.Field]; exists {
			strValue := fmt.Sprintf("%v", value)
			data[action.Field] = regexp.MustCompile(".").ReplaceAllStringFunc(strValue, func(s string) string {
				if s >= "A" && s <= "Z" {
					return string(s[0] + 32)
				}
				return s
			})
		}
		return nil

	default:
		return fmt.Errorf("unknown action type: %s", action.Type)
	}
}

// GetRules returns all rules for a topic
func (e *Engine) GetRules(topic string) []*Rule {
	e.mu.RLock()
	defer e.mu.RUnlock()

	rules := make([]*Rule, len(e.rules[topic]))
	copy(rules, e.rules[topic])
	return rules
}

// GetAllRules returns all rules
func (e *Engine) GetAllRules() map[string][]*Rule {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make(map[string][]*Rule)
	for topic, rules := range e.rules {
		result[topic] = make([]*Rule, len(rules))
		copy(result[topic], rules)
	}
	return result
}

// ClearRules clears all rules
func (e *Engine) ClearRules() {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.rules = make(map[string][]*Rule)
	monitoring.Info("All transformation rules cleared")
}
