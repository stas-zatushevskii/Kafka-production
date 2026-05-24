package app

type OrderEvent struct {
	OrderID    string  `json:"order_id" avro:"order_id"`
	CustomerID string  `json:"customer_id" avro:"customer_id"`
	Amount     float64 `json:"amount" avro:"amount"`
	Currency   string  `json:"currency" avro:"currency"`
	CreatedAt  string  `json:"created_at" avro:"created_at"`
	Source     string  `json:"source" avro:"source"`
}
