package consumekafkaproduct

// Usecase : function contract for consume kafka product usecase
type Usecase interface {
	// ConsumeAndStoreToES  from kafka to  ES using single Repo
	ConsumeAndStoreToES(topic string)
}
