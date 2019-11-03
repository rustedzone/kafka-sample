package consumekafkaproduct

// ConsumeAndStoreToES  from kafka to  ES using single Repo
func (u *UseCase) ConsumeAndStoreToES(topic string) {
	// gonna run infinite loop
	u.repo.consumerKafkaES.Consume(topic)
}
