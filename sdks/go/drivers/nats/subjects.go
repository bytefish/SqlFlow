package natsdriver

import "encoding/base64"

func encodeQueueName(queueName string) string {
	return base64.RawURLEncoding.EncodeToString(
		[]byte(queueName),
	)
}