package sqshandler

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// SqsConn : struct for sqs conneciton
type SqsConn struct {
	QueueURL    string
	PullMaxMsg  int64
	PullVisible int64
	PushDelay   int64
}

// NewQueue : init a new SQS queue
func NewQueue(sqsurl string) (*SqsConn, error) {
	s := &SqsConn{
		QueueURL:    sqsurl,
		PullMaxMsg:  1,
		PullVisible: 1,
		PushDelay:   1,
	}
	return s, nil
}

func getSession() *sqs.SQS {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	svc := sqs.New(sess)
	return svc
}

// PullMessage : Pulls a Message from SQS
func (s *SqsConn) PullMessage() (*sqs.ReceiveMessageOutput, error) {
	svc := getSession()
	params := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(s.QueueURL),
		MaxNumberOfMessages: aws.Int64(s.PullMaxMsg),
		VisibilityTimeout:   aws.Int64(s.PullVisible),
	}
	resp, err := svc.ReceiveMessage(params)
	if err != nil {
		return resp, err
	}
	return resp, err
}

// PushMessage : Push a Message string to SQS
func (s *SqsConn) PushMessage(msg string) (*sqs.SendMessageOutput, error) {
	svc := getSession()
	params := &sqs.SendMessageInput{
		MessageBody:  aws.String(msg),
		QueueUrl:     aws.String(s.QueueURL),
		DelaySeconds: aws.Int64(s.PushDelay),
	}
	resp, err := svc.SendMessage(params)
	if err != nil {
		return resp, err
	}
	return resp, err
}

// DeleteMessage : Delete a msg from SQS
func (s *SqsConn) DeleteMessage(msgid string) (*sqs.DeleteMessageOutput, error) {
	svc := getSession()
	params := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(s.QueueURL),
		ReceiptHandle: aws.String(msgid),
	}
	resp, err := svc.DeleteMessage(params)
	if err != nil {
		return resp, err
	}
	return resp, err
}

// Info : return information from queue
func (s *SqsConn) Info() string {
	return s.QueueURL
}
