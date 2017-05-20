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
func NewQueue(sqsurl string, pullMax int64, pullVis int64, pushDel int64) (*SqsConn, error) {
	s := &SqsConn{
		QueueURL:    sqsurl,
		PullMaxMsg:  pullMax,
		PullVisible: pullVis,
		PushDelay:   pushDel,
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
func (s *SqsConn) PushMessage(msg string) error {
	svc := getSession()
	params := &sqs.SendMessageInput{
		MessageBody:  aws.String(msg),
		QueueUrl:     aws.String(s.QueueURL),
		DelaySeconds: aws.Int64(s.PushDelay),
	}
	_, err := svc.SendMessage(params)
	if err != nil {
		return err
	}
	return nil
}

// DeleteMessage : Delete a msg from SQS
func (s *SqsConn) DeleteMessage(msgid string) error {
	svc := getSession()
	params := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(s.QueueURL),
		ReceiptHandle: aws.String(msgid),
	}
	_, err := svc.DeleteMessage(params)
	if err != nil {
		return err
	}
	return nil
}

// Info : return information from queue
func (s *SqsConn) Info() string {
	return s.QueueURL
}
