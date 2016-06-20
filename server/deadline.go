package server

import (
	"fmt"
	"log"
	"time"

	"github.com/golang/glog"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/transport"

	"jingoal.com/dfs/instrument"
)

// streamFunc represents function which process stream operation.
type streamFunc func(interface{}, interface{}, []interface{}) error

// withStreamDeadline processes a stream grpc calling with deadline.
func (f streamFunc) withStreamDeadline(serviceName string, req interface{}, stream interface{}, args ...interface{}) error {
	if grpcStream, ok := stream.(grpc.Stream); ok {
		_, err := bizFunc(streamBizFunc).withDeadline(serviceName, grpcStream, req, f, args)

		return err
	}

	return f(req, stream, args)
}

// bizFunc represents function which process biz logic.
type bizFunc func(interface{}, interface{}, []interface{}) (interface{}, error)

// withDeadline processes a normal grpc calling with deadline.
func (f bizFunc) withDeadline(serviceName string, env interface{}, req interface{}, args ...interface{}) (r interface{}, e error) {
	startTime := time.Now()

	entry(serviceName)

	defer func() {
		elapse := time.Since(startTime)
		me := &instrument.Measurements{
			Name:  serviceName,
			Value: float64(elapse.Nanoseconds()),
		}

		if se, ok := e.(transport.StreamError); ok && (se.Code == codes.DeadlineExceeded) || (e == context.DeadlineExceeded) {
			instrument.TimeoutHistogram <- me
			glog.Infof("%s, deadline exceeded, %v seconds.", serviceName, elapse.Seconds())
		} else if e != nil {
			instrument.FailedCounter <- me
			glog.Infof("%s error %v, in %v seconds.", serviceName, e, elapse.Seconds())
		} else {
			instrument.SuccessDuration <- me
			glog.Infof("%s finished in %v seconds.", serviceName, elapse.Seconds())
		}

		exit(serviceName)
	}()

	if deadline, ok := getDeadline(env); ok {
		timeout := deadline.Sub(startTime)

		if timeout <= 0 {
			log.Printf("%s timeout is %v, deadline is %v", serviceName, timeout, deadline)
			e = context.DeadlineExceeded
			return
		}

		type Result struct {
			r interface{}
			e error
		}
		results := make(chan *Result)

		ticker := time.NewTicker(timeout)
		defer func() {
			ticker.Stop()
		}()

		go func() {
			result := &Result{}
			// Do business.
			result.r, result.e = f(env, req, args)
			results <- result
			close(results)
		}()

		select {
		case result := <-results:
			r = result.r
			e = result.e
			return
		case <-ticker.C:
			e = context.DeadlineExceeded
			return
		}
	}

	instrument.NoDeadlineCounter <- &instrument.Measurements{
		Name:  serviceName,
		Value: 1,
	}
	return f(env, req, args)
}

// streamBizFunc is an instance of bizFunc.
func streamBizFunc(stream interface{}, req interface{}, args []interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("parameter number %d", len(args))
	}

	sFunc, ok := args[0].(streamFunc)
	if !ok {
		return nil, AssertionError
	}
	as, ok := args[1].([]interface{})
	if !ok {
		return nil, AssertionError
	}

	return nil, sFunc(req, stream, as)
}

func getDeadline(env interface{}) (deadline time.Time, ok bool) {
	switch t := env.(type) {
	case context.Context:
		deadline, ok = t.Deadline()
	case grpc.Stream:
		deadline, ok = t.Context().Deadline()
	default:
		return
	}
	return
}

func checkTimeout(size int64, rate float64, given time.Duration) (time.Duration, error) {
	if rate != 0.0 && size != 0 {
		rate := float64(rate * 1024) // convert unit of rate from kbit/s to bit/s
		size := float64(size * 8)    // convert unit of size from bytes to bits
		need := time.Duration(size / rate * float64(time.Second))
		if given.Nanoseconds() < need.Nanoseconds() {
			return time.Duration(need), context.DeadlineExceeded
		}
	}

	return given, nil
}
