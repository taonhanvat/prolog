package auth

import (
	"fmt"

	"github.com/casbin/casbin/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Authorizer struct {
	enforcer *casbin.Enforcer
}

func New(model, policy string) (*Authorizer, error) {
	enforcer, err := casbin.NewEnforcer(model, policy)
	return &Authorizer{enforcer}, err
}

func (a *Authorizer) Authorize(subject, action string) error {
	ok, err := a.enforcer.Enforce(subject, action)
	if err != nil {
		return err
	}
	if !ok {
		return status.Error(
			codes.PermissionDenied,
			fmt.Sprintf("%s is not permitted to %s", subject, action),
		)
	}
	return nil
}
