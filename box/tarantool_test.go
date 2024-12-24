package box_test

import (
	"context"
	"errors"
	"log"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-iproto"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/box"
	"github.com/tarantool/go-tarantool/v2/test_helpers"
)

var server = "127.0.0.1:3013"
var dialer = tarantool.NetDialer{
	Address:  server,
	User:     "test",
	Password: "test",
}

func validateInfo(t testing.TB, info box.Info) {
	var err error

	// Check all fields run correctly.
	_, err = uuid.Parse(info.UUID)
	require.NoErrorf(t, err, "validate instance uuid is valid")

	require.NotEmpty(t, info.Version)
	// Check that pid parsed correctly.
	require.NotEqual(t, info.PID, 0)
}

func TestBox_Sugar_Info(t *testing.T) {
	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	info, err := box.New(conn).Info(context.TODO())
	require.NoError(t, err)

	validateInfo(t, info)
}

func TestBox_Info(t *testing.T) {
	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	fut := conn.Do(box.NewInfoRequest())
	require.NotNil(t, fut)

	resp := &box.InfoResponse{}
	err = fut.GetTyped(resp)
	require.NoError(t, err)

	validateInfo(t, resp.Info)
}

func TestBox_Sugar_Schema_UserCreate(t *testing.T) {
	const (
		username = "exists"
		password = "exists"
	)

	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	// Create new user
	err = b.Schema().User().Create(ctx, username, box.UserCreateOptions{Password: password})
	require.NoError(t, err)

	t.Run("can connect with new credentials", func(t *testing.T) {
		t.Parallel()
		// Check that password is valid and we can connect to tarantool with such credentials
		var newUserDialer = tarantool.NetDialer{
			Address:  server,
			User:     username,
			Password: password,
		}

		// We can connect with our new credentials
		newUserConn, err := tarantool.Connect(ctx, newUserDialer, tarantool.Opts{})
		require.NoError(t, err)
		require.NotNil(t, newUserConn)
		require.NoError(t, newUserConn.Close())
	})
	t.Run("create user already exists error", func(t *testing.T) {
		t.Parallel()
		// Get error that user already exists
		err := b.Schema().User().Create(ctx, username, box.UserCreateOptions{Password: password})
		require.Error(t, err)

		// Require that error code is ER_USER_EXISTS
		var boxErr tarantool.Error
		errors.As(err, &boxErr)
		require.Equal(t, iproto.ER_USER_EXISTS, boxErr.Code)
	})

	t.Run("exists method return true", func(t *testing.T) {
		t.Parallel()
		// Check that already exists by exists call procedure
		exists, err := b.Schema().User().Exists(ctx, username)
		require.True(t, exists)
		require.NoError(t, err)
	})

	t.Run("no error if IfNotExists option is true", func(t *testing.T) {
		t.Parallel()

		err := b.Schema().User().Create(ctx, username, box.UserCreateOptions{
			Password:    password,
			IfNotExists: true,
		})

		require.NoError(t, err)
	})
}

func TestBox_Sugar_Schema_UserPassword(t *testing.T) {
	const (
		username = "passwd"
		password = "passwd"
	)

	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	// Require password hash
	hash, err := b.Schema().User().Password(ctx, username)
	require.NoError(t, err)
	require.NotEmpty(t, hash)
}

func TestBox_Sugar_Schema_UserDrop(t *testing.T) {
	const (
		username = "to_drop"
		password = "to_drop"
	)

	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	t.Run("drop user after create", func(t *testing.T) {
		// Create new user
		err = b.Schema().User().Create(ctx, username, box.UserCreateOptions{Password: password})
		require.NoError(t, err)

		// Try to drop user
		err = b.Schema().User().Drop(ctx, username, box.UserDropOptions{})
		require.NoError(t, err)

		t.Run("error double drop without IfExists option", func(t *testing.T) {
			// Require error cause user already deleted
			err = b.Schema().User().Drop(ctx, "some_strange_not_existing_name",
				box.UserDropOptions{})
			require.Error(t, err)

			var boxErr tarantool.Error

			// Require that error code is ER_NO_SUCH_USER
			errors.As(err, &boxErr)
			require.Equal(t, iproto.ER_NO_SUCH_USER, boxErr.Code)
		})
		t.Run("ok double drop with IfExists option", func(t *testing.T) {
			// Require no error with IfExists: true option
			err = b.Schema().User().Drop(ctx, "some_strange_not_existing_name",
				box.UserDropOptions{IfExists: true})
			require.NoError(t, err)
		})
	})

	t.Run("drop not existing user", func(t *testing.T) {
		t.Parallel()
		// Require error cause user already deleted
		err = b.Schema().User().Drop(ctx, "some_strange_not_existing_name", box.UserDropOptions{})
		require.Error(t, err)

		var boxErr tarantool.Error

		// Require that error code is ER_NO_SUCH_USER
		errors.As(err, &boxErr)
		require.Equal(t, iproto.ER_NO_SUCH_USER, boxErr.Code)
	})
}

func runTestMain(m *testing.M) int {
	instance, err := test_helpers.StartTarantool(test_helpers.StartOpts{
		Dialer:       dialer,
		InitScript:   "testdata/config.lua",
		Listen:       server,
		WaitStart:    100 * time.Millisecond,
		ConnectRetry: 10,
		RetryTimeout: 500 * time.Millisecond,
	})
	defer test_helpers.StopTarantoolWithCleanup(instance)

	if err != nil {
		log.Printf("Failed to prepare test Tarantool: %s", err)
		return 1
	}

	return m.Run()
}

func TestMain(m *testing.M) {
	code := runTestMain(m)
	os.Exit(code)
}
