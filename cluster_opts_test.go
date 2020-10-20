package hasql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClusterDefaults(t *testing.T) {
	f := newFixture(t, 1)
	c, err := NewCluster(f.ClusterNodes(), f.PrimaryChecker)
	require.NoError(t, err)
	defer func() { require.NoError(t, c.Close()) }()

	require.Equal(t, DefaultUpdateInterval, c.updateInterval)
	require.Equal(t, DefaultUpdateTimeout, c.updateTimeout)
}

func TestWithUpdateInterval(t *testing.T) {
	f := newFixture(t, 1)
	d := time.Hour
	c, err := NewCluster(f.ClusterNodes(), f.PrimaryChecker, WithUpdateInterval(d))
	require.NoError(t, err)
	defer func() { require.NoError(t, c.Close()) }()

	require.Equal(t, d, c.updateInterval)
}

func TestWithUpdateTimeout(t *testing.T) {
	f := newFixture(t, 1)
	d := time.Hour
	c, err := NewCluster(f.ClusterNodes(), f.PrimaryChecker, WithUpdateTimeout(d))
	require.NoError(t, err)
	defer func() { require.NoError(t, c.Close()) }()

	require.Equal(t, d, c.updateTimeout)
}

func TestWithNodePicker(t *testing.T) {
	var called bool
	picker := func([]Node) Node {
		called = true
		return nil
	}
	f := newFixture(t, 1)
	c, err := NewCluster(f.ClusterNodes(), f.PrimaryChecker, WithNodePicker(picker))
	require.NoError(t, err)
	defer func() { require.NoError(t, c.Close()) }()

	c.picker(nil)
	require.True(t, called)
}

func TestWithTracer(t *testing.T) {
	var called bool
	tracer := Tracer{NotifiedWaiters: func() { called = true }}
	f := newFixture(t, 1)
	c, err := NewCluster(f.ClusterNodes(), f.PrimaryChecker, WithTracer(tracer))
	require.NoError(t, err)
	defer func() { require.NoError(t, c.Close()) }()

	c.tracer.NotifiedWaiters()
	require.True(t, called)
}
