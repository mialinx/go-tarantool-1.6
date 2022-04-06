<a href="http://tarantool.org">
	<img src="https://avatars2.githubusercontent.com/u/2344919?v=2&s=250" align="right">
</a>

[![Go Reference][godoc-badge]][godoc-url]
[![Actions Status][actions-badge]][actions-url]
[![Code Coverage][coverage-badge]][coverage-url]
[![Telegram][telegram-badge]][telegram-url]
[![GitHub Discussions][discussions-badge]][discussions-url]
[![Stack Overflow][stackoverflow-badge]][stackoverflow-url]

# Client in Go for Tarantool

The package `go-tarantool` contains everything you need to connect to
[Tarantool 1.6+][tarantool-site].

The advantage of integrating Go with Tarantool, which is an application server
plus a DBMS, is that Go programmers can handle databases and perform on-the-fly
recompilations of embedded Lua routines, just as in C, with responses that are
faster than other packages according to public benchmarks.

## Table of contents

* [Installation](#installation)
* [Hello World](#hello-world)
* [API reference](#api-reference)
* [Walking\-through example in Go](#walking-through-example-in-go)
* [Help](#help)
* [Contributing](#contributing)
* [Alternative connectors](#alternative-connectors)

## Installation

We assume that you have Tarantool version 1.6+ and a modern Linux or BSD
operating system.

You need a current version of `go`, version 1.13 or later (use `go version` to
check the version number). Do not use `gccgo-go`.

**Note:** If your `go` version is older than 1.3 or if `go` is not installed,
download and run the latest tarball from [golang.org][golang-dl].

The package `go-tarantool` is located in [tarantool/go-tarantool][go-tarantool]
repository. To download and install, say:

```
$ go get github.com/tarantool/go-tarantool
```

This should put the source and binary files in subdirectories of
`/usr/local/go`, so that you can access them by adding
`github.com/tarantool/go-tarantool` to the `import {...}` section at the start
of any Go program.

<h2>Hello World</h2>

In the "[Connectors](https://www.tarantool.io/en/doc/latest/getting_started/getting_started_go/)"
chapter of the Tarantool manual, there is an explanation of a very short (18-line)
program written in Go. Follow the instructions at the start of the "Connectors"
chapter carefully. Then cut and paste the example into a file named `example.go`,
and run it. You should see: nothing.

If that is what you see, then you have successfully installed `go-tarantool` and
successfully executed a program that manipulated the contents of a Tarantool
database.

<h2>API reference</h2>

Read the [Tarantool manual](http://tarantool.org/doc.html) to find descriptions
of terms like "connect", "space", "index", and the requests for creating and
manipulating database objects or Lua functions.

The source files for the requests library are:
* [connection.go](https://github.com/tarantool/go-tarantool/blob/master/connection.go)
  for the `Connect()` function plus functions related to connecting, and
* [request.go](https://github.com/tarantool/go-tarantool/blob/master/request.go)
  for data-manipulation functions and Lua invocations.

See comments in those files for syntax details:
```
Ping
closeConnection
Select
Insert
Replace
Delete
Update
Upsert
Call
Call17
Eval
```

The supported requests have parameters and results equivalent to requests in the
Tarantool manual. There are also Typed and Async versions of each data-manipulation
function.

The source file for error-handling tools is
[errors.go](https://github.com/tarantool/go-tarantool/blob/master/errors.go),
which has structure definitions and constants whose names are equivalent to names
of errors that the Tarantool server returns.

## Walking-through example in Go

We can now have a closer look at the `example.go` program and make some observations
about what it does.

```go
package main

import (
     "fmt"
     "github.com/tarantool/go-tarantool"
)

func main() {
   opts := tarantool.Opts{User: "guest"}
   conn, err := tarantool.Connect("127.0.0.1:3301", opts)
   // conn, err := tarantool.Connect("/path/to/tarantool.socket", opts)
   if err != nil {
       fmt.Println("Connection refused:", err)
   }
   resp, err := conn.Insert(999, []interface{}{99999, "BB"})
   if err != nil {
     fmt.Println("Error", err)
     fmt.Println("Code", resp.Code)
   }
}
```

**Observation 1:** the line "`github.com/tarantool/go-tarantool`" in the
`import(...)` section brings in all Tarantool-related functions and structures.

**Observation 2:** the line beginning with "`Opts :=`" sets up the options for
`Connect()`. In this example, there is only one thing in the structure, a user
name. The structure can also contain:

* `Pass` (password),
* `Timeout` (maximum number of milliseconds to wait before giving up),
* `Reconnect` (number of seconds to wait before retrying if a connection fails),
* `MaxReconnect` (maximum number of times to retry).

**Observation 3:** the line containing "`tarantool.Connect`" is essential for
beginning any session. There are two parameters:

* a string with `host:port` format, and
* the option structure that was set up earlier.

**Observation 4:** the `err` structure will be `nil` if there is no error,
otherwise it will have a description which can be retrieved with `err.Error()`.

**Observation 5:** the `Insert` request, like almost all requests, is preceded by
"`conn.`" which is the name of the object that was returned by `Connect()`.
There are two parameters:

* a space number (it could just as easily have been a space name), and
* a tuple.

## Help

To contact `go-tarantool` developers on any problems, create an issue at
[tarantool/go-tarantool](http://github.com/tarantool/go-tarantool/issues).

The developers of the [Tarantool server](http://github.com/tarantool/tarantool)
will also be happy to provide advice or receive feedback.

## Contributing

See [the contributing guide](CONTRIBUTING.md) for detailed instructions on how
to get started with our project.

## Alternative connectors

There are two more connectors from the open-source community available:
* [viciious/go-tarantool](https://github.com/viciious/go-tarantool),
* [FZambia/tarantool](https://github.com/FZambia/tarantool).

See feature comparison in [documentation](https://www.tarantool.io/en/doc/latest/book/connectors/#go-feature-comparison).

[tarantool-site]: https://tarantool.io/
[godoc-badge]: https://pkg.go.dev/badge/github.com/tarantool/go-tarantool.svg
[godoc-url]: https://pkg.go.dev/github.com/tarantool/go-tarantool
[actions-badge]: https://github.com/tarantool/go-tarantool/actions/workflows/testing.yml/badge.svg
[actions-url]: https://github.com/tarantool/go-tarantool/actions/workflows/testing.yml
[coverage-badge]: https://coveralls.io/repos/github/tarantool/go-tarantool/badge.svg?branch=master
[coverage-url]: https://coveralls.io/github/tarantool/go-tarantool?branch=master
[telegram-badge]: https://img.shields.io/badge/Telegram-join%20chat-blue.svg
[telegram-url]: http://telegram.me/tarantool
[discussions-badge]: https://img.shields.io/github/discussions/tarantool/tarantool
[discussions-url]: https://github.com/tarantool/tarantool/discussions
[stackoverflow-badge]: https://img.shields.io/badge/stackoverflow-tarantool-orange.svg
[stackoverflow-url]: https://stackoverflow.com/questions/tagged/tarantool
[golang-dl]: https://go.dev/dl/
[go-tarantool]: https://github.com/tarantool/go-tarantool
