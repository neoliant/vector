---
title: Unit Testing Your Configs
description: Learn how to write and execute unit tests for your Vector configs
author_github: jeffail
domain: config
weight: 4
tags: ["testing", "configs", "unit testing", "level up", "guides", "guide"]
---

{{< requirement >}}
Before you begin, this guide assumes the following:

* You understand the [basic Vector concepts][concepts]
* You understand [how to set up a basic pipeline][pipeline]

[concepts]: /docs/about/concepts
[pipeline]: /docs/setup/quickstart
{{< /requirement >}}

You can define unit tests in a Vector configuration file that cover a network of
transforms within the topology. These tests help you develop configs containing
larger and more complex topologies and to improve their maintainability.

The full spec can be found [here][docs.reference.configuration.tests]. This guide covers
writing and executing a unit test for the following config:

```toml title="vector.toml"
[sources.over_tcp]
  type = "socket"
  mode = "tcp"
  address = "0.0.0.0:9000"

[transforms.foo]
  type = "grok_parser"
  inputs = ["over_tcp"]
  pattern = "%{TIMESTAMP_ISO8601:timestamp} %{LOGLEVEL:level} %{GREEDYDATA:message}"

[transforms.bar]
  type = "add_fields"
  inputs = ["foo"]
  [transforms.bar.fields]
    new_field = "this is a static value"

[transforms.baz]
  type = "remove_fields"
  inputs = ["foo"]
  fields = ["level"]

[sinks.over_http]
  type = "http"
  inputs = ["baz"]
  uri = "http://localhost:4195/post"
  encoding = "text"
```

In this config we:

* Parse a log line into the fields `timestamp`, `level` and `message` with the
  transform `foo`.
* Add a static string field `new_field` using the transform `bar`.
* Remove the field `level` with the transform `baz`.

In reality, it's unlikely that a config this simple would be worth the investment
of writing unit tests. Regardless, for the purpose of this guide we've concluded
that yes, we do wish to unit test this config.

Specifically, we need to ensure that the resulting events of our topology
(whatever comes out of the `baz` transform) always meets the following
requirements:

* Does *not* contain the field `level`.
* Contains the field `new_field`, with a static value `this is a static value`.
* Has a `timestamp` and `message` field containing the values extracted from the
  raw message of the input log.

Otherwise our system fails and an annoying relative (uncle Cecil) moves in to
live with us indefinitely. We will do _anything_ to prevent that.

## Input

First we shall write a single unit test at the bottom of our config called
`check_simple_log`. Each test must define input events (usually just one), which
initiates the test by injecting those events into a transform of the topology:

```toml
[[tests]]
  name = "check_simple_log"

  [[tests.inputs]]
    insert_at = "foo"
    type = "raw"
    value = "2019-11-28T12:00:00+00:00 info Sorry, I'm busy this week Cecil"
```

Here we've specified that our test should begin by injecting an event at the
transform `foo`. The `raw` input type creates a log with only a `message` field
and `timestamp` (set to the time of the test), where `message` is populated with
the contents of the `value` field.

## Outputs

This test won't run in its current state because there's nothing to check. In
order to perform checks with this unit test we define an output to inspect:

```toml
[[tests]]
  name = "check_simple_log"

  [[tests.inputs]]
    insert_at = "foo"
    type = "raw"
    value = "2019-11-28T12:00:00+00:00 info Sorry, I'm busy this week Cecil"

  [[tests.outputs]]
    extract_from = "baz"

    [[tests.outputs.conditions]]
      type = "check_fields"
      "level.exists" = false
      "new_field.equals" = "this is a static value"
      "timestamp.equals" = "2019-11-28T12:00:00+00:00"
      "message.equals" = "Sorry, I'm busy this week Cecil"
```

We can define any number of outputs for a test, and must specify at which
transform the output events should be extracted for checking. This allows us to
check the events from different transforms in a single test. For our purposes we
only need to check the output of `baz`.

An output can also have any number of conditions to check, and these are how we
determine whether a test has failed or succeeded. In order for the test to pass
each condition for an output must resolve to `true`.

It's possible for a topology to result in >1 events extracted from a single
transform, in which case each condition must pass for one or more of the
extracted events in order for the test to pass.

An output without any conditions cannot fail a test, and instead prints the
input and output events of a transform during the test. This is useful when
building a config as it allows us to inspect the behavior of each transform in
isolation.

The only condition we've defined here is a `check_fields` type. This is
currently the _only_ condition type on offer, and it allows us to specify any
number of field queries (of the format `"<field>.<predicate>" = "<argument>"`).

## Executing

With this test added to the bottom of our config we are now able to execute it.
Executing tests within a config file can be done with the `test` subcommand:

```bash
vector test ./example.toml
```

Doing this results in the following output:

```shell
vector test ./example.toml
Running ./example.toml tests
test ./example.toml: check_simple_log ... failed

failures:

--- ./example.toml ---

test 'check_simple_log':

check transform 'baz' failed conditions:
  condition[0]: predicates failed: [ new_field.equals: "this is a static value" ]
payloads (events encoded as JSON):
  input: {"level":"info","timestamp":"2019-11-28T12:00:00+00:00","message":"Sorry, I'm busy this week Cecil"}
  output: {"timestamp":"2019-11-28T12:00:00+00:00","message":"Sorry, I'm busy this week Cecil"}
```

Whoops! Something isn't right. Vector has told us that condition `0` (our only
condition) failed for the predicate `new_field.equals`. We also get to see a
JSON encoded representation of the input and output of the transform `baz`.
Try reviewing our config topology to see if you can spot the mistake.

**Spoiler alert**: The problem is that transform `baz` is configured with the input
`foo`, which means `bar` is skipped in the topology!

{{< info >}}
Side note: We would have also caught this particular issue with:

```shell
vector validate --topology ./example.toml
```

{{< /info >}}

The fix is easy, we simply change the input of `baz` from `foo` to `bar`:

```diff
--- a/example.toml
+++ b/example.toml
@@ -16,7 +16,7 @@

 [transforms.baz]
   type = "remove_fields"
-  inputs = ["foo"]
+  inputs = ["bar"]
   fields = ["level"]
```

And running our test again gives us an exit status 0:

```sh
vector test ./example.toml
Running ./example.toml tests
Test ./example.toml: check_simple_log ... passed
```

The test passed! Now if we configure our CI system to execute our test we can
ensure that uncle Cecil remains in Shoreditch after any future config change.
What an insufferable hipster he is.

[docs.about.concepts]: /docs/about/concepts
[docs.reference.configuration.tests]: /docs/reference/configuration/tests
[docs.setup.quickstart]: /docs/setup/quickstart
