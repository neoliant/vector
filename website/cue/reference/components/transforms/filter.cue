package metadata

components: transforms: filter: {
	title: "Filter"

	description: """
		Filters events based on a set of conditions using VRL or
		[Datadog Search Syntax](\(urls.datadog_search_syntax)).
		"""

	classes: {
		commonly_used: true
		development:   "stable"
		egress_method: "stream"
		stateful:      false
	}

	features: {
		filter: {}
	}

	support: {
		targets: {
			"aarch64-unknown-linux-gnu":      true
			"aarch64-unknown-linux-musl":     true
			"armv7-unknown-linux-gnueabihf":  true
			"armv7-unknown-linux-musleabihf": true
			"x86_64-apple-darwin":            true
			"x86_64-pc-windows-msv":          true
			"x86_64-unknown-linux-gnu":       true
			"x86_64-unknown-linux-musl":      true
		}
		requirements: []
		warnings: []
		notices: []
	}

	configuration: {
		condition: {
			description: """
				The condition to be matched against every input event. Only messages that pass the condition will
				be forwarded.
				"""
			required: true
			warnings: []
			type: condition: {
				syntax: ["datadog_search", "vrl_boolean_expression"]
				examples: [
					{
						syntax: "vrl_boolean_expression"
						source: #".status_code != 200 && !includes(["info", "debug"], .severity)"#
					},
					{
						syntax: "datadog_search"
						source: "@http.status_code:[400 TO 499]"
					},
				]
			}
		}
	}

	input: {
		logs: true
		metrics: {
			counter:      true
			distribution: true
			gauge:        true
			histogram:    true
			set:          true
			summary:      true
		}
	}

	examples: [
		{
			title: "Drop debug logs"
			configuration: {
				condition: #".level != "debug""#
			}
			input: [
				{
					log: {
						level:   "debug"
						message: "I'm a noisy debug log"
					}
				},
				{
					log: {
						level:   "info"
						message: "I'm a normal info log"
					}
				},
			]
			output: [
				{
					log: {
						level:   "info"
						message: "I'm a normal info log"
					}
				},
			]
		},
		{
			title: "Using Datadog Search syntax"
			configuration: {
				condition: {
					syntax: "datadog_search"
					source: #"*stack*"#
				}
			}
			input: [
				{
					log: {
						message: "This line won't match..."
					}
				},
				{
					log: {
						message: "Needle in a haystack"
					}
				},
				{
					log: {
						message: "... and neither will this one"
					}
				},
			]
			output: [
				{
					log: {
						message: "Needle in a haystack"
					}
				},
			]
		},
	]

	telemetry: metrics: {
		events_discarded_total: components.sources.internal_metrics.output.metrics.events_discarded_total
	}
}
