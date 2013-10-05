## 0.0.5

Breaking

	- CassandraCql::Error::Base is now CassandraCql::Error and inherits off of StandardError

Features

	- Stress tool (tools/stress.rb)

Bugfixes

	- JRuby has different Socket#send and Socket#recv semantics
	- Client#new with timeout option

## 0.0.4

Breaking

	- None

Features

	- Better detection of closed connections

Bugfixes

	- None

## 0.0.3

Breaking

	- None

Features

	- None

Bugfixes

  - Multi-byte UTF-8 characters

## 0.0.2

Breaking

	- None

Features

	- CassandraCql::Client#reset_connection
	- Attempt to reset connection on socket error (like after forking)

Bugfixes

	- None

## 0.0.1

Breaking

	- None

Features

	- Initial release

Bugfixes

	- None
