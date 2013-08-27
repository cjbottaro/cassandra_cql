# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'cassandra_cql/version'

Gem::Specification.new do |gem|
  gem.name          = "cassandra_cql"
  gem.version       = CassandraCql::VERSION
  gem.authors       = ["Christopher J. Bottaro"]
  gem.email         = ["cjbottaro@alumni.cs.utexas.edu"]
  gem.description   = %q{Synchronous CQL binary protocol client for Cassandra}
  gem.summary       = %q{Synchronous CQL binary protocol client for Cassandra}
  gem.homepage      = ""

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]

  gem.add_dependency("simple_uuid")

  gem.add_development_dependency("pry")
  gem.add_development_dependency("rspec")
  gem.add_development_dependency("perftools.rb")
  gem.add_development_dependency("cql-rb")
  gem.add_development_dependency("cassandra-cql")
  gem.add_development_dependency("snappy")
  gem.add_development_dependency("rr")
end
