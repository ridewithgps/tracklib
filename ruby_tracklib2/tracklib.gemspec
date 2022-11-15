require_relative 'lib/tracklib/version'

Gem::Specification.new do |spec|
  spec.name          = "tracklib2"
  spec.version       = Tracklib2::VERSION
  spec.authors       = ["Dan Larkin"]
  spec.email         = ["dan@danlarkin.org"]

  spec.summary       = "tracklib"
  spec.description   = "RWGPS tracklib ruby gem"
  spec.homepage      = "https://ridewithgps.com"
  spec.licenses      = ["Apache-2.0", "MIT"]
  spec.files         = ["tracklib.gemspec",
                        "Rakefile",
                        "Gemfile",
                        "lib/tracklib.rb",
                        "lib/tracklib/version.rb",
                        "Cargo.toml",
                        "Cargo.lock"]
  spec.files        += Dir["src/**/*.rs"]

  spec.required_ruby_version = Gem::Requirement.new(">= 2.3.0")
  spec.require_paths = ["lib"]
  spec.extensions = ["Rakefile"]

  spec.add_development_dependency "rspec"

  spec.add_dependency 'rutie', '~> 0.0.4'
end
