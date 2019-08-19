lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "tracklib/version"

Gem::Specification.new do |spec|
  spec.name          = "tracklib"
  spec.version       = Tracklib::VERSION
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
                        "Cargo.lock",
                        "src/lib.rs"]

  spec.require_paths = ["lib"]
  spec.extensions    = ["ext/Rakefile"]

  spec.add_development_dependency "rspec"

  spec.add_runtime_dependency "rake", "~> 12.3"
  spec.add_runtime_dependency 'thermite', '~> 0.13'
  spec.add_runtime_dependency "rutie", "~> 0.0.4"
end
