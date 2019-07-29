lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "ruby_tracklib/version"

Gem::Specification.new do |spec|
  spec.name          = "ruby_tracklib"
  spec.version       = RubyTracklib::VERSION
  spec.authors       = ["Dan Larkin"]
  spec.email         = ["dan@danlarkin.org"]

  spec.summary       = "tracklib"
  spec.description   = "RWGPS tracklib ruby gem"
  spec.homepage      = "https://www.ridewithgps.com"
  spec.licenses      = ["Apache-2.0", "MIT"]

  spec.files         = ["ruby_tracklib.gemspec",
                        "Rakefile",
                        "Gemfile",
                        "Gemfile.lock",
                        "lib/ruby_tracklib.rb",
                        "lib/ruby_tracklib/version.rb",

                        "Cargo.toml",
                        "Cargo.lock",
                        "src/lib.rs"]

  spec.require_paths = ["lib"]
  spec.extensions    = "Rakefile"

  spec.add_dependency "bundler", "~> 1.17"
  spec.add_dependency "rake", "~> 12.3"

  spec.add_development_dependency "minitest", "~> 5.11"
  spec.add_dependency "rutie", "~> 0.0.4"
end
