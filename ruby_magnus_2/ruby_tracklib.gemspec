# frozen_string_literal: true

require_relative "lib/ruby_tracklib/version"

Gem::Specification.new do |spec|
  spec.name            = "ruby_tracklib"
  spec.version         = RubyTracklib::VERSION
  spec.authors         = ["Dan Larkin"]
  spec.email           = ["dan@danlarkin.org"]
  spec.summary         = "tracklib"
  spec.description     = "RWGPS tracklib ruby gem"
  spec.homepage        = "https://ridewithgps.com"
  spec.licenses        = ["Apache-2.0", "MIT"]

  spec.files           = ["ruby_tracklib.gemspec",
                          "Rakefile",
                          "Gemfile",
                          "Cargo.toml",
                          "Cargo.lock"]
  spec.files          += Dir["lib/**/*.rb"]
  spec.files          += Dir["ext/**/*"]
  spec.require_paths   = ["lib"]
  spec.extensions      = ["ext/ruby_tracklib/extconf.rb"]

  spec.add_dependency "rb_sys", "~> 0.9.39"

  spec.metadata["rubygems_mfa_required"] = "true"
  spec.required_ruby_version             = ">= 2.7.4"
  spec.required_rubygems_version         = ">= 3.3.11"
end
