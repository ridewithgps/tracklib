# frozen_string_literal: true

require_relative "ruby_tracklib/version"

# Precompiled platform gems stage the .so under lib/ruby_tracklib/<ruby-abi>/;
# source-built gems put it directly in lib/ruby_tracklib/. Try the versioned
# path first, fall back to the flat path.
begin
  ruby_abi = RUBY_VERSION.match(/\d+\.\d+/)[0]
  require_relative "ruby_tracklib/#{ruby_abi}/ruby_tracklib"
rescue LoadError
  require_relative "ruby_tracklib/ruby_tracklib"
end

module RubyTracklib
  class Error < StandardError; end
  # Your code goes here...
end
