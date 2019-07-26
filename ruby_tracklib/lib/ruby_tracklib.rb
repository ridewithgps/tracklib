require "ruby_tracklib/version"
require "rutie"

module RubyTracklib
  class Error < StandardError; end
  Rutie.new(:ruby_tracklib, {lib_path: "../../target/release"}).init 'Init_Tracklib', __dir__
end

class RWTFile
  def hello_from_ruby()
    puts "I am a ruby method"
    return 5
  end
end
