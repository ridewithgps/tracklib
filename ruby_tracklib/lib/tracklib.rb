require "tracklib/version"
require "rutie"

module Tracklib
  class Error < StandardError; end
  Rutie.new(:tracklib, {lib_path: "../lib", lib_prefix: ""}).init 'Init_Tracklib', __dir__
end

class RWTFile
  def hello_from_ruby()
    puts "I am a ruby method"
    return 5
  end
end
