require "tracklib/version"
require 'rutie'

module Tracklib
  class TracklibError < StandardError; end
  Rutie.new(:tracklib).init 'Init_Tracklib', __dir__
end
