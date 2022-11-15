require "tracklib/version"
require "rutie"

module Tracklib2
  unless defined?(TrackReader)
    Rutie.new(:ruby_tracklib_version_two).init 'Init_Tracklib', __dir__
  end
end
