require "tracklib/version"
require "rutie"

module Tracklib
  unless defined?(TrackReader)
    Rutie.new(:ruby_tracklib).init 'Init_Tracklib', __dir__
  end
end
