# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tracklib do
  context "section_hexes" do
    it "works without elevation data" do
      # Points with only x and y - no elevation
      data = [
        { "x" => -122.5, "y" => 45.5 },
        { "x" => -122.6, "y" => 45.6 },
        { "x" => -122.7, "y" => 45.7 }
      ]
      schema = Tracklib::Schema.new([["x", :f64, 6], ["y", :f64, 6]])
      section = Tracklib::Section.standard(schema, data)
      buf = Tracklib.write_track([], [section])
      reader = Tracklib::TrackReader.new(buf)

      # section_hexes should work and return hexes
      hexes = reader.section_hexes(0, 10, :forward)
      expect(hexes).not_to be_empty
    end

    it "works with elevation data" do
      data = [
        { "x" => -122.5, "y" => 45.5, "e" => 100 },
        { "x" => -122.6, "y" => 45.6, "e" => 200 },
        { "x" => -122.7, "y" => 45.7, "e" => 300 }
      ]
      schema = Tracklib::Schema.new([["x", :f64, 6], ["y", :f64, 6], ["e", :f64, 1]])
      section = Tracklib::Section.standard(schema, data)
      buf = Tracklib.write_track([], [section])
      reader = Tracklib::TrackReader.new(buf)

      hexes = reader.section_hexes(0, 10, :forward)
      expect(hexes).not_to be_empty
    end

    it "returns empty for empty track" do
      data = []
      schema = Tracklib::Schema.new([["x", :f64, 6], ["y", :f64, 6]])
      section = Tracklib::Section.standard(schema, data)
      buf = Tracklib.write_track([], [section])
      reader = Tracklib::TrackReader.new(buf)

      hexes = reader.section_hexes(0, 10, :none)
      expect(hexes).to be_empty
    end
  end
end
