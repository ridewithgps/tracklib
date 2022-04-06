require "spec_helper"

describe Tracklib do
  it "can roundtrip an I64 column" do
    data = [{"a" => 0},
            {},
            {"a" => 40},
            {"a" => -40.0}]

    section = Tracklib::Section.new(:standard, [["a", :i64]], data)
    buf = Tracklib::write_track([], [section])
    reader = Tracklib::TrackReader::new(buf)
    expect(reader.section_data(0)).to eq(data)
  end

  it "can roundtrip an F64 column" do
    data = [{"a" => 0},
            {},
            {"a" => 11.2},
            {"a" => -400.000003}]

    section = Tracklib::Section.new(:standard, [["a", :f64]], data)
    buf = Tracklib::write_track([], [section])
    reader = Tracklib::TrackReader::new(buf)
    expect(reader.section_data(0)).to eq(data)
  end

  it "can roundtrip a Bool column" do
    data = [{"a" => false},
            {},
            {"a" => true},
            {"a" => true},
            {}]

    section = Tracklib::Section.new(:standard, [["a", :bool]], data)
    buf = Tracklib::write_track([], [section])
    reader = Tracklib::TrackReader::new(buf)
    expect(reader.section_data(0)).to eq(data)
  end

  it "can roundtrip a String column" do
    data = [{"a" => "RWGPS"},
            {},
            {"a" => "Supercalifragilisticexpialidocious"},
            {"a" => ""}]

    section = Tracklib::Section.new(:standard, [["a", :string]], data)
    buf = Tracklib::write_track([], [section])
    reader = Tracklib::TrackReader::new(buf)
    expect(reader.section_data(0)).to eq(data)
  end
end
