require "spec_helper"

describe Tracklib do
  it "can roundtrip an I64 column" do
    data = [{"a" => 0},
            {},
            {},
            {"a" => 40},
            {"a" => -40.0}]

    schema = Tracklib::Schema.new([["a", :i64]])
    section = Tracklib::Section.new(:standard, schema, data)
    buf = Tracklib::write_track([], [section])
    reader = Tracklib::TrackReader::new(buf)
    expect(reader.section_data(0)).to eq(data)
  end

  it "can roundtrip an F64 column" do
    data = [{"a" => 0},
            {},
            {"a" => 11.2},
            {"a" => -400.000003}]

    schema = Tracklib::Schema.new([["a", :f64, 7]])
    section = Tracklib::Section.new(:standard, schema, data)
    buf = Tracklib::write_track([], [section])
    reader = Tracklib::TrackReader::new(buf)
    expect(reader.section_data(0)).to eq(data)
  end

  it "can roundtrip an U64 column" do
    data = [{"a" => 0},
            {},
            {"a" => 11},
            {"a" => 400},
            {"a" => 20}]

    schema = Tracklib::Schema.new([["a", :u64]])
    section = Tracklib::Section.new(:standard, schema, data)
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

    schema = Tracklib::Schema.new([["a", :bool]])
    section = Tracklib::Section.new(:standard, schema, data)
    buf = Tracklib::write_track([], [section])
    reader = Tracklib::TrackReader::new(buf)
    expect(reader.section_data(0)).to eq(data)
  end

  it "can roundtrip a String column" do
    data = [{"a" => "RWGPS"},
            {},
            {"a" => "Supercalifragilisticexpialidocious"},
            {"a" => ""}]

    schema = Tracklib::Schema.new([["a", :string]])
    section = Tracklib::Section.new(:standard, schema, data)
    buf = Tracklib::write_track([], [section])
    reader = Tracklib::TrackReader::new(buf)
    expect(reader.section_data(0)).to eq(data)
  end

  it "can roundtrip a BoolArray column" do
    data = [{"a" => [true, false]},
            {},
            {"a" => [false, false, false, false, false, false, false, false, false, false, false]},
            {"a" => []}]

    schema = Tracklib::Schema.new([["a", :bool_array]])
    section = Tracklib::Section.new(:standard, schema, data)
    buf = Tracklib::write_track([], [section])
    reader = Tracklib::TrackReader::new(buf)
    expect(reader.section_data(0)).to eq(data)
  end

  it "can roundtrip a U64Array column" do
    data = [{"a" => [0, 20, 1, 5_000]},
            {},
            {"a" => []},
            {"a" => [80_000_000, 5]}]

    schema = Tracklib::Schema.new([["a", :u64_array]])
    section = Tracklib::Section.new(:standard, schema, data)
    buf = Tracklib::write_track([], [section])
    reader = Tracklib::TrackReader::new(buf)
    expect(reader.section_data(0)).to eq(data)
  end

  it "can roundtrip a ByteArray column" do
    example_string = [0, 65].pack("C*")
    expect(example_string.encoding()). to eq(Encoding::find("ASCII-8BIT"))
    data = [{"a" => "RWGPS"},
            {},
            {"a" => ""},
            {"a" => example_string}]

    schema = Tracklib::Schema.new([["a", :byte_array]])
    section = Tracklib::Section.new(:standard, schema, data)
    buf = Tracklib::write_track([], [section])
    reader = Tracklib::TrackReader::new(buf)
    expect(reader.section_data(0)).to eq(data)
  end

  it "can roundtrip all types at once" do
    data = [{"i64" => -200,
             "f64" => 37.89,
             "u64" => 80_000_000_000,
             "bool" => true,
             "string" => "RWGPS",
             "boolarray" => [false, true, false],
             "u64array" => [20, 10, 11],
             "bytearray" => "RWGPS"}]

    schema = Tracklib::Schema.new([["i64", :i64],
                                   ["f64", :f64, 2],
                                   ["u64", :u64],
                                   ["bool", :bool],
                                   ["string", :string],
                                   ["boolarray", :bool_array],
                                   ["u64array", :u64_array],
                                   ["bytearray", :byte_array]])
    section = Tracklib::Section.new(:standard, schema, data)
    buf = Tracklib::write_track([], [section])
    reader = Tracklib::TrackReader::new(buf)
    expect(reader.section_data(0)).to eq(data)
  end

  it "can roundtrip metadata" do
    metadata = [[:created_at, Time.new(1970, 1, 2, 11, 12, 13, "UTC")],
                [:track_type, :route, 1000]]
    buf = Tracklib::write_track(metadata, [])
    reader = Tracklib::TrackReader::new(buf)
    expect(reader.metadata()).to eq(metadata)
  end
end
